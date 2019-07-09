// Copyright [2019] <Daniel Weber>

#include "interdedup_decompress.h"

#include <cassert>
#include <cstdlib>

#include <algorithm>
#include <iostream>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/lzma.hpp>
#include <boost/log/trivial.hpp>

#include "memdump.h"
#include "request_handler.h"
#include "utils.h"

namespace mscr {

enum class compression {
  ZIP7, GZIP, BZIP2, NOINNER
};


static int decompress_file(const std::string &file_in, const compression &inner,
        std::string *file_out) {
  /* returns the decompressed body from the
   * chosen compression */
  BOOST_LOG_TRIVIAL(debug) << "starting inner decompression";
  std::stringstream input_stream(file_in);
  boost::iostreams::filtering_istream in;
  switch (inner) {
    case compression::ZIP7:
      in.push(boost::iostreams::lzma_decompressor());
      break;
    case compression::GZIP:
      in.push(boost::iostreams::gzip_decompressor());
      break;
    case compression::BZIP2:
      in.push(boost::iostreams::bzip2_decompressor());
      break;
    case compression::NOINNER:
      // nothing to do here
      *file_out = file_in;
      BOOST_LOG_TRIVIAL(debug) << "finished inner decompression";
      return 0;
  }
  in.push(input_stream);
  std::stringstream readbuf;
  boost::iostreams::copy(in, readbuf);
  *file_out = readbuf.str();
  BOOST_LOG_TRIVIAL(debug) << "finished inner decompression";
  return 0;
}


static int read_header(std::istream *file, std::string *method,
    uint32_t *pagesize, uint64_t *uncompressed_size) {
  // parse magic number
  std::string magicnum = read_string(file);
  BOOST_LOG_TRIVIAL(debug) << "reading header";
  BOOST_LOG_TRIVIAL(debug) << "\t magicnum: " << magicnum;
  if (magicnum != "MBCR") {
    BOOST_LOG_TRIVIAL(error) << "magic number mismatch";
    return 1;
  }

  // parse method
  *method = read_string(file);
  BOOST_LOG_TRIVIAL(debug) <<  "\t method: " << *method;

  // parse major version
  auto major_version = read_num_LE(file, 2);
  BOOST_LOG_TRIVIAL(debug) << "\t major version: " << major_version;

  // parse minor version
  auto minor_version = read_num_LE(file, 2);
  BOOST_LOG_TRIVIAL(debug) << "\t minor version: " << minor_version;

  // parse page size
  *pagesize = read_num_LE(file, 4);
  BOOST_LOG_TRIVIAL(debug) << "\t pagesize: " << *pagesize;

  // parse uncompressed size
  *uncompressed_size = read_num_LE(file, 8);
  BOOST_LOG_TRIVIAL(debug) << "\t uncompressed size: " << *uncompressed_size;
  BOOST_LOG_TRIVIAL(debug) << "finished reading header";

  return 0;
}


static int parse_method(std::string method, compression *inner, bool *intra,
    bool *diffing) {
  // sanity check for interdedup
  if (method.find("interdedup") == std::string::npos) {
    return 1;
  }
  method.erase(0, ((std::string) "interdedup").size());

  // check intra
  if (str_starts_with(method, "nointra")) {
    *intra = false;
    method.erase(0, ((std::string) "nointra").size());
  } else {
    *intra = true;
  }

  // check diffing
  if (str_starts_with(method, "delta")) {
    *diffing = true;
    method.erase(0, ((std::string) "delta").size());
  } else {
    *diffing = false;
  }

  // check inner compression
  if (str_starts_with(method, "7zip")) {
    *inner = compression::ZIP7;
  } else if (str_starts_with(method, "gzip")) {
    *inner = compression::GZIP;
  } else if (str_starts_with(method, "bzip2")) {
    *inner = compression::BZIP2;
  } else {
    *inner = compression::NOINNER;

    // we should have consumed the whole method string
    if (!method.empty()) {
      BOOST_LOG_TRIVIAL(error) << "Method is not empty after parsing.";
      return 1;
    }
  }
  return 0;
}


static std::vector<uint32_t> parse_pagenr_list(std::istream *fp) {
  // will hold the final list of page numbers
  std::vector<uint32_t> pagenr_list;
  uint32_t sz = read_num_LE(fp, 4);
  pagenr_list.reserve(sz);

  // keeps track of the previous number for delta encoding
  uint32_t prev = 0;

  // go through the list number by number
  for (uint32_t i = 0; i < sz; ++i) {
    // parse number
    uint32_t num;
    uint32_t curr1B = read_num_LE(fp, 1);

    if ((curr1B & 128) == 128) {
      // msb = 1 --> right 7 bits used for number
      num = curr1B & 127;
    } else {
      // msb = 0 --> read 3 more bytes
      uint32_t secByte = read_num_LE(fp, 1);
      uint32_t thirdByte = read_num_LE(fp, 1);
      uint32_t fourthByte = read_num_LE(fp, 1);
      num = (curr1B << 24) | (secByte << 16) | (thirdByte << 8) | fourthByte;
    }

    // decode delta if it's not the first number
    if (i == 0) {
      pagenr_list.push_back(num);
      prev = num;
    } else {
      num = prev + num + 1;
      pagenr_list.push_back(num);
      prev = num;
    }
  }

  return pagenr_list;
}


static void parse_interval(std::istream *fp, bool *last, uint32_t *left,
    uint32_t *right) {
  /* interval [l, r]
   * format:  t[1b], d[2b], l[29b], r-l[{0,1,2,4}B]
   * t = termination bit
   * d = size of r -l
   * l = page number after left side of interval
   * r = offset to the right side
   */

  // parse t, d and l
  uint32_t left4B = read_num_LE(fp, 4);
  uint32_t upper3b = (left4B & (7 << 29)) >> 29;
  uint32_t size = upper3b & 3;
  *last = (upper3b >> 2) == 1;
  *left = left4B & ((1 << 29) - 1);

  // d = 0b11 encodes 4
  if (size == 3) {
    size = 4;
  }

  // compute r
  if (size == 0) {
    *right = *left;
  } else if (size == 1 || size == 2 || size == 4) {
    uint32_t delta = read_num_LE(fp, size);
    *right = *left + delta;
  } else {
    BOOST_LOG_TRIVIAL(error) << "invalid interval size: " << size;
  }
}


static std::vector<std::pair<uint32_t, uint32_t>> parse_interval_list(
    std::istream *fp) {
  // interval list = list of (left, right) pairs
  std::vector<std::pair<uint32_t, uint32_t>> intervals;

  // parse until we see a set termination bit
  bool last = false;
  while (!last) {
    uint32_t left, right;
    parse_interval(fp, &last, &left, &right);
    std::pair<uint32_t, uint32_t> iv = std::make_pair(left, right);
    intervals.push_back(iv);
  }
  return intervals;
}


static std::pair<uint32_t, uint32_t> decode_patch(std::istream *fp) {
  // parse first two bytes
  uint32_t firstB = read_num_LE(fp, 1);
  uint32_t secB = read_num_LE(fp, 1);

  // decode size + offset
  if ((firstB & 128) == 128) {
    // first bit is set --> encoded in 3B
    uint32_t thirdB = read_num_LE(fp, 1);

    // decode patch
    firstB &= 127;
    uint32_t rebuildB = (firstB << 16) | (secB << 8) | thirdB;
    uint32_t size = 1 + ((rebuildB & 0xFFF000) >> 12);
    uint32_t offset = rebuildB & 0xFFF;

    return std::make_pair(size, offset);
  } else {
    // msb is not set --> simply take the two bytes
    firstB += 1;  // we encoded length - 1

    return std::make_pair(firstB, secB);
  }
}


static std::vector<std::pair<uint32_t, std::string>> parse_diff(
    std::istream *fp) {
  // diff = list of patches
  std::vector<std::pair<uint32_t, std::string>> diff;

  // parse number of patches
  uint32_t patch_count = read_num_LE(fp, 2);

  // parse patch by patch
  for (uint32_t i = 0; i < patch_count; i++) {
    std::pair<uint32_t, uint32_t> size_offset = decode_patch(fp);

    // a patch should never be larger than 2KiB
    assert(size_offset.first <= 2048);
    char readbuf[2048];

    // read complete patch and put it into the diff
    fp->read(readbuf, size_offset.first);
    std::string patchbytes(readbuf, readbuf + size_offset.first);
    diff.emplace_back(size_offset.second, patchbytes);
  }
  return diff;
}


static std::string apply_diff(const std::string &refpage,
    const std::vector<std::pair<uint32_t, std::string>> &diff) {
  // will hold the final page after the patches have been applied
  std::string rebuild_page = refpage;

  // apply patch by patch
  uint32_t offset = 0;
  for (auto patch : diff) {
    // "seek" to the right offset
    offset += patch.first;

    // apply the patch by replacing bytes accordingly
    std::string patchbytes = patch.second;
    for (uint32_t i = 0; i < patchbytes.size(); i++) {
      rebuild_page[offset + i] = patchbytes[i];
    }

    // continue "seeking"
    offset += patchbytes.size();
  }
  return rebuild_page;
}


void interdedup_decompress(request_handler *handler, const char *filename_in,
    const char *out_filename) {

  // open compressed file for reading
  std::ifstream f_compressed(filename_in, std::ios::binary);
  if (f_compressed.fail()) {
    BOOST_LOG_TRIVIAL(error) << "error opening dumpfile " << filename_in;
    return;
  }

  // set pagesize + uncompressed_size
  uint32_t pagesize;
  std::string method;
  uint64_t uncompressed_size;
  int ret = read_header(&f_compressed, &method, &pagesize, &uncompressed_size);
  if (ret) {
    BOOST_LOG_TRIVIAL(error) << "error in header parsing";
    return;
  }

  // set inner + intra + diffing
  compression compression_used;
  bool intra_used;
  bool diffing;
  ret = parse_method(method, &compression_used, &intra_used, &diffing);
  if (ret) {
    BOOST_LOG_TRIVIAL(error) << "error in method parsing";
    return;
  }

  // extract body
  std::noskipws(f_compressed);  // prevents iterator from skipping whitespace
  std::string compressed_body(std::istream_iterator<char>(f_compressed), {});

  // decompress body
  std::string file_body;
  int res = decompress_file(compressed_body, compression_used, &file_body);
  if (res != 0 || file_body.empty()) {
    BOOST_LOG_TRIVIAL(error) << "inner decompression failure";
    return;
  }
  // convert body to stream so we can easily operate on it
  std::stringstream f_body(file_body);

  BOOST_LOG_TRIVIAL(debug) << "got uncompressed file body";

  std::string ref_dump_path = read_string(&f_body);
  if (ref_dump_path.empty()) {
    BOOST_LOG_TRIVIAL(error) << "invalid reference dump path in header";
    return;
  }
  BOOST_LOG_TRIVIAL(debug) << "reference dump: " << ref_dump_path;

  // parse reference pagenrs + intervals
  std::unordered_map<uint32_t, uint32_t> fills;
  std::vector<uint32_t> ref_pagenrs = parse_pagenr_list(&f_body);
  for (uint32_t ref_pagenr : ref_pagenrs) {
    auto iv_list = parse_interval_list(&f_body);
    for (std::pair<uint32_t, uint32_t> interval : iv_list) {
      uint32_t left = interval.first;
      uint32_t right = interval.second;
      // sanity check
      if (left > right) {
        BOOST_LOG_TRIVIAL(error) << "invalid interval";
        return;
      }

      // map deduplicated page numbers to page numbers in reference dump
      for (uint32_t pagenr = left; pagenr <= right; ++pagenr) {
        fills[pagenr] = ref_pagenr;
      }
    }
  }

  // parse diffs
  std::unordered_map<uint32_t, std::vector<std::pair<uint32_t, std::string>>>
      diffs;
  if (diffing) {
    std::vector<uint32_t> diffpages = parse_pagenr_list(&f_body);
    for (uint32_t pagenum : diffpages) {
      diffs[pagenum] = parse_diff(&f_body);
    }
  }

  // parse new pages
  std::unordered_map<uint32_t, std::string> newpages;
  std::set<std::string> newdistinct;
  if (!intra_used) {
    auto iv_newpages = parse_interval_list(&f_body);
    auto page_content = new char[pagesize];

    // parse intervals and pages accordingly
    for (std::pair<uint32_t, uint32_t> interval : iv_newpages) {
      uint32_t left = interval.first;
      uint32_t right = interval.second;
      for (uint32_t pagenr = left; pagenr <= right; ++pagenr) {
        f_body.read(page_content, pagesize);

        // explicit cast because of null-byte issues
        std::string page_content_str(page_content, page_content + pagesize);

        newpages[pagenr] = page_content_str;
        newdistinct.insert(page_content_str);
      }
    }
    delete[](page_content);
  } else {
    // number of intradeduplicate new pages
    uint32_t page_count = read_num_LE(&f_body, 4);

    // parse intervals (one for each page)
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> intervals;
    intervals.reserve(page_count);
    for (uint32_t i = 0; i < page_count; i++) {
      auto iv = parse_interval_list(&f_body);
      intervals.push_back(std::move(iv));
    }

    // unfold intradeduplication
    auto page_content = new char[pagesize];
    for (uint32_t i = 0; i < page_count; i++) {
      f_body.read(page_content, pagesize);
      std::string page(page_content, page_content + pagesize);
      for (auto iv : (intervals[i])) {
        uint32_t left = iv.first;
        uint32_t right = iv.second;
        for (uint32_t pnum = left; pnum <= right; pnum++) {
          newpages[pnum] = page;
        }
      }
    }
    delete[](page_content);
  }

  // load reference dump
  BOOST_LOG_TRIVIAL(debug) << "loading refdump";
  std::shared_ptr<memdump> refdump;
  if (handler != nullptr) {
    refdump = handler->get_refdump(ref_dump_path, pagesize);
  } else {
    refdump = std::make_shared<memdump>(ref_dump_path);
    refdump->readDumpfile(pagesize);
  }

  // parse reference dump (if not done already)
  auto ref_pages = refdump->getNumToPage();

  // open final file (append .processing because we are not finished yet)
  std::string out_filename_processing(out_filename);
  out_filename_processing += ".processing";
  auto f_out = std::ofstream(out_filename_processing.c_str(), std::ios::binary);

  // reconstruct page by page
  for (uint32_t pagenr = 0; pagenr < uncompressed_size / pagesize; pagenr++) {
    if (fills.count(pagenr) > 0) {
      // we got a deduplicated page with a different page number
      uint32_t refnum = fills[pagenr];
      std::string page_content = (*ref_pages)[refnum];
      f_out.write(page_content.c_str(), pagesize);
    } else if (diffing && (diffs.count(pagenr) > 0)) {
      // we got a diffed page
      auto page_content = apply_diff((*ref_pages)[pagenr], diffs[pagenr]);
      if (page_content.empty()) {
        BOOST_LOG_TRIVIAL(error) << "aborting due to error when applying diffs";
        return;
      }
      f_out.write(page_content.c_str(), pagesize);
    } else if (newpages.count(pagenr) > 0) {
      // we got a completely new page
      std::string page_content = newpages[pagenr];
      f_out.write(page_content.c_str(), pagesize);
    } else {
      // we got a deduplicated page at the same page number
      std::string page_content = (*ref_pages)[pagenr];
      f_out.write(page_content.c_str(), pagesize);
    }
  }

  // flush stream before renaming file
  f_out.close();

  // remove the file if it already exists. This is faster than overwriting
  std::remove(out_filename);

  // remove temporary file
  std::rename(out_filename_processing.c_str(), out_filename);
  BOOST_LOG_TRIVIAL(info) << "decompressed file was saved as " << out_filename;
}

}  // namespace mscr

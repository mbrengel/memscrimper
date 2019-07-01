// Copyright [2019] <Daniel Weber>

#include "interdedup_compress.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/iostreams/copy.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/lzma.hpp>
#include <boost/log/trivial.hpp>

#include "memdump.h"
#include "utils.h"


namespace mscr {

static void create_pagenr_list(const std::set<uint32_t> &nums,
                               std::string *s_out) {
  // write number of pages
  auto sz = static_cast<uint32_t>(nums.size());
  std::vector<char> len = int_to_byte_LE<uint32_t>(sz, 4);
  s_out->append(&len[0], 4);

  // write pagenr by pagenr
  bool first = true;
  uint32_t prev = 0;
  uint32_t curr;
  for (const uint32_t &page_num : nums) {
    // delta-encode page number
    if (first) {
      curr = page_num;
      first = false;
    } else {
      curr = page_num - prev - 1;
    }

    // write page number
    if (curr < 128) {
      // we fit in one byte -> we set the highest bit to 1
      std::vector<char> page_numb = int_to_byte_BE(curr | 128, 1);
      s_out->append(&page_numb[0], 1);
    } else {
      // we need 4 bytes to store the pagenum
      std::vector<char> page_numb = int_to_byte_BE(curr, 4);
      s_out->append(&page_numb[0], 4);
    }
    prev = page_num;
  }
}


static std::vector<std::pair<uint32_t, uint32_t>> intervalize(
    const std::set<uint32_t> &numbers) {
  // check if empty
  std::vector<std::pair<uint32_t, uint32_t>> result;
  if (numbers.empty()) {
    return result;
  }

  // holds the current interval
  std::pair<uint32_t, uint32_t> curr = std::make_pair(*(numbers.begin()),
                                           *(numbers.begin()));

  // build the intervals starting from the second element on
  for (auto it = ++std::begin(numbers); it != numbers.end(); it++) {
    uint32_t x = *it;
    if (curr.second + 1 == x) {
      curr.second = x;
    } else {
      result.push_back(curr);
      curr.first = x;
      curr.second = x;
    }
  }
  result.push_back(curr);

  return result;
}


static std::string create_interval(uint32_t left, uint32_t right, bool islast) {
  /* interval [l, r]
   * format:  t[1b], d[2b], l[29b], r-l[{0,1,2,4}B]
   * t = termination bit
   * d = size of r -l
   * l = page number after left side of interval
   * r = offset to the right side */

  // check bounds
  if (left >= (1 << 29)) {
    BOOST_LOG_TRIVIAL(error) << "left interval is too big";
    return "";
  }

  // determine termination bit
  uint32_t last;
  if (islast) {
    last = 4;
  } else {
    last = 0;
  }

  /* we do not append r-l when we encode only 1 page this will shift the
   * termination bit to the left and write the pagenum */
  uint32_t bytelen = 0;
  uint32_t data = 0;
  if (left == right) {
    data = ((last << 29) | left);
    std::vector<char> ch_vec = int_to_byte_LE(data, 4);
    return std::string(ch_vec.begin(), ch_vec.end());
  }

  // encode delta
  uint32_t delta = right - left;
  if (delta < (1 << 8)) {
    bytelen = 1;
    data = ((last | 1) << 29) | left;
  } else if (delta < (1 << 16)) {
    bytelen = 2;
    data = ((last | 2) << 29) | left;
  } else {
    bytelen = 4;
    data = ((last | 3) << 29) | left;
  }

  // encode everything
  std::vector<char> ch_vec = int_to_byte_LE(data, 4);
  std::vector<char> ch_vec_RF = int_to_byte_LE(delta, bytelen);
  ch_vec.insert(ch_vec.end(), ch_vec_RF.begin(), ch_vec_RF.end());

  return std::string(ch_vec.begin(), ch_vec.end());
}


static std::vector<std::string> create_interval_list(
    const std::vector<std::pair<uint32_t, uint32_t>> &intervals) {
  std::vector<std::string> result;
  uint32_t len = intervals.size();
  result.reserve(len);

  for (uint32_t i = 0; i < len; ++i) {
    auto interval = intervals[i];
    uint32_t left = interval.first;
    uint32_t right = interval.second;
    result.push_back(create_interval(left, right, i + 1 == len));
  }

  return result;
}


static std::string create_method(bool intra, bool diffing,
    const compression &inner) {
  std::string method = "interdedup";

  // encode intra
  if (!intra) {
    method.append("nointra");
  }

  // encode diffing
  if (diffing) {
    method.append("delta");
  }

  // encode inner compression
  switch (inner) {
    case compression::GZIP:
      method.append("gzip");
      break;
    case compression::ZIP7:
      method.append("7zip");
      break;
    case compression::BZIP2:
      method.append("bzip2");
      break;
    case compression::NOINNER:
      break;
  }

  return method;
}


static std::string create_header(const std::string &method,
    uint64_t uncompressed_size, uint32_t majorversion, uint32_t minorversion,
    const std::string &magicnum, uint32_t pagesize) {
  std::string head;

  /* encode magic number + method (note: pushing instead of appending because
   * of null-bytes) */
  head += magicnum;
  head.push_back('\0');  // += operator will only copy till it hits nullbyte
  head += method;
  head.push_back('\0');

  // encode major version
  for (const char c : int_to_byte_LE(majorversion, 2)) {
    head.push_back(c);
  }

  // encode minor version
  for (const char c : int_to_byte_LE(minorversion, 2)) {
    head.push_back(c);
  }

  // encode page size
  for (const char c : int_to_byte_LE(pagesize, 4)) {
    head.push_back(c);
  }

  // encode uncompressed size
  for (const char c : int_to_byte_LE<uint64_t>(uncompressed_size, 8)) {
    head.push_back(c);
  }

  return head;
}


static int compress_file(const std::string &file_in, const compression &inner,
    std::string *file_out) {
  /* returns the compressed body from the
   * chosen compression */
  BOOST_LOG_TRIVIAL(debug) << "starting inner compression";
  boost::iostreams::filtering_ostream out;
  boost::iostreams::lzma_params lzma_par;
  switch (inner) {
    case compression::ZIP7:
      lzma_par.level = boost::iostreams::lzma::default_compression;
      out.push(boost::iostreams::lzma_compressor(lzma_par));
      break;
    case compression::BZIP2:
      out.push(boost::iostreams::bzip2_compressor());
      break;
    case compression::GZIP:
      out.push(boost::iostreams::gzip_compressor());
      break;
    case compression::NOINNER:
      // nothing to do here
      *file_out = file_in;
      BOOST_LOG_TRIVIAL(debug) << "finished inner compression";
      return 0;
  }
  std::stringstream writebuf;
  out.push(writebuf);
  std::stringstream input_stream(file_in);
  boost::iostreams::copy(input_stream, out);
  *file_out = writebuf.str();
  BOOST_LOG_TRIVIAL(debug) << "finished inner compression";
  return 0;
}


static int generate_patches(const std::string &ref_page,
    const std::string &delta_page,
    std::vector<std::pair<uint32_t, std::string>> *out) {
  /* note that both pages have to be the same size in order for this to make
   * sense */
  uint32_t pagesize = ref_page.size();

  // keeps track of the previous indices
  uint32_t previ = 0;
  bool first = true;

  // keeps track of streaks of identical bytes
  std::string samebytes;

  // encodes a list of patches, i.e., a list of (offset, bytes) pairs
  std::vector<std::pair<uint32_t, std::string>> ret;

  for (uint32_t i = 0; i < pagesize; i++) {
    // compare byte by byte
    char ref_byte = ref_page[i];
    char delta_byte = delta_page[i];

    if (ref_byte == delta_byte) {
      // keep track of streaks of identical bytes
      samebytes += delta_byte;
    } else {
      // build the current patch
      std::pair<uint32_t, std::string> curr_patch;

      /* if the streak of identical bytes is not larger than 2, we would need
       * two patches whose overhead would increase 2 bytes - hence, we just
       * include the identical bytes in a single patch */
      if (samebytes.size() <= 2 && !first) {
        ret.back().second += samebytes;
        ret.back().second += delta_byte;
      } else {
        if (first) {
          first = false;
          curr_patch.first = i;
          curr_patch.second.clear();
        } else {
          curr_patch.first = i - previ - ret.back().second.size();
          curr_patch.second.clear();
        }
        previ = i;
        curr_patch.second += (delta_byte);
        ret.push_back(curr_patch);
      }
      samebytes.clear();
    }
  }

  /* make sure individual patches are smaller than 2048 bytes to comply with
   * our file format specification, which is achieved by partitioning longer
   * patches into chunks of 2048 bytes */
  for (auto patch : ret) {
    uint32_t offset = patch.first;
    std::string bytes = patch.second;
    uint32_t bytelen = patch.second.size();
    if (bytelen > 2048) {
      std::string first_data = bytes.substr(0, 2048);
      std::string second_data = bytes.substr(2048, 2048);
      out->emplace_back(offset, first_data);
      out->emplace_back(0, second_data);
    } else {
      out->emplace_back(offset, patch.second);
    }
  }

  return 0;
}


static std::string patch_encode(uint32_t offset, uint32_t len) {
  // will hold the encoding of l - 1 and o
  std::vector<char> ret_vec;

  // we encode l - 1
  len -= 1;

  if (offset < 256 && len < 128) {
    /* encode o and l with two bytes such that the msb of the first byte is set
     * to 0 */
    std::vector<char> len_vec = int_to_byte_BE(len, 1);
    ret_vec.insert(ret_vec.end(), len_vec.begin(), len_vec.end());
    std::vector<char> off_vec = int_to_byte_BE(offset, 1);
    ret_vec.insert(ret_vec.end(), off_vec.begin(), off_vec.end());
  } else {
    /* encode o and l with three bytes such that the msb of the first byte is
     * set to 1 */
    uint32_t len_off = (len << 12) | offset;
    uint32_t a = (len_off & 0xFF0000) >> 16;
    a |= 128;
    std::vector<char> a_vec = int_to_byte_BE(a, 1);
    ret_vec.insert(ret_vec.end(), a_vec.begin(), a_vec.end());
    std::vector<char> last2 = int_to_byte_BE(len_off & 0xFFFF, 2);
    ret_vec.insert(ret_vec.end(), last2.begin(), last2.end());
  }

  return std::string(ret_vec.begin(), ret_vec.end());
}


static std::string create_diff(const std::string &ref_page,
    const std::string &delta_page) {
  // generate patches
  std::vector<std::pair<uint32_t, std::string>> patches;
  generate_patches(ref_page, delta_page, &patches);

  // create diff patch by patch
  std::string diff;
  uint32_t patchnum = 0;
  for (auto patch : patches) {
    patchnum++;
    uint32_t offset = patch.first;
    std::string bytes = patch.second;
    diff += patch_encode(offset, bytes.size());
    diff += bytes;
  }

  // build final diff
  std::vector<char> num_bin = int_to_byte_LE(patchnum, 2);
  std::string res = std::string(num_bin.begin(), num_bin.end());
  res += diff;

  return res;
}


void interdedup_compress(std::shared_ptr<memdump> ref, const memdump &srcdump,
    const char *out_filename, compression inner, bool diffing, bool intra) {
  // keep track of pages of both the reference and the source dump
  auto src_pages = srcdump.getPages();
  auto ref_pages = ref->getPages();
  uint32_t pagesize = ref_pages->begin()->first.size();

  // print some debug output
  if (diffing) {
    BOOST_LOG_TRIVIAL(debug) << "DIFFING enabled";
  }
  if (intra) {
    BOOST_LOG_TRIVIAL(debug) << "INTRA enabled";
  }

  // maps each diffable page num to a diff
  std::unordered_map<uint32_t, std::string> diffs;

  // collects all new (undiffable) page numbers we cannot deduplicate
  std::set<uint32_t> new_pagenrs;

  // maps page numbers of new/unique pages to their content
  std::unordered_map<uint32_t, std::string> new_pages;

  // maps refnumbs to page numbers that will be deduped by it
  std::unordered_map<uint32_t, std::set<uint32_t>> dedups;

  // holds all page numbers of deduplicated pages
  std::set<uint32_t> dedup_pagenrs;

  // maps page numbers to pages of the reference memory dump
  auto ref_num_to_page = ref->getNumToPage();

  // holds page numbers of diffable pages
  std::set<uint32_t> diff_pagenrs;

  /* maps each page to the page numbers where it occurs (will be used for
   * intradeduplicating the new pages */
  std::unordered_map<std::string, std::set<uint32_t>> same_newpages;

  // go through each page of the source dump
  for (const auto &key_value : *src_pages) {
    std::string srcpage = key_value.first;

    if (ref_pages->find(srcpage) != ref_pages->end()) {
      /* this page occurs in the reference dump
       * get all the page numbers where the page occurrs in the source dump,
       * but not in the reference dump */
      std::set<uint32_t> dedup_pages;
      std::set_difference((*src_pages).at(srcpage).begin(),
                (*src_pages).at(srcpage).end(),
                (*ref_pages).at(srcpage).begin(),
                (*ref_pages).at(srcpage).end(),
                std::inserter(dedup_pages, dedup_pages.begin()));

      // if thare are >0 such page numbers, then add them to our dedups
      if (!dedup_pages.empty()) {
        uint32_t pagenr = *((*ref_pages).at(srcpage).begin());
        dedups[pagenr] = dedup_pages;
        dedup_pagenrs.insert(pagenr);
      }
    } else {
      /* the page is not in the reference dump, but maybe a similar page sits
       * somewhere else */
      for (const uint32_t &pagenum : (*src_pages).at(srcpage)) {
        // go through all page numbers of the page
        if (diffing) {
          // diff the page with its counterpart in the reference dump
          std::string diff = create_diff((*ref_num_to_page)[pagenum], srcpage);

          /* remmber the diff if if storing it is cheaper than storing the page
           * itself */
          if (diff.size() < pagesize) {
            diffs[pagenum] = diff;
            diff_pagenrs.insert(pagenum);
            continue;
          }
        }

        /* if requested, keep track of intradeduplicate occurrences if diffing
         * was not successful, otherwise just consider it a new unique page */
        if (intra) {
          (same_newpages[srcpage]).insert(pagenum);
        } else {
          new_pagenrs.insert(pagenum);
          new_pages[pagenum] = srcpage;
        }
      }
    }
  }

  std::string tmpf;
  // write reference dump path
  tmpf.append(ref->getPath().c_str(), ref->getPath().size());
  tmpf.append("\0", 1);

  // write page number list containing all deduplicatable page numbers
  create_pagenr_list(dedup_pagenrs, &tmpf);

  // write interval lists for each deduplicated page
  for (const auto &dedup_pnum : dedup_pagenrs) {
    auto dedupset = dedups[dedup_pnum];
    auto iv_list = create_interval_list(intervalize(dedupset));
    for (const std::string &iv : iv_list) {
      tmpf.append(iv.c_str(), iv.size());
    }
  }

  // write diffs if requested
  if (diffing) {
    create_pagenr_list(diff_pagenrs, &tmpf);
    for (auto pagenum : diff_pagenrs) {
      std::string diff = diffs[pagenum];
      tmpf.append(diff.c_str(), diff.size());
    }
  }

  BOOST_LOG_TRIVIAL(debug) << "wrote diffs + interval-lists to file";
  if (intra) {
    // intervalize new pages
    std::vector<std::string> intrapages;
    intrapages.reserve(same_newpages.size());
    std::unordered_map<std::string, std::vector<std::pair<
        uint32_t, uint32_t>>> new_pagenrs_iv;
    for (auto key_value : same_newpages) {
      std::string page = key_value.first;
      intrapages.push_back(page);
      new_pagenrs_iv[page] = (intervalize(same_newpages[page]));
    }

    // write number of distinct new pages
    std::vector<char> num_vec = int_to_byte_LE(new_pagenrs_iv.size(), 4);
    tmpf.append(&num_vec[0], num_vec.size());

    // write intervals of page numbers
    for (const std::string &page : intrapages) {
      auto ivlist = create_interval_list(new_pagenrs_iv[page]);
      for (auto iv : ivlist) {
        tmpf.append(&iv[0], iv.size());
      }
    }

    // write the actual pages covering all intervals
    for (const std::string &page : intrapages) {
      tmpf.append(page.c_str(), page.size());
    }
  } else {
    // write intervalized new page numbers
    auto new_pagenrs_iv = intervalize(new_pagenrs);
    for (const std::string &iv : create_interval_list(new_pagenrs_iv)) {
      tmpf.append(iv.c_str(), iv.size());
    }

    // write actual new unique pages
    for (const uint32_t &pagenr : new_pagenrs) {
      std::string page = new_pages[pagenr];
      tmpf.append(page.c_str(), page.size());
    }
  }

  // apply inner compression
  std::string tmpfile2;
  int ret = compress_file(tmpf, inner, &tmpfile2);
  if (ret != 0) {
    BOOST_LOG_TRIVIAL(error) << "inner compression failed";
    return;
  }

  // write header
  std::string method = create_method(intra, diffing, inner);
  uint64_t filesize = get_filesize(srcdump.getPath().c_str());
  if (filesize <= 0) {
    BOOST_LOG_TRIVIAL(error) << "filesize of " << srcdump.getPath()
                             << " is invalid.";
    return;
  }
  BOOST_LOG_TRIVIAL(debug) << "Original filesize: " << filesize;
  const uint32_t major_version = 2;
  const uint32_t minor_version = 1;
  const std::string magicbyte = "MBCR";
  std::string header = create_header(method, filesize, major_version,
                                     minor_version, magicbyte, pagesize);
  std::string out_filename_processing(out_filename);
  out_filename_processing += ".processing";
  std::ofstream final_file = std::ofstream(out_filename_processing.c_str(),
                                 std::ios::binary);
  final_file.write(header.c_str(), header.size());

  // write body
  final_file << tmpfile2;
  // flush stream before renaming file
  final_file.close();

  // remove the file if it already exists. This is faster than overwriting
  std::remove(out_filename);

  // remove the ".processing"-ending
  std::rename(out_filename_processing.c_str(), out_filename);
  BOOST_LOG_TRIVIAL(info) << "finished compressing file to " << out_filename;
}

}  // namespace mscr

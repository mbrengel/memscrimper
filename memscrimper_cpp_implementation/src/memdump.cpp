// Copyright [2019] <Daniel Weber>

#include "memdump.h"

#include <fstream>
#include <iostream>

#include <boost/log/trivial.hpp>


namespace mscr {

memdump::memdump(const std::string &path) : path_(path) {}


std::string memdump::getPath() const {
  return this->path_;
}


const std::unordered_map<std::string, std::set<uint32_t>>*
    memdump::getPages() const {
  return &(this->page_map_);
}


std::unordered_map<uint32_t, std::string>* memdump::getNumToPage() {
  // num_to_page map is evaluated lazily on demand to save memory
  
  std::lock_guard<std::mutex> m_lock(this->mu_);
  if (this->num_to_page_.empty()) {
    for (auto key_value : this->page_map_) {
      for (uint32_t pagenum : key_value.second) {
        this->num_to_page_[pagenum] = key_value.first;
      }
    }
  }
  return &this->num_to_page_;
}


int memdump::readDumpfile(uint32_t pagesize) {
  // open file
  std::ifstream file(this->path_.c_str(), std::ios::binary);

  if (file.fail()) {
    BOOST_LOG_TRIVIAL(error) << "Could not open " << this->path_;
    return 1;
  }

  // read file page by page
  uint32_t pagenr = 0;
  auto page_content = new char[pagesize];
  while (file.read(page_content, pagesize)) {
    std::string page_content_str(page_content, page_content + pagesize);
    (this->page_map_[std::move(page_content_str)]).insert(pagenr);
    pagenr++;
  }

  // check if we consumed the whole file
  if (!file.eof()) {
    BOOST_LOG_TRIVIAL(error) << "reading file failed (did not reach eof)";
    delete[](page_content);
    return 1;
  }

  // clean up
  delete[](page_content);
  BOOST_LOG_TRIVIAL(debug) << "finished reading " << this->path_;

  return 0;
}

}  // namespace mscr

// Copyright [2019] <Daniel Weber>

#ifndef MEMDUMP_H_
#define MEMDUMP_H_

#include <mutex>
#include <set>
#include <string>
#include <unordered_map>

namespace mscr {

class memdump {
 public:
  explicit memdump(const std::string &name);
  std::string getPath() const;
  const std::unordered_map<std::string, std::set<uint32_t>> * getPages() const;
  std::unordered_map<uint32_t, std::string> * getNumToPage();
  int readDumpfile(uint32_t pagesize);

 private:
  std::mutex mu_;
  std::string path_;
  std::unordered_map<std::string, std::set<uint32_t>> page_map_;
  std::unordered_map<uint32_t, std::string> num_to_page_;
};

} // namespace mscr

#endif  // MEMDUMP_H_

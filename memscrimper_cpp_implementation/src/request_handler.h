// Copyright [2019] <Daniel Weber>

#ifndef REQUEST_HANDLER_H_
#define REQUEST_HANDLER_H_

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>

#include "memdump.h"

namespace mscr {

class request_handler {
 public:
  explicit request_handler(uint32_t thread_count);
  ~request_handler();
  int handle_request(std::string msg);
  std::shared_ptr<memdump> get_refdump(std::string path, uint32_t pagesize);

 private:
  std::vector<std::shared_ptr<memdump>> refdumps_;
  std::recursive_mutex mu_;
  boost::asio::thread_pool worker_threads_;
  int add_reference(std::string msg);
  int del_reference(std::string msg);
  int compress_dump(std::string msg);
  int decompress_dump(std::string msg);
  int find_refdump(const std::string &path);
};

}  // namespace mscr

#endif  // REQUEST_HANDLER_H

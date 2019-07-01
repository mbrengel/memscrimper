// Copyright 2018 <Daniel Weber>

#ifndef SOCKET_API_H_
#define SOCKET_API_H_

#include <string>
#include <unordered_set>

#include "request_handler.h"

namespace mscr {

class command_socket {
 public:
  explicit command_socket(const std::string &sock_path, request_handler* handler);
  ~command_socket();
  void start_listen();

 private:
  static bool shutdown_;
  request_handler* handler_;
  std::string sock_path_;
  int srv_sock_;
  struct sockaddr_un srv_addr_;
  std::unordered_set<int> open_socks_;
  std::unordered_map<int, int> timeout_counters_;
  static void sig_handler(int _);
  void handle_client_connection(int cl_sock, int epfd, struct epoll_event* ev);
};

}  // namespace mscr

#endif  // SOCKET_API_H_

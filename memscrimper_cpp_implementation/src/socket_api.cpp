// Copyright [2019] <Daniel Weber>

#include "socket_api.h"

#include <csignal>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <iostream>
#include <stdexcept>
#include <vector>

#include <boost/log/trivial.hpp>

#include "request_handler.h"
#include "utils.h"

namespace mscr {

// timeout in ms for epoll_wait
constexpr int kTimeoutEpoll = 5000;

// size of epoll queue
constexpr int kEpollQueueSize = 255;
// number of EPOLL-circles before timeout
constexpr int kTimeoutForClients = 1000;

// size of main socket backlog
constexpr int kBacklogSize = 10;

bool command_socket::shutdown_(false);

command_socket::command_socket(const std::string &sock_path, request_handler* handler)
  : handler_(handler), sock_path_(sock_path),
    srv_sock_(socket(AF_LOCAL, SOCK_STREAM, 0)) {
  // initialize socket
  this->srv_addr_.sun_family = AF_LOCAL;
  snprintf(this->srv_addr_.sun_path, sizeof(this->srv_addr_.sun_path), "%s",
      this->sock_path_.c_str());

  // destroy socket file if it exists
  unlink(this->sock_path_.c_str());

  // make socket non-blocking
  int saved_flags = fcntl(this->srv_sock_, F_GETFL);
  if (saved_flags < 0) {
    BOOST_LOG_TRIVIAL(error) << "error getting socket flags on main socket";
    close(this->srv_sock_);
    throw std::runtime_error("error setting socket options");
  }
  saved_flags |= O_NONBLOCK;
  int ret = fcntl(this->srv_sock_, F_SETFL, saved_flags);
  if (ret < 0) {
    BOOST_LOG_TRIVIAL(error) << "error switching main socket to non-blocking";
    close(this->srv_sock_);
    throw std::runtime_error("error setting socket options");
  }

  // bind socket
  ret = bind(this->srv_sock_,
    reinterpret_cast<struct sockaddr *>(&(this->srv_addr_)),
    SUN_LEN(&(this->srv_addr_)));
  if (ret < 0) {
    BOOST_LOG_TRIVIAL(error) << "failed to bind socket";
    close(this->srv_sock_);
    throw std::runtime_error("error binding socket");
  }

  // enable signal handlers
  signal(SIGINT, reinterpret_cast<sighandler_t>(sig_handler));
  signal(SIGTERM, reinterpret_cast<sighandler_t>(sig_handler));
}


command_socket::~command_socket() {
  // close main socket
  close(this->srv_sock_);

  // close all other open sockets
  for (int sock : this->open_socks_) {
    BOOST_LOG_TRIVIAL(info) << "closing fd " << sock;
    close(sock);
  }

  // destroy socket file
  BOOST_LOG_TRIVIAL(info) << "deleting socketfile";
  unlink(this->sock_path_.c_str());
}


void command_socket::sig_handler(int) {
  /* this will be triggered if SIGNALS are caught and shutdown = true will
   * cause actual closing logic to happen */
  shutdown_ = true;
}


void command_socket::start_listen() {
  // start listening
  int ret = listen(this->srv_sock_, kBacklogSize);
  if (ret) {
    BOOST_LOG_TRIVIAL(error) << "error on listen call to main socket";
    return;
  }

  struct epoll_event events[kEpollQueueSize];
  int epfd = epoll_create(kEpollQueueSize);
  this->open_socks_.insert(epfd);
  static struct epoll_event ev;

  // add main socket
  ev.events = EPOLLIN | EPOLLPRI | EPOLLERR | EPOLLHUP;
  ev.data.fd = this->srv_sock_;
  int res = epoll_ctl(epfd, EPOLL_CTL_ADD, this->srv_sock_, &ev);
  if (res && !shutdown_) {
    BOOST_LOG_TRIVIAL(error) << "Epoll error when adding main socket";
    return;
  }
  this->open_socks_.insert(this->srv_sock_);

  // dispatch loop
  while (!shutdown_) {
    // handle timeouts
    for (auto it = this->timeout_counters_.begin();
          it != this->timeout_counters_.end();) {
      int sock = it->first;
      int curr_timeout = it->second;

      // check if client exceeded timeout
      if (curr_timeout >= kTimeoutForClients) {
        BOOST_LOG_TRIVIAL(info) << "Client " << sock << " exceeded timeout";
        close(sock);
        this->open_socks_.erase(sock);
        this->timeout_counters_.erase(it++);
      } else {
        // increase timeout counter
        this->timeout_counters_[sock] += 1;
        it++;
      }
    }

    // wait for event
    int num_rdy = epoll_wait(epfd, events, 10, kTimeoutEpoll);
    if (num_rdy < 0 && !shutdown_) {
      if (errno == EINTR) {
        // we were killed by an interrupt (e.g., system() fork)
        BOOST_LOG_TRIVIAL(warning) << "caught interrupt on epoll_wait";
        continue;
      } else {
        // we got an unexpected error
        BOOST_LOG_TRIVIAL(error) << "unexpected error on epoll_wait: "
                                 << strerror(errno);

        // note that this will close the listener
        return;
      }
    }

    // process all events
    for (int i = 0; i < num_rdy; i++) {
      // get corresponding socket file descriptor holding the event
      int sock = events[i].data.fd;
      BOOST_LOG_TRIVIAL(debug) << "handling socket " << std::to_string(sock);

      if (sock == this->srv_sock_) {
        // if we're on the main socket, we have to add a new connection
        struct sockaddr_un cl_addr;
        memset(&cl_addr, '\0', sizeof(cl_addr));
        socklen_t cl_len;
        memset(&cl_len, '\0', sizeof(cl_len));

        // accept new client connection
        int cl_sock = accept(this->srv_sock_,
            reinterpret_cast<struct sockaddr *>(&cl_addr), &cl_len);
        if (cl_sock < 0) {
          if (errno == EAGAIN || errno == EWOULDBLOCK) {
          } else {
            BOOST_LOG_TRIVIAL(error) << "error accepting client connection";
          }
          continue;
        }

        // make socket non-blocking
        int saved_flags = fcntl(cl_sock, F_GETFL);
        if (saved_flags < 0) {
          BOOST_LOG_TRIVIAL(error) << "error getting socket flags";
          close(cl_sock);
          continue;
        }
        saved_flags |= O_NONBLOCK;
        int ret = fcntl(cl_sock, F_SETFL, saved_flags);
        if (ret < 0) {
          BOOST_LOG_TRIVIAL(error) << "error setting socket to non-blocking";
          close(cl_sock);
          continue;
        }

        // add socket to epoll set
        ev.data.fd = cl_sock;
        ret = epoll_ctl(epfd, EPOLL_CTL_ADD, cl_sock, &ev);
        if (ret < 0) {
          BOOST_LOG_TRIVIAL(error) << "error adding new client to epoll";
          close(cl_sock);
          continue;
        }
        this->open_socks_.insert(cl_sock);

        // initialize timeout counter
        this->timeout_counters_[cl_sock] = 0;
      } else {
        // reset timeout counter
        this->timeout_counters_[sock] = 0;

        // handle client connection
        handle_client_connection(sock, epfd, &ev);
      }
    }
  }
  BOOST_LOG_TRIVIAL(info) << "shutting down";
}


void command_socket::handle_client_connection(int cl_sock, int epfd, struct epoll_event* ev) {
  // store read bytes here -- we know that no message exceeds 2048B payload + 1 Byte size
  char read_buf[2049] = { 0 };

  // read first byte
  int read_byte = read(cl_sock, read_buf, 1);

  if (read_byte <= 0) {
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      // socket would block -> ignore the request and try again later
      return;
    }

    /* peer closed socket or something else failed - bye bye
     * at first remove fd from epoll set (before closing - otherwise delete from epoll set won't
     * work)
     * this is important because forks from bp::child(utils.cpp) can still hold the fd */
    epoll_ctl(epfd, EPOLL_CTL_DEL, cl_sock, nullptr);
    BOOST_LOG_TRIVIAL(debug) << "closing fd " << cl_sock;
    int ret = close(cl_sock);

    // check if we closed the socket successfully
    if (ret) {
      BOOST_LOG_TRIVIAL(error) << "error closing fd " << cl_sock << "err: " << strerror(errno);
      // we failed on closing so we add the fd back to our epoll set
      ev->data.fd = cl_sock;
      epoll_ctl(epfd, EPOLL_CTL_ADD, cl_sock, ev);
    } else {
      // remove closed socket from management lists
      this->timeout_counters_.erase(cl_sock);
      this->open_socks_.erase(cl_sock);
    }
  } else {
    // first byte encodes our message length
    uint32_t msglen = static_cast<uint8_t>(read_buf[0]) * 8;

    // read rest of the message
    uint32_t read_bytes = read(cl_sock, read_buf + 1, msglen);
    std::string msg(read_buf, read_bytes + 1);
    auto msgid = static_cast<uint8_t>(read_buf[1]);

    // sanity check
    if (read_bytes != msglen) {
      BOOST_LOG_TRIVIAL(warning) << "received broken or incomplete message. "
        << "expected " << msglen << "B - read: " << read_bytes << "B."
        << "Aborting Request.";
      // pack MSG_ID
      std::vector<char> cl_ack = int_to_byte_LE(msgid, 1);  // msg id
      // append failure byte (0x0)
      cl_ack.push_back((int_to_byte_LE(0, 1)).front());
      // send failed ACK to client
      send(cl_sock, cl_ack.data(), 2, 0);
    } else {
      // pack MSG_ID
      std::vector<char> cl_ack = int_to_byte_LE(msgid, 1);
      // append success byte (0x1)
      cl_ack.push_back((int_to_byte_LE(1, 1)).front());
      // send ACK to client
      send(cl_sock, cl_ack.data(), 2, 0);
      /* cast to string so we can pass by value
       * read_buf +2 will cut of MSG_LEN and MSG_ID (only needed for networking) */
      std::string msg(read_buf + 2 , read_bytes - 1);

      // request handler will process the request
      this->handler_->handle_request(msg);
    }
  }
}

}  // namespace mscr

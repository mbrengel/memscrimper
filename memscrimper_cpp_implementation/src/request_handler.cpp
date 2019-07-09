// Copyright [2019] <Daniel Weber>

#include "request_handler.h"

#include <algorithm>
#include <iostream>
#include <memory>
#include <thread>

#include <boost/asio.hpp>
#include <boost/asio/thread_pool.hpp>
#include <boost/bind.hpp>
#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>

#include "interdedup_compress.h"
#include "interdedup_decompress.h"
#include "utils.h"


namespace mscr {

request_handler::request_handler(uint32_t thread_count) : worker_threads_(thread_count) {
  BOOST_LOG_TRIVIAL(info) << "starting workerpool with " << thread_count
                          << " threads";
}


request_handler::~request_handler() {
  this->worker_threads_.join();
}


int request_handler::handle_request(std::string msg) {
  // parse opcode
  auto opcode = static_cast<uint8_t>(msg[0]);
  // we cut away opcode byte, because this will not be needed anymore
  msg.erase(0, 1);

  // dispatch
  switch (opcode) {
    case 0x00:
      BOOST_LOG_TRIVIAL(info) << "got request: add reference";
      boost::asio::post(this->worker_threads_,
          boost::bind(&request_handler::add_reference,
              this, msg));
      break;

    case 0x01:
      BOOST_LOG_TRIVIAL(info) << "got request: compress";
      boost::asio::post(this->worker_threads_,
               boost::bind(&request_handler::compress_dump,
                   this, msg));
      break;

    case 0x02:
      BOOST_LOG_TRIVIAL(info) << "got request: decompress";
      boost::asio::post(this->worker_threads_,
               boost::bind(&request_handler::decompress_dump,
                   this, msg));
      break;

    case 0x04:
      BOOST_LOG_TRIVIAL(info) << "got request: delete reference";
      boost::asio::post(this->worker_threads_,
               boost::bind(&request_handler::del_reference,
                     this, msg));
      break;

    default:
      BOOST_LOG_TRIVIAL(error) << "got request: unknown opcode - received: "
                               << std::to_string(opcode);
      return 1;
  }

  return 0;
}


int request_handler::compress_dump(std::string msg_str) {
  // parse reference path
  const char *msg = msg_str.c_str();
  int msg_offset = 0;  // skip first (len) and second (opcode) byte
  std::string ref_path(msg + msg_offset);
  BOOST_LOG_TRIVIAL(debug) << "ref_path: " << ref_path;
  msg_offset += ref_path.size() + 1;

  // parse source dump path
  std::string srcdump_path(msg + msg_offset);
  BOOST_LOG_TRIVIAL(debug) << "srcdump_path: " << srcdump_path;
  msg_offset += srcdump_path.size() + 1;

  // parse out file path
  std::string filename_out(msg + msg_offset);
  BOOST_LOG_TRIVIAL(debug) << "file_out: " << filename_out;
  msg_offset += filename_out.size() + 1;

  // parse page size
  uint32_t pagesize = read_num_LE(msg + msg_offset, 4);
  BOOST_LOG_TRIVIAL(debug) << "pagesize: " << pagesize;
  msg_offset += 4;

  // parse intra
  bool intra = msg[msg_offset] == '\x01';
  BOOST_LOG_TRIVIAL(debug) << "intra: " << intra;
  msg_offset += 1;

  // parse diffing
  bool diffing = msg[msg_offset] == '\x01';
  BOOST_LOG_TRIVIAL(debug) << "diffing: " << diffing;
  msg_offset += 1;

  // parse inner compression
  compression inner;
  switch (msg[msg_offset]) {
    case '\x00':
      inner = compression::ZIP7;
      BOOST_LOG_TRIVIAL(debug) << "inner: zip7";
      break;

    case '\x01':
      inner = compression::GZIP;
      BOOST_LOG_TRIVIAL(debug) << "inner: gzip";
      break;

    case '\x02':
      inner = compression::BZIP2;
      BOOST_LOG_TRIVIAL(debug) << "inner: bzip2";
      break;

    case '\x03':
      inner = compression ::NOINNER;
      BOOST_LOG_TRIVIAL(debug) << "inner: noinner";
      break;

    default:
      BOOST_LOG_TRIVIAL(error) << "invalid inner compression method";
      return 1;
  }

  // retrieve reference dump
  std::shared_ptr<memdump> refdump = get_refdump(ref_path, pagesize);
  if (refdump == nullptr) {
    return 1;
  }

  // parse source dump
  memdump srcdump(srcdump_path);
  int ret = srcdump.readDumpfile(pagesize);
  if (ret != 0) {
    BOOST_LOG_TRIVIAL(error) << "error reading srcdump";
    return 1;
  }

  // compress
  interdedup_compress(refdump, srcdump, filename_out.c_str(),
      inner, diffing, intra);

  return 0;
}


int request_handler::decompress_dump(std::string msg_str) {
  // parse source dump
  const char *msg = msg_str.c_str();
  int msg_offset = 0;
  std::string dump_path(msg + msg_offset);
  msg_offset += dump_path.size() + 1;

  // parse out path
  std::string out_path(msg + msg_offset);

  // decompress
  interdedup_decompress(this, dump_path.c_str(), out_path.c_str());

  return 0;
}


int request_handler::add_reference(std::string msg_str) {
  // parse reference dump path
  const char *msg = msg_str.c_str();
  int msg_offset = 0;
  std::string ref_path(msg + msg_offset);
  BOOST_LOG_TRIVIAL(debug) << "ref_path: " << ref_path;
  msg_offset += ref_path.size() + 1;

  // parse page size
  auto pagesize = static_cast<uint32_t>(read_num_LE(msg + msg_offset, 4));
  BOOST_LOG_TRIVIAL(debug) << "pagesize: " << pagesize;

  // parse reference dump
  std::shared_ptr<memdump> refdump = std::make_shared<memdump>(ref_path);
  int ret = refdump->readDumpfile(pagesize);
  if (ret != 0) {
    BOOST_LOG_TRIVIAL(error) << "error reading dumpfile";
    return 1;
  }

  // replace dump if necessary
  std::lock_guard<std::recursive_mutex> m_lock(this->mu_);
  int pos = find_refdump(ref_path);
  if (pos != -1) {
    this->refdumps_.erase(this->refdumps_.begin() + pos);
  }

  // add dump
  this->refdumps_.push_back(std::move(refdump));
  BOOST_LOG_TRIVIAL(debug) << "added refdump (number of saved refdumps: "
                           << this->refdumps_.size() << ")";

  return 0;
}


int request_handler::del_reference(std::string msg_str) {
  // parse reference path
  const char *msg = msg_str.c_str();
  int msg_offset = 0;
  std::string ref_path(msg + msg_offset);

  // remove dump
  std::lock_guard<std::recursive_mutex> m_lock(this->mu_);
  int pos = find_refdump(ref_path);
  if (pos != -1) {
    this->refdumps_.erase(this->refdumps_.begin() + pos);
  }
  BOOST_LOG_TRIVIAL(debug) << "removed refdump (number of saved refdumps: "
                           << refdumps_.size() << ")";

  return 0;
}


std::shared_ptr<memdump> request_handler::get_refdump(std::string path,
    uint32_t pagesize) {
  {
    std::lock_guard<std::recursive_mutex> m_lock(this->mu_);
    int pos = find_refdump(path);
    if (pos != -1) {
      // we already have the dump - just return it
      BOOST_LOG_TRIVIAL(debug) << "refdump already loaded";
      return this->refdumps_[pos];
    }
  }  // destroy lock_guard again

  // parse the dump
  std::shared_ptr<memdump> dump = std::make_shared<memdump>(path);
  int ret = dump->readDumpfile(pagesize);
  if (ret != 0) {
    BOOST_LOG_TRIVIAL(error) << "error reading refdump";
    return nullptr;
  }

  // double check if the dump is added meanwhile to prevent data races
  std::lock_guard<std::recursive_mutex> m_lock(this->mu_);
  if (find_refdump(path) == -1) {
    this->refdumps_.push_back(dump);
  }
  int num_dumps = this->refdumps_.size();
  BOOST_LOG_TRIVIAL(debug) << "added refdump (number of saved refdumps: "
              << num_dumps << ")";
  return dump;
}


int request_handler::find_refdump(const std::string &path) {
  std::lock_guard<std::recursive_mutex> m_lock(this->mu_);
  for (uint32_t i = 0; i < this->refdumps_.size(); i++) {
    std::string curr_path = (this->refdumps_[i])->getPath();
      if (curr_path == path) {
        return i;
      }
  }
  return -1;
}

}  // namespace mscr

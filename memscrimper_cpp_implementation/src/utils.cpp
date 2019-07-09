// Copyright [2019] <Daniel Weber>

#include "utils.h"

#include <cassert>

#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <boost/log/trivial.hpp>


namespace mscr {

std::vector<char> int_to_byte_BE(uint32_t number, int bytelen) {
  std::vector<char> result(bytelen);

  for (int i = 0; i < bytelen; ++i) {
    result[bytelen - i - 1] = number >> (i * 8);
  }
  return result;
}


uint64_t read_num_LE(std::istream *file, int length) {
  assert(length <= 8);
  char c;
  uint64_t number = 0;

  // read byte by byte
  for (int i = 0; i < length; i++) {
    file->read(&c, 1);
    // make sure to first cast to uint8_t because of sign-extension issues
    number += static_cast<uint64_t>(static_cast<uint8_t>(c)) << (8 * i);
  }
  return number;
}


uint64_t read_num_LE(const char *array_begin, int length) {
  assert(length <= 8);
  char c;
  uint64_t number = 0;

  // go through the array byte by byte
  for (int i = 0; i < length; i++) {
    c = array_begin[i];

    // make sure to first cast to uint8_t because of sign-extension issues
    number += static_cast<uint64_t>(static_cast<uint8_t>(c)) << (8 * i);
  }
  return number;
}


std::string read_string(std::istream *file) {
  char c;
  std::string result;
  file->read(&c, 1);
  while (c != '\0') {
    result += c;
    file->read(&c, 1);
  }
  return result;
}


bool str_starts_with(const std::string &str, std::string prefix) {
  return !str.compare(0, prefix.size(), prefix);
}


uint64_t get_filesize(const char *filename) {
  std::ifstream in(filename, std::ifstream::binary | std::ifstream::ate);

  if (in.fail()) {
    return 0;
  }
  return static_cast<uint64_t>(in.tellg());
}

}  // namespace mscr

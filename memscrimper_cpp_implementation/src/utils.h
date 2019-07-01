// Copyright [2019] <Daniel Weber>

#ifndef UTILS_H_
#define UTILS_H_

#include <cstdint>

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

namespace mscr {

std::vector<char> int_to_byte_BE(uint32_t number, int bytelen);
std::string read_string(std::istream *file);
uint64_t read_num_LE(std::istream *file, int length);
uint64_t read_num_LE(const char *array_begin, int length);
bool str_starts_with(const std::string &str, std::string prefix);
uint64_t get_filesize(const char *filename);

template<class T = uint32_t>
std::vector<char> int_to_byte_LE(T number, int bytelen) {
  std::vector<char> result(bytelen);
  for (int i = 0; i < bytelen; ++i) {
      result[i] = number >> (i * 8);
  }
  return result;
}

}  // namespace mscr

#endif  // UTILS_H

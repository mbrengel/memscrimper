// Copyright [2019] <Daniel Weber>

#ifndef INTERDEDUP_DECOMPRESS_H_
#define INTERDEDUP_DECOMPRESS_H_

#include "./request_handler.h"

namespace mscr {

void interdedup_decompress(request_handler *handler, const char *filename_in,
    const char *filename_out);

}  // namespace mscr

#endif  // INTERDEDUP_DECOMPRESS_H_

// Copyright {2019] <Daniel Weber>

#ifndef INTERDEDUP_COMPRESS_H_
#define INTERDEDUP_COMPRESS_H_

#include <memory>

#include "memdump.h"

namespace mscr {

enum class compression {ZIP7, GZIP, BZIP2, NOINNER};

void interdedup_compress(std::shared_ptr<memdump> ref, const memdump &srcdump,
                         const char *filename, compression inner, bool diffing,
                         bool intra);

}  // namespace mscr

#endif  // INTERDEDUP_COMPRESS_H_

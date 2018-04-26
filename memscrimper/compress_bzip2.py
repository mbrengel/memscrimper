#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import util
import sys
import os
import logging
import struct

# bz2 does not allow fileobjects in BZ2File
import bz2file


def compress(source, target, pagesize=4096):
    logging.debug("Starting compression of %s to %s", repr(source), repr(target))
    logging.debug("Page size: %d",  pagesize)
    size = os.path.getsize(source)
    with open(target, "wb") as ftarget:
        ftarget.write(util.create_header("bzip2", size))
        with bz2file.BZ2File(filename=ftarget, mode="wb", compresslevel=9) as ftarget:
            for i, page in enumerate(util.get_pages(source, pagesize=pagesize)):
                if i % 100 == 0 or (i+1) * pagesize == size:
                    sys.stdout.write("\rProgress: {:.2f}%".format(float(i * pagesize) / size * 100))
                    sys.stdout.flush()
                ftarget.write(page)
    sys.stdout.write("\n")
    logging.debug("Done")


def decompress(source, target):
    logging.debug("Starting decompression of %s to %s", repr(source), repr(target))
    with open(source, "rb") as fsource:
        logging.debug("Parsing header")
        magic, method, majorversion, minorversion, pagesize, uncompressed_size = util.parse_header(fsource)
        logging.debug("    Magic number: %s", repr(magic))
        logging.debug("    Method: %s", repr(method))
        logging.debug("    Major version number: %d", majorversion)
        logging.debug("    Minor version number: %d", minorversion)
        logging.debug("    Page size: %d", pagesize)
        logging.debug("    Uncompressed size: %d", uncompressed_size)
        with open(target, "wb") as ftarget:
            curr_size = 0.0
            pagecnt = 0
            with bz2file.BZ2File(filename=fsource, mode="rb", compresslevel=9) as fsource:
                while True:
                    if pagecnt % 100 == 0 or curr_size == uncompressed_size:
                        sys.stdout.write("\rProgress: {:.2f}%".format(curr_size / uncompressed_size * 100))
                        sys.stdout.flush()
                    page = fsource.read(pagesize)
                    if not page:
                        break
                    ftarget.write(page)
                    curr_size += len(page)
                    pagecnt += 1
            sys.stdout.write("\n")
    logging.debug("Done")


def main(argv):
    # set up logging
    util.create_dir("logs")
    util.configure_logging("bzip2", "logs/bzip2.log")

    # check args
    if len(argv) != 4:
        print "Usage: {} <c/d> <source> <target>".format(argv[0])
        return -1

    # check if first argument is valid
    if argv[1] != "c" and argv[1] != "d":
        logging.error("First argument %s should be 'c' or 'd'", repr(argv[1]))
        return -1

    # check if files do (not) exist
    source = argv[2]
    target = argv[3]
    if not os.path.isfile(source):
        logging.error("Source %s does not exist", repr(source))
        return -1
    if os.path.isfile(target) and os.path.getsize(target) > 0:
        logging.error("Target %s already exists and is non-empty", repr(target))
        return -1

    # compress/decompress
    if argv[1] == "c":
        compress(source, target)
    else:
        decompress(source, target)

    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))

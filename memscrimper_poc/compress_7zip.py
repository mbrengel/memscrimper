#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

import util
import sys
import os
import logging
import gzip
import struct
import subprocess


def compress(source, target, pagesize=4096):
    logging.debug("Starting compression of %s to %s", repr(source), repr(target))
    logging.debug("Page size: %d",  pagesize)
    size = os.path.getsize(source)
    with open(target, "wb") as ftarget:
        ftarget.write(util.create_header("7zip", size))
        ftarget.flush()
        p = subprocess.Popen(["7za", "a", "-an", "-txz", "-mx=9", "-so", source], stdout=ftarget, stderr=subprocess.PIPE)
        p.communicate()
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
        fsource.flush()
        with open(target, "wb") as ftarget:
            p = subprocess.Popen(["7za", "x", "-an", "-txz", "-si", "-so"], stdin=fsource, stdout=ftarget, stderr=subprocess.PIPE)
            p.communicate()
    logging.debug("Done")


def main(argv):
    # set up logging
    util.create_dir("logs")
    util.configure_logging("7zip", "logs/7zip.log")

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

#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from collections import defaultdict as dd

import argparse
import gzip
import util
import sys
import os
import logging
import shutil
import struct
import subprocess
import tempfile

import bz2file


def compress(source, target, inner, pagesize=4096):
    # some info
    logging.debug("Starting compression of %s to %s", repr(source), repr(target))
    logging.debug("Page size: %d",  pagesize)

    # pages + page numbers bookkeeping
    pagenrs = dd(list)
    pages = []
    for i, page in enumerate(util.get_pages(source, pagesize=pagesize)):
        pagenrs[page].append(i)
        pages.append(page)
    pages_set = set(pages)

    # remove pages which just occurr once from dictionary and intervalize values
    for page in pagenrs.keys():
        if len(pagenrs[page]) == 1:
            del pagenrs[page]
        else:
            pagenrs[page] = util.intervalize(pagenrs[page])

    # write file
    util.create_dir(".tmp")
    tmphandle, tmpfile = tempfile.mkstemp(dir=".tmp")
    try:
        with open(tmpfile, "wb") as ftmp:
            ftmp.write(struct.pack("<I", len(pagenrs)))
            inorder = []
            seen = set()
            for page in pages:
                if page in pagenrs and page not in seen:
                    inorder.append(page)
                    seen.add(page)
            for page in inorder:
                ftmp.write(page)
            for page in inorder:
                ftmp.write(util.create_interval_list(pagenrs[page]))
            for page in pages:
                if page not in pagenrs:
                    ftmp.write(page)
        with open(tmpfile, "rb") as ftmp, open(target, "wb") as ftarget:
            ftarget.write(util.create_header("intradedup{}".format("" if inner is None else inner), os.path.getsize(source)))
            ftarget.flush()
            if inner is None:
                shutil.copyfileobj(ftmp, ftarget)
            elif inner == "gzip":
                with gzip.GzipFile(fileobj=ftarget, mode="wb", compresslevel=9) as ftarget:
                    shutil.copyfileobj(ftmp, ftarget)
            elif inner == "bzip2":
                with bz2file.BZ2File(filename=ftarget, mode="wb", compresslevel=9) as ftarget:
                    shutil.copyfileobj(ftmp, ftarget)
            elif inner == "7zip":
                p = subprocess.Popen(["7za", "a", "-an", "-txz", "-mx=9", "-si", "-so", source], stdin=ftmp, stdout=ftarget, stderr=subprocess.PIPE)
                p.communicate()
    finally:
        os.close(tmphandle)
        os.remove(tmpfile)

    # some info
    total = sum(b - a + 1 for l in pagenrs.values() for a, b in l)
    logging.debug("Deduplicated pages: %d/%d (%d/%d)", total, len(pages), len(pagenrs), len(pages_set))
    logging.debug("Done")

    return 0


def decompress(source, target):
    # some info
    logging.debug("Starting decompression of %s to %s", repr(source), repr(target))

    with open(source, "rb") as fsource:
        # some info
        logging.debug("Parsing header")
        magic, method, majorversion, minorversion, pagesize, uncompressed_size = util.parse_header(fsource)
        logging.debug("    Magic number: %s", repr(magic))
        logging.debug("    Method: %s", repr(method))
        logging.debug("    Major version number: %d", majorversion)
        logging.debug("    Minor version number: %d", minorversion)
        logging.debug("    Page size: %d", pagesize)
        logging.debug("    Uncompressed size: %d", uncompressed_size)
        inner = method.split("intradedup")[1]
        if not method.startswith("intradedup") or inner not in ("", "gzip", "bzip2", "7zip"):
            logging.error("Invalid method %s", repr(method))
            return -1

        fsource.flush()
        tmphandle, tmpfile = None, None
        if inner == "gzip":
            fsource = gzip.GzipFile(fileobj=fsource, mode="rb", compresslevel=9)
        elif inner == "bzip2":
            fsource = bz2file.BZ2File(filename=fsource, mode="rb", compresslevel=9)
        elif inner == "7zip":
            util.create_dir(".tmp")
            tmphandle, tmpfile = tempfile.mkstemp(dir=".tmp")
            with open(tmpfile, "wb") as ftmp:
                p = subprocess.Popen(["7za", "x", "-txz", "-si", "-so"], stdin=fsource, stdout=ftmp, stderr=subprocess.PIPE)
                p.communicate()
            fsource = open(tmpfile, "rb")
        try:
            # parse dictionary
            distinct = util.parse_int(fsource, 4)
            fills = {}
            pagelist = []
            for _ in xrange(distinct):
                page = fsource.read(pagesize)
                pagelist.append(page)
            for i in xrange(distinct):
                for left, right in util.parse_interval_list(fsource):
                    for pagenr in xrange(left, right + 1):
                        fills[pagenr] = pagelist[i]

            # reconstruct file
            pagenr = 0
            seen = set()
            with open(target, "wb") as ftarget:
                while True:
                    if pagenr in fills:
                        ftarget.write(fills[pagenr])
                        seen.add(fills[pagenr])
                    else:
                        page = fsource.read(pagesize)
                        seen.add(page)
                        if not page:
                            pagenr += 1
                            break
                        ftarget.write(page)
                    pagenr += 1
                while pagenr in fills:
                    ftarget.write(fills[pagenr])
                    seen.add(fills[pagenr])
                    pagenr += 1
            logging.debug("Deduplicated pages: %d/%d (%d/%d)", len(fills), uncompressed_size / pagesize, distinct, len(seen))
        finally:
            if tmphandle is not None:
                os.close(tmphandle)
                os.remove(tmpfile)
            if inner is not None:
                fsource.close()
    logging.debug("Done")

    return 0


def main():
    # cli args checking
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action")
    parser_c = subparsers.add_parser("c")
    parser_c.add_argument("source", type=str)
    parser_c.add_argument("target", type=str)
    parser_c.add_argument("--inner", type=str)
    parser_d = subparsers.add_parser("d")
    parser_d.add_argument("source", type=str)
    parser_d.add_argument("target", type=str)
    args = parser.parse_args()

    # set up logging
    util.create_dir("logs")
    if args.action == "c":
        method = "intradedup" + ("" if args.inner is None else args.inner)
    elif args.action == "d":
        with open(args.source, "rb") as f:
            method = util.parse_header(f)[1]
    util.configure_logging(method, "logs/{}.log".format(method))

    # check if files do (not) exist
    if not os.path.isfile(args.source):
        logging.error("Source %s does not exist", repr(source))
        return -1
    if os.path.isfile(args.target) and os.path.getsize(args.target) > 0:
        logging.error("Target %s already exists and is non-empty", repr(args.target))
        return -1

    # compress/decompress
    if args.action == "c":
        return compress(args.source, args.target, args.inner)
    elif args.action == "d":
        return decompress(args.source, args.target)

if __name__ == "__main__":
    sys.exit(main())

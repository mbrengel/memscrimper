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


def compress(source, target, reference, nointra, delta, inner, pagesize=4096):
    # some info
    logging.debug("Starting compression of %s to %s", repr(source), repr(target))
    logging.debug("Page size: %d",  pagesize)
    logging.debug("Reference dump: %s", reference)

    # pages + page numbers bookkeeping
    reference_pages, reference_pagenrs = [], {}
    for i, page in enumerate(util.get_pages(reference)):
        reference_pages.append(page)
        if page not in reference_pagenrs:
            reference_pagenrs[page] = i
    reference_pages_set = set(reference_pages)

    # find new + duplicatable pages
    dedups = dd(list)
    diffs = dd()
    diff_seen = set()
    if nointra:
        new_pagenrs = []
    else:
        new_pagenrs = dd(list)
    new_pages = []
    same_distinct, same_total = set(), 0
    source_pages = []
    for i, page in enumerate(util.get_pages(source)):
        source_pages.append(page)
        if reference_pages[i] != page:
            if page not in reference_pages_set:
                if delta is not None:
                    d = util.create_diff(reference_pages[i], page)
                    if d is not None:
                        diff_seen.add(page)
                        diffs[i] = d
                        continue
                if nointra:
                    new_pagenrs.append(i)
                else:
                    new_pagenrs[page].append(i)
                new_pages.append(page)
            else:
                dedups[page].append(i)
        else:
            same_total += 1
            same_distinct.add(page)
    source_pages_set = set(source_pages)
    newpagescnt = len(new_pages), len(set(new_pages))

    # intervalize
    if nointra:
        new_pagenrs = util.intervalize(new_pagenrs)
    else:
        new_pagenrs = {page: util.intervalize(new_pagenrs[page]) for page in new_pagenrs}
    dedups = {page: util.intervalize(dedups[page]) for page in dedups}

    # write file
    util.create_dir(".tmp")
    tmphandle, tmpfile = tempfile.mkstemp(dir=".tmp")
    try:
        with open(tmpfile, "wb") as ftmp:
            ftmp.write(reference + "\x00")
            inorder = []
            seen = set()
            for page in reference_pages:
                if page in dedups and page not in seen:
                    inorder.append(page)
                    seen.add(page)
            util.create_pagenr_list([reference_pagenrs[page] for page in inorder], ftmp)
            for page in inorder:
                ftmp.write(util.create_interval_list(dedups[page]))
            if delta is not None:
                util.create_pagenr_list(sorted(diffs), ftmp)
                for pagenr in sorted(diffs):
                    ftmp.write(diffs[pagenr])
            if nointra:
                ftmp.write(util.create_interval_list(new_pagenrs))
                for page in new_pages:
                    ftmp.write(page)
            else:
                ftmp.write(struct.pack("<I", len(new_pagenrs)))
                for page in new_pagenrs:
                    ftmp.write(util.create_interval_list(new_pagenrs[page]))
                for page in new_pagenrs:
                    ftmp.write(page)
        with open(tmpfile, "rb") as ftmp, open(target, "wb") as ftarget:
            ftarget.write(util.create_header(create_method_name(nointra, delta, inner), os.path.getsize(source)))
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
    dedup_distinct = len(set(dedups.keys()) | same_distinct)
    dedup_total = same_total + sum(b - a + 1 for l in dedups.values() for a, b in l)
    logging.debug("Deduplicated pages at the same offset: %d/%d (%d/%d)", same_total, len(source_pages), len(same_distinct), len(source_pages_set))
    logging.debug("Deduplicated pages at different offsets: %d/%d (%d/%d)", dedup_total - same_total, len(source_pages), len(dedups), len(source_pages_set))
    logging.debug("Deduplicated pages in total: %d/%d (%d/%d)", dedup_total, len(source_pages), dedup_distinct, len(source_pages_set))
    if delta is not None:
        logging.debug("Diffed pages: %d/%d (%d/%d)", len(diffs), len(source_pages), len(diff_seen), len(source_pages_set))
    logging.debug("New pages: %d/%d (%d/%d)", newpagescnt[0], len(source_pages), newpagescnt[1], len(source_pages_set))
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
        nointra, delta, inner = parse_method_name(method)
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
            reference = util.parse_string(fsource)
            logging.debug("Reference dump: %s", reference)

            # parse deduplicated pages
            fills = {}
            reference_list = list(util.parse_pagenr_list(fsource))
            for i in xrange(len(reference_list)):
                for left, right in util.parse_interval_list(fsource):
                    for pagenr in xrange(left, right+1):
                        fills[pagenr] = reference_list[i]

            # parse diffs
            if delta is not None:
                diffs = {}
                pagenrs = list(util.parse_pagenr_list(fsource))
                for i in xrange(len(pagenrs)):
                    diffs[pagenrs[i]] = util.parse_diff(fsource, pagesize)

            # parse new pages
            newpages = {}
            newdistinct = set()
            if nointra:
                for left, right in list(util.parse_interval_list(fsource)):
                    for j in xrange(left, right + 1):
                        page = fsource.read(pagesize)
                        newdistinct.add(page)
                        newpages[j] = page
            else:
                newcnt = util.parse_int(fsource, 4)
                intervals = []
                for _ in xrange(newcnt):
                    intervals.append(list(util.parse_interval_list(fsource)))
                for i in xrange(newcnt):
                    page = fsource.read(pagesize)
                    for left, right in intervals[i]:
                        for j in xrange(left, right + 1):
                            newdistinct.add(page)
                            newpages[j] = page
        finally:
            if tmphandle is not None:
                os.close(tmphandle)
                os.remove(tmpfile)
            if inner is not None:
                fsource.close()

        # reconstruct file
        pagenr = 0
        final = uncompressed_size / pagesize
        same_distinct, same_total = set(), 0
        different_distinct, different_total = set(),  0
        seen = set()
        diff_seen = set()
        with open(reference, "rb") as freference:
            with open(target, "wb") as ftarget:
                while pagenr < final:
                    if pagenr in fills:
                        freference.seek(pagesize * fills[pagenr])
                        page = freference.read(pagesize)
                        seen.add(page)
                        different_distinct.add(page)
                        different_total += 1
                        ftarget.write(page)
                    elif delta is not None and pagenr in diffs:
                        freference.seek(pagenr * pagesize)
                        page = freference.read(pagesize)
                        newpage = util.apply_diff(page, diffs[pagenr])
                        diff_seen.add(newpage)
                        ftarget.write(newpage)
                    elif pagenr in newpages:
                        seen.add(newpages[pagenr])
                        ftarget.write(newpages[pagenr])
                    else:
                        freference.seek(pagesize * pagenr)
                        page = freference.read(pagesize)
                        seen.add(page)
                        same_distinct.add(page)
                        same_total += 1
                        ftarget.write(page)
                    pagenr += 1

    # some info
    logging.debug("New pages: %d/%d (%d/%d)", len(newpages), final, len(newdistinct), len(seen))
    logging.debug("Deduplicated pages at the same offset: %d/%d (%d/%d)", same_total, final, len(same_distinct), len(seen))
    logging.debug("Deduplicated pages at different offsets: %d/%d (%d/%d)", len(fills), final, len(different_distinct), len(seen))
    logging.debug("Deduplicated pages in total: %d/%d (%d/%d)", same_total + len(fills), final, len(same_distinct | different_distinct), len(seen))
    if delta is not None:
        logging.debug("Diffed pages: %d/%d (%d/%d)", len(diffs), final, len(diff_seen), len(seen))
    logging.debug("Done")

    return 0


def create_method_name(nointra, delta, inner):
    nointra = "nointra" if nointra else ""
    delta = delta+"delta" if delta is not None else ""
    inner = inner if inner is not None else ""

    return "interdedup{}{}{}".format(nointra, delta, inner)


def parse_method_name(s):
    assert s.startswith("interdedup")
    s = s[len("interdedup"):]
    delta = None
    nointra = False
    if s.startswith("nointra"):
        nointra = True
        s = s[len("nointra"):]
    delta = None
    if "delta" in s:
        delta = s[:s.index("delta")]
        s = s[s.index("delta") + len(delta)-1:]
    inner = s if len(s) > 1 else None

    return nointra, delta, inner

def main():
    # cli args checking
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="action")
    parser_c = subparsers.add_parser("c")
    parser_c.add_argument("source", type=str)
    parser_c.add_argument("target", type=str)
    parser_c.add_argument("reference", type=str)
    parser_c.add_argument("--inner", type=str)
    parser_c.add_argument("--delta", type=str)
    parser_c.add_argument("--nointra", action="store_true")
    parser_d = subparsers.add_parser("d")
    parser_d.add_argument("source", type=str)
    parser_d.add_argument("target", type=str)
    args = parser.parse_args()

    # create method name
    if args.action == "c":
        method = create_method_name(args.nointra, args.delta, args.inner)
    elif args.action == "d":
        with open(args.source, "rb")  as f:
            method = util.parse_header(f)[1]

    # set up logging
    util.create_dir("logs")
    util.configure_logging(method, "logs/{}.log".format(method))

    # check if files do (not) exist
    if not os.path.isfile(args.source):
        logging.error("Source %s does not exist", repr(args.source))
        return -1
    if os.path.isfile(args.target) and os.path.getsize(args.target) > 0:
        logging.error("Target %s already exists and is non-empty", repr(args.target))
        return -1
    if args.action == "c" and not os.path.isfile(args.reference):
        logging.error("Reference %s does not exist", repr(args.reference))
        return -1

    # compress/decompress
    if args.action == "c":
        return compress(args.source, args.target, args.reference, args.nointra, args.delta, args.inner)
    elif args.action == "d":
        return decompress(args.source, args.target)

if __name__ == "__main__":
    sys.exit(main())

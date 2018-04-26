#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

from logging.handlers import RotatingFileHandler

import cStringIO
import errno
import logging
import os
import struct
import sys

import colorama


def create_dir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def create_header(method, uncompressed_size, majorversion=1, minorversion=1, magic_number="MBCR", pagesize=4096):
    ret = ""
    ret += magic_number + "\x00"
    ret += method + "\x00"
    ret += struct.pack("<H", majorversion)
    ret += struct.pack("<H", minorversion)
    ret += struct.pack("<I", pagesize)
    ret += struct.pack("<Q", uncompressed_size)
    return ret


def parse_header(f):
    magic = parse_string(f)
    method = parse_string(f)
    majorversion = parse_int(f, 2)
    minorversion = parse_int(f, 2)
    pagesize = parse_int(f, 4)
    uncompressed_size = parse_int(f, 8)

    return magic, method, majorversion, minorversion, pagesize, uncompressed_size


def binlen(n):
    if n == 0:
        return 1
    ret = 0
    while n > 0:
        ret += 1
        n >>= 1
    return ret


def diff(page1, page2):
    pagesize = len(page1)
    ret = []
    intermediate = bytearray()
    previ = None
    for i in xrange(pagesize):
        first, second = page1[i], page2[i]
        if first == second:
            intermediate.append(second)
        elif first != second:
            if len(intermediate) <= 2 and previ is not None:
                curr[1] += intermediate
                curr[1].append(second)
            else:
                if previ is None:
                    curr = [i, bytearray()]
                else:
                    curr = [i - previ - len(ret[-1][1]), bytearray()]
                previ = i
                ret.append(curr)
                curr[1].append(second)
            intermediate = bytearray()
    fixed = []
    for i, (a, b) in enumerate(ret):
        if len(b) > 2048:
            overhead = len(b) - 2048
            la, lb = a, b[:overhead]
            ra, rb = 0, b[overhead:]
            fixed += [(la, lb), (ra, rb)]
        else:
            fixed.append((a, b))
    return fixed


def apply_diff(page, d):
    ret = bytearray(page)
    offset = 0
    for (rel, bs) in d:
        offset += rel
        for i in xrange(len(bs)):
            ret[offset+i] = bs[i]
        offset += len(bs)
    return str(ret)


def create_diff(page1, page2):
    def encode(rel, sz):
        ret = ""
        sz = sz - 1
        if rel < 128 and sz < 128:
            ret += struct.pack("BB", sz, rel)
        else:
            blop = (sz << 12) | rel
            a = (blop & 0xFF0000) >> 16
            a |= 128
            b = (blop & 0xFF00) >> 8
            c = blop & 0xFF
            ret = struct.pack("BBB", a, b, c)
        return ret
    pagesize = len(page1)
    ret = ""
    num = 0
    for rel, bs in diff(page1, page2):
        num += 1
        ret += encode(rel, len(bs))
        ret += str(bs)
        if len(ret) + 2 >= pagesize:
            return None
    return struct.pack("<H", num) + ret


def parse_diff(f, pagesize):
    def decode(f):
        a, b = struct.unpack("BB", f.read(2))
        if a & 128 == 128:
            a &= 127
            c = struct.unpack("B", f.read(1))[0]
            blop = (a << 16) | (b << 8) | c
            return (blop & 0xFFF), (blop & 0xFFF000) >> 12
        else:
            return b, a
    ret = []
    for _ in xrange(parse_int(f, 2)):
        rel, sz = decode(f)
        ret.append((rel, f.read(sz+1)))
    return ret


def create_pagenr_list(pagenrs, f):
    f.write(struct.pack("<I", len(pagenrs)))
    prev = None
    for pagenr in pagenrs:
        if prev is None:
            curr = pagenr
        else:
            curr = pagenr - prev - 1
        if curr < 128:
            f.write(struct.pack(">B", curr | 128))
        else:
            f.write(struct.pack(">I", curr))
        prev = pagenr


def parse_pagenr_list(f):
    n = parse_int(f, 4)
    prev = None
    for _ in xrange(n):
        a = parse_int(f, 1)
        if a & 128 == 128:
            a &= 127
        else:
            b = parse_int(f, 1)
            c = parse_int(f, 1)
            d = parse_int(f, 1)
            a = (a << 24) | (b << 16) | (c << 8) | d
        if prev is None:
            yield a
            prev = a
        else:
            yield prev + a + 1
            prev = prev + a + 1


def create_interval(left, right, last=False):
    assert left < 1 << 29
    if last:
        last = 4 # 0b100
    else:
        last = 0 # 0b000
    if left == right:
        return struct.pack("<I", (last << 29) | left)
    delta = right - left
    if delta < 1 << 8:
        fmt = "B"
        data = left | ((last | 1) << 29)
    elif delta < 1 << 16:
        fmt = "H"
        data = left | ((last | 2) << 29)
    else:
        fmt = "I"
        data = left | ((last | 3) << 29)
    return struct.pack("<I{}".format(fmt), data, delta)


def create_interval_list(intervals):
    ret = ""
    l = len(intervals)
    for i, (left, right) in enumerate(intervals):
        ret += create_interval(left, right, last=i+1==l)
    return ret


def parse_interval(f):
    left = parse_int(f, 4)
    upper = (left & (7 << 29)) >> 29
    sz = upper & 3
    last = (upper >> 2) == 1
    if sz == 3:
        sz = 4
    left &= (1 << 29) - 1
    if sz == 0:
        delta = 0
    elif sz in [1, 2, 4]:
        delta = parse_int(f, sz)
    else:
        logging.error("Parsing error: interval size %d is not in [1, 2, 4]", sz)
        sys.exit(-1)
    return last, left, left + delta


def parse_interval_list(f):
    last = False
    while not last:
        last, left, right = parse_interval(f)
        yield left, right


# [1, 2, 3, 5, 6, 7, 8, 9, 13] -> [[1, 3], [5, 9], [13, 13]]
def intervalize(l):
    if len(l) == 0:
        return []
    ret = []
    curr = [l[0], l[0]]
    for x in l[1:]:
        if curr[1] + 1 == x:
            curr[1] = x
        else:
            ret.append(curr)
            curr = [x, x]
    ret.append(curr)
    return ret


def parse_int(f, sz):
    szmap = { 1: "B", 2:"H", 4:"I", 8: "Q" }
    return struct.unpack("<{}".format(szmap[sz]), f.read(sz))[0]


def parse_string(f):
    ret = ""
    while True:
        c = f.read(1)
        if c == "\x00":
            return ret
        ret += c


def configure_logging(identifier, logfile):
    # enable cross-platform colored output
    colorama.init()

    # get the root logger and make it verbose
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # this allows us to set an upper threshold for the log levels since the
    # setLevel method only sets a lower one
    class UpperThresholdFilter(logging.Filter):
        def __init__(self, threshold, *args, **kwargs):
            self._threshold = threshold
            super(UpperThresholdFilter, self).__init__(*args, **kwargs)

        def filter(self, rec):
            return rec.levelno <= self._threshold

    # use colored output and use different colors for different levels
    class ColorFormatter(logging.Formatter):
        def __init__(self, colorfmt, *args, **kwargs):
            self._colorfmt = colorfmt
            super(ColorFormatter, self).__init__(*args, **kwargs)

        def format(self, record):
            if record.levelno == logging.INFO:
                color = colorama.Fore.GREEN
            elif record.levelno == logging.WARNING:
                color = colorama.Fore.YELLOW
            elif record.levelno == logging.ERROR:
                color = colorama.Fore.RED
            elif record.levelno == logging.DEBUG:
                color = colorama.Fore.CYAN
            else:
                color = ""
            self._fmt = self._colorfmt.format(color, colorama.Style.RESET_ALL)
            return logging.Formatter.format(self, record)

    # configure formatter
    logfmt = "{{}}[%(asctime)s|{}|%(levelname).3s]{{}} %(message)s".format(identifier)
    formatter = ColorFormatter(logfmt)

    # configure stdout handler
    stdouthandler = logging.StreamHandler(sys.stdout)
    stdouthandler.setLevel(logging.DEBUG)
    stdouthandler.addFilter(UpperThresholdFilter(logging.INFO))
    stdouthandler.setFormatter(formatter)
    logger.addHandler(stdouthandler)

    # configure stderr handler
    stderrhandler = logging.StreamHandler(sys.stderr)
    stderrhandler.setLevel(logging.WARNING)
    stderrhandler.setFormatter(formatter)
    logger.addHandler(stderrhandler)

    # configure file handler (no colored messages here)
    filehandler = RotatingFileHandler(logfile, maxBytes=1024 * 1024 * 100, backupCount=5)
    filehandler.setLevel(logging.DEBUG)
    filehandler.setFormatter(logging.Formatter(logfmt.format("", "")))
    logger.addHandler(filehandler)


def get_pages(path, pagesize=4096):
    with open(path, "rb") as f:
        while True:
            page = f.read(pagesize)
            if not page:
                break
            yield page

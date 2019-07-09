#! /usr/bin/env python3
# Copyright [2019] <Daniel Weber>

import socket
import struct
import enum
import math
import time
import collections


class Compression(enum.Enum):
    ZIP7 = 0
    GZIP = 1
    BZIP2 = 2
    NOINNER = 3


class MscrClient:
    _sock_path = ""  # holds socket path
    _cl_sock = ""  # holds client socket
    _await_answer = True  # bool to check if we should wait for answer from service
    _ack_buffer = collections.deque(maxlen=256)  # used as a ring buffer
    _messageid = 0
    _sending_attempts = 0
    _verbose = False

    def __init__(self, sock_path, await_answer, sending_attempts=3, verbose=False):
        """
        :param sock_path: string
                    path to memscrimper socketfile
        :param await_answer: bool
                    Block while waiting for command acknowledge
        :param sending_attempts: how often the client tries to resend a command on failure
        :param verbose: enable debug prints
        """
        self._sock_path = sock_path
        self._cl_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._cl_sock.connect(sock_path)
        self._cl_sock.settimeout(15)  # set timeout for all operations
        self._await_answer = await_answer
        self._sending_attempts = sending_attempts
        self._verbose = verbose

    def __del__(self):
        self._cl_sock.close()

    # Message-format: LEN(1B) | MSGID(1B) | OPCODE(1B) | OPTIONS(XB)
    # LEN will be denoted as x8. and we use nullbytes as padding after the last option
    # the LEN-byte itself will not be part of the amount in len

    def add_referencedump(self, reference_path, pagesize):
        """
        OPCODE: 0x00
        :param reference_path: string
                    path to reference dump
        :param pagesize: int
                    pagesize of reference dump
        :return: True : success - False: failure (only if used in blocking mode)
        """

        msg_len_nopad = 6  # +1 for msgid / +1 for opcode / +4 for ps
        msg_len_nopad += len(reference_path) + 1  # +1 for nullbyte
        pad_len = self._calc_padding(msg_len_nopad)
        msg = struct.pack("B", math.floor((msg_len_nopad + pad_len) / 8))
        self._debug_print("LEN-byte: %d" % math.floor((msg_len_nopad + pad_len) / 8))

        msgid = self._get_message_id()
        msg += struct.pack("B", (msgid))  # MSGID

        msg += b'\x00'  # OPCODE

        msg += str.encode(reference_path)
        msg += b'\0'

        msg += struct.pack("<I", pagesize)
        msg += b'\0' * pad_len  # padding

        self._debug_print("msglen: %d" % (msg_len_nopad + pad_len))
        self._debug_print("referencepath: %s" % reference_path)

        assert ((len(msg) - 1) % 8 == 0)
        return self._send_message(msg, msgid)

    def del_referencedump(self, reference_path):
        """
        OPCODE 0x04
        :param reference_path: string
                    path to reference dump
        :return: True: success - False: failure (only if used in blocking mode)
        """
        msg_len_nopad = 2  # OPCODE + MSG_ID
        msg_len_nopad += len(reference_path) + 1
        pad_len = self._calc_padding(msg_len_nopad)
        msg = struct.pack("B", math.floor((msg_len_nopad + pad_len) / 8))

        msgid = self._get_message_id()
        msg += struct.pack("B", msgid)  # MSGID

        msg += b'\x04'  # OPCODE

        msg += str.encode(reference_path)
        msg += b'\0'

        msg += b'\0' * pad_len  # padding

        assert ((len(msg) - 1) % 8 == 0)
        return self._send_message(msg, msgid)

    def compress_dump(self, reference_path, dump_path, out_path, pagesize, intra,
                      diffing, inner):
        """
        OPCODE: 0x01
        :param reference_path: string
                    path to reference dump
        :param dump_path: string
                    path to dumpffile, which will be compressed
        :param out_path: string
                    filename for compressed file
        :param intra: bool
                    usage of intradeduplication
        :param diffing: bool
                    usage of diffing
        :param inner: Compression
                    method used for inner compression
        :return: True: success - False: failure (only if used in blocking mode)
        """
        msg_len_nopad = 5  # opcode / msg_id / intra / diffing / inner
        msg_len_nopad += len(reference_path) + 1
        msg_len_nopad += len(dump_path) + 1
        msg_len_nopad += len(out_path) + 1
        msg_len_nopad += 4  # pagesize
        pad_len = self._calc_padding(msg_len_nopad)
        msg = struct.pack("B", math.floor((msg_len_nopad + pad_len) / 8))

        msgid = self._get_message_id()
        msg += struct.pack("B", msgid)  # MSGID

        msg += b'\x01'  # OPCODE

        msg += str.encode(reference_path)
        msg += b'\0'

        msg += str.encode(dump_path)
        msg += b'\0'

        msg += str.encode(out_path)
        msg += b'\0'

        msg += struct.pack("<I", pagesize)

        if intra:
            msg += b'\x01'
        else:
            msg += b'\x00'

        if diffing:
            msg += b'\x01'
        else:
            msg += b'\x00'

        if inner == Compression.ZIP7:
            msg += b'\x00'
        elif inner == Compression.GZIP:
            msg += b'\x01'
        elif inner == Compression.BZIP2:
            msg += b'\x02'
        else:
            assert (inner == Compression.NOINNER)
            msg += b'\x03'

        msg += b'\0' * pad_len  # padding
        assert ((len(msg) - 1) % 8 == 0)
        return self._send_message(msg, msgid)

    def decompress_dump(self, dump_path, out_path):
        """
        OPCODE: 0x02
        :param dump_path: string
                    path of compressed dumpfile
        :param out_path: string
                    filename to uncompressed file
        :return: True: success - False: failure (only if used in blocking mode)
        """
        msg_len_nopad = 2  # OPCODE / msg_id
        msg_len_nopad += len(dump_path) + 1
        msg_len_nopad += len(out_path) + 1

        pad_len = self._calc_padding(msg_len_nopad)
        msg = struct.pack("B", math.floor((msg_len_nopad + pad_len) / 8))

        msgid = self._get_message_id()
        msg += struct.pack("B", msgid)  # MSGID

        msg += b'\x02'  # OPCODE

        msg += str.encode(dump_path)
        msg += b'\0'

        msg += str.encode(out_path)
        msg += b'\0'

        msg += b'\0' * pad_len

        assert ((len(msg) - 1) % 8 == 0)
        return self._send_message(msg, msgid)

    def _send_message(self, message, message_id):
        """
        :param message: content of the message to be send
        :param message_id: ID of the message to be send
        :return: returns True if message was sent successfully
        """
        attempts = 0
        success = False
        while attempts < self._sending_attempts:
            self._cl_sock.sendall(message)
            attempts += 1
            if self._await_answer is True:
                success = self._check_ack(message_id)
                if not success:
                    self._debug_print("command was not transmitted correctly (send attempt %d/%d)" %
                                      (attempts, self._sending_attempts))
                    # wait until we try again
                    time.sleep(30)
            else:
                # we do not care if the message was sent successfully
                success = True
            if success is True:
                break
        if success is True:
            self._debug_print("message sent (send attempt %d/%d)"
                              % (attempts, self._sending_attempts))
            return True
        else:
            self._debug_print("failed to sent message after %d attempts" % attempts)
            return False

    def _calc_padding(self, msg_len):
        return 8 - (msg_len % 8)

    def _get_message_id(self):
        # increase msg-id + make sure that we fit in 1B
        curr_id = self._messageid
        self._messageid = (self._messageid + 1) % 256
        return curr_id

    def _find_ack_for_id(self, message_id):
        for ack in self._ack_buffer:
            if ack[0] == message_id:
                # found matching ACK message
                return ack
        return None

    def _check_ack(self, message_id):
        retries = 5
        i = 0
        while i < retries:
            # try reading ACKs from the socket
            try:
                ack = self._cl_sock.recv(2)
                self._ack_buffer.append(ack)
            except socket.timeout:
                self._debug_print("recv timeout (check attempt %d/%d)" % (i + 1, retries))

            # check if have the corresponding ACK in our ACK buffer
            ack = self._find_ack_for_id(message_id)
            if ack:
                # found ACK bit so we check if it is ACK or NACK
                self._debug_print("checking ACK message for id %d" % message_id)
                if len(ack) != 2 or ack[1] != 0x01:
                    self._debug_print("msg_id: %d - ack bit: %d" % (ack[0], ack[1]))
                    return False
                else:
                    self._debug_print("sending succeeded")
                    return True
            else:
                self._debug_print("Did not found ACK (check attempt %d/%d)" % (i + 1, retries))
            i += 1
        # exceeded maximum retry count
        return False

    def _debug_print(self, msg):
        if self._verbose is True:
            print("[DBG] " + msg)

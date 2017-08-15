import io

from .abc import CrcCheckFailed, ABCRecordBatchWriter, ABCRecordBatchReader

from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode, lz4_encode_old_kafka,
    # gzip_decode, snappy_decode, lz4_decode, lz4_decode_old_kafka
)
from kafka.protocol.message import Message, MessageSet
from kafka.protocol.types import Int32, Int64


class LegacyRecordBatchWriter(ABCRecordBatchWriter):

    def __init__(self, magic, compression_type, batch_size):
        self._magic = magic
        self._compression_type = compression_type
        self._batch_size = batch_size
        self._buffer = io.BytesIO()
        self._buffer.write(Int32.encode(0))  # first 4 bytes for batch size
        self._first_message = True

    def _is_full(self, key, value):
        """return True if batch does not have free capacity for append message
        """
        if self._first_message:
            # batch must contain at least one message
            return False
        needed_bytes = MessageSet.HEADER_SIZE + Message.HEADER_SIZE
        if key is not None:
            needed_bytes += len(key)
        if value is not None:
            needed_bytes += len(value)
        return self._buffer.tell() + needed_bytes > self._batch_size

    def append(self, offset, timestamp, key, value, headers=None):
        """Append message to batch. Return True is message appended, False if
        no more room
        """
        assert not headers, "Headers not supported in v0/v1"

        if self._is_full(key, value):
            return False

        self._first_message = False
        # `.encode()` is a weak method for some reason, so we need to save
        # reference before calling it.
        if self._magic == 0:
            msg_inst = Message(value, key=key, magic=self._magic)
        else:
            msg_inst = Message(value, key=key, magic=self._magic,
                               timestamp=timestamp)

        encoded = msg_inst.encode()
        msg = Int64.encode(offset) + Int32.encode(len(encoded))
        self._buffer.write(msg)
        self._buffer.write(encoded)
        return True

    def _maybe_compress(self):
        if self._compression_type:
            data = self._buffer.getbuffer()[4:]  # Skip length Int32 bytes
            if self._compression_type == Message.CODEC_GZIP:
                compressed = gzip_encode(data.tobytes())
            elif self._compression_type == Message.CODEC_SNAPPY:
                compressed = snappy_encode(data)
            elif self._compression_type == Message.CODEC_LZ4:
                if self._magic == 0:
                    compressed = lz4_encode_old_kafka(data.tobytes())
                else:
                    compressed = lz4_encode(data.tobytes())

            msg = Message(compressed, attributes=self._compression_type,
                          magic=self._magic)
            encoded = msg.encode()
            # if compressed message is longer than original
            # we should send it as is (not compressed)
            header_size = 12   # 4(all size) + 8(offset) + 4(compressed size)
            if len(encoded) + header_size < len(data):
                # write compressed message set (with header) to buffer
                # using memory view (for avoid memory copying)
                data[:8] = Int64.encode(0)  # offset 0
                data[8:12] = Int32.encode(len(encoded))
                data[12:12 + len(encoded)] = encoded
                data.release()
                self._buffer.seek(16 + len(encoded))
                self._buffer.truncate()
                return True
        return False

    def close(self):
        """Compress batch to be ready for send"""
        self._maybe_compress()
        # update batch size (first 4 bytes of buffer)
        buffer_len = self._buffer.tell() - 4
        self._buffer.seek(0)
        self._buffer.write(Int32.encode(buffer_len))
        self._buffer.seek(0)
        return self._buffer


class LegacyRecordBatchReader(ABCRecordBatchReader):

    def __init__(self, buffer, validate_crc=True):
        self._messages = MessageSet.decode(buffer)
        if validate_crc:
            for offset, size, msg in self._messages:
                if not msg.validate_crc():
                    raise CrcCheckFailed()
        self._maybe_uncompress()

    def _maybe_uncompress(self):
        messages = self._messages
        if len(messages) != 1:
            return
        offset, size, msg = messages[0]

        if msg.is_compressed():
            # If relative offset is used, we need to decompress the entire
            # message first to compute the absolute offset.
            inner_mset = msg.decompress()
            # Only 1 compressed message can be in the outer set
            assert not inner_mset[0][2].is_compressed()
            self._messages = inner_mset

    def __iter__(self):
        for offset, size, msg in self._messages:
            if msg.is_compressed():
                # If relative offset is used, we need to decompress the entire
                # message first to compute the absolute offset.
                inner_mset = msg.decompress()
                if msg.magic > 0:
                    last_offset, _, _ = inner_mset[-1]
                    absolute_base_offset = offset - last_offset
                else:
                    absolute_base_offset = -1

                for inner_offset, inner_size, inner_msg in inner_mset:
                    if msg.magic > 0:
                        # When magic value is greater than 0, the timestamp
                        # of a compressed message depends on the
                        # typestamp type of the wrapper message:
                        if msg.timestamp_type == 0:  # CREATE_TIME (0)
                            inner_timestamp = inner_msg.timestamp
                        else:  # LOG_APPEND_TIME (1)
                            inner_timestamp = msg.timestamp
                    else:
                        inner_timestamp = 0

                    if absolute_base_offset >= 0:
                        inner_offset += absolute_base_offset

                    yield (
                        0, inner_timestamp, inner_offset, inner_msg.key,
                        inner_msg.value, None
                    )
            else:
                yield 0, msg.timestamp, offset, msg.key, msg.value, None

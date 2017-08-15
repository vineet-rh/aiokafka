# See:
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecordBatch.java
# https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/\
#    apache/kafka/common/record/DefaultRecord.java

# RecordBatch and Record implementation for magic 2 and above.
# The schema is given below:

# RecordBatch =>
#  BaseOffset => Int64
#  Length => Int32
#  PartitionLeaderEpoch => Int32
#  Magic => Int8
#  CRC => Uint32
#  Attributes => Int16
#  LastOffsetDelta => Int32 // also serves as LastSequenceDelta
#  FirstTimestamp => Int64
#  MaxTimestamp => Int64
#  ProducerId => Int64
#  ProducerEpoch => Int16
#  BaseSequence => Int32
#  Records => [Record]

# Record =>
#   Length => Varint
#   Attributes => Int8
#   TimestampDelta => Varlong
#   OffsetDelta => Varint
#   Key => Bytes
#   Value => Bytes
#   Headers => [HeaderKey HeaderValue]
#     HeaderKey => String
#     HeaderValue => Bytes

# Note that when compression is enabled (see attributes below), the compressed
# record data is serialized directly following the count of the number of
# records. (ie Records => [Record], but without length bytes)

# The CRC covers the data from the attributes to the end of the batch (i.e. all
# the bytes that follow the CRC). It is located after the magic byte, which
# means that clients must parse the magic byte before deciding how to interpret
# the bytes between the batch length and the magic byte. The partition leader
# epoch field is not included in the CRC computation to avoid the need to
# recompute the CRC when this field is assigned for every batch that is
# received by the broker. The CRC-32C (Castagnoli) polynomial is used for the
# computation.

# The current RecordBatch attributes are given below:
#
# * Unused (6-15)
# * Control (5)
# * Transactional (4)
# * Timestamp Type (3)
# * Compression Type (0-2)

import io
import struct
from ._crc32c import crc as crc32c_py
from .abc import CrcCheckFailed, ABCRecordBatchWriter, ABCRecordBatchReader

from kafka.codec import (
    gzip_encode, snappy_encode, lz4_encode,
    gzip_decode, snappy_decode, lz4_decode
)


def _encode_varint(buffer: io.BytesIO, num: int):
    # Shift sign to the end of number
    num = (num << 1) ^ (num >> 63)
    # Max 10 bytes. We assert whose are allocated
    buf = bytearray(10)

    for i in range(10):
        # 7 lowest bits from the number and set 8th if we still have pending
        # bits left to encode
        buf[i] = num & 0x7f | (0x80 if num > 0x7f else 0)
        num = num >> 7
        if num == 0:
            break
    else:
        # Max size of endcoded double is 10 bytes for unsigned values
        raise ValueError("Out of double range")
    buffer.write(buf[:i + 1])


def _decode_varint(buffer: io.BytesIO) -> int:
    value = 0
    shift = 0
    pos = buffer.tell()
    inner_buffer = buffer.getbuffer()[pos:]
    for i in range(10):
        try:
            byte = inner_buffer[i]
        except IndexError:
            raise ValueError("End of byte stream")
        if byte & 0x80 != 0:
            value |= (byte & 0x7f) << shift
            shift += 7
        else:
            value |= byte << shift
            break
    else:
        # Max size of endcoded double is 10 bytes for unsigned values
        raise ValueError("Out of double range")
    buffer.seek(pos + i + 1)
    # Normalize sign
    return (value >> 1) ^ -(value & 1)


def _calc_crc32c(memview_from):
    crc = crc32c_py(memview_from)
    return crc


class RecordBatchBase:

    HEADER_STRUCT = struct.Struct(
        ">q"  # BaseOffset => Int64
        "i"  # Length => Int32
        "i"  # PartitionLeaderEpoch => Int32
        "b"  # Magic => Int8
        "I"  # CRC => Uint32
        "h"  # Attributes => Int16
        "i"  # LastOffsetDelta => Int32 // also serves as LastSequenceDelta
        "q"  # FirstTimestamp => Int64
        "q"  # MaxTimestamp => Int64
        "q"  # ProducerId => Int64
        "h"  # ProducerEpoch => Int16
        "i"  # BaseSequence => Int32
        "i"  # Records count => Int32
    )
    # Byte offset in HEADER_STRUCT of attributes field. Used to calculate CRC
    ATTRIBUTES_OFFSET = struct.calcsize(">qiibI")
    CRC_OFFSET = struct.calcsize(">qiib")
    AFTER_LEN_OFFSET = struct.calcsize(">qi")

    CODEC_MASK = 0x07
    CODEC_NONE = 0x00
    CODEC_GZIP = 0x01
    CODEC_SNAPPY = 0x02
    CODEC_LZ4 = 0x03
    TIMESTAMP_TYPE_MASK = 0x08
    TRANSACTIONAL_MASK = 0x10
    CONTROL_MASK = 0x20

    LOG_APPEND_TIME = 1
    CREATE_TIME = 0


class RecordBatchWriter(RecordBatchBase, ABCRecordBatchWriter):

    __slots__ = ("_magic", "_compression_type", "_is_transactional",
                 "_producer_id", "_producer_epoch", "_base_sequence",
                 "_max_timestamp", "_first_timestamp", "_last_offset",
                 "_num_records", "_buffer", "_batch_size")

    def __init__(
            self, *, magic, compression_type, is_transactional,
            producer_id, producer_epoch, base_sequence, batch_size):
        assert magic >= 2
        self._magic = magic
        self._compression_type = compression_type & self.CODEC_MASK
        self._is_transactional = bool(is_transactional)
        # KIP-98 fields for EOS
        self._producer_id = producer_id
        self._producer_epoch = producer_epoch
        self._base_sequence = base_sequence

        self._first_timestamp = None
        self._max_timestamp = None
        self._last_offset = 0
        self._num_records = 0

        # We init with a padding for header. We will write it later
        self._buffer = io.BytesIO(bytes(self.HEADER_STRUCT.size))
        self._buffer.seek(self.HEADER_STRUCT.size)
        self._batch_size = batch_size

    def _get_attributes(self, include_compression_type=True):
        attrs = 0
        if include_compression_type:
            attrs |= self._compression_type
        # Timestamp Type is set by Broker
        if self._is_transactional:
            attrs |= self.TRANSACTIONAL_MASK
        # Control batches are only created by Broker
        return attrs

    def append(self, offset, timestamp, key, value, headers):
        """ Write message to messageset buffer with MsgVersion 2
        """
        if self._first_timestamp is None:
            self._first_timestamp = timestamp
            self._max_timestamp = timestamp
            timestamp_delta = 0
        else:
            timestamp_delta = timestamp - self._first_timestamp
            if self._max_timestamp < timestamp:
                self._max_timestamp = timestamp
        self._last_offset = offset
        self._num_records += 1
        offset_delta = offset  # Base offset is always 0 on Produce

        # We can't write record right away to out buffer, we need to precompute
        # the length as first value...
        message_buffer = io.BytesIO()
        message_buffer.write(b"\x00")  # Attributes
        _encode_varint(message_buffer, timestamp_delta)
        _encode_varint(message_buffer, offset_delta)
        _encode_varint(message_buffer, len(key))
        message_buffer.write(key)
        _encode_varint(message_buffer, len(value))
        message_buffer.write(value)
        _encode_varint(message_buffer, len(headers))

        for h_key, h_value in headers:
            _encode_varint(message_buffer, len(h_key))
            message_buffer.write(h_key)
            _encode_varint(message_buffer, len(h_value))
            message_buffer.write(h_value)

        message_len = message_buffer.tell()

        # Check if we can write this record
        tmp_buf = io.BytesIO()
        _encode_varint(tmp_buf, message_len)
        if self._buffer.tell() + message_len + tmp_buf.tell() > \
                self._batch_size:
            return False

        _encode_varint(self._buffer, message_len)
        self._buffer.write(message_buffer.getbuffer())
        message_buffer.close()
        return True

    def write_header(self, use_compression_type=True):
        batch_len = self._buffer.tell()
        inner_buffer = self._buffer.getbuffer()
        self.HEADER_STRUCT.pack_into(
            inner_buffer, 0,
            0,  # BaseOffset, set by broker
            batch_len - self.AFTER_LEN_OFFSET,  # Size from here to batch end
            0,  # PartitionLeaderEpoch, set by broker
            self._magic,
            0,  # CRC will be set below, as we need a filled buffer for it
            self._get_attributes(use_compression_type),
            self._last_offset,
            self._first_timestamp,
            self._max_timestamp,
            self._producer_id,
            self._producer_epoch,
            self._base_sequence,
            self._num_records
        )
        crc = _calc_crc32c(inner_buffer[self.ATTRIBUTES_OFFSET:])
        struct.pack_into(">I", inner_buffer, self.CRC_OFFSET, crc)

    def _maybe_compress(self):
        if self._compression_type != self.CODEC_NONE:
            data = self._buffer.getbuffer()[self.HEADER_STRUCT.size:]
            if self._compression_type == self.CODEC_GZIP:
                compressed = gzip_encode(data.tobytes())
            elif self._compression_type == self.CODEC_SNAPPY:
                compressed = snappy_encode(data)
            elif self._compression_type == self.CODEC_LZ4:
                compressed = lz4_encode(data.tobytes())
            compressed_size = len(compressed)
            if len(data) <= compressed_size:
                # We did not get any benefit from compression, lets send
                # uncompressed
                return False
            else:
                data[:compressed_size] = compressed
                data.release()
                self._buffer.seek(compressed_size + self.HEADER_STRUCT.size)
                self._buffer.truncate()
                return True

    def close(self):
        send_compressed = self._maybe_compress()
        self.write_header(send_compressed)
        self._buffer.seek(0)
        return self._buffer


class RecordBatchReader(RecordBatchBase, ABCRecordBatchReader):

    def __init__(self, buffer, validate_crc=True):
        self._buffer = buffer
        self._header_data = self._read_header(validate_crc)
        self._maybe_uncompress()
        self._num_records = self._header_data[12]
        self._next_record_index = 0

    @property
    def base_offset(self):
        return self._header_data[0]

    @property
    def magic(self):
        return self._header_data[3]

    @property
    def crc(self):
        return self._header_data[4]

    @property
    def attributes(self):
        return self._header_data[5]

    @property
    def compression_type(self):
        return self.attributes & self.CODEC_MASK

    @property
    def timestamp_type(self):
        return int(bool(self.attributes & self.TIMESTAMP_TYPE_MASK))

    @property
    def is_transactional(self):
        return bool(self.attributes & self.TRANSACTIONAL_MASK)

    @property
    def is_control_batch(self):
        return bool(self.attributes & self.CONTROL_MASK)

    @property
    def first_timestamp(self):
        return self._header_data[7]

    @property
    def max_timestamp(self):
        return self._header_data[8]

    def __len__(self):
        return self._num_records

    def _read_header(self, validate_crc):
        inner_buffer = self._buffer.getbuffer()
        header_data = self.HEADER_STRUCT.unpack_from(inner_buffer)
        if validate_crc:
            crc = header_data[4]
            verify_crc = _calc_crc32c(inner_buffer[self.ATTRIBUTES_OFFSET:])
            if crc != verify_crc:
                raise CrcCheckFailed()
        self._buffer.seek(self.HEADER_STRUCT.size)
        return header_data

    def _maybe_uncompress(self):
        compression_type = self.compression_type
        if compression_type != self.CODEC_NONE:
            inner_buffer = self._buffer.getbuffer()
            data = inner_buffer[self.HEADER_STRUCT.size:]
            if compression_type == self.CODEC_GZIP:
                uncompressed = gzip_decode(data)
            if compression_type == self.CODEC_SNAPPY:
                uncompressed = snappy_decode(data.tobytes())
            if compression_type == self.CODEC_LZ4:
                uncompressed = lz4_decode(data.tobytes())
            self._buffer = io.BytesIO(uncompressed)

    def _read_msg(self):
        # Record =>
        #   Length => Varint
        #   Attributes => Int8
        #   TimestampDelta => Varlong
        #   OffsetDelta => Varint
        #   Key => Bytes
        #   Value => Bytes
        #   Headers => [HeaderKey HeaderValue]
        #     HeaderKey => String
        #     HeaderValue => Bytes

        buffer = self._buffer
        length = _decode_varint(buffer)
        start_pos = buffer.tell()
        attrs = _decode_varint(buffer)  # attrs can be skiped
        ts_delta = _decode_varint(buffer)
        if self.timestamp_type == self.LOG_APPEND_TIME:
            timestamp = self.max_timestamp
        else:
            timestamp = self.first_timestamp + ts_delta
        offset_delta = _decode_varint(buffer)
        offset = self.base_offset + offset_delta
        key_len = _decode_varint(buffer)
        key = buffer.read(key_len)
        value_len = _decode_varint(buffer)
        value = buffer.read(value_len)
        header_count = _decode_varint(buffer)
        headers = []
        for i in range(header_count):
            h_key_len = _decode_varint(buffer)
            h_key = buffer.read(h_key_len)
            h_value_len = _decode_varint(buffer)
            h_value = buffer.read(h_value_len)
            headers.append((h_key, h_value))
        assert buffer.tell() - start_pos == length
        return (attrs, timestamp, offset, key, value, headers)

    def __iter__(self):
        return self

    def __next__(self):
        if self._next_record_index >= self._num_records:
            raise StopIteration
        self._next_record_index += 1
        return self._read_msg()

    # def seek(self, record_index):
    #     if record_index >= self._num_records:
    #         raise IndexError(record_index)

    #     buffer = self._buffer
    #     if self.compression_type == self.CODEC_NONE:
    #         buffer.seek(self.HEADER_STRUCT.size)
    #     else:
    #         buffer.seek(0)
    #     for i in range(record_index):
    #         r_len = _decode_varint(buffer)
    #         buffer.seek(buffer.tell() + r_len)
    #     self._next_record_index = record_index

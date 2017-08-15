import abc


class CrcCheckFailed(Exception):
    pass


class ABCRecordBatchWriter(abc.ABC):

    @abc.abstractmethod
    def append(self, offset, timestamp, key, value, headers):
        """ Writes record to internal buffer
        """

    @abc.abstractmethod
    def close(self):
        """ Stop appending new messages, write header, compress if needed and
        return compressed bytes ready to be sent.

            Returns: io.BytesIO buffer with ready to send data
        """


class ABCRecordBatchReader(abc.ABC):

    @abc.abstractmethod
    def __init__(self, buffer, validate_crc):
        """ Initialize with io.BytesIO buffer that can be read from
        """
        pass

    @abc.abstractmethod
    def __iter__(self):
        """ Return iterator over records
        """

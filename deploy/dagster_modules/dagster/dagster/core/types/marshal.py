import pickle
from abc import ABCMeta, abstractmethod

import six

from dagster import check
from dagster.utils import PICKLE_PROTOCOL


class SerializationStrategy(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    '''This is the base class for serialization / deserialization of dagster objects. The primary
    use case is serialize to / from files, but any file-like objects (e.g. byte buffers) are
    supported.
    '''

    def __init__(self, name, write_mode='wb', read_mode='rb'):
        self._name = name
        self._write_mode = write_mode
        self._read_mode = read_mode

    @property
    def name(self):
        return self._name

    @property
    def read_mode(self):
        return self._read_mode

    @property
    def write_mode(self):
        return self._write_mode

    @abstractmethod
    def serialize(self, value, write_file_obj):
        '''Core serialization method.'''

    @abstractmethod
    def deserialize(self, read_file_obj):
        '''Core deserialization method'''

    def serialize_to_file(self, value, write_path):
        check.str_param(write_path, 'write_path')

        with open(write_path, self.write_mode) as write_obj:
            return self.serialize(value, write_obj)

    def deserialize_from_file(self, read_path):
        check.str_param(read_path, 'read_path')

        with open(read_path, self.read_mode) as read_obj:
            return self.deserialize(read_obj)


class PickleSerializationStrategy(SerializationStrategy):  # pylint: disable=no-init
    def __init__(self, name='pickle'):
        super(PickleSerializationStrategy, self).__init__(name)

    def serialize(self, value, write_file_obj):
        pickle.dump(value, write_file_obj, PICKLE_PROTOCOL)

    def deserialize(self, read_file_obj):
        return pickle.load(read_file_obj)

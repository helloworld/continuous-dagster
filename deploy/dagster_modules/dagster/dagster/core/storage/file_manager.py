import io
import os
import shutil
import uuid
from abc import ABCMeta, abstractmethod, abstractproperty
from contextlib import contextmanager

import six

from dagster import check
from dagster.core.instance import DagsterInstance
from dagster.core.types.decorator import dagster_type
from dagster.utils import mkdir_p

from .temp_file_manager import TempfileManager


# pylint: disable=no-init
@dagster_type
class FileHandle(six.with_metaclass(ABCMeta)):
    '''A file handle is a reference to a file.
    
    Files can be be resident in the local file system, an object store, or any arbitrary place
    where a file can be stored.

    This exists to handle the very common case where you wish to write a computation that reads,
    transforms, and writes files, but where the same code can work in local developement as well
    as in a cluster where the files would be stored in globally available object store such as s3.
    '''

    @abstractproperty
    def path_desc(self):
        ''' This is a properly to return a *representation* of the path for
        diplay purposes. Should not be used in a programatically meaningful
        way beyond display'''
        raise NotImplementedError()


@dagster_type
class LocalFileHandle(FileHandle):
    def __init__(self, path):
        self._path = check.str_param(path, 'path')

    @property
    def path(self):
        return self._path

    @property
    def path_desc(self):
        return self._path


class FileManager(six.with_metaclass(ABCMeta)):  # pylint: disable=no-init
    '''
    The base class for all file managers in dagster. The file manager is a user-facing
    abstraction that allows a dagster user to pass files in between solids, and the file
    manager is responsible for marshalling those files to and from the nodes where
    the actual dagster computation occur. If the user does their file manipulations using
    this abstraction, it is straightforward to write a pipeline that executes both (a) in
    a local development environment with no external dependencies where files are availble
    on the filesystem and (b) in a cluster environment where those files would need to be on
    a distributed filesystem (e.g. hdfs) or an object store (s3). The business logic
    remains constant and a new implementation of the FileManager is swapped out based on the
    system storage specified by the operator.
    '''

    @abstractmethod
    def copy_handle_to_local_temp(self, file_handle):
        '''
        Take a file handle and make it available as a local temp file. Returns a path.

        In an implementation like an S3FileManager, this would download the file from s3
        to local filesystem, to files created (typically) by the python tempfile module.

        These temp files are *not* guaranteed to be able across solid boundaries. For
        files that must work across solid boundaries, use the read, read_data, write, and
        write_data methods on this class.
        '''
        raise NotImplementedError()

    @abstractmethod
    def delete_local_temp(self):
        '''
        Delete all the local temporary files created by copy_handle_to_local_temp. This should
        typically only be called by framework implementors.
        '''
        raise NotImplementedError()

    @abstractmethod
    def read(self, file_handle, mode='rb'):
        '''Return a file-like stream for the file handle. Defaults to binary mode read.
        This may incur an expensive network call for file managers backed by object stores
        such as s3.
        '''
        raise NotImplementedError()

    @abstractmethod
    def read_data(self, file_handle):
        '''Return the bytes for a given file handle. This may incur an expensive network
        call for file managers backed by object stores such as s3.
        '''
        raise NotImplementedError()

    @abstractmethod
    def write(self, file_obj, mode='wb'):
        '''Write the bytes contained within the given file_obj into the file manager.
        This returns a FileHandle corressponding to the newly created file. FileManagers
        typically return a subclass of FileHandle approprirate for their implementation. E.g.
        a LocalFileManager returns a LocalFileHandle, an S3FileManager returns an S3FileHandle,
        and so forth.
        '''
        raise NotImplementedError()

    @abstractmethod
    def write_data(self, data):
        '''Similar to FileManager.write, but instead of a file-like-object, this
        just takes a raw bytes objects. Returns a FileHandle subclass.'''
        raise NotImplementedError()


@contextmanager
def local_file_manager(instance, run_id):
    manager = None
    try:
        manager = LocalFileManager.for_instance(instance, run_id)
        yield manager
    finally:
        if manager:
            manager.delete_local_temp()


def check_file_like_obj(obj):
    check.invariant(obj and hasattr(obj, 'read') and hasattr(obj, 'write'))


class LocalFileManager(FileManager):
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._base_dir_ensured = False
        self._temp_file_manager = TempfileManager()

    @staticmethod
    def for_instance(instance, run_id):
        check.inst_param(instance, 'instance', DagsterInstance)
        return LocalFileManager(instance.file_manager_directory(run_id))

    def ensure_base_dir_exists(self):
        if self._base_dir_ensured:
            return

        mkdir_p(self.base_dir)

        self._base_dir_ensured = True

    def copy_handle_to_local_temp(self, file_handle):
        check.inst_param(file_handle, 'file_handle', FileHandle)
        with self.read(file_handle, 'rb') as handle_obj:
            temp_file_obj = self._temp_file_manager.tempfile()
            temp_file_obj.write(handle_obj.read())
            temp_name = temp_file_obj.name
            temp_file_obj.close()
            return temp_name

    @contextmanager
    def read(self, file_handle, mode='rb'):
        check.inst_param(file_handle, 'file_handle', LocalFileHandle)
        check.str_param(mode, 'mode')
        check.param_invariant(mode in {'r', 'rb'}, 'mode')

        with open(file_handle.path, mode) as file_obj:
            yield file_obj

    def read_data(self, file_handle):
        with self.read(file_handle, mode='rb') as file_obj:
            return file_obj.read()

    def write_data(self, data):
        check.inst_param(data, 'data', bytes)
        return self.write(io.BytesIO(data), mode='wb')

    def write(self, file_obj, mode='wb'):
        check_file_like_obj(file_obj)
        self.ensure_base_dir_exists()

        dest_file_path = os.path.join(self.base_dir, str(uuid.uuid4()))
        with open(dest_file_path, mode) as dest_file_obj:
            shutil.copyfileobj(file_obj, dest_file_obj)
            return LocalFileHandle(dest_file_path)

    def delete_local_temp(self):
        self._temp_file_manager.close()

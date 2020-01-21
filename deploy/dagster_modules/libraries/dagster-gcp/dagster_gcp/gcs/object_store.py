import logging
from io import BytesIO

from google.cloud import storage

from dagster import check
from dagster.core.definitions.events import ObjectStoreOperation, ObjectStoreOperationType
from dagster.core.storage.object_store import ObjectStore
from dagster.core.types.marshal import SerializationStrategy


class GCSObjectStore(ObjectStore):
    def __init__(self, bucket, client=None):
        self.bucket = check.str_param(bucket, 'bucket')
        self.client = client or storage.Client()
        self.bucket_obj = self.client.get_bucket(bucket)
        assert self.bucket_obj.exists()
        super(GCSObjectStore, self).__init__('gs', sep='/')

    def set_object(self, key, obj, serialization_strategy=None):
        check.str_param(key, 'key')

        logging.info('Writing GCS object at: ' + self.uri_for_key(key))

        # cannot check obj since could be arbitrary Python object
        check.inst_param(
            serialization_strategy, 'serialization_strategy', SerializationStrategy
        )  # cannot be none here

        if self.has_object(key):
            logging.warning('Removing existing GCS key: {key}'.format(key=key))
            self.rm_object(key)

        with BytesIO() as bytes_io:
            serialization_strategy.serialize(obj, bytes_io)
            bytes_io.seek(0)
            self.bucket_obj.blob(key).upload_from_file(bytes_io)

        return ObjectStoreOperation(
            op=ObjectStoreOperationType.SET_OBJECT,
            key=self.uri_for_key(key),
            dest_key=None,
            obj=obj,
            serialization_strategy_name=serialization_strategy.name,
            object_store_name=self.name,
        )

    def get_object(self, key, serialization_strategy=None):
        check.str_param(key, 'key')
        check.param_invariant(len(key) > 0, 'key')

        file_obj = BytesIO()
        self.bucket_obj.blob(key).download_to_file(file_obj)
        file_obj.seek(0)

        obj = serialization_strategy.deserialize(file_obj)
        return ObjectStoreOperation(
            op=ObjectStoreOperationType.GET_OBJECT,
            key=self.uri_for_key(key),
            dest_key=None,
            obj=obj,
            serialization_strategy_name=serialization_strategy.name,
            object_store_name=self.name,
        )

    def has_object(self, key):
        check.str_param(key, 'key')
        check.param_invariant(len(key) > 0, 'key')
        blobs = self.client.list_blobs(self.bucket, prefix=key)
        return len(list(blobs)) > 0

    def rm_object(self, key):
        check.str_param(key, 'key')
        check.param_invariant(len(key) > 0, 'key')

        if self.bucket_obj.blob(key).exists():
            self.bucket_obj.blob(key).delete()

        return ObjectStoreOperation(
            op=ObjectStoreOperationType.RM_OBJECT,
            key=self.uri_for_key(key),
            dest_key=None,
            obj=None,
            serialization_strategy_name=None,
            object_store_name=self.name,
        )

    def cp_object(self, src, dst):
        check.str_param(src, 'src')
        check.str_param(dst, 'dst')

        source_blob = self.bucket_obj.blob(src)
        self.bucket_obj.copy_blob(source_blob, self.bucket_obj, dst)

        return ObjectStoreOperation(
            op=ObjectStoreOperationType.CP_OBJECT,
            key=self.uri_for_key(src),
            dest_key=self.uri_for_key(dst),
            object_store_name=self.name,
        )

    def uri_for_key(self, key, protocol=None):
        check.str_param(key, 'key')
        protocol = check.opt_str_param(protocol, 'protocol', default='gs://')
        return protocol + self.bucket + '/' + '{key}'.format(key=key)

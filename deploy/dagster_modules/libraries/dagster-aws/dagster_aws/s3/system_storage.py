from dagster import Field, String, SystemStorageData, system_storage
from dagster.core.storage.intermediates_manager import IntermediateStoreIntermediatesManager
from dagster.core.storage.system_storage import fs_system_storage, mem_system_storage

from .file_manager import S3FileManager
from .intermediate_store import S3IntermediateStore


@system_storage(
    name='s3',
    is_persistent=True,
    config={
        's3_bucket': Field(String),
        's3_prefix': Field(String, is_optional=True, default_value='dagster'),
    },
    required_resource_keys={'s3'},
)
def s3_system_storage(init_context):
    s3_session = init_context.resources.s3.session
    s3_key = '{prefix}/storage/{run_id}/files'.format(
        prefix=init_context.system_storage_config['s3_prefix'],
        run_id=init_context.pipeline_run.run_id,
    )
    return SystemStorageData(
        file_manager=S3FileManager(
            s3_session=s3_session,
            s3_bucket=init_context.system_storage_config['s3_bucket'],
            s3_base_key=s3_key,
        ),
        intermediates_manager=IntermediateStoreIntermediatesManager(
            S3IntermediateStore(
                s3_session=s3_session,
                s3_bucket=init_context.system_storage_config['s3_bucket'],
                s3_prefix=init_context.system_storage_config['s3_prefix'],
                run_id=init_context.pipeline_run.run_id,
                type_storage_plugin_registry=init_context.type_storage_plugin_registry,
            )
        ),
    )


s3_plus_default_storage_defs = [mem_system_storage, fs_system_storage, s3_system_storage]

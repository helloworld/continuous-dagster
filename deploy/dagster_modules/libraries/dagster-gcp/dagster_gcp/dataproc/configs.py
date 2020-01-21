from dagster import Field, String

from .configs_dataproc_cluster import define_dataproc_cluster_config
from .configs_dataproc_job import define_dataproc_job_config


def define_dataproc_create_cluster_config():
    cluster_name = Field(
        String,
        description='''Required. The cluster name. Cluster names within a project must be unique.
        Names of deleted clusters can be reused.''',
        is_optional=False,
    )

    return {
        'projectId': _define_project_id_config(),
        'region': _define_region_config(),
        'clusterName': cluster_name,
        'cluster_config': define_dataproc_cluster_config(),
    }


def define_dataproc_submit_job_config():
    return {
        'job': define_dataproc_job_config(),
        'projectId': _define_project_id_config(),
        'region': _define_region_config(),
    }


def _define_project_id_config():
    return Field(
        String,
        description='''Required. Project ID for the project which the client acts on behalf of. Will
        be passed when creating a dataset / job. If not passed, falls back to the default inferred
        from the environment.''',
        is_optional=False,
    )


def _define_region_config():
    return Field(String, is_optional=False)

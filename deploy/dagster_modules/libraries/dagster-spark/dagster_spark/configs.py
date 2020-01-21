'''Spark Configuration

In this file we define the key configuration parameters for submitting Spark jobs. Spark can be run
in a variety of deployment contexts. See the Spark documentation at
https://spark.apache.org/docs/latest/submitting-applications.html for a more in-depth summary of
Spark deployment contexts and configuration.
'''
from dagster import Field

from .configs_spark import spark_config
from .types import SparkDeployMode


def define_spark_config():
    '''Spark configuration.

    See the Spark documentation for reference:
        https://spark.apache.org/docs/latest/submitting-applications.html
    '''

    master_url = Field(
        str,
        description='The master URL for the cluster (e.g. spark://23.195.26.187:7077)',
        is_optional=False,
    )

    deploy_mode = Field(
        SparkDeployMode,
        description='''Whether to deploy your driver on the worker nodes (cluster) or locally as an
        external client (client) (default: client). A common deployment strategy is to submit your
        application from a gateway machine that is physically co-located with your worker machines
        (e.g. Master node in a standalone EC2 cluster). In this setup, client mode is appropriate.
        In client mode, the driver is launched directly within the spark-submit process which acts
        as a client to the cluster. The input and output of the application is attached to the
        console. Thus, this mode is especially suitable for applications that involve the REPL (e.g.
        Spark shell).''',
        is_optional=True,
    )

    application_jar = Field(
        str,
        description='''Path to a bundled jar including your application and all
                        dependencies. The URL must be globally visible inside of your cluster, for
                        instance, an hdfs:// path or a file:// path that is present on all nodes.
                        ''',
        is_optional=False,
    )

    application_arguments = Field(
        str,
        description='Arguments passed to the main method of your main class, if any',
        is_optional=True,
    )

    spark_home = Field(
        str,
        description='The path to your spark installation. Defaults to $SPARK_HOME at runtime if not provided.',
        is_optional=True,
    )

    return {
        'master_url': master_url,
        'deploy_mode': deploy_mode,
        'application_jar': application_jar,
        'spark_conf': spark_config(),
        'spark_home': spark_home,
        'application_arguments': application_arguments,
    }

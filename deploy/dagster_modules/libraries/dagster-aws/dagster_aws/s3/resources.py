from dagster import Field, resource

from .utils import create_s3_session


class S3Resource(object):
    def __init__(self, s3_session):
        self.session = s3_session

    def put_object(self, **kwargs):
        '''This mirrors the put_object boto3 API:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Bucket.put_object

        The config schema for this API is applied to the put_object_to_s3_bytes solid vs. at the
        resource level here.
        '''
        return self.session.put_object(**kwargs)

    def upload_fileobj(self, fileobj, bucket, key):
        '''This mirrors the upload_file boto3 API:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.upload_fileobj
        '''
        return self.session.upload_fileobj(fileobj, bucket, key)


@resource(
    {
        'use_unsigned_session': Field(
            bool,
            description='Specifies whether to use an unsigned S3 session',
            is_optional=True,
            default_value=True,
        ),
        'region_name': Field(
            str, description='Specifies a custom region for the S3 session', is_optional=True
        ),
        'endpoint_url': Field(
            str, description='Specifies a custom endpoint for the S3 session', is_optional=True
        ),
    }
)
def s3_resource(context):
    use_unsigned_session = context.resource_config['use_unsigned_session']
    region_name = context.resource_config.get('region_name')
    endpoint_url = context.resource_config.get('endpoint_url')
    return S3Resource(
        create_s3_session(
            signed=not use_unsigned_session, region_name=region_name, endpoint_url=endpoint_url
        )
    )

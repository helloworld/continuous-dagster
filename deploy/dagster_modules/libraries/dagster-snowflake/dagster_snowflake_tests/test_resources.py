from dagster_snowflake import snowflake_resource

from dagster import ModeDefinition, execute_solid, solid
from dagster.seven import mock

from .utils import create_mock_connector


@mock.patch('snowflake.connector.connect', new_callable=create_mock_connector)
def test_snowflake_resource(snowflake_connect):
    @solid(required_resource_keys={'snowflake'})
    def snowflake_solid(context):
        assert context.resources.snowflake
        with context.resources.snowflake.get_connection() as _:
            pass

    result = execute_solid(
        snowflake_solid,
        environment_dict={
            'resources': {
                'snowflake': {
                    'config': {
                        'account': 'foo',
                        'user': 'bar',
                        'password': 'baz',
                        'database': 'TESTDB',
                        'schema': 'TESTSCHEMA',
                        'warehouse': 'TINY_WAREHOUSE',
                    }
                }
            }
        },
        mode_def=ModeDefinition(resource_defs={'snowflake': snowflake_resource}),
    )
    assert result.success
    snowflake_connect.assert_called_once_with(
        account='foo',
        user='bar',
        password='baz',
        database='TESTDB',
        schema='TESTSCHEMA',
        warehouse='TINY_WAREHOUSE',
    )

from dagster_graphql.test.utils import execute_dagster_graphql

from dagster import check
from dagster.config.config_type import ALL_CONFIG_BUILTINS
from dagster.utils import file_relative_path

from .setup import csv_hello_world_solids_config, define_context

CONFIG_VALIDATION_QUERY = '''
query PipelineQuery(
    $environmentConfigData: EnvironmentConfigData,
    $pipeline: ExecutionSelector!,
    $mode: String!
) {
    isPipelineConfigValid(
        environmentConfigData: $environmentConfigData,
        pipeline: $pipeline,
        mode: $mode
    ) {
        __typename
        ... on PipelineConfigValidationValid {
            pipeline { name }
        }
        ... on PipelineConfigValidationInvalid {
            pipeline { name }
            errors {
                __typename
                ... on RuntimeMismatchConfigError {
                    type { key }
                    valueRep
                }
                ... on MissingFieldConfigError {
                    field { name }
                }
                ... on MissingFieldsConfigError {
                    fields { name }
                }
                ... on FieldNotDefinedConfigError {
                    fieldName
                }
                ... on FieldsNotDefinedConfigError {
                    fieldNames
                }
                ... on SelectorTypeConfigError {
                    incomingFields
                }
                message
                reason
                stack {
                    entries {
                        __typename
                        ... on EvaluationStackPathEntry {
                            field {
                                name
                                configType {
                                   key  
                                }
                            }
                        }
                        ... on EvaluationStackListItemEntry {
                            listIndex
                        }
                    }
                }
            }
        }
        ... on PipelineNotFoundError {
            pipelineName
        }
    }
}
'''


def field_stack(error_data):
    return [
        entry['field']['name']
        for entry in error_data['stack']['entries']
        if entry['__typename'] == 'EvaluationStackPathEntry'
    ]


def single_error_data(result):
    assert len(result.data['isPipelineConfigValid']['errors']) == 1
    return result.data['isPipelineConfigValid']['errors'][0]


def find_error(result, field_stack_to_find, reason):
    llist = list(find_errors(result, field_stack_to_find, reason))
    assert len(llist) == 1
    return llist[0]


def find_errors(result, field_stack_to_find, reason):
    error_datas = result.data['isPipelineConfigValid']['errors']
    for error_data in error_datas:
        if field_stack_to_find == field_stack(error_data) and error_data['reason'] == reason:
            yield error_data


def execute_config_graphql(pipeline_name, environment_dict, mode):
    return execute_dagster_graphql(
        define_context(),
        CONFIG_VALIDATION_QUERY,
        {
            'environmentConfigData': environment_dict,
            'pipeline': {'name': pipeline_name},
            'mode': mode,
        },
    )


def test_pipeline_not_found():
    result = execute_config_graphql(pipeline_name='nope', environment_dict={}, mode='default')

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineNotFoundError'
    assert result.data['isPipelineConfigValid']['pipelineName'] == 'nope'


def test_basic_valid_config():
    result = execute_config_graphql(
        pipeline_name='csv_hello_world',
        environment_dict=csv_hello_world_solids_config(),
        mode='default',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationValid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'csv_hello_world'


def test_root_field_not_defined():
    result = execute_config_graphql(
        pipeline_name='csv_hello_world',
        environment_dict={
            'solids': {
                'sum_solid': {'inputs': {'num': file_relative_path(__file__, '../data/num.csv')}}
            },
            'nope': {},
        },
        mode='default',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'csv_hello_world'
    errors = result.data['isPipelineConfigValid']['errors']
    assert len(errors) == 1
    error = errors[0]
    assert error['__typename'] == 'FieldNotDefinedConfigError'
    assert error['fieldName'] == 'nope'
    assert not error['stack']['entries']


def test_basic_invalid_not_defined_field():
    result = execute_config_graphql(
        pipeline_name='csv_hello_world',
        environment_dict={'solids': {'sum_solid': {'inputs': {'num': 'foo.txt', 'extra': 'nope'}}}},
        mode='default',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'csv_hello_world'
    assert len(result.data['isPipelineConfigValid']['errors']) == 1
    error_data = result.data['isPipelineConfigValid']['errors'][0]
    assert ['solids', 'sum_solid', 'inputs'] == field_stack(error_data)
    assert error_data['reason'] == 'FIELD_NOT_DEFINED'
    assert error_data['fieldName'] == 'extra'


def test_multiple_not_defined_fields():
    result = execute_config_graphql(
        pipeline_name='csv_hello_world',
        environment_dict={
            'solids': {
                'sum_solid': {
                    'inputs': {'num': 'foo.txt', 'extra_one': 'nope', 'extra_two': 'nope'}
                }
            }
        },
        mode='default',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'csv_hello_world'
    assert len(result.data['isPipelineConfigValid']['errors']) == 1
    error_data = result.data['isPipelineConfigValid']['errors'][0]
    assert ['solids', 'sum_solid', 'inputs'] == field_stack(error_data)
    assert error_data['reason'] == 'FIELDS_NOT_DEFINED'
    assert error_data['fieldNames'] == ['extra_one', 'extra_two']


def test_root_wrong_type():
    result = execute_config_graphql(
        pipeline_name='csv_hello_world', environment_dict=123, mode='default'
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'csv_hello_world'
    assert len(result.data['isPipelineConfigValid']['errors']) == 1
    error_data = result.data['isPipelineConfigValid']['errors'][0]
    assert error_data['reason'] == 'RUNTIME_TYPE_MISMATCH'


def test_basic_invalid_config_type_mismatch():
    result = execute_config_graphql(
        pipeline_name='csv_hello_world',
        environment_dict={'solids': {'sum_solid': {'inputs': {'num': 123}}}},
        mode='default',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'csv_hello_world'
    assert len(result.data['isPipelineConfigValid']['errors']) == 1
    error_data = result.data['isPipelineConfigValid']['errors'][0]
    assert error_data['message']
    assert error_data['stack']
    assert error_data['stack']['entries']
    assert error_data['reason'] == 'RUNTIME_TYPE_MISMATCH'
    assert error_data['valueRep'] == '123'
    assert error_data['type']['key'] == 'Path'

    assert ['solids', 'sum_solid', 'inputs', 'num'] == field_stack(error_data)


def test_basic_invalid_config_missing_field():
    result = execute_config_graphql(
        pipeline_name='csv_hello_world',
        environment_dict={'solids': {'sum_solid': {'inputs': {}}}},
        mode='default',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'csv_hello_world'
    assert len(result.data['isPipelineConfigValid']['errors']) == 1
    error_data = result.data['isPipelineConfigValid']['errors'][0]

    assert ['solids', 'sum_solid', 'inputs'] == field_stack(error_data)
    assert error_data['reason'] == 'MISSING_REQUIRED_FIELD'
    assert error_data['field']['name'] == 'num'


def test_mode_resource_config_works():
    result = execute_config_graphql(
        pipeline_name='multi_mode_with_resources',
        environment_dict={'resources': {'op': {'config': 2}}},
        mode='add_mode',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationValid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'multi_mode_with_resources'

    result = execute_config_graphql(
        pipeline_name='multi_mode_with_resources',
        environment_dict={'resources': {'op': {'config': 2}}},
        mode='mult_mode',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationValid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'multi_mode_with_resources'

    result = execute_config_graphql(
        pipeline_name='multi_mode_with_resources',
        environment_dict={'resources': {'op': {'config': {'num_one': 2, 'num_two': 3}}}},
        mode='double_adder',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationValid'
    assert result.data['isPipelineConfigValid']['pipeline']['name'] == 'multi_mode_with_resources'


def test_missing_resource():
    result = execute_config_graphql(
        pipeline_name='multi_mode_with_resources',
        environment_dict={'resources': {}},
        mode='add_mode',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    error_data = single_error_data(result)
    assert error_data['reason'] == 'MISSING_REQUIRED_FIELD'
    assert error_data['field']['name'] == 'op'


def test_undefined_resource():
    result = execute_config_graphql(
        pipeline_name='multi_mode_with_resources',
        environment_dict={'resources': {'nope': {}}},
        mode='add_mode',
    )

    assert not result.errors
    assert result.data
    assert result.data['isPipelineConfigValid']['__typename'] == 'PipelineConfigValidationInvalid'
    assert {'FieldNotDefinedConfigError', 'MissingFieldConfigError'} == {
        error_data['__typename'] for error_data in result.data['isPipelineConfigValid']['errors']
    }


def test_more_complicated_works():
    result = execute_config_graphql(
        pipeline_name='more_complicated_nested_config',
        environment_dict={
            'solids': {
                'a_solid_with_multilayered_config': {
                    'config': {
                        'field_any': {'123': 123},
                        'field_one': 'foo.txt',
                        'field_two': 'yup',
                        'field_three': 'mmmhmmm',
                        'nested_field': {'field_four_str': 'yaya', 'field_five_int': 234},
                    }
                }
            }
        },
        mode='default',
    )
    assert not result.errors
    assert result.data
    valid_data = result.data['isPipelineConfigValid']
    assert valid_data['__typename'] == 'PipelineConfigValidationValid'
    assert valid_data['pipeline']['name'] == 'more_complicated_nested_config'


def test_multiple_missing_fields():

    result = execute_config_graphql(
        pipeline_name='more_complicated_nested_config',
        environment_dict={'solids': {'a_solid_with_multilayered_config': {'config': {}}}},
        mode='default',
    )

    assert not result.errors
    assert result.data
    valid_data = result.data['isPipelineConfigValid']

    assert valid_data['__typename'] == 'PipelineConfigValidationInvalid'
    assert valid_data['pipeline']['name'] == 'more_complicated_nested_config'
    assert len(valid_data['errors']) == 1
    error_data = valid_data['errors'][0]
    missing_names = {field_data['name'] for field_data in error_data['fields']}
    assert missing_names == {'nested_field', 'field_one', 'field_any'}
    assert field_stack(error_data) == ['solids', 'a_solid_with_multilayered_config', 'config']


def test_more_complicated_multiple_errors():
    result = execute_config_graphql(
        pipeline_name='more_complicated_nested_config',
        environment_dict={
            'solids': {
                'a_solid_with_multilayered_config': {
                    'config': {
                        'field_any': [],
                        # 'field_one': 'foo.txt', # missing
                        'field_two': 'yup',
                        'field_three': 'mmmhmmm',
                        'extra_one': 'kjsdkfjd',  # extra
                        'nested_field': {
                            'field_four_str': 23434,  # runtime type
                            'field_five_int': 234,
                            'extra_two': 'ksjdkfjd',  # another extra
                        },
                    }
                }
            }
        },
        mode='default',
    )

    assert not result.errors
    assert result.data
    valid_data = result.data['isPipelineConfigValid']

    assert valid_data['__typename'] == 'PipelineConfigValidationInvalid'
    assert valid_data['pipeline']['name'] == 'more_complicated_nested_config'
    assert len(valid_data['errors']) == 4

    missing_error_one = find_error(
        result, ['solids', 'a_solid_with_multilayered_config', 'config'], 'MISSING_REQUIRED_FIELD'
    )
    assert ['solids', 'a_solid_with_multilayered_config', 'config'] == field_stack(
        missing_error_one
    )
    assert missing_error_one['reason'] == 'MISSING_REQUIRED_FIELD'
    assert missing_error_one['field']['name'] == 'field_one'

    not_defined_one = find_error(
        result, ['solids', 'a_solid_with_multilayered_config', 'config'], 'FIELD_NOT_DEFINED'
    )
    assert ['solids', 'a_solid_with_multilayered_config', 'config'] == field_stack(not_defined_one)
    assert not_defined_one['reason'] == 'FIELD_NOT_DEFINED'
    assert not_defined_one['fieldName'] == 'extra_one'

    runtime_type_error = find_error(
        result,
        ['solids', 'a_solid_with_multilayered_config', 'config', 'nested_field', 'field_four_str'],
        'RUNTIME_TYPE_MISMATCH',
    )
    assert [
        'solids',
        'a_solid_with_multilayered_config',
        'config',
        'nested_field',
        'field_four_str',
    ] == field_stack(runtime_type_error)
    assert runtime_type_error['reason'] == 'RUNTIME_TYPE_MISMATCH'
    assert runtime_type_error['valueRep'] == '23434'
    assert runtime_type_error['type']['key'] == 'String'

    not_defined_two = find_error(
        result,
        ['solids', 'a_solid_with_multilayered_config', 'config', 'nested_field'],
        'FIELD_NOT_DEFINED',
    )

    assert ['solids', 'a_solid_with_multilayered_config', 'config', 'nested_field'] == field_stack(
        not_defined_two
    )
    assert not_defined_two['reason'] == 'FIELD_NOT_DEFINED'
    assert not_defined_two['fieldName'] == 'extra_two'

    # TODO: two more errors


def test_config_list():
    result = execute_config_graphql(
        pipeline_name='pipeline_with_list',
        environment_dict={'solids': {'solid_with_list': {'config': [1, 2]}}},
        mode='default',
    )

    assert not result.errors
    assert result.data
    valid_data = result.data['isPipelineConfigValid']
    assert valid_data['__typename'] == 'PipelineConfigValidationValid'
    assert valid_data['pipeline']['name'] == 'pipeline_with_list'


def test_config_list_invalid():
    result = execute_config_graphql(
        pipeline_name='pipeline_with_list',
        environment_dict={'solids': {'solid_with_list': {'config': 'foo'}}},
        mode='default',
    )

    assert not result.errors
    assert result.data
    valid_data = result.data['isPipelineConfigValid']
    assert valid_data['__typename'] == 'PipelineConfigValidationInvalid'
    assert valid_data['pipeline']['name'] == 'pipeline_with_list'
    assert len(valid_data['errors']) == 1
    assert ['solids', 'solid_with_list', 'config'] == field_stack(valid_data['errors'][0])


def test_config_list_item_invalid():
    result = execute_config_graphql(
        pipeline_name='pipeline_with_list',
        environment_dict={'solids': {'solid_with_list': {'config': [1, 'foo']}}},
        mode='default',
    )

    assert not result.errors
    assert result.data
    valid_data = result.data['isPipelineConfigValid']
    assert valid_data['__typename'] == 'PipelineConfigValidationInvalid'
    assert valid_data['pipeline']['name'] == 'pipeline_with_list'
    assert len(valid_data['errors']) == 1
    entries = valid_data['errors'][0]['stack']['entries']
    assert len(entries) == 4
    assert ['solids', 'solid_with_list', 'config'] == field_stack(valid_data['errors'][0])

    last_entry = entries[3]
    assert last_entry['__typename'] == 'EvaluationStackListItemEntry'
    assert last_entry['listIndex'] == 1


def pipeline_named(result, name):
    for pipeline_data in result.data['pipelines']['nodes']:
        if pipeline_data['name'] == name:
            return pipeline_data
    check.failed('Did not find')


def has_config_type_with_key_prefix(config_types_data, prefix):
    for config_type_data in config_types_data:
        if config_type_data['key'].startswith(prefix):
            return True

    return False


def has_config_type(config_types_data, name):
    for config_type_data in config_types_data:
        if config_type_data.get('givenName') == name:
            return True

    return False


def test_smoke_test_config_type_system():
    result = execute_dagster_graphql(
        define_context(),
        ALL_CONFIG_TYPES_QUERY,
        {'pipelineName': 'more_complicated_nested_config', 'mode': 'default'},
    )

    config_types_data = result.data['environmentSchemaOrError']['allConfigTypes']

    assert has_config_type_with_key_prefix(config_types_data, 'Shape.')

    for builtin_config_type in ALL_CONFIG_BUILTINS:
        assert has_config_type(config_types_data, builtin_config_type.given_name)


ALL_CONFIG_TYPES_QUERY = '''
fragment configTypeFragment on ConfigType {
  __typename
  key
  description
  isSelector
  typeParamKeys
  recursiveConfigTypes {
    key
    description
    ... on CompositeConfigType {
        fields {
            name
            isOptional
            description
        }
    }
    ... on WrappingConfigType {
        ofType { key }
    }
  }
  ... on EnumConfigType {
    givenName
    values {
      value
      description
    }
  }
  ... on RegularConfigType {
    givenName
  }
  ... on CompositeConfigType {
    fields {
      name
      isOptional
      description
    }
  }
  ... on WrappingConfigType {
    ofType { key }
  }
  ... on ScalarUnionConfigType {
    scalarType { key }
    nonScalarType { key }
  }
}

query allConfigTypes($pipelineName: String!, $mode: String!) {
  environmentSchemaOrError(selector: { name: $pipelineName }, mode: $mode ) {
    ... on EnvironmentSchema {
      allConfigTypes {
        ...configTypeFragment
      }
    }
  }
}
'''


def get_field_data(config_type_data, name):
    for field_data in config_type_data['fields']:
        if field_data['name'] == name:
            return field_data


def get_field_names(config_type_data):
    return {field_data['name'] for field_data in config_type_data.get('fields', [])}

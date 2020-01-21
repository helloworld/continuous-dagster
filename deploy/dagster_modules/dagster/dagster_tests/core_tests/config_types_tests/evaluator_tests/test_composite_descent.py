import pytest

from dagster import (
    DagsterInvalidConfigError,
    Enum,
    EnumValue,
    Field,
    InputDefinition,
    Output,
    String,
    composite_solid,
    execute_pipeline,
    lambda_solid,
    pipeline,
    solid,
)
from dagster.core.system_config.composite_descent import composite_descent


def test_single_level_pipeline():
    @solid(config=int)
    def return_int(context):
        return context.solid_config

    @pipeline
    def return_int_pipeline():
        return_int()

    result = execute_pipeline(return_int_pipeline, {'solids': {'return_int': {'config': 2}}})

    assert result.success
    assert result.result_for_solid('return_int').output_value() == 2


def test_single_solid_pipeline_composite_descent():
    @solid(config=int)
    def return_int(context):
        return context.solid_config

    @pipeline
    def return_int_pipeline():
        return_int()

    solid_config_dict = composite_descent(return_int_pipeline, {'return_int': {'config': 3}})

    assert solid_config_dict['return_int'].config == 3

    result = execute_pipeline(return_int_pipeline, {'solids': {'return_int': {'config': 3}}})

    assert result.success
    assert result.result_for_solid('return_int').output_value() == 3


def test_single_layer_pipeline_composite_descent():
    @solid(config=int)
    def return_int(context):
        return context.solid_config

    @composite_solid
    def return_int_passthrough():
        return_int()

    @pipeline
    def return_int_pipeline_passthrough():
        return_int_passthrough()

    solid_config_dict = composite_descent(
        return_int_pipeline_passthrough,
        {'return_int_passthrough': {'solids': {'return_int': {'config': 34}}}},
    )

    handle = 'return_int_passthrough.return_int'
    assert solid_config_dict[handle].config == 34

    result = execute_pipeline(
        return_int_pipeline_passthrough,
        {'solids': {'return_int_passthrough': {'solids': {'return_int': {'config': 34}}}},},
    )

    assert result.success
    assert result.result_for_handle(handle).output_value() == 34


def test_single_layer_pipeline_hardcoded_config_mapping():
    @solid(config=int)
    def return_int(context):
        return context.solid_config

    @composite_solid(config={}, config_fn=lambda _cfg: {'return_int': {'config': 35}})
    def return_int_hardcode_wrap():
        return_int()

    @pipeline
    def return_int_hardcode_wrap_pipeline():
        return_int_hardcode_wrap()

    solid_config_dict = composite_descent(return_int_hardcode_wrap_pipeline, {})

    assert solid_config_dict['return_int_hardcode_wrap.return_int'].config == 35


def test_single_layer_pipeline_computed_config_mapping():
    @solid(config=int)
    def return_int(context):
        return context.solid_config

    def _config_fn(cfg):
        return {'return_int': {'config': cfg['number'] + 1}}

    @composite_solid(config={'number': int}, config_fn=_config_fn)
    def return_int_plus_one():
        return_int()

    @pipeline
    def return_int_hardcode_wrap_pipeline():
        return_int_plus_one()

    solid_config_dict = composite_descent(
        return_int_hardcode_wrap_pipeline, {'return_int_plus_one': {'config': {'number': 23}}}
    )

    assert solid_config_dict['return_int_plus_one.return_int'].config == 24


def test_mix_layer_computed_mapping():
    @solid(config=int)
    def return_int(context):
        return context.solid_config

    @composite_solid(
        config={'number': int}, config_fn=lambda cfg: {'return_int': {'config': cfg['number'] + 1}},
    )
    def layer_three_wrap():
        return_int()

    def _layer_two_double_wrap_cfg_fn(cfg):
        if cfg['inject_error']:
            return {'layer_three_wrap': {'config': {'number': 'a_string'}}}
        else:
            return {'layer_three_wrap': {'config': {'number': cfg['number'] + 1}}}

    @composite_solid(
        config={'number': int, 'inject_error': bool}, config_fn=_layer_two_double_wrap_cfg_fn
    )
    def layer_two_double_wrap():
        layer_three_wrap()

    @composite_solid
    def layer_two_passthrough():
        return_int()

    @composite_solid
    def layer_one():
        layer_two_passthrough()
        layer_two_double_wrap()

    @pipeline
    def layered_config():
        layer_one()

    solid_config_dict = composite_descent(
        layered_config,
        {
            'layer_one': {
                'solids': {
                    'layer_two_passthrough': {'solids': {'return_int': {'config': 234}}},
                    'layer_two_double_wrap': {'config': {'number': 5, 'inject_error': False}},
                }
            }
        },
    )

    assert solid_config_dict['layer_one.layer_two_passthrough.return_int'].config == 234
    # this passed through both config fns which each added one
    assert (
        solid_config_dict['layer_one.layer_two_double_wrap.layer_three_wrap.return_int'].config == 7
    )

    with pytest.raises(DagsterInvalidConfigError) as exc_info:
        composite_descent(
            layered_config,
            {
                'layer_one': {
                    'solids': {
                        'layer_two_passthrough': {'solids': {'return_int': {'config': 234}}},
                        'layer_two_double_wrap': {'config': {'number': 234, 'inject_error': True}},
                    }
                }
            },
        )

    assert 'Solid "layer_two_double_wrap" with definition "layer_two_double_wrap"' in str(
        exc_info.value
    )

    assert (
        'Error 1: Type failure at path "root:layer_three_wrap:config:number" on type "Int". '
        'Value at path root:layer_three_wrap:config:number is not valid. Expected "Int"'
    ) in str(exc_info.value)

    result = execute_pipeline(
        layered_config,
        {
            'solids': {
                'layer_one': {
                    'solids': {
                        'layer_two_passthrough': {'solids': {'return_int': {'config': 55}}},
                        'layer_two_double_wrap': {'config': {'number': 7, 'inject_error': False}},
                    }
                }
            },
        },
    )

    assert (
        result.result_for_handle('layer_one.layer_two_passthrough.return_int').output_value() == 55
    )
    assert (
        result.result_for_handle(
            'layer_one.layer_two_double_wrap.layer_three_wrap.return_int'
        ).output_value()
        == 9
    )


def test_nested_input_via_config_mapping():
    @solid
    def add_one(_, num):
        return num + 1

    @composite_solid(
        config={}, config_fn=lambda _cfg: {'add_one': {'inputs': {'num': {'value': 2}}}}
    )
    def wrap_add_one():
        add_one()

    @pipeline
    def wrap_add_one_pipeline():
        wrap_add_one()

    solid_config_dict = composite_descent(wrap_add_one_pipeline, {})
    assert solid_config_dict['wrap_add_one.add_one'].inputs == {'num': {'value': 2}}

    result = execute_pipeline(wrap_add_one_pipeline)
    assert result.success
    assert result.result_for_handle('wrap_add_one.add_one').output_value() == 3


def test_double_nested_input_via_config_mapping():
    @lambda_solid
    def number(num):
        return num

    @composite_solid(config_fn=lambda _: {'number': {'inputs': {'num': {'value': 4}}}}, config={})
    def wrap_solid():  # pylint: disable=unused-variable
        return number()

    @composite_solid
    def double_wrap(num):
        number(num)
        return wrap_solid()

    @pipeline
    def wrap_pipeline_double_nested_input():
        return double_wrap()

    solid_handle_dict = composite_descent(
        wrap_pipeline_double_nested_input, {'double_wrap': {'inputs': {'num': {'value': 2}}}}
    )
    assert solid_handle_dict['double_wrap.wrap_solid.number'].inputs == {'num': {'value': 4}}
    assert solid_handle_dict['double_wrap'].inputs == {'num': {'value': 2}}

    result = execute_pipeline(
        wrap_pipeline_double_nested_input,
        {'solids': {'double_wrap': {'inputs': {'num': {'value': 2}}}}},
    )
    assert result.success


def test_provide_one_of_two_inputs_via_config():
    @solid(
        config={'config_field_a': Field(String), 'config_field_b': Field(String)},
        input_defs=[InputDefinition('input_a', String), InputDefinition('input_b', String)],
    )
    def basic(context, input_a, input_b):
        res = '.'.join(
            [
                context.solid_config['config_field_a'],
                context.solid_config['config_field_b'],
                input_a,
                input_b,
            ]
        )
        yield Output(res)

    @composite_solid(
        input_defs=[InputDefinition('input_a', String)],
        config_fn=lambda cfg: {
            'basic': {
                'config': {
                    'config_field_a': cfg['config_field_a'],
                    'config_field_b': cfg['config_field_b'],
                },
                'inputs': {'input_b': {'value': 'set_input_b'}},
            }
        },
        config={'config_field_a': Field(String), 'config_field_b': Field(String)},
    )
    def wrap_all_config_one_input(input_a):
        return basic(input_a)

    @pipeline(name='config_mapping')
    def config_mapping_pipeline():
        return wrap_all_config_one_input()

    solids_config_dict = {
        'wrap_all_config_one_input': {
            'config': {'config_field_a': 'override_a', 'config_field_b': 'override_b'},
            'inputs': {'input_a': {'value': 'set_input_a'}},
        }
    }

    result = execute_pipeline(config_mapping_pipeline, {'solids': solids_config_dict})
    assert result.success

    assert result.success
    assert (
        result.result_for_solid('wrap_all_config_one_input').output_value()
        == 'override_a.override_b.set_input_a.set_input_b'
    )


@solid(config=Field(String, is_optional=True))
def scalar_config_solid(context):
    yield Output(context.solid_config)


@solid(config=Field(String, is_optional=False))
def required_scalar_config_solid(context):
    yield Output(context.solid_config)


@composite_solid(
    config={'override_str': Field(String)},
    config_fn=lambda cfg: {'layer2': {'config': cfg['override_str']}},
)
def wrap():
    return scalar_config_solid.alias('layer2')()


@composite_solid(
    config={'nesting_override': Field(String)},
    config_fn=lambda cfg: {'layer1': {'config': {'override_str': cfg['nesting_override']}}},
)
def nesting_wrap():
    return wrap.alias('layer1')()


@pipeline
def wrap_pipeline():
    return nesting_wrap.alias('layer0')()


@composite_solid
def wrap_no_mapping():
    return required_scalar_config_solid.alias('layer2')()


@composite_solid
def nesting_wrap_no_mapping():
    return wrap_no_mapping.alias('layer1')()


@pipeline
def no_wrap_pipeline():
    return nesting_wrap_no_mapping.alias('layer0')()


def get_fully_unwrapped_config():
    return {
        'solids': {'layer0': {'solids': {'layer1': {'solids': {'layer2': {'config': 'blah'}}}}}}
    }


def test_direct_composite_descent_with_error():
    @composite_solid(
        config={'override_str': Field(int)},
        config_fn=lambda cfg: {'layer2': {'config': cfg['override_str']}},
    )
    def wrap_coerce_to_wrong_type():
        return scalar_config_solid.alias('layer2')()

    @composite_solid(
        config={'nesting_override': Field(int)},
        config_fn=lambda cfg: {'layer1': {'config': {'override_str': cfg['nesting_override']}}},
    )
    def nesting_wrap_wrong_type_at_leaf():
        return wrap_coerce_to_wrong_type.alias('layer1')()

    @pipeline
    def wrap_pipeline_with_error():
        return nesting_wrap_wrong_type_at_leaf.alias('layer0')()

    with pytest.raises(DagsterInvalidConfigError) as exc_info:
        composite_descent(
            wrap_pipeline_with_error, {'layer0': {'config': {'nesting_override': 214}}}
        )

    assert 'In pipeline wrap_pipeline_with_error at stack layer0:layer1:' in str(exc_info.value)

    assert (
        'Solid "layer1" with definition "wrap_coerce_to_wrong_type" has a configuration error.'
        in str(exc_info.value)
    )

    assert (
        '''Error 1: Type failure at path "root:layer2:config" on type "String". '''
        '''Value at path root:layer2:config is not valid. Expected "String".'''
        in str(exc_info.value)
    )


def test_new_nested_solids_no_mapping():
    result = execute_pipeline(no_wrap_pipeline, get_fully_unwrapped_config())

    assert result.success
    assert result.result_for_handle('layer0.layer1.layer2').output_value() == 'blah'


def test_new_multiple_overrides_pipeline():

    result = execute_pipeline(
        wrap_pipeline,
        {
            'solids': {'layer0': {'config': {'nesting_override': 'blah'}}},
            'loggers': {'console': {'config': {'log_level': 'ERROR'}}},
        },
    )

    assert result.success
    assert result.result_for_handle('layer0.layer1.layer2').output_value() == 'blah'


def test_config_mapped_enum():
    from enum import Enum as PythonEnum

    class TestPythonEnum(PythonEnum):
        VALUE_ONE = 0
        OTHER = 1

    DagsterEnumType = Enum(
        'TestEnum',
        [
            EnumValue('VALUE_ONE', TestPythonEnum.VALUE_ONE),
            EnumValue('OTHER', TestPythonEnum.OTHER),
        ],
    )

    @solid(config={'enum': DagsterEnumType})
    def return_enum(context):
        return context.solid_config['enum']

    @composite_solid(
        config={'num': int},
        config_fn=lambda cfg: {
            'return_enum': {'config': {'enum': 'VALUE_ONE' if cfg['num'] == 1 else 'OTHER'}}
        },
    )
    def wrapping_return_enum():
        return return_enum()

    @pipeline
    def wrapping_return_enum_pipeline():
        wrapping_return_enum()

    assert (
        execute_pipeline(
            wrapping_return_enum_pipeline,
            {'solids': {'wrapping_return_enum': {'config': {'num': 1}}}},
        ).output_for_solid('wrapping_return_enum')
        == TestPythonEnum.VALUE_ONE
    )

    assert (
        execute_pipeline(
            wrapping_return_enum_pipeline,
            {'solids': {'wrapping_return_enum': {'config': {'num': -11}}}},
        ).output_for_solid('wrapping_return_enum')
        == TestPythonEnum.OTHER
    )

    @solid(config={'num': int})
    def return_int(context):
        return context.solid_config['num']

    @composite_solid(
        config={'enum': DagsterEnumType},
        config_fn=lambda cfg: {
            'return_int': {'config': {'num': 1 if cfg['enum'] == TestPythonEnum.VALUE_ONE else 2}}
        },
    )
    def wrap_return_int():
        return return_int()

    @pipeline
    def wrap_return_int_pipeline():
        wrap_return_int()

    assert (
        execute_pipeline(
            wrap_return_int_pipeline,
            {'solids': {'wrap_return_int': {'config': {'enum': 'VALUE_ONE'}}}},
        ).output_for_solid('wrap_return_int')
        == 1
    )

    assert (
        execute_pipeline(
            wrap_return_int_pipeline,
            {'solids': {'wrap_return_int': {'config': {'enum': 'OTHER'}}}},
        ).output_for_solid('wrap_return_int')
        == 2
    )

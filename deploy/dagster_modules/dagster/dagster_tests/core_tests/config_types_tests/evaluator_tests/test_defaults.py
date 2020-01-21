import pytest

from dagster import Any, Enum, EnumValue, Field, Noneable, Permissive, String
from dagster.check import CheckError, ParameterCheckError
from dagster.config.config_type import ConfigType, ConfigTypeKind
from dagster.config.field import resolve_to_config_type
from dagster.config.field_utils import Selector
from dagster.config.post_process import post_process_config


def test_post_process_config():
    scalar_config_type = resolve_to_config_type(String)
    assert post_process_config(scalar_config_type, 'foo') == 'foo'
    assert post_process_config(scalar_config_type, 3) == 3
    assert post_process_config(scalar_config_type, {}) == {}
    assert post_process_config(scalar_config_type, None) is None

    enum_config_type = resolve_to_config_type(
        Enum('an_enum', [EnumValue('foo'), EnumValue('bar', python_value=3)])
    )
    assert post_process_config(enum_config_type, 'foo') == 'foo'
    assert post_process_config(enum_config_type, 'bar') == 3
    with pytest.raises(CheckError, match='config_value should be pre-validated'):
        post_process_config(enum_config_type, 'baz')
    with pytest.raises(CheckError, match='config_value should be pre-validated'):
        post_process_config(enum_config_type, None)

    list_config_type = resolve_to_config_type([str])

    assert post_process_config(list_config_type, ['foo']) == ['foo']
    assert post_process_config(list_config_type, None) == []
    with pytest.raises(CheckError, match='Null array member not caught'):
        assert post_process_config(list_config_type, [None]) == [None]

    nullable_list_config_type = resolve_to_config_type([Noneable(str)])
    assert post_process_config(nullable_list_config_type, ['foo']) == ['foo']
    assert post_process_config(nullable_list_config_type, [None]) == [None]
    assert post_process_config(nullable_list_config_type, None) == []

    composite_config_type = resolve_to_config_type(
        {
            'foo': String,
            'bar': {'baz': [str]},
            'quux': Field(str, is_optional=True, default_value='zip'),
            'quiggle': Field(str, is_optional=True),
        }
    )

    with pytest.raises(CheckError, match='Missing non-optional composite member'):
        post_process_config(composite_config_type, {})

    with pytest.raises(CheckError, match='Missing non-optional composite member'):
        post_process_config(composite_config_type, {'bar': {'baz': ['giraffe']}, 'quux': 'nimble'})

    with pytest.raises(CheckError, match='Missing non-optional composite member'):
        post_process_config(composite_config_type, {'foo': 'zowie', 'quux': 'nimble'})

    assert post_process_config(
        composite_config_type, {'foo': 'zowie', 'bar': {'baz': ['giraffe']}, 'quux': 'nimble'}
    ) == {'foo': 'zowie', 'bar': {'baz': ['giraffe']}, 'quux': 'nimble'}

    assert post_process_config(
        composite_config_type, {'foo': 'zowie', 'bar': {'baz': ['giraffe']}}
    ) == {'foo': 'zowie', 'bar': {'baz': ['giraffe']}, 'quux': 'zip'}

    assert post_process_config(
        composite_config_type, {'foo': 'zowie', 'bar': {'baz': ['giraffe']}, 'quiggle': 'squiggle'}
    ) == {'foo': 'zowie', 'bar': {'baz': ['giraffe']}, 'quux': 'zip', 'quiggle': 'squiggle'}

    nested_composite_config_type = resolve_to_config_type(
        {
            'fruts': {
                'apple': Field(String),
                'banana': Field(String, is_optional=True),
                'potato': Field(String, is_optional=True, default_value='pie'),
            }
        }
    )

    with pytest.raises(CheckError, match='Missing non-optional composite member'):
        post_process_config(nested_composite_config_type, {'fruts': None})

    with pytest.raises(CheckError, match='Missing non-optional composite member'):
        post_process_config(
            nested_composite_config_type, {'fruts': {'banana': 'good', 'potato': 'bad'}}
        )

    assert post_process_config(
        nested_composite_config_type, {'fruts': {'apple': 'strawberry'}}
    ) == {'fruts': {'apple': 'strawberry', 'potato': 'pie'}}

    assert post_process_config(
        nested_composite_config_type, {'fruts': {'apple': 'a', 'banana': 'b', 'potato': 'c'}}
    ) == {'fruts': {'apple': 'a', 'banana': 'b', 'potato': 'c'}}

    any_config_type = resolve_to_config_type(Any)

    assert post_process_config(any_config_type, {'foo': 'bar'}) == {'foo': 'bar'}

    assert post_process_config(
        ConfigType('gargle', given_name='bargle', kind=ConfigTypeKind.ANY), 3
    )

    selector_config_type = resolve_to_config_type(
        Selector(
            {
                'one': Field(String),
                'another': {'foo': Field(String, default_value='bar', is_optional=True)},
                'yet_another': Field(String, default_value='quux', is_optional=True),
            }
        )
    )

    with pytest.raises(CheckError):
        post_process_config(selector_config_type, 'one')

    with pytest.raises(ParameterCheckError):
        post_process_config(selector_config_type, None)

    with pytest.raises(ParameterCheckError, match='Expected dict with single item'):
        post_process_config(selector_config_type, {})

    with pytest.raises(CheckError):
        post_process_config(selector_config_type, {'one': 'foo', 'another': 'bar'})

    assert post_process_config(selector_config_type, {'one': 'foo'}) == {'one': 'foo'}

    assert post_process_config(selector_config_type, {'one': None}) == {'one': None}

    assert post_process_config(selector_config_type, {'one': {}}) == {'one': {}}

    assert post_process_config(selector_config_type, {'another': {}}) == {'another': {'foo': 'bar'}}

    singleton_selector_config_type = resolve_to_config_type(
        Selector({'foo': Field(String, default_value='bar', is_optional=True)})
    )

    assert post_process_config(singleton_selector_config_type, None) == {'foo': 'bar'}

    permissive_dict_config_type = resolve_to_config_type(
        Permissive(
            {'foo': Field(String), 'bar': Field(String, default_value='baz', is_optional=True)}
        )
    )

    with pytest.raises(CheckError, match='Missing non-optional composite member'):
        post_process_config(permissive_dict_config_type, None)

    assert post_process_config(permissive_dict_config_type, {'foo': 'wow', 'mau': 'mau'}) == {
        'foo': 'wow',
        'bar': 'baz',
        'mau': 'mau',
    }

from dagster.core.decorator_utils import (
    InvalidDecoratedFunctionInfo,
    split_function_parameters,
    validate_decorated_fn_non_positionals,
    validate_decorated_fn_positionals,
)


def decorated_function_one_positional():
    def foo(bar):
        return bar

    return foo


def decorated_function_two_positionals_one_kwarg():
    def foo_kwarg(bar, baz, qux=True):
        return bar, baz, qux

    return foo_kwarg


def test_get_function_positional_parameters_ok():
    positionals, non_positionals = split_function_parameters(
        decorated_function_one_positional(), ['bar']
    )
    validate_decorated_fn_positionals(positionals, ['bar'])
    validate_decorated_fn_non_positionals(set(), non_positionals)
    assert 'bar' in {positional.name for positional in positionals}
    assert not non_positionals


def test_get_function_positional_parameters_multiple():
    positionals, non_positionals = split_function_parameters(
        decorated_function_two_positionals_one_kwarg(), ['bar', 'baz']
    )
    validate_decorated_fn_positionals(positionals, ['bar', 'baz'])
    validate_decorated_fn_non_positionals({'qux'}, non_positionals)
    assert {positional.name for positional in positionals} == {'bar', 'baz'}
    assert {non_positional.name for non_positional in non_positionals} == {'qux'}


def test_get_function_positional_parameters_invalid():
    positionals, _ = split_function_parameters(decorated_function_one_positional(), ['bat'])
    assert validate_decorated_fn_positionals(positionals, ['bat']) == 'bat'


def test_get_function_non_positional_parameters_invalid():
    _, non_positionals = split_function_parameters(
        decorated_function_two_positionals_one_kwarg(), ['bar', 'baz']
    )
    invalid_function_info = validate_decorated_fn_non_positionals(set(), non_positionals)
    assert invalid_function_info.error_type == InvalidDecoratedFunctionInfo.TYPES['missing_name']

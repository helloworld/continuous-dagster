import collections
from typing import Any, Dict, List, Optional, Tuple, Union

import pytest

from dagster import (
    DagsterInvalidDefinitionError,
    InputDefinition,
    Int,
    as_dagster_type,
    composite_solid,
    dagster_type,
    execute_solid,
    lambda_solid,
    solid,
)


def test_single_typed_input():
    @solid
    def add_one_infer(_context, num: int):
        return num + 1

    @solid(input_defs=[InputDefinition('num', Int)])
    def add_one_ex(_context, num):
        return num + 1

    assert len(add_one_infer.input_defs) == 1

    assert add_one_ex.input_defs[0].name == add_one_infer.input_defs[0].name
    assert (
        add_one_ex.input_defs[0].runtime_type.name == add_one_infer.input_defs[0].runtime_type.name
    )


def test_precedence():
    @solid(input_defs=[InputDefinition('num', Int)])
    def add_one(_context, num: Any):
        return num + 1

    assert add_one.input_defs[0].runtime_type.name == 'Int'


def test_double_typed_input():
    @solid
    def subtract(_context, num_one: int, num_two: int):
        return num_one + num_two

    assert subtract
    assert len(subtract.input_defs) == 2
    assert subtract.input_defs[0].name == 'num_one'
    assert subtract.input_defs[0].runtime_type.name == 'Int'

    assert subtract.input_defs[1].name == 'num_two'
    assert subtract.input_defs[1].runtime_type.name == 'Int'


def test_one_arg_typed_lambda_solid():
    @lambda_solid
    def one_arg(num: int):
        return num

    assert one_arg
    assert len(one_arg.input_defs) == 1
    assert one_arg.input_defs[0].name == 'num'
    assert one_arg.input_defs[0].runtime_type.name == 'Int'
    assert len(one_arg.output_defs) == 1


def test_single_typed_input_and_output():
    @solid
    def add_one(_context, num: int) -> int:
        return num + 1

    assert add_one
    assert len(add_one.input_defs) == 1
    assert add_one.input_defs[0].name == 'num'
    assert add_one.input_defs[0].runtime_type.name == 'Int'

    assert len(add_one.output_defs) == 1
    assert add_one.output_defs[0].runtime_type.name == 'Int'


def test_single_typed_input_and_output_lambda():
    @lambda_solid
    def add_one(num: int) -> int:
        return num + 1

    assert add_one
    assert len(add_one.input_defs) == 1
    assert add_one.input_defs[0].name == 'num'
    assert add_one.input_defs[0].runtime_type.name == 'Int'

    assert len(add_one.output_defs) == 1
    assert add_one.output_defs[0].runtime_type.name == 'Int'


def test_wrapped_input_and_output_lambda():
    @lambda_solid
    def add_one(nums: List[int]) -> Optional[List[int]]:
        return [num + 1 for num in nums]

    assert add_one
    assert len(add_one.input_defs) == 1
    assert add_one.input_defs[0].name == 'nums'
    assert add_one.input_defs[0].runtime_type.is_list
    assert add_one.input_defs[0].runtime_type.inner_type.name == 'Int'

    assert len(add_one.output_defs) == 1
    assert add_one.output_defs[0].runtime_type.is_nullable
    assert add_one.output_defs[0].runtime_type.inner_type.is_list


def test_autowrapping_python_types():
    class Foo(object):
        pass

    @lambda_solid
    def _test_non_dagster_class_input(num: Foo):
        return num

    @lambda_solid
    def _test_non_dagster_class_output() -> Foo:
        return 1

    # Optional[X] is represented as Union[X, NoneType] - test that we throw on other Unions
    with pytest.raises(DagsterInvalidDefinitionError):

        @lambda_solid
        def _test_union_not_optional(num: Union[int, str]):
            return num


def test_kitchen_sink():
    @dagster_type
    class Custom(object):
        pass

    @lambda_solid
    def sink(
        n: int, f: float, b: bool, s: str, x: Any, o: Optional[str], l: List[str], c: Custom
    ):  # pylint: disable=unused-argument
        pass

    assert sink.input_defs[0].name == 'n'
    assert sink.input_defs[0].runtime_type.name == 'Int'

    assert sink.input_defs[1].name == 'f'
    assert sink.input_defs[1].runtime_type.name == 'Float'

    assert sink.input_defs[2].name == 'b'
    assert sink.input_defs[2].runtime_type.name == 'Bool'

    assert sink.input_defs[3].name == 's'
    assert sink.input_defs[3].runtime_type.name == 'String'

    assert sink.input_defs[4].name == 'x'
    assert sink.input_defs[4].runtime_type.name == 'Any'

    assert sink.input_defs[5].name == 'o'
    assert sink.input_defs[5].runtime_type.is_nullable

    assert sink.input_defs[6].name == 'l'
    assert sink.input_defs[6].runtime_type.is_list

    assert sink.input_defs[7].name == 'c'
    assert sink.input_defs[7].runtime_type.name == 'Custom'


def test_composites():
    @lambda_solid
    def emit_one() -> int:
        return 1

    @lambda_solid
    def subtract(n1: int, n2: int) -> int:
        return n1 - n2

    @composite_solid
    def add_one(a: int) -> int:
        return subtract(a, emit_one())

    assert add_one.input_mappings


def test_emit_dict():
    @lambda_solid
    def emit_dict() -> dict:
        return {'foo': 'bar'}

    solid_result = execute_solid(emit_dict)

    assert solid_result.output_value() == {'foo': 'bar'}


def test_dict_input():
    @lambda_solid
    def intake_dict(inp: dict) -> str:
        return inp['foo']

    solid_result = execute_solid(intake_dict, input_values={'inp': {'foo': 'bar'}})
    assert solid_result.output_value() == 'bar'


def test_emit_dagster_dict():
    @lambda_solid
    def emit_dagster_dict() -> Dict:
        return {'foo': 'bar'}

    solid_result = execute_solid(emit_dagster_dict)

    assert solid_result.output_value() == {'foo': 'bar'}


def test_dict_dagster_input():
    @lambda_solid
    def intake_dagster_dict(inp: Dict) -> str:
        return inp['foo']

    solid_result = execute_solid(intake_dagster_dict, input_values={'inp': {'foo': 'bar'}})
    assert solid_result.output_value() == 'bar'


def test_python_tuple_input():
    @lambda_solid
    def intake_tuple(inp: tuple) -> int:
        return inp[1]

    assert execute_solid(intake_tuple, input_values={'inp': (3, 4)}).output_value() == 4


def test_python_tuple_output():
    @lambda_solid
    def emit_tuple() -> tuple:
        return (4, 5)

    assert execute_solid(emit_tuple).output_value() == (4, 5)


def test_python_built_in_output():
    class MyOrderedDict(collections.OrderedDict):
        pass

    OrderedDict = as_dagster_type(MyOrderedDict)

    @lambda_solid
    def emit_ordered_dict() -> OrderedDict:
        return OrderedDict([('foo', 'bar')])

    output_value = execute_solid(emit_ordered_dict).output_value()
    assert output_value == OrderedDict([('foo', 'bar')])
    assert isinstance(output_value, OrderedDict)
    assert isinstance(output_value, MyOrderedDict)
    assert isinstance(output_value, collections.OrderedDict)


def test_python_autowrap_built_in_output():
    @lambda_solid
    def emit_counter() -> collections.Counter:
        return collections.Counter([1, 1, 2])

    output_value = execute_solid(emit_counter).output_value()
    assert output_value == collections.Counter([1, 1, 2])
    assert isinstance(output_value, collections.Counter)


def test_nested_kitchen_sink():
    @lambda_solid
    def no_execute() -> Optional[List[Tuple[List[int], str, Dict[str, Optional[List[str]]]]]]:
        pass

    assert (
        no_execute.output_defs[0].runtime_type.display_name
        == '[Tuple[[Int],String,Dict[String,[String]?]]]?'
    )

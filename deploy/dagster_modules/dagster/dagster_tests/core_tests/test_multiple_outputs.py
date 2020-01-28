import pytest

from dagster import (
    DagsterInstance,
    DagsterInvariantViolationError,
    DagsterStepOutputNotFoundError,
    ExecutionTargetHandle,
    InputDefinition,
    Output,
    OutputDefinition,
    execute_pipeline,
    pipeline,
    solid,
)


def test_multiple_outputs():
    @solid(
        name='multiple_outputs',
        input_defs=[],
        output_defs=[OutputDefinition(name='output_one'), OutputDefinition(name='output_two')],
    )
    def multiple_outputs(_):
        yield Output(output_name='output_one', value='foo')
        yield Output(output_name='output_two', value='bar')

    @pipeline
    def muptiple_outputs_pipeline():
        multiple_outputs()

    result = execute_pipeline(muptiple_outputs_pipeline)
    solid_result = result.solid_result_list[0]

    assert solid_result.solid.name == 'multiple_outputs'
    assert solid_result.output_value('output_one') == 'foo'
    assert solid_result.output_value('output_two') == 'bar'

    with pytest.raises(
        DagsterInvariantViolationError,
        match="Output 'not_defined' not defined in solid 'multiple_outputs'",
    ):
        solid_result.output_value('not_defined')


def test_wrong_multiple_output():
    @solid(
        name='multiple_outputs', input_defs=[], output_defs=[OutputDefinition(name='output_one')]
    )
    def multiple_outputs(_):
        yield Output(output_name='mismatch', value='foo')

    @pipeline
    def wrong_multiple_outputs_pipeline():
        multiple_outputs()

    with pytest.raises(DagsterInvariantViolationError):
        execute_pipeline(wrong_multiple_outputs_pipeline)


def test_multiple_outputs_of_same_name_disallowed():
    # make this illegal until it is supported

    @solid(
        name='multiple_outputs', input_defs=[], output_defs=[OutputDefinition(name='output_one')]
    )
    def multiple_outputs(_):
        yield Output(output_name='output_one', value='foo')
        yield Output(output_name='output_one', value='foo')

    @pipeline
    def muptiple_outputs_pipeline():
        multiple_outputs()

    with pytest.raises(DagsterInvariantViolationError):
        execute_pipeline(muptiple_outputs_pipeline)


def define_multi_out():
    @solid(
        name='multiple_outputs',
        input_defs=[],
        output_defs=[
            OutputDefinition(name='output_one'),
            OutputDefinition(name='output_two', is_optional=True),
        ],
    )
    def multiple_outputs(_):
        yield Output(output_name='output_one', value='foo')

    @solid(name='downstream_one', input_defs=[InputDefinition('some_input')], output_defs=[])
    def downstream_one(_, some_input):
        del some_input

    @solid
    def downstream_two(_, some_input):
        del some_input
        raise Exception('do not call me')

    @pipeline
    def multiple_outputs_only_emit_one_pipeline():
        output_one, output_two = multiple_outputs()
        downstream_one(output_one)
        downstream_two(output_two)

    return multiple_outputs_only_emit_one_pipeline


def test_multiple_outputs_only_emit_one():
    result = execute_pipeline(define_multi_out())
    assert result.success

    solid_result = result.result_for_solid('multiple_outputs')
    assert set(solid_result.output_values.keys()) == set(['output_one'])

    with pytest.raises(
        DagsterInvariantViolationError,
        match="Output 'not_defined' not defined in solid 'multiple_outputs'",
    ):
        solid_result.output_value('not_defined')

    with pytest.raises(DagsterInvariantViolationError, match='Did not find result output_two'):
        solid_result.output_value('output_two')

    with pytest.raises(
        DagsterInvariantViolationError,
        match=(
            'Tried to get result for solid not_present in multiple_outputs_only_emit_one_pipeline. '
            'No such top level solid.'
        ),
    ):
        result.result_for_solid('not_present')

    assert result.result_for_solid('downstream_two').skipped


def test_multiple_outputs_only_emit_one_multiproc():
    pipe = ExecutionTargetHandle.for_pipeline_python_file(
        __file__, 'define_multi_out'
    ).build_pipeline_definition()
    result = execute_pipeline(
        pipe,
        environment_dict={'storage': {'filesystem': {}}, 'execution': {'multiprocess': {}}},
        instance=DagsterInstance.local_temp(),
    )
    assert result.success

    solid_result = result.result_for_solid('multiple_outputs')
    assert set(solid_result.output_values.keys()) == set(['output_one'])

    with pytest.raises(
        DagsterInvariantViolationError,
        match="Output 'not_defined' not defined in solid 'multiple_outputs'",
    ):
        solid_result.output_value('not_defined')

    with pytest.raises(DagsterInvariantViolationError, match='Did not find result output_two'):
        solid_result.output_value('output_two')

    with pytest.raises(
        DagsterInvariantViolationError,
        match=(
            'Tried to get result for solid not_present in multiple_outputs_only_emit_one_pipeline. '
            'No such top level solid.'
        ),
    ):
        result.result_for_solid('not_present')

    assert result.result_for_solid('downstream_two').skipped


def test_missing_non_optional_output_fails():
    @solid(
        name='multiple_outputs',
        input_defs=[],
        output_defs=[OutputDefinition(name='output_one'), OutputDefinition(name='output_two')],
    )
    def multiple_outputs(_):
        yield Output(output_name='output_one', value='foo')

    @pipeline
    def missing_non_optional_pipeline():
        multiple_outputs()

    with pytest.raises(DagsterStepOutputNotFoundError):
        execute_pipeline(missing_non_optional_pipeline)

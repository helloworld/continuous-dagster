# pylint doesn't know about pytest fixtures
# pylint: disable=unused-argument

import os
from contextlib import contextmanager

import pytest
from dagster_celery import celery_executor

from dagster import (
    CompositeSolidExecutionResult,
    ExecutionTargetHandle,
    InputDefinition,
    Int,
    ModeDefinition,
    Output,
    OutputDefinition,
    PipelineExecutionResult,
    SolidExecutionResult,
    default_executors,
    execute_pipeline,
    execute_pipeline_iterator,
    lambda_solid,
    pipeline,
    seven,
    solid,
)
from dagster.core.instance import DagsterInstance
from dagster.core.test_utils import nesting_composite_pipeline

celery_mode_defs = [ModeDefinition(executor_defs=default_executors + [celery_executor])]

BUILDKITE = os.getenv('BUILDKITE')
skip_ci = pytest.mark.skipif(
    bool(BUILDKITE),
    reason='Tests hang forever on buildkite for reasons we don\'t currently understand',
)


@solid
def simple(_):
    return 1


@solid
def add_one(_, num):
    return num + 1


@pipeline(mode_defs=celery_mode_defs)
def test_pipeline():
    return simple()


@pipeline(mode_defs=celery_mode_defs)
def test_serial_pipeline():
    return add_one(simple())


@solid(output_defs=[OutputDefinition(name='value_one'), OutputDefinition(name='value_two')])
def emit_values(_context):
    yield Output(1, 'value_one')
    yield Output(2, 'value_two')


@lambda_solid(input_defs=[InputDefinition('num_one'), InputDefinition('num_two')])
def subtract(num_one, num_two):
    return num_one - num_two


@pipeline(mode_defs=celery_mode_defs)
def test_diamond_pipeline():
    value_one, value_two = emit_values()
    return subtract(num_one=add_one(num=value_one), num_two=add_one.alias('renamed')(num=value_two))


@pipeline(mode_defs=celery_mode_defs)
def test_parallel_pipeline():
    value = simple()
    for i in range(10):
        add_one.alias('add_one_' + str(i))(value)


@pipeline(mode_defs=celery_mode_defs)
def test_more_parallel_pipeline():
    value = simple()
    for i in range(500):
        add_one.alias('add_one_' + str(i))(value)


def composite_pipeline():
    return nesting_composite_pipeline(6, 2, mode_defs=celery_mode_defs)


@solid(
    output_defs=[
        OutputDefinition(Int, 'out_1', is_optional=True),
        OutputDefinition(Int, 'out_2', is_optional=True),
        OutputDefinition(Int, 'out_3', is_optional=True),
    ]
)
def foo(_):
    yield Output(1, 'out_1')


@solid
def bar(_, input_arg):
    return input_arg


@pipeline(mode_defs=celery_mode_defs)
def test_optional_outputs():
    # pylint: disable=no-member
    foo_res = foo()
    bar.alias('first_consumer')(input_arg=foo_res.out_1)
    bar.alias('second_consumer')(input_arg=foo_res.out_2)
    bar.alias('third_consumer')(input_arg=foo_res.out_3)


@solid
def fails(_):
    raise Exception('argjhgjh')


@pipeline(mode_defs=celery_mode_defs)
def test_fails():
    fails()


def events_of_type(result, event_type):
    return [event for event in result.event_list if event.event_type_value == event_type]


@contextmanager
def execute_pipeline_on_celery(pipeline_name,):
    with seven.TemporaryDirectory() as tempdir:
        result = execute_pipeline(
            ExecutionTargetHandle.for_pipeline_python_file(
                __file__, pipeline_name
            ).build_pipeline_definition(),
            environment_dict={
                'storage': {'filesystem': {'config': {'base_dir': tempdir}}},
                'execution': {'celery': {}},
            },
            instance=DagsterInstance.local_temp(tempdir=tempdir),
        )
        yield result


@contextmanager
def execute_eagerly_on_celery(pipeline_name):
    with seven.TemporaryDirectory() as tempdir:
        result = execute_pipeline(
            ExecutionTargetHandle.for_pipeline_python_file(
                __file__, pipeline_name
            ).build_pipeline_definition(),
            environment_dict={
                'storage': {'filesystem': {'config': {'base_dir': tempdir}}},
                'execution': {'celery': {'config': {'config_source': {'task_always_eager': True}}}},
            },
            instance=DagsterInstance.local_temp(tempdir=tempdir),
        )
        yield result


@skip_ci
def test_execute_on_celery(dagster_celery_worker):
    with execute_pipeline_on_celery('test_pipeline') as result:
        assert result.result_for_solid('simple').output_value() == 1
        assert len(result.step_event_list) == 4
        assert len(events_of_type(result, 'STEP_START')) == 1
        assert len(events_of_type(result, 'STEP_OUTPUT')) == 1
        assert len(events_of_type(result, 'OBJECT_STORE_OPERATION')) == 1
        assert len(events_of_type(result, 'STEP_SUCCESS')) == 1


@skip_ci
def test_execute_serial_on_celery(dagster_celery_worker):
    with execute_pipeline_on_celery('test_serial_pipeline') as result:
        assert result.result_for_solid('simple').output_value() == 1
        assert result.result_for_solid('add_one').output_value() == 2
        assert len(result.step_event_list) == 10
        assert len(events_of_type(result, 'STEP_START')) == 2
        assert len(events_of_type(result, 'STEP_INPUT')) == 1
        assert len(events_of_type(result, 'STEP_OUTPUT')) == 2
        assert len(events_of_type(result, 'OBJECT_STORE_OPERATION')) == 3
        assert len(events_of_type(result, 'STEP_SUCCESS')) == 2


@skip_ci
def test_execute_diamond_pipeline_on_celery(dagster_celery_worker):
    with execute_pipeline_on_celery('test_diamond_pipeline') as result:
        assert result.result_for_solid('emit_values').output_values == {
            'value_one': 1,
            'value_two': 2,
        }
        assert result.result_for_solid('add_one').output_value() == 2
        assert result.result_for_solid('renamed').output_value() == 3
        assert result.result_for_solid('subtract').output_value() == -1


@skip_ci
def test_execute_parallel_pipeline_on_celery(dagster_celery_worker):
    with execute_pipeline_on_celery('test_parallel_pipeline') as result:
        assert len(result.solid_result_list) == 11


@skip_ci
@pytest.mark.skip
def test_execute_more_parallel_pipeline_on_celery():
    with execute_pipeline_on_celery('test_more_parallel_pipeline') as result:
        assert len(result.solid_result_list) == 501


@skip_ci
def test_execute_composite_pipeline_on_celery(dagster_celery_worker):
    with execute_pipeline_on_celery('composite_pipeline') as result:
        assert result.success
        assert isinstance(result, PipelineExecutionResult)
        assert len(result.solid_result_list) == 1
        composite_solid_result = result.solid_result_list[0]
        assert len(composite_solid_result.solid_result_list) == 2
        for r in composite_solid_result.solid_result_list:
            assert isinstance(r, CompositeSolidExecutionResult)
        composite_solid_results = composite_solid_result.solid_result_list
        for i in range(6):
            next_level = []
            assert len(composite_solid_results) == pow(2, i + 1)
            for res in composite_solid_results:
                assert isinstance(res, CompositeSolidExecutionResult)
                for r in res.solid_result_list:
                    next_level.append(r)
            composite_solid_results = next_level
        assert len(composite_solid_results) == 128
        assert all(
            (isinstance(r, SolidExecutionResult) and r.success for r in composite_solid_results)
        )


@skip_ci
def test_execute_optional_outputs_pipeline_on_celery(dagster_celery_worker):
    with execute_pipeline_on_celery('test_optional_outputs') as result:
        assert len(result.solid_result_list) == 4
        assert sum([int(x.skipped) for x in result.solid_result_list]) == 2
        assert sum([int(x.success) for x in result.solid_result_list]) == 2


@skip_ci
def test_execute_fails_pipeline_on_celery(dagster_celery_worker):
    with execute_pipeline_on_celery('test_fails') as result:
        assert len(result.solid_result_list) == 1
        assert not result.solid_result_list[0].success
        assert result.solid_result_list[0].failure_data.error.message == 'Exception: argjhgjh\n'


def test_execute_eagerly_on_celery():
    with execute_eagerly_on_celery('test_pipeline') as result:
        assert result.result_for_solid('simple').output_value() == 1
        assert len(result.step_event_list) == 4
        assert len(events_of_type(result, 'STEP_START')) == 1
        assert len(events_of_type(result, 'STEP_OUTPUT')) == 1
        assert len(events_of_type(result, 'OBJECT_STORE_OPERATION')) == 1
        assert len(events_of_type(result, 'STEP_SUCCESS')) == 1


def test_execute_eagerly_serial_on_celery():
    with execute_eagerly_on_celery('test_serial_pipeline') as result:
        assert result.result_for_solid('simple').output_value() == 1
        assert result.result_for_solid('add_one').output_value() == 2
        assert len(result.step_event_list) == 10
        assert len(events_of_type(result, 'STEP_START')) == 2
        assert len(events_of_type(result, 'STEP_INPUT')) == 1
        assert len(events_of_type(result, 'STEP_OUTPUT')) == 2
        assert len(events_of_type(result, 'OBJECT_STORE_OPERATION')) == 3
        assert len(events_of_type(result, 'STEP_SUCCESS')) == 2


def test_execute_eagerly_diamond_pipeline_on_celery():
    with execute_eagerly_on_celery('test_diamond_pipeline') as result:
        assert result.result_for_solid('emit_values').output_values == {
            'value_one': 1,
            'value_two': 2,
        }
        assert result.result_for_solid('add_one').output_value() == 2
        assert result.result_for_solid('renamed').output_value() == 3
        assert result.result_for_solid('subtract').output_value() == -1


def test_execute_eagerly_parallel_pipeline_on_celery():
    with execute_eagerly_on_celery('test_parallel_pipeline') as result:
        assert len(result.solid_result_list) == 11


@pytest.mark.skip
def test_execute_eagerly_more_parallel_pipeline_on_celery():
    with execute_eagerly_on_celery('test_more_parallel_pipeline') as result:
        assert len(result.solid_result_list) == 501


def test_execute_eagerly_composite_pipeline_on_celery():
    with execute_eagerly_on_celery('composite_pipeline') as result:
        assert result.success
        assert isinstance(result, PipelineExecutionResult)
        assert len(result.solid_result_list) == 1
        composite_solid_result = result.solid_result_list[0]
        assert len(composite_solid_result.solid_result_list) == 2
        for r in composite_solid_result.solid_result_list:
            assert isinstance(r, CompositeSolidExecutionResult)
        composite_solid_results = composite_solid_result.solid_result_list
        for i in range(6):
            next_level = []
            assert len(composite_solid_results) == pow(2, i + 1)
            for res in composite_solid_results:
                assert isinstance(res, CompositeSolidExecutionResult)
                for r in res.solid_result_list:
                    next_level.append(r)
            composite_solid_results = next_level
        assert len(composite_solid_results) == 128
        assert all(
            (isinstance(r, SolidExecutionResult) and r.success for r in composite_solid_results)
        )


def test_execute_eagerly_optional_outputs_pipeline_on_celery():
    with execute_eagerly_on_celery('test_optional_outputs') as result:
        assert len(result.solid_result_list) == 4
        assert sum([int(x.skipped) for x in result.solid_result_list]) == 2
        assert sum([int(x.success) for x in result.solid_result_list]) == 2


def test_execute_eagerly_fails_pipeline_on_celery():
    with execute_eagerly_on_celery('test_fails') as result:
        assert len(result.solid_result_list) == 1
        assert not result.solid_result_list[0].success
        assert result.solid_result_list[0].failure_data.error.message == 'Exception: argjhgjh\n'


def test_bad_broker():
    event_stream = execute_pipeline_iterator(
        ExecutionTargetHandle.for_pipeline_python_file(
            __file__, 'test_diamond_pipeline'
        ).build_pipeline_definition(),
        environment_dict={
            'storage': {'filesystem': {}},
            'execution': {'celery': {'config': {'broker': 'bad@bad.bad'}}},
        },
        instance=DagsterInstance.local_temp(),
    )

    # ensure an engine event with an error is yielded if we cant connect to the broker
    saw_engine_error = False
    try:
        for event in event_stream:
            if event.is_engine_event:
                saw_engine_error = bool(event.engine_event_data.error)
    except Exception:  # pylint: disable=broad-except
        pass

    assert saw_engine_error

import os

from dagster_pandas.examples.pandas_hello_world.pipeline import (
    pandas_hello_world,
    pandas_hello_world_fails,
)

from dagster import execute_pipeline
from dagster.cli.pipeline import do_execute_command
from dagster.utils import script_relative_path


def test_execute_pipeline():
    environment = {
        'solids': {
            'sum_solid': {'inputs': {'num': {'csv': {'path': script_relative_path('num.csv')}}}}
        }
    }

    result = execute_pipeline(pandas_hello_world, environment_dict=environment)

    assert result.success

    assert result.result_for_solid('sum_solid').output_value().to_dict('list') == {
        'num1': [1, 3],
        'num2': [2, 4],
        'sum': [3, 7],
    }

    assert result.result_for_solid('sum_sq_solid').output_value().to_dict('list') == {
        'num1': [1, 3],
        'num2': [2, 4],
        'sum': [3, 7],
        'sum_sq': [9, 49],
    }


def test_cli_execute():

    # currently paths in env files have to be relative to where the
    # script has launched so we have to simulate that
    cwd = os.getcwd()
    try:

        os.chdir(script_relative_path('../..'))

        do_execute_command(
            pipeline=pandas_hello_world,
            env_file_list=[
                script_relative_path('../../dagster_pandas/examples/pandas_hello_world/*.yaml')
            ],
        )
    finally:
        # restore cwd
        os.chdir(cwd)


def test_cli_execute_failure():

    # currently paths in env files have to be relative to where the
    # script has launched so we have to simulate that
    # with pytest.raises(DagsterExecutionStepExecutionError) as e_info:
    cwd = os.getcwd()
    try:

        os.chdir(script_relative_path('../..'))

        result = do_execute_command(
            pipeline=pandas_hello_world_fails,
            env_file_list=[
                script_relative_path('../../dagster_pandas/examples/pandas_hello_world/*.yaml')
            ],
        )
        failures = [event for event in result.step_event_list if event.is_failure]
    finally:
        # restore cwd
        os.chdir(cwd)

    assert len(failures) == 1
    assert 'I am a programmer and I make error' in failures[0].step_failure_data.error.message

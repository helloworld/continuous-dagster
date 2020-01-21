import json
import logging
import re
from contextlib import contextmanager

import pytest

from dagster import ModeDefinition, check, execute_solid, pipeline, solid
from dagster.core.definitions import SolidHandle
from dagster.core.events import DagsterEvent
from dagster.core.execution.context.logger import InitLoggerContext
from dagster.core.execution.plan.objects import StepFailureData
from dagster.core.log_manager import DagsterLogManager
from dagster.loggers import colored_console_logger, json_console_logger
from dagster.utils.error import SerializableErrorInfo

REGEX_UUID = r'[a-z-0-9]{8}\-[a-z-0-9]{4}\-[a-z-0-9]{4}\-[a-z-0-9]{4}\-[a-z-0-9]{12}'
REGEX_TS = r'\d{4}\-\d{2}\-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}'

DAGSTER_DEFAULT_LOGGER = 'dagster'


@contextmanager
def _setup_logger(name, log_levels=None):
    '''Test helper that creates a new logger.

    Args:
        name (str): The name of the logger.
        log_levels (Optional[Dict[str, int]]): Any non-standard log levels to expose on the logger
            (e.g., logger.success)
    '''
    log_levels = check.opt_dict_param(log_levels, 'log_levels')

    class TestLogger(logging.Logger):  # py27 compat
        pass

    logger = TestLogger(name)

    captured_results = []

    def log_fn(msg, *args, **kwargs):  # pylint:disable=unused-argument
        captured_results.append(msg)

    def int_log_fn(lvl, msg, *args, **kwargs):  # pylint:disable=unused-argument
        captured_results.append(msg)

    for level in ['debug', 'info', 'warning', 'error', 'critical'] + list(
        [x.lower() for x in log_levels.keys()]
    ):
        setattr(logger, level, log_fn)
        setattr(logger, 'log', int_log_fn)

    yield (captured_results, logger)


def _regex_match_kv_pair(regex, kv_pairs):
    return any([re.match(regex, kv_pair) for kv_pair in kv_pairs])


def test_logging_no_loggers_registered():
    dl = DagsterLogManager('none', {}, [])
    dl.debug('test')
    dl.info('test')
    dl.warning('test')
    dl.error('test')
    dl.critical('test')


def test_logging_basic():
    with _setup_logger('test') as (captured_results, logger):

        dl = DagsterLogManager('123', {}, [logger])
        dl.debug('test')
        dl.info('test')
        dl.warning('test')
        dl.error('test')
        dl.critical('test')

        assert captured_results == ['system - 123 - test'] * 5


def test_logging_custom_log_levels():
    with _setup_logger('test', {'FOO': 3}) as (_captured_results, logger):

        dl = DagsterLogManager('123', {}, [logger])
        with pytest.raises(AttributeError):
            dl.foo('test')  # pylint: disable=no-member


def test_logging_integer_log_levels():
    with _setup_logger('test', {'FOO': 3}) as (_captured_results, logger):

        dl = DagsterLogManager('123', {}, [logger])
        with pytest.raises(AttributeError):
            dl.log(3, 'test')  # pylint: disable=no-member


def test_logging_bad_custom_log_levels():
    with _setup_logger('test') as (_, logger):

        dl = DagsterLogManager('123', {}, [logger])
        with pytest.raises(check.CheckError):
            dl._log('test', 'foobar', {})  # pylint: disable=protected-access


def test_multiline_logging_complex():
    msg = 'DagsterEventType.STEP_FAILURE for step start.materialization.output.result.0'
    kwargs = {
        'pipeline': 'example',
        'pipeline_name': 'example',
        'step_key': 'start.materialization.output.result.0',
        'solid': 'start',
        'solid_definition': 'emit_num',
        'dagster_event': DagsterEvent(
            event_type_value='STEP_FAILURE',
            pipeline_name='error_monster',
            step_key='start.materialization.output.result.0',
            solid_handle=SolidHandle('start', 'emit_num', None),
            step_kind_value='MATERIALIZATION_THUNK',
            logging_tags={
                'pipeline': 'error_monster',
                'step_key': 'start.materialization.output.result.0',
                'solid': 'start',
                'solid_definition': 'emit_num',
            },
            event_specific_data=StepFailureData(
                error=SerializableErrorInfo(
                    message="FileNotFoundError: [Errno 2] No such file or directory: '/path/to/file'\n",
                    stack=['a stack message'],
                    cls_name='FileNotFoundError',
                ),
                user_failure_data=None,
            ),
        ),
    }

    with _setup_logger(DAGSTER_DEFAULT_LOGGER) as (captured_results, logger):

        dl = DagsterLogManager('123', {}, [logger])
        dl.info(msg, **kwargs)

    expected_results = [
        'error_monster - 123 - STEP_FAILURE - DagsterEventType.STEP_FAILURE for step '
        'start.materialization.output.result.0',
        '            cls_name = "FileNotFoundError"',
        '               solid = "start"',
        '    solid_definition = "emit_num"',
        '            step_key = "start.materialization.output.result.0"',
        '',
        'a stack message',
    ]

    assert captured_results[0].split('\n') == expected_results


def test_default_context_logging():
    called = {}

    @solid(input_defs=[], output_defs=[])
    def default_context_solid(context):
        called['yes'] = True
        for logger in context.log.loggers:
            assert logger.level == logging.DEBUG

    execute_solid(default_context_solid)

    assert called['yes']


def test_colored_console_logger_with_integer_log_level():
    @pipeline
    def pipe():
        pass

    colored_console_logger.logger_fn(
        InitLoggerContext({'name': 'dagster', 'log_level': 4}, pipe, colored_console_logger, '')
    )


def test_json_console_logger(capsys):
    @solid
    def hello_world(context):
        context.log.info('Hello, world!')

    execute_solid(
        hello_world,
        mode_def=ModeDefinition(logger_defs={'json': json_console_logger}),
        environment_dict={'loggers': {'json': {'config': {}}}},
    )

    captured = capsys.readouterr()

    found_msg = False
    for line in captured.err.split('\n'):
        if line:
            parsed = json.loads(line)
            if parsed['dagster_meta']['orig_message'] == 'Hello, world!':
                found_msg = True

    assert found_msg

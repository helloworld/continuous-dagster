import os
import time

import pytest

from dagster.core.engine.child_process_executor import (
    ChildProcessCommand,
    ChildProcessCrashException,
    ChildProcessDoneEvent,
    ChildProcessEvent,
    ChildProcessStartEvent,
    ChildProcessSystemErrorEvent,
    execute_child_process_command,
)


class DoubleAStringChildProcessCommand(ChildProcessCommand):
    def __init__(self, a_str):
        self.a_str = a_str

    def execute(self):
        yield self.a_str + self.a_str


class AnError(Exception):
    pass


class ThrowAnErrorCommand(ChildProcessCommand):  # pylint: disable=no-init
    def execute(self):
        raise AnError('Oh noes!')


class CrashyCommand(ChildProcessCommand):  # pylint: disable=no-init
    def execute(self):
        # access inner API to simulate hard crash
        os._exit(1)  # pylint: disable=protected-access


class LongRunningCommand(ChildProcessCommand):  # pylint: disable=no-init
    def execute(self):
        time.sleep(1.0)
        yield 1


def test_basic_child_process_command():
    events = list(
        filter(
            lambda x: x and not isinstance(x, ChildProcessEvent),
            execute_child_process_command(DoubleAStringChildProcessCommand('aa')),
        )
    )
    assert events == ['aaaa']


def test_basic_child_process_command_with_process_events():
    events = list(
        filter(lambda x: x, execute_child_process_command(DoubleAStringChildProcessCommand('aa')))
    )
    assert len(events) == 3

    assert isinstance(events[0], ChildProcessStartEvent)
    child_pid = events[0].pid
    assert child_pid != os.getpid()
    assert events[1] == 'aaaa'
    assert isinstance(events[2], ChildProcessDoneEvent)
    assert events[2].pid == child_pid


def test_child_process_uncaught_exception():
    results = list(
        filter(
            lambda x: x and isinstance(x, ChildProcessSystemErrorEvent),
            execute_child_process_command(ThrowAnErrorCommand()),
        )
    )
    assert len(results) == 1

    assert 'AnError' in str(results[0].error_info.message)


def test_child_process_crashy_process():
    with pytest.raises(ChildProcessCrashException):
        list(execute_child_process_command(CrashyCommand()))


@pytest.mark.skip('too long')
def test_long_running_command():
    list(execute_child_process_command(LongRunningCommand()))

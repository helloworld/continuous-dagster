import hashlib
import os
from collections import defaultdict

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers.polling import PollingObserver

from dagster import check
from dagster.core.serdes import ConfigurableClass, ConfigurableClassData
from dagster.utils import ensure_dir, touch_file

from .compute_log_manager import (
    MAX_BYTES_FILE_READ,
    ComputeIOType,
    ComputeLogFileData,
    ComputeLogManager,
    ComputeLogSubscription,
)

WATCHDOG_POLLING_TIMEOUT = 2.5

IO_TYPE_EXTENSION = {ComputeIOType.STDOUT: 'out', ComputeIOType.STDERR: 'err'}

MAX_FILENAME_LENGTH = 255


class LocalComputeLogManager(ComputeLogManager, ConfigurableClass):
    def __init__(self, base_dir, inst_data=None):
        self._base_dir = base_dir
        self._subscription_manager = LocalComputeLogSubscriptionManager(self)
        self._inst_data = check.opt_inst_param(inst_data, 'inst_data', ConfigurableClassData)

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {'base_dir': str}

    @staticmethod
    def from_config_value(inst_data, config_value):
        return LocalComputeLogManager(inst_data=inst_data, **config_value)

    def _run_directory(self, run_id):
        return os.path.join(self._base_dir, run_id, 'compute_logs')

    def get_local_path(self, run_id, step_key, io_type):
        check.inst_param(io_type, 'io_type', ComputeIOType)
        return self._get_local_path(run_id, step_key, IO_TYPE_EXTENSION[io_type])

    def complete_artifact_path(self, run_id, step_key):
        return self._get_local_path(run_id, step_key, 'complete')

    def _get_local_path(self, run_id, step_key, extension):
        filename = "{}.{}".format(step_key, extension)
        if len(filename) > MAX_FILENAME_LENGTH:
            filename = "{}.{}".format(hashlib.md5(step_key.encode('utf-8')).hexdigest(), extension)
        return os.path.join(self._run_directory(run_id), filename)

    def read_logs_file(self, run_id, step_key, io_type, cursor=0, max_bytes=MAX_BYTES_FILE_READ):
        path = self.get_local_path(run_id, step_key, io_type)

        if not os.path.exists(path) or not os.path.isfile(path):
            return ComputeLogFileData(path=path, data=None, cursor=0, size=0, download_url=None)

        # See: https://docs.python.org/2/library/stdtypes.html#file.tell for Windows behavior
        with open(path, 'rb') as f:
            f.seek(cursor, os.SEEK_SET)
            data = f.read(max_bytes)
            cursor = f.tell()
            stats = os.fstat(f.fileno())

        # local download path
        download_url = self.download_url(run_id, step_key, io_type)
        return ComputeLogFileData(
            path=path,
            data=data.decode('utf-8'),
            cursor=cursor,
            size=stats.st_size,
            download_url=download_url,
        )

    def is_compute_completed(self, run_id, step_key):
        return os.path.exists(self.complete_artifact_path(run_id, step_key))

    def on_compute_start(self, step_context):
        pass

    def on_compute_finish(self, step_context):
        touchpath = self.complete_artifact_path(step_context.run_id, step_context.step.key)
        touch_file(touchpath)

    def download_url(self, run_id, step_key, io_type):
        check.inst_param(io_type, 'io_type', ComputeIOType)
        return "/download/{}/{}/{}".format(run_id, step_key, io_type.value)

    def on_subscribe(self, subscription):
        self._subscription_manager.add_subscription(subscription)


class LocalComputeLogSubscriptionManager(object):
    def __init__(self, manager):
        self._manager = manager
        self._subscriptions = defaultdict(list)
        self._watchers = {}
        self._observer = PollingObserver(WATCHDOG_POLLING_TIMEOUT)
        self._observer.start()

    def _key(self, run_id, step_key):
        return '{}:{}'.format(run_id, step_key)

    def add_subscription(self, subscription):
        check.inst_param(subscription, 'subscription', ComputeLogSubscription)
        key = self._key(subscription.run_id, subscription.step_key)
        self._subscriptions[key].append(subscription)
        self.watch(subscription.run_id, subscription.step_key)

    def remove_all_subscriptions(self, run_id, step_key):
        key = self._key(run_id, step_key)
        for subscription in self._subscriptions.pop(key, []):
            subscription.complete()

    def watch(self, run_id, step_key):
        key = self._key(run_id, step_key)
        if key in self._watchers:
            return

        update_paths = [
            self._manager.get_local_path(run_id, step_key, ComputeIOType.STDOUT),
            self._manager.get_local_path(run_id, step_key, ComputeIOType.STDERR),
        ]
        complete_paths = [self._manager.complete_artifact_path(run_id, step_key)]
        directory = os.path.dirname(
            self._manager.get_local_path(run_id, step_key, ComputeIOType.STDERR)
        )

        ensure_dir(directory)
        self._watchers[key] = self._observer.schedule(
            LocalComputeLogFilesystemEventHandler(
                self, run_id, step_key, update_paths, complete_paths
            ),
            directory,
        )

    def notify_subscriptions(self, run_id, step_key):
        key = self._key(run_id, step_key)
        for subscription in self._subscriptions[key]:
            subscription.fetch()

    def unwatch(self, run_id, step_key, handler):
        key = self._key(run_id, step_key)
        if key in self._watchers:
            self._observer.remove_handler_for_watch(handler, self._watchers[key])
        del self._watchers[key]


class LocalComputeLogFilesystemEventHandler(PatternMatchingEventHandler):
    def __init__(self, manager, run_id, step_key, update_paths, complete_paths):
        self.manager = manager
        self.run_id = run_id
        self.step_key = step_key
        self.update_paths = update_paths
        self.complete_paths = complete_paths
        patterns = update_paths + complete_paths
        super(LocalComputeLogFilesystemEventHandler, self).__init__(patterns=patterns)

    def on_created(self, event):
        if event.src_path in self.complete_paths:
            self.manager.remove_all_subscriptions(self.run_id, self.step_key)
            self.manager.unwatch(self.run_id, self.step_key, self)

    def on_modified(self, event):
        if event.src_path in self.update_paths:
            self.manager.notify_subscriptions(self.run_id, self.step_key)


class NoOpComputeLogManager(LocalComputeLogManager):
    def enabled(self, _step_context):
        return False

import os
from collections import namedtuple

import yaml

from dagster import check
from dagster.core.serdes import ConfigurableClassData, whitelist_for_serdes

from .config import DAGSTER_CONFIG_YAML_FILENAME, dagster_instance_config


def _runs_directory(base):
    return os.path.join(base, 'history', '')


def compute_logs_directory(base):
    return os.path.join(base, 'storage')


def _event_logs_directory(base):
    return os.path.join(base, 'history', 'runs', '')


def configurable_class_data_or_default(config_value, field_name, default):
    if config_value.get(field_name):
        return ConfigurableClassData(
            config_value[field_name]['module'],
            config_value[field_name]['class'],
            yaml.dump(config_value[field_name]['config'], default_flow_style=False),
        )
    return default


@whitelist_for_serdes
class InstanceRef(
    namedtuple(
        '_InstanceRef',
        'local_artifact_storage_data run_storage_data event_storage_data compute_logs_data '
        'run_launcher_data dagit_settings',
    )
):
    def __new__(
        self,
        local_artifact_storage_data,
        run_storage_data,
        event_storage_data,
        compute_logs_data,
        run_launcher_data,
        dagit_settings,
    ):
        return super(self, InstanceRef).__new__(
            self,
            local_artifact_storage_data=check.inst_param(
                local_artifact_storage_data, 'local_artifact_storage_data', ConfigurableClassData
            ),
            run_storage_data=check.inst_param(
                run_storage_data, 'run_storage_data', ConfigurableClassData
            ),
            event_storage_data=check.inst_param(
                event_storage_data, 'event_storage_data', ConfigurableClassData
            ),
            compute_logs_data=check.inst_param(
                compute_logs_data, 'compute_logs_data', ConfigurableClassData
            ),
            run_launcher_data=check.opt_inst_param(
                run_launcher_data, 'run_launcher_data', ConfigurableClassData
            ),
            dagit_settings=check.opt_dict_param(dagit_settings, 'dagit_settings'),
        )

    @staticmethod
    def from_dir(base_dir, config_filename=DAGSTER_CONFIG_YAML_FILENAME, overrides=None):
        overrides = check.opt_dict_param(overrides, 'overrides')
        config_value = dagster_instance_config(
            base_dir, config_filename=config_filename, overrides=overrides
        )

        local_artifact_storage_data = configurable_class_data_or_default(
            config_value,
            'local_artifact_storage',
            ConfigurableClassData(
                'dagster.core.storage.root',
                'LocalArtifactStorage',
                yaml.dump({'base_dir': base_dir}, default_flow_style=False),
            ),
        )

        run_storage_data = configurable_class_data_or_default(
            config_value,
            'run_storage',
            ConfigurableClassData(
                'dagster.core.storage.runs',
                'SqliteRunStorage',
                yaml.dump({'base_dir': _runs_directory(base_dir)}, default_flow_style=False),
            ),
        )

        event_storage_data = configurable_class_data_or_default(
            config_value,
            'event_log_storage',
            ConfigurableClassData(
                'dagster.core.storage.event_log',
                'SqliteEventLogStorage',
                yaml.dump({'base_dir': _event_logs_directory(base_dir)}, default_flow_style=False),
            ),
        )

        compute_logs_data = configurable_class_data_or_default(
            config_value,
            'compute_logs',
            ConfigurableClassData(
                'dagster.core.storage.local_compute_log_manager',
                'LocalComputeLogManager',
                yaml.dump({'base_dir': compute_logs_directory(base_dir)}, default_flow_style=False),
            ),
        )

        run_launcher_data = configurable_class_data_or_default(config_value, 'run_launcher', None)

        return InstanceRef(
            local_artifact_storage_data=local_artifact_storage_data,
            run_storage_data=run_storage_data,
            event_storage_data=event_storage_data,
            compute_logs_data=compute_logs_data,
            run_launcher_data=run_launcher_data,
            dagit_settings=config_value.get('dagit'),
        )

    @staticmethod
    def from_dict(instance_ref_dict):
        def value_for_ref_item(k, v):
            if v is None:
                return None
            if k == 'dagit_settings':
                return v
            return ConfigurableClassData(*v)

        return InstanceRef(**{k: value_for_ref_item(k, v) for k, v in instance_ref_dict.items()})

    @property
    def local_artifact_storage(self):
        return self.local_artifact_storage_data.rehydrate()

    @property
    def run_storage(self):
        return self.run_storage_data.rehydrate()

    @property
    def event_storage(self):
        return self.event_storage_data.rehydrate()

    @property
    def compute_log_manager(self):
        return self.compute_logs_data.rehydrate()

    @property
    def run_launcher(self):
        return self.run_launcher_data.rehydrate() if self.run_launcher_data else None

    def to_dict(self):
        return self._asdict()

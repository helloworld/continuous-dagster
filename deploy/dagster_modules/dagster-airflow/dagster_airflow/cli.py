import os
from datetime import datetime, timedelta

import click
import six
import yaml

from dagster import check, seven
from dagster.cli.load_handle import handle_for_pipeline_cli_args
from dagster.utils import load_yaml_from_glob_list
from dagster.utils.indenting_printer import IndentingStringIoPrinter


def construct_environment_yaml(preset_name, env, pipeline_name, module_name):
    # Load environment dict from either a preset or yaml file globs
    if preset_name:
        if env:
            raise click.UsageError('Can not use --preset with --env.')

        cli_args = {
            'fn_name': pipeline_name,
            'pipeline_name': pipeline_name,
            'module_name': module_name,
        }
        pipeline = handle_for_pipeline_cli_args(cli_args).build_pipeline_definition()
        environment_dict = pipeline.get_preset(preset_name).environment_dict

    else:
        env = list(env)
        environment_dict = load_yaml_from_glob_list(env) if env else {}

    # If not provided by the user, ensure we have storage location defined
    if 'storage' not in environment_dict:
        system_tmp_path = seven.get_system_temp_directory()
        dagster_tmp_path = os.path.join(system_tmp_path, 'dagster-airflow', pipeline_name)
        environment_dict['storage'] = {
            'filesystem': {'config': {'base_dir': six.ensure_str(dagster_tmp_path)}}
        }

    return environment_dict


def construct_scaffolded_file_contents(module_name, pipeline_name, environment_dict):
    yesterday = datetime.now() - timedelta(1)

    printer = IndentingStringIoPrinter(indent_level=4)
    printer.line('\'\'\'')
    printer.line(
        'The airflow DAG scaffold for {module_name}.{pipeline_name}'.format(
            module_name=module_name, pipeline_name=pipeline_name
        )
    )
    printer.blank_line()
    printer.line('Note that this docstring must contain the strings "airflow" and "DAG" for')
    printer.line('Airflow to properly detect it as a DAG')
    printer.line('See: http://bit.ly/307VMum')
    printer.line('\'\'\'')
    printer.line('import datetime')
    printer.blank_line()
    printer.line('import yaml')
    printer.line('from dagster_airflow.factory import make_airflow_dag')
    printer.blank_line()
    printer.line('#' * 80)
    printer.comment('#')
    printer.comment('# This environment is auto-generated from your configs and/or presets')
    printer.comment('#')
    printer.line('#' * 80)
    printer.line('ENVIRONMENT = \'\'\'')
    printer.line(yaml.dump(environment_dict, default_flow_style=False))
    printer.line('\'\'\'')
    printer.blank_line()
    printer.blank_line()
    printer.line('#' * 80)
    printer.comment('#')
    printer.comment('# NOTE: these arguments should be edited for your environment')
    printer.comment('#')
    printer.line('#' * 80)
    printer.line('DEFAULT_ARGS = {')
    with printer.with_indent():
        printer.line("'owner': 'airflow',")
        printer.line("'depends_on_past': False,")

        # start date -> yesterday
        printer.line(
            "'start_date': datetime.datetime(%s, %d, %d),"
            % (yesterday.year, yesterday.month, yesterday.day)
        )
        printer.line("'email': ['airflow@example.com'],")
        printer.line("'email_on_failure': False,")
        printer.line("'email_on_retry': False,")
    printer.line('}')
    printer.blank_line()
    printer.line('dag, tasks = make_airflow_dag(')
    with printer.with_indent():
        printer.comment(
            'NOTE: you must ensure that {module_name} is '.format(module_name=module_name)
        )
        printer.comment('installed or available on sys.path, otherwise, this import will fail.')
        printer.line('module_name=\'{module_name}\','.format(module_name=module_name))
        printer.line('pipeline_name=\'{pipeline_name}\','.format(pipeline_name=pipeline_name))
        printer.line("environment_dict=yaml.safe_load(ENVIRONMENT),")
        printer.line("dag_kwargs={'default_args': DEFAULT_ARGS, 'max_active_runs': 1}")
    printer.line(')')

    return printer.read().encode()


@click.group()
def main():
    pass


@main.command()
@click.option(
    '--module-name', '-m', type=click.STRING, help='The name of the source module', required=True
)
@click.option('--pipeline-name', type=click.STRING, help='The name of the pipeline', required=True)
@click.option(
    '--output-path',
    '-o',
    type=click.Path(),
    help='Optional. If unset, $AIRFLOW_HOME will be used.',
    default=os.getenv('AIRFLOW_HOME'),
)
@click.option(
    '-e',
    '--env',
    type=click.STRING,
    multiple=True,
    help=(
        'Specify one or more environment files. These can also be file patterns. '
        'If more than one environment file is captured then those files are merged. '
        'Files listed first take precendence. They will smash the values of subsequent '
        'files at the key-level granularity. If the file is a pattern then you must '
        'enclose it in double quotes'
    ),
)
@click.option(
    '-p',
    '--preset',
    type=click.STRING,
    help='Specify a preset to use for this pipeline. Presets are defined on pipelines under '
    'preset_defs.',
)
def scaffold(module_name, pipeline_name, output_path, env, preset):
    '''Creates a DAG file for a specified dagster pipeline'''

    check.invariant(isinstance(env, tuple))
    check.invariant(
        output_path is not None,
        'You must specify --output-path or set AIRFLOW_HOME to use this script.',
    )

    environment_dict = construct_environment_yaml(preset, env, pipeline_name, module_name)
    file_contents = construct_scaffolded_file_contents(module_name, pipeline_name, environment_dict)

    # Ensure output_path/dags exists
    dags_path = os.path.join(os.path.expanduser(output_path), 'dags')
    if not os.path.isdir(dags_path):
        os.makedirs(dags_path)

    dag_file = os.path.join(os.path.expanduser(output_path), 'dags', pipeline_name + '.py')

    click.echo('Wrote DAG scaffold to file: %s' % dag_file)

    with open(dag_file, 'wb') as f:
        f.write(file_contents)


if __name__ == '__main__':
    main()  # pylint:disable=no-value-for-parameter

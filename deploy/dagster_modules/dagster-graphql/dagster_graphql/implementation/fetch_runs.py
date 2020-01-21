from graphql.execution.base import ResolveInfo

from dagster import RunConfig, check
from dagster.config.validate import validate_config
from dagster.core.definitions import create_environment_schema
from dagster.core.definitions.pipeline import ExecutionSelector, PipelineRunsFilter
from dagster.core.execution.api import create_execution_plan

from .fetch_pipelines import (
    get_dauphin_pipeline_from_selector_or_raise,
    get_dauphin_pipeline_reference_from_selector,
)
from .utils import UserFacingGraphQLError, capture_dauphin_error


def get_validated_config(graphene_info, dauphin_pipeline, environment_dict, mode):
    check.str_param(mode, 'mode')

    pipeline = dauphin_pipeline.get_dagster_pipeline()

    environment_schema = create_environment_schema(pipeline, mode)

    validated_config = validate_config(environment_schema.environment_type, environment_dict)

    if not validated_config.success:
        raise UserFacingGraphQLError(
            graphene_info.schema.type_named('PipelineConfigValidationInvalid')(
                pipeline=dauphin_pipeline,
                errors=[
                    graphene_info.schema.type_named(
                        'PipelineConfigValidationError'
                    ).from_dagster_error(graphene_info, err)
                    for err in validated_config.errors
                ],
            )
        )

    return validated_config


def get_run(graphene_info, run_id):
    instance = graphene_info.context.instance
    run = instance.get_run_by_id(run_id)
    if not run:
        return graphene_info.schema.type_named('PipelineRunNotFoundError')(run_id)
    else:
        return graphene_info.schema.type_named('PipelineRun')(run)


def get_run_tags(graphene_info):
    instance = graphene_info.context.instance
    return [
        graphene_info.schema.type_named('PipelineTagAndValues')(key=key, values=values)
        for key, values in instance.get_run_tags()
    ]


def get_runs(graphene_info, filters, cursor=None, limit=None):
    check.inst_param(filters, 'filters', PipelineRunsFilter)
    check.opt_str_param(cursor, 'cursor')
    check.opt_int_param(limit, 'limit')

    instance = graphene_info.context.instance
    runs = []

    if filters.run_id:
        run = instance.get_run_by_id(filters.run_id)
        if run:
            runs = [run]
    elif filters.pipeline_name or filters.tags or filters.status:
        runs = instance.get_runs(filters, cursor, limit)
    else:
        runs = instance.get_runs(cursor=cursor, limit=limit)

    return [graphene_info.schema.type_named('PipelineRun')(run) for run in runs]


@capture_dauphin_error
def validate_pipeline_config(graphene_info, selector, environment_dict, mode):
    check.inst_param(graphene_info, 'graphene_info', ResolveInfo)
    check.inst_param(selector, 'selector', ExecutionSelector)
    check.opt_str_param(mode, 'mode')

    dauphin_pipeline = get_dauphin_pipeline_from_selector_or_raise(graphene_info, selector)
    get_validated_config(graphene_info, dauphin_pipeline, environment_dict, mode)
    return graphene_info.schema.type_named('PipelineConfigValidationValid')(dauphin_pipeline)


@capture_dauphin_error
def get_execution_plan(graphene_info, selector, environment_dict, mode):
    check.inst_param(graphene_info, 'graphene_info', ResolveInfo)
    check.inst_param(selector, 'selector', ExecutionSelector)
    check.opt_str_param(mode, 'mode')

    dauphin_pipeline = get_dauphin_pipeline_reference_from_selector(graphene_info, selector)
    get_validated_config(graphene_info, dauphin_pipeline, environment_dict, mode)
    return graphene_info.schema.type_named('ExecutionPlan')(
        dauphin_pipeline,
        create_execution_plan(
            dauphin_pipeline.get_dagster_pipeline(), environment_dict, RunConfig(mode=mode)
        ),
    )


@capture_dauphin_error
def get_stats(graphene_info, run_id):
    stats = graphene_info.context.instance.get_run_stats(run_id)
    return graphene_info.schema.type_named('PipelineRunStatsSnapshot')(stats)

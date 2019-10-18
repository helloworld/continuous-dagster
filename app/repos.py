from dagster import RepositoryDefinition, pipeline, solid
from dagster import schedules, ScheduleDefinition
from dagster_cron import SystemCronScheduler

@schedules(scheduler=SystemCronScheduler)
def define_scheduler():
    hello_world_every_minute = ScheduleDefinition(
        name="hello_world_every_minute",
        cron_schedule="* * * * *",
        pipeline_name="hello_world_pipeline",
        environment_dict={}
    )

    return [hello_world_every_minute]

@solid
def hello_world(context):
    context.log.info('Hello, world!')

@pipeline
def hello_world_pipeline():
    hello_world()

def define_repo():
    return RepositoryDefinition(
        name='scheduler_demo_repository',
        pipeline_defs=[hello_world_pipeline]
    )

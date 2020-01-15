from dagster import RepositoryDefinition, pipeline, solid
from dagster import schedules, ScheduleDefinition
from dagster_cron import SystemCronScheduler


@schedules(scheduler=SystemCronScheduler)
def define_scheduler():
    def create_hello_world_schedule(name):
        return ScheduleDefinition(
            name=name,
            cron_schedule="* * * * *",
            pipeline_name="hello_world_pipeline",
            environment_dict={},
        )

    hello_world_every_minute = ScheduleDefinition(
        name="hello_world_every_minute",
        cron_schedule="* * * * *",
        pipeline_name="hello_world_pipeline",
        environment_dict={},
    )

    goodbye_world_every_minute = ScheduleDefinition(
        name="goodbye_world_every_minute",
        cron_schedule="* * * * *",
        pipeline_name="goodbye_world_pipeline",
        environment_dict={},
    )

    hello_world_schedules = [
        create_hello_world_schedule("hello_world_{i}".format(i=i)) for i in range(10)
    ]

    return [hello_world_every_minute, goodbye_world_every_minute].extend(hello_world_schedules)


@solid
def hello_world(context):
    context.log.info('Hello, world!')


@pipeline
def hello_world_pipeline():
    hello_world()


@solid
def goodbye_world(context):
    context.log.info('Goodbye, world!')


@pipeline
def goodbye_world_pipeline():
    goodbye_world()


def define_repo():
    return RepositoryDefinition(
        name='scheduler_demo_repository',
        pipeline_defs=[hello_world_pipeline, goodbye_world_pipeline],
    )

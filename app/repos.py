from dagster import RepositoryDefinition, pipeline, solid
from dagster import schedules, ScheduleDefinition, InputDefinition, Nothing, OutputDefinition, Int
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

    s = [create_hello_world_schedule("hello_world_{i}".format(i=i)) for i in range(10)]
    s.extend([hello_world_every_minute, goodbye_world_every_minute])
    return s


# @solid
# def hello_world(context):
#     context.log.info('Hello, world!')


# @pipeline
# def hello_world_pipeline():
#     hello_world()


# @solid
# def goodbye_world(context):
#     context.log.info('Goodbye, world!')


# import time


# @solid
# def order_1(_):
#     pass


# @solid
# def order_2(_):
#     pass


# @solid
# def one(_):
#     return 1


# @solid(
#     input_defs=[InputDefinition("num", Int), InputDefinition("order_dependencies", Nothing),]
# )
# def process(_, num):
#     pass


# @pipeline
# def simple_pipeline():
#     A = order_1()
#     B = order_2()
#     process(one(), [A, B])


# @solid
# def slow(_):
#     time.sleep(1)


# @solid
# def very_slow(_):
#     time.sleep(5)


# @solid
# def one(_):
#     return 1


# @solid
# def two(_):
#     return 2


@solid(
    input_defs=[
        InputDefinition("number_two", Int),
        InputDefinition("number_one", Int),
        InputDefinition("order_dependency_1", Nothing),
        InputDefinition("order_dependency_2", Nothing),
    ]
)
def add(context, number_one, number_two):
    context.log.info("number_one {}".format(number_one))
    context.log.info("number two {}".format(number_two))
    return number_one + number_two


# @pipeline
# def addition_pipeline():
#     A = slow()
#     B = very_slow()
#     add(one(), two(), A, B)
#     # add(A, B, number_one=one(), number_two=two())


# @pipeline
# def goodbye_world_pipeline():
#     goodbye_world()


def define_repo():
    return RepositoryDefinition(
        name='scheduler_demo_repository',
        # pipeline_defs=[hello_world_pipeline, goodbye_world_pipeline, addition_pipeline],
    )

from dagster_graphql.client.execute import execute_remote_pipeline_run

for i in range(100):
    res = execute_remote_pipeline_run(
        host="https://continuous-dagster.onrender.com",
        pipeline_name="hello_world_pipeline",
        environment_dict={},
        tags={"remote": "true"},
    )

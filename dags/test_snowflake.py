from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    "test_snowflake",
    start_date=datetime(2023,1,1),
    schedule=None,
    catchup=False,
):

    test = SnowflakeOperator(
        task_id="test_conn",
        snowflake_conn_id="snowflake_sandboxdata_conn",
        sql="SELECT CURRENT_TIMESTAMP();",
    )

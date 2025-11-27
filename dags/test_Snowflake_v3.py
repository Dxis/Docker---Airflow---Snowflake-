from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

default_args = {
    'owner': 'diego',
    'start_date': datetime(2025, 11, 17),
    'retries': 1
}

def create_table_if_not_exists():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sql = """
    CREATE TABLE IF NOT EXISTS PUBLIC.TBL_EXEMPLO (
        ID INT AUTOINCREMENT PRIMARY KEY,
        NOME STRING,
        VALOR INT,
        DT_CRIACAO TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    hook.run(sql)

def insert_data_dynamic():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    values = [
        ('Produto A', 100),
        ('Produto B', 200)
    ]
    for nome, valor in values:
        hook.run(f"INSERT INTO PUBLIC.TBL_EXEMPLO (NOME, VALOR) VALUES ('{nome}', {valor})")

with DAG(
    dag_id='snowflake_hook_dynamic_insert',
    default_args=default_args,
    schedule=None,
    catchup=False
) as dag:

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=create_table_if_not_exists
    )

    insert_data = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data_dynamic
    )

    create_table >> insert_data

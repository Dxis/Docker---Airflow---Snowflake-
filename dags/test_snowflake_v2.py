from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime

with DAG(
    "test_snowflake_SnowflakeOperator_CRUD",
    start_date=datetime(2025,1,1),
    schedule=None,
    catchup=False,
):

    # 1️⃣ Criar tabela no schema PUBLIC se não existir
    create_table = SnowflakeOperator(
        task_id='create_table',
        snowflake_conn_id='snowflake_conn',
        sql="""
        CREATE TABLE IF NOT EXISTS PUBLIC.TBL_EXEMPLO (
            ID INT AUTOINCREMENT PRIMARY KEY,
            NOME STRING,
            VALOR INT,
            DT_CRIACAO TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        );
        """
    )

    # 2️⃣ Inserir dados
    insert_data = SnowflakeOperator(
        task_id='insert_data',
        snowflake_conn_id='snowflake_conn',
        sql="""
        INSERT INTO PUBLIC.TBL_EXEMPLO (NOME, VALOR)
        VALUES 
            ('Produto A', 100),
            ('Produto B', 200);
        """
    )

    # Ordem de execução
    create_table >> insert_data

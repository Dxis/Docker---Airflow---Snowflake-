from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import os
import shutil

BASE_PATH = "/opt/airflow/data/recepcionado"

with DAG(
    dag_id="trigger_process_json",
    start_date=datetime.now() - timedelta(days=1),
    schedule=None,
    catchup=False,
    tags=["json", "snowflake"],
) as dag:

    @task
    def obter_arquivo(**kwargs):
        """Recebe o caminho do arquivo passado via TriggerDagRun."""
        conf = kwargs.get("dag_run").conf
        arquivo = conf.get("arquivo") if conf else None
        if not arquivo:
            raise ValueError("Nenhum arquivo recebido via TriggerDagRun.")
        print(f">>> Arquivo recebido: {arquivo}")
        return arquivo

    @task
    def upload_snowflake(arquivo):
        """Faz o PUT do arquivo JSON para o stage no Snowflake."""
        print(f">>> Inicio  upload_snowflake - Arquivo recebido: {arquivo}")
        hook = SnowflakeHook(snowflake_conn_id="snowflake_sandboxdata_conn")
        print(f">>> Conexão com sucesso SnowflakeHook - snowflake_sandboxdata_conn ")
        
        # Se o caminho for relativo, junta com BASE_PATH
        file_path = arquivo if os.path.isabs(arquivo) else os.path.join(BASE_PATH, arquivo)
        file_name = os.path.basename(file_path)
        print(f">>> file_name: {file_name}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Arquivo não encontrado no container: {file_path}")

        print(f">>> Inicio upload Storage: {file_name}")
        sql_put = f"""
            PUT file://{file_path}
            @SANDBOXDATA.PUBLIC.STORAGE
            AUTO_COMPRESS=FALSE
            OVERWRITE=TRUE;
        """
        print(f">>> Final para Storage: {file_name}")

        hook.run(sql_put)
        print(f">>> Finalizado upload_snowflake: {file_name}")
        return file_name

    @task
    def inserir_tblsandbox(file_name):
        print(f">>> Insere registro na tabela principal e retorna o ID gerado.")
        print(f">>> Inicio  inserir_tblsandbox - Arquivo recebido: {file_name}")

        hook = SnowflakeHook(snowflake_conn_id="snowflake_sandboxdata_conn")
        print(f">>> Conexão com sucesso SnowflakeHook - snowflake_sandboxdata_conn ")

        # 1️⃣ Definir o database na sessão
        hook.run("USE DATABASE SANDBOXDATA;")

        # 2️⃣ Obter próximo valor da sequence (apenas schema + nome)
        sql_nextval = 'SELECT SANDBOXDATA.PUBLIC.SEQ_FILES_UID.NEXTVAL;'
        next_id = hook.get_first(sql_nextval)[0]
        print(f">>> Próximo ID obtido da sequence: {next_id}")

        # 3️⃣ Inserir registro usando o ID obtido
        sql_insert = f"""
            INSERT INTO SANDBOXDATA.PUBLIC.tblSandbox (id_file, ds_file)
            VALUES ({next_id}, '{file_name}');
        """
        hook.run(sql_insert)
        print(f">>> Finalizado inserir_tblsandbox: {file_name}")
        print(f">>> ID gerado: {next_id}")

        return next_id      
    
    @task
    def copy_into_stage(id_file, file_name):
        """Copia dados do JSON para a tabela stage no Snowflake."""
        hook = SnowflakeHook(snowflake_conn_id="snowflake_sandboxdata_conn")

        sql_copy = f"""
            COPY INTO public.tblSandbox_Stage
            FROM (
                SELECT 
                    {id_file} AS id_file,
                    PARSE_JSON(t.$1) AS RAW_STATUS
                FROM @SANDBOXDATA.PUBLIC.STORAGE/{file_name} t
            )
            FILE_FORMAT = (TYPE = 'JSON')
            FORCE = TRUE;
        """

        hook.run(sql_copy)
        print(">>> COPY INTO tblSandbox_Stage concluído.")

    @task
    def mover_final(arquivo: str):
        """
        Move o arquivo físico da pasta 'recepcionado' para 'processado'.
        """
        print(">>> inicio mover_final")
        print(f"Insumo: {arquivo}")

        dst =  arquivo.replace("recepcionado", "processado")

        print(f">>> Origem: {arquivo}")
        print(f">>> Destino teste: {dst}")

        if os.path.exists(arquivo):
            shutil.move(arquivo, dst)
            print(f">>> Arquivo movido para processado: {dst}")
        else:
            print(f"!!! Arquivo não encontrado na origem: {arquivo}")


    # Não chamar a função ainda, apenas criar o "wrapper" da task
    arquivo = obter_arquivo()
    file_name = upload_snowflake(arquivo)
    id_file = inserir_tblsandbox(file_name)
    copy_stage = copy_into_stage(id_file, file_name)
    mover = mover_final(arquivo)

    # Dependências explícitas
    arquivo >> file_name >> id_file >> copy_stage >> mover

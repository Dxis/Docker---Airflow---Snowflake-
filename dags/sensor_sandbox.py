from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
import shutil

BASE_PATH = "/opt/airflow/data"

def checar_pasta(**context):
    print("===== INICIANDO VERIFICAÇÃO DA PASTA =====")
    print(f"Pasta monitorada: {BASE_PATH}")

    try:
        arquivos = os.listdir(BASE_PATH)
    except Exception as e:
        print(f"ERRO ao tentar listar a pasta: {e}")
        return None

    print(f"Arquivos encontrados: {arquivos}")

    for file in arquivos:
        if file.lower().endswith(".json"):
            print(f">>> ✔ ARQUIVO JSON ENCONTRADO: {file}")
            return file  # apenas o nome do arquivo

    print(">>> ⚠ Nenhum arquivo JSON encontrado.")
    return None

def validar_arquivo(**context):
    arquivo = context["ti"].xcom_pull(task_ids="checar_pasta")
    print(f">>> VALIDAÇÃO — arquivo encontrado? {arquivo is not None}")
    return arquivo is not None

def mover_para_recepcionado(**context):
    arquivo = context["ti"].xcom_pull(task_ids="checar_pasta")
    if arquivo:
        src = os.path.join(BASE_PATH, arquivo)
        dst_dir = os.path.join(BASE_PATH, "recepcionado")
        os.makedirs(dst_dir, exist_ok=True)
        dst = os.path.join(dst_dir, arquivo)
        shutil.move(src, dst)
        print(f">>> ✔ Arquivo movido para recepcionado: {dst}")
        return dst
    else:
        print(">>> Nenhum arquivo para mover.")
        return None

with DAG(
    dag_id="sensor_sandbox",
    start_date=datetime(2025, 1, 1),
    schedule=timedelta(seconds=20),
    catchup=False,
):

    checar = PythonOperator(
        task_id="checar_pasta",
        python_callable=checar_pasta,
    )

    short = ShortCircuitOperator(
        task_id="validar",
        python_callable=validar_arquivo,
    )

    mover = PythonOperator(
        task_id="mover_para_recepcionado",
        python_callable=mover_para_recepcionado,
    )

    trigger = TriggerDagRunOperator(
        task_id="disparar_dag_processamento",
        trigger_dag_id="trigger_process_json",
        conf={"arquivo": "{{ ti.xcom_pull('mover_para_recepcionado') }}"},
    )

    checar >> short >> mover >> trigger

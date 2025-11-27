# Projeto Airflow + Snowflake

Este projeto demonstra um pipeline de ingestão de arquivos JSON para o Snowflake utilizando **Apache Airflow**.  
O fluxo do projeto envolve dois DAGs principais:

---

## 1️⃣ DAG Sensor: `sensor_sandbox`

Este DAG fica ativo monitorando continuamente a pasta `/opt/airflow/data` em busca de arquivos `.json`. Ele segue a **ordem de execução**:

1. **`checar_pasta`** (`PythonOperator`)  
   - Lista os arquivos na pasta.
   - Retorna o caminho do primeiro arquivo `.json` encontrado.

2. **`validar`** (`ShortCircuitOperator`)  
   - Verifica se algum arquivo foi encontrado.  
   - Se **nenhum arquivo** for encontrado, o DAG **para** e aguarda a próxima execução.

3. **`disparar_dag_processamento`** (`TriggerDagRunOperator`)  
   - Dispara o DAG `trigger_process_json`.
   - Passa o caminho do arquivo JSON via `conf`.

> **Resumo da ordem:**  
> `checar_pasta` → `validar` → `disparar_dag_processamento`

---

## 2️⃣ DAG de Processamento: `trigger_process_json`

Este DAG é disparado pelo sensor e realiza a ingestão do arquivo JSON no Snowflake. A **ordem correta das tarefas** é:

1. Recebe via parâmetro o arquivo JSON a ser processado.
2. **`inserir_tblsandbox`** (`PythonOperator`)  
   - Conecta ao Snowflake.
   - Obtém o próximo ID da sequence `seq_files_uid`.
   - Insere o registro na tabela `tblSandbox` com o nome do arquivo.
3. Move o arquivo para a pasta `processado` ou `erro`, dependendo do resultado.
4. Mantém controle de IDs usando a sequence, garantindo unicidade.

> **Resumo da ordem:**  
> O arquivo é processado somente **após o sensor confirmar que ele existe** e disparar este DAG.

---

## 3️⃣ Banco de Dados e Estrutura Snowflake

- **Database:** `SANDBOXDATA`  
- **Schema:** `PUBLIC`  
- **Tabela principal:** `tblSandbox` (`id_file`, `ds_file`)  
- **Sequence:** `seq_files_uid` para controle de IDs.

### Usuários e permissões

| Usuário             | Senha Exemplo         | Função / Acesso                                    |
|--------------------|----------------------|--------------------------------------------------|
| `ANA_SANDBOX`      | `SenhaForteeee123`   | Usuário crud - banco - storage       |
| `HENRIQUE_SANDBOX` | `Sandboxdata@123`    | Usuário crud - banco - storage   |

> Ambos os usuários possuem acesso ao warehouse `sandboxdata_WH` e ao schema `SANDBOXDATA.PUBLIC`.

### Warehouse

- **Nome:** `sandboxdata_WH`  
- **Tamanho:** XSMALL  
- **Auto Suspend:** 60 segundos  
- **Auto Resume:** TRUE

---

## 4️⃣ Observações importantes

- O DAG sensor evita consumo desnecessário de recursos, pausando quando não há arquivos.
- As senhas apresentadas são apenas exemplos, **substitua-as em produção**.
- O pipeline garante que **nenhum arquivo será processado sem validação prévia**.
- Permissões e roles são configuradas para que cada usuário tenha apenas o acesso necessário.

---

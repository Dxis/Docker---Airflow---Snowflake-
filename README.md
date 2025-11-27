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
acesso via dbeaver 
- MNLTDMC-DL26504.snowflakecomputing.com porta 443
 
- **Database:** `SANDBOXDATA`  
- **Schema:** `sandboxdata.PUBLIC`  
- **Tabela principal:** `tblSandbox` (`id_file`, `ds_file`)  
- **Sequence:** `seq_files_uid` para controle de IDs.
- **Role:** sandboxdata_role

### Usuários e permissões


 
 

| Usuário             | Senha Exemplo         | Função / Acesso                                    |
|--------------------|----------------------|--------------------------------------------------|
| `ANA_SANDBOX`      | `SenhaForteeee123`   | Usuário crud - banco - storage       |
| `HENRIQUE_SANDBOX` | `Sandboxdata@123`    | Usuário crud - banco - storage   |

> Ambos os usuários possuem acesso ao warehouse `sandboxdata_WH` e ao schema `SANDBOXDATA.PUBLIC`.>
> 
<img width="826" height="597" alt="image" src="https://github.com/user-attachments/assets/737d2bdc-5464-4e57-b140-bbc51d21bd5d" />

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

Acesso BD via Postgress 



<img width="1146" height="718" alt="image" src="https://github.com/user-attachments/assets/8d8fc17a-08c2-450c-8568-240d412c3195" />

<img width="1113" height="568" alt="image" src="https://github.com/user-attachments/assets/16ba2c5f-62b7-4655-b99e-ba84df9f8e85" />


#testes

![Screenshot_2](https://github.com/user-attachments/assets/2f77aa25-6945-4f43-af65-b374ff7cd9d5)

![Screenshot_3](https://github.com/user-attachments/assets/8e3377cb-be66-4f78-bcf4-c3dc82929857)


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas_gbq
import json
from google.cloud import storage
import datetime
import os

def extrair_dados(**context):
    ontem = (datetime.date.today()).strftime("%Y-%m-%d")

    query = """SELECT CASE
                    WHEN FIM_BASE_ORDEM = '0001-01-01' THEN NULL
                    ELSE FIM_BASE_ORDEM
                END AS FIM_BASE_ORDEM,
                CASE
                    WHEN INICIO_BASE_ORDEM = '0001-01-01' THEN NULL
                    ELSE INICIO_BASE_ORDEM
                END AS INICIO_BASE_ORDEM,
                HORA_INICIO_BASE_ORDEM,HORA_FIM_BASE_ORDEM,PRIORIDADE_ORDEM,
                NOME_CRIADO_ORDEM,NOME_MODIFICADO_ORDEM,PLANEJADO,PROGRAMADO,
                ENCERRADO,AREA,GRUPO_PLANEJAMENTO_ORDEM,ORDEM,DESCRICAO_ORDEM,
                TIPO_ORDEM,OPORTUNIDADE,DATA_MODIFICACAO,CENTRO_TRABALHO,
                DATA_CRIACAO,CRIADO_POR,MODIFICADO_POR,TAG,DESC_TAG,CRITICIDADE,
                STATUS_USUARIO_ORDEM,STATUS_SISTEMA_ORDEM,REVISAO,
                CUSTO_PLANEJADO,CUSTO_REAL
            FROM `sz-00022-ws.TABELAS_SAP.ORDENS`;"""

    df = pandas_gbq.read_gbq(query, project_id="sz-00022-ws")
    linhas = json.loads(df.to_json(orient="records"))

    ordens = []
    for linha in linhas:
        DATA_CRIACAO = (datetime.datetime.fromtimestamp(linha['DATA_CRIACAO']/1000)+datetime.timedelta(1)).strftime("%Y-%m-%d") if linha['DATA_CRIACAO'] else None
        DATA_MODIFICACAO = (datetime.datetime.fromtimestamp(linha['DATA_MODIFICACAO']/1000)+datetime.timedelta(1)).strftime("%Y-%m-%d") if linha['DATA_MODIFICACAO'] else None
        INICIO_BASE_ORDEM = (datetime.datetime.fromtimestamp(linha['INICIO_BASE_ORDEM']/1000)+datetime.timedelta(1)).strftime("%Y-%m-%d") if linha['INICIO_BASE_ORDEM'] else None
        FIM_BASE_ORDEM = (datetime.datetime.fromtimestamp(linha['FIM_BASE_ORDEM']/1000)+datetime.timedelta(1)).strftime("%Y-%m-%d") if linha['FIM_BASE_ORDEM'] else None

        ordens.append({
            "ORDEM": linha['ORDEM'],
            "DESCRICAO_ORDEM": linha['DESCRICAO_ORDEM'],
            "TIPO_ORDEM": linha['TIPO_ORDEM'],
            "OPORTUNIDADE": linha['OPORTUNIDADE'],
            "CENTRO_TRABALHO": linha['CENTRO_TRABALHO'],
            "DATA_CRIACAO": DATA_CRIACAO,
            "CRIADO_POR": linha['CRIADO_POR'],
            "DATA_MODIFICACAO": DATA_MODIFICACAO,
            "MODIFICADO_POR": linha['MODIFICADO_POR'],
            "TAG": linha['TAG'],
            "DESC_TAG": linha['DESC_TAG'],
            "CRITICIDADE": linha['CRITICIDADE'],
            "STATUS_USUARIO_ORDEM": linha['STATUS_USUARIO_ORDEM'],
            "STATUS_SISTEMA_ORDEM": linha['STATUS_SISTEMA_ORDEM'],
            "DATA_HISTORICO": ontem,
            "REVISAO": linha['REVISAO'],
            "GRUPO_PLANEJAMENTO_ORDEM": linha['GRUPO_PLANEJAMENTO_ORDEM'],
            "INICIO_BASE_ORDEM": INICIO_BASE_ORDEM,
            "HORA_INICIO_BASE_ORDEM": linha['HORA_INICIO_BASE_ORDEM'],
            "FIM_BASE_ORDEM": FIM_BASE_ORDEM,
            "HORA_FIM_BASE_ORDEM": linha['HORA_FIM_BASE_ORDEM'],
            "PRIORIDADE_ORDEM": linha['PRIORIDADE_ORDEM'],
            "NOME_CRIADO_ORDEM": linha['NOME_CRIADO_ORDEM'],
            "NOME_MODIFICADO_ORDEM": linha['NOME_MODIFICADO_ORDEM'],
            "PLANEJADO": linha['PLANEJADO'],
            "PROGRAMADO": linha['PROGRAMADO'],
            "ENCERRADO": linha['ENCERRADO'],
            "AREA": linha['AREA'],
            "CUSTO_PLANEJADO": linha['CUSTO_PLANEJADO'],
            "CUSTO_REAL": linha['CUSTO_REAL']
        })

    local_path = f"csv/ordens_{ontem}.json"

    # Salva no disco
    with open(local_path, "w", encoding="utf-8") as f:
        for entrada in ordens:
            linha = json.dumps(entrada, ensure_ascii=False)
            f.write(linha + "\n")

    # envia pro GCS
    client = storage.Client()
    bucket = client.bucket("airflow_vps")
    blob = bucket.blob(f"historico/ordens_{ontem}.json")
    blob.upload_from_filename(local_path, content_type="application/json")

    # push só o caminho no GCS
    context["ti"].xcom_push(key="ordens_path", value=f"gs://airflow_vps/historico/ordens_{ontem}.json")

    os.remove(local_path)


with DAG(
    dag_id='exportar_historico_ordens',
    schedule_interval='30 2 * * *',
    start_date=datetime.datetime(2025, 1, 5),
    catchup=False,
    tags=['bigquery', 'gcs', 'exportacao','historico','ordem'],
) as dag:

    criar_json = PythonOperator(
        task_id='criar_json',
        python_callable=extrair_dados,
        provide_context=True,
    )

    carregar_bigquery = GCSToBigQueryOperator(
        task_id="carregar_bigquery",
        bucket="airflow_vps",
        source_objects=["historico/ordens_{{ ds }}.json"], 
        destination_project_dataset_table="sz-00022-ws.TABELAS_SAP.HISTORICO_ORDENS",
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=[
            {"name": "ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DESCRICAO_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "TIPO_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "OPORTUNIDADE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "CENTRO_TRABALHO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DATA_CRIACAO", "type": "DATE", "mode": "NULLABLE"},
            {"name": "CRIADO_POR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DATA_MODIFICACAO", "type": "DATE", "mode": "NULLABLE"},
            {"name": "MODIFICADO_POR", "type": "STRING", "mode": "NULLABLE"},
            {"name": "TAG", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DESC_TAG", "type": "STRING", "mode": "NULLABLE"},
            {"name": "CRITICIDADE", "type": "STRING", "mode": "NULLABLE"},
            {"name": "STATUS_USUARIO_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "STATUS_SISTEMA_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DATA_HISTORICO", "type": "DATE", "mode": "NULLABLE"},
            {"name": "REVISAO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "GRUPO_PLANEJAMENTO_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "INICIO_BASE_ORDEM", "type": "DATE", "mode": "NULLABLE"},
            {"name": "HORA_INICIO_BASE_ORDEM", "type": "TIME", "mode": "NULLABLE"},
            {"name": "FIM_BASE_ORDEM", "type": "DATE", "mode": "NULLABLE"},
            {"name": "HORA_FIM_BASE_ORDEM", "type": "TIME", "mode": "NULLABLE"},
            {"name": "PRIORIDADE_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "NOME_CRIADO_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "NOME_MODIFICADO_ORDEM", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PLANEJADO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "PROGRAMADO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ENCERRADO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "AREA", "type": "STRING", "mode": "NULLABLE"},
            {"name": "CUSTO_PLANEJADO", "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "CUSTO_REAL", "type": "NUMERIC", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
    )

    criar_json >> carregar_bigquery

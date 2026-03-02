from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.task_group import TaskGroup
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="Criacao_BI",
    default_args=default_args,
    description="Diário views indicadores BI",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="0 5 * * *",
    catchup=False,
    tags=["bigquery", "gcs", "ordens"],
) as dag:

    diario_ordens = BigQueryInsertJobOperator(
        task_id="ordens",
        project_id="sz-int-aecorsoft-di-prd",     #
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.BI.ORDENS` AS
                    SELECT *
                    FROM `sz-00022-ws.TABELAS_SAP.ORDENS`
                """,
                "useLegacySql": False,
            }
        },
        location="southamerica-east1",
    )

    diario_notas = BigQueryInsertJobOperator(
        task_id="notas",
        project_id="sz-int-aecorsoft-di-prd",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.BI.NOTAS` AS
                    SELECT *
                    FROM `sz-00022-ws.TABELAS_SAP.NOTAS`
                """,
                "useLegacySql": False,
            }
        },
        location="southamerica-east1",
    )

    diario_backlog = BigQueryInsertJobOperator(
        task_id="backlog",
        project_id="sz-int-aecorsoft-di-prd",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.BI.BACKLOG` AS
                    SELECT *
                    FROM `sz-00022-ws.TABELAS_SAP.BACKLOG`
                """,
                "useLegacySql": False,
            }
        },
        location="southamerica-east1",
    )

    diario_pbcusto = BigQueryInsertJobOperator(
        task_id="pb_custo",
        project_id="sz-int-aecorsoft-di-prd",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.BI.PB_CUSTO` AS
                    SELECT *
                    FROM `sz-00022-ws.TABELAS_SAP.PB_CUSTO`
                """,
                "useLegacySql": False,
            }
        },
        location="southamerica-east1",
    )

    diario_realizadocustogeral = BigQueryInsertJobOperator(
        task_id="realizado_custo_geral",
        project_id="sz-int-aecorsoft-di-prd",
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.BI.REALIZADO_CUSTO_GERAL` AS
                    SELECT *
                    FROM `sz-00022-ws.TABELAS_SAP.REALIZADO_CUSTO_GERAL`
                """,
                "useLegacySql": False,
            }
        },
        location="southamerica-east1",
    )

    diario_ordens >> diario_notas >> diario_backlog >> diario_pbcusto >> diario_realizadocustogeral
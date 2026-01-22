from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import pendulum

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="historico_estoque_mensal",
    default_args=default_args,
    description="Snapshot mensal da view de estoque no BigQuery (dia 1)",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="30 0 * * 1",  #segunda às 00:30
    catchup=False,
    tags=['bigquery', 'gcs', 'historico','estoque']
) as dag:

    snapshot_mensal_fixa = BigQueryInsertJobOperator(
        task_id="snapshot_mensal_estoque",
        configuration={
            "query": {
                "query": """
                INSERT INTO `sz-00022-ws.INDICADORES_PLANEJAMENTO.HISTORICO_ESTOQUE`
                SELECT
                  *,
                  CURRENT_TIMESTAMP() AS DATA_HISTORICO
                FROM `sz-00022-ws.INDICADORES_PLANEJAMENTO.VW_ESTOQUE`;
                """,
                "useLegacySql": False,
            }
        },
        location="southamerica-east1",
        gcp_conn_id='google_cloud_default'
    )

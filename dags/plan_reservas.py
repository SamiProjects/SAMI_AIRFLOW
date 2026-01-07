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
    dag_id="historico_reservas_semanal",
    default_args=default_args,
    description="Snapshot semanal da view de reservas no BigQuery",
    start_date=pendulum.datetime(2024, 1, 1, tz=local_tz),
    schedule_interval="30 0 * * 1",  # segunda-feira às 00:30
    catchup=False,
    tags=['bigquery', 'gcs', 'historico','reservas']
) as dag:

    snapshot_semanal_fixa = BigQueryInsertJobOperator(
        task_id="snapshot_semanal_reservas",
        configuration={
            "query": {
                "query": """
                INSERT INTO `sz-00022-ws.INDICADORES_PLANEJAMENTO.HISTORICO_RESERVAS`
                SELECT
                  *,
                  CURRENT_TIMESTAMP() AS DATA_HISTORICO
                FROM `sz-00022-ws.INDICADORES_PLANEJAMENTO.VW_RESERVAS`;
                """,
                "useLegacySql": False,
            }
        },
        location="southamerica-east1",
        gcp_conn_id='google_cloud_default'
    )

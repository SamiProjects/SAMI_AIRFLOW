from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import storage
from datetime import datetime
import os

def baixar_csv_para_csv_dir(bucket_name,bucket_blob,path_download):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_blob)
    blob.download_to_filename(path_download)

with DAG(
    dag_id='exportar_status_ordem_semana_plami',
    schedule_interval='0 5 * * 2',
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'exportacao','ordem','semana'],
) as dag:

    criar_tabela_temp_bq = BigQueryInsertJobOperator(
        task_id='criar_tabela_temp_bq',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.PLAMI.TMP_STATUS_ORDEM_SEMANA_PLAMI` AS
                    SELECT * FROM `sz-00022-ws.PLAMI.STATUS_ORDEM_SEMANA_PLAMI`;
                """,
                "useLegacySql": False,
            }
        },
        location='southamerica-east1',
        gcp_conn_id='google_cloud_default'
    )

    exportar_para_gcs = BigQueryToGCSOperator(
        task_id='exportar_tabela_bq_gcs',
        source_project_dataset_table='sz-00022-ws.PLAMI.TMP_STATUS_ORDEM_SEMANA_PLAMI',
        destination_cloud_storage_uris=[
            'gs://airflow_vps/status_ordem_semana_plami.csv'
        ],
        export_format='CSV',
        field_delimiter='\x1f',
        print_header=True,
        compression='NONE',
        gcp_conn_id='google_cloud_default',
        location='southamerica-east1',
        force_rerun=True,
    )   

    baixar_csv = PythonOperator(
        task_id='baixar_csv_gcs',
        python_callable=baixar_csv_para_csv_dir,
        op_args=['airflow_vps', 'status_ordem_semana_plami.csv', '/opt/airflow/csv/status_ordem_semana_plami.csv']
    )

    criar_tabela_temp_pg = BashOperator(
        task_id='criar_tabela_temp_pg',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 -c "
    DROP TABLE IF EXISTS status_ordem_semana_plami_temp;
    CREATE TABLE status_ordem_semana_plami_temp
    (LIKE status_ordem_semana_plami INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY INCLUDING STORAGE);

    -- remove apenas o índice se existir
    DROP INDEX IF EXISTS idx_status_ordem_semana_plami_ordem_semana;
"
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    carregar_csv_postgres = BashOperator(
        task_id='carregar_csv_postgres',
        bash_command="""
            cat /opt/airflow/csv/status_ordem_semana_plami.csv | \
            psql "$PG_CONN" -c "
                COPY status_ordem_semana_plami_temp (
                    ordem,
                    status_sistema_ordem,
                    semana
                  
                )
                FROM STDIN
                DELIMITER E'\\x1f' CSV HEADER;
            "
        """,
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    swap_tabelas = BashOperator(
        task_id='swap_tabelas',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 <<EOF
BEGIN;

-- renomeia a tabela original para backup
ALTER TABLE status_ordem_semana_plami RENAME TO status_ordem_semana_plami_old;

-- renomeia a staging para virar a definitiva
ALTER TABLE status_ordem_semana_plami_temp RENAME TO status_ordem_semana_plami;

-- cria índice na coluna ordem
CREATE INDEX idx_status_ordem_semana_plami_ordem_semana
    ON status_ordem_semana_plami (ordem, semana);

-- remove a tabela antiga
DROP TABLE status_ordem_semana_plami_old;

COMMIT;
EOF
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    limpar_csv_local = BashOperator(
    task_id='limpar_csv_local',
    bash_command='rm -f /opt/airflow/csv/status_ordem_semana_plami.csv'
    )  

    criar_tabela_temp_bq >> exportar_para_gcs >> baixar_csv >> criar_tabela_temp_pg >> carregar_csv_postgres >> swap_tabelas >> limpar_csv_local

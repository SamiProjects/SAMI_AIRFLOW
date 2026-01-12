from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import storage
from datetime import datetime
import os
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
 
def baixar_csv_para_csv_dir(bucket_name,bucket_blob,path_download):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(bucket_blob)
    blob.download_to_filename(path_download)
 
def baixar_csvs(bucket_name, prefix, local_dir):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
 
    os.makedirs(local_dir, exist_ok=True)
 
    for blob in blobs:
        local_file = os.path.join(local_dir, os.path.basename(blob.name))
        blob.download_to_filename(local_file)
 
def limpar_arquivos_gcs(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
 
    for blob in blobs:
        blob.delete()
        print(f"Removido: {blob.name}")
 
with DAG(
    dag_id='exportar_materiais',
    schedule_interval='30 3 * * *',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'exportacao','plami'],
) as dag:
 
    #COLETANDO TODAS AS ORDENS
    criar_tabela_temp_materiais_bq = BigQueryInsertJobOperator(
        task_id='criar_tabela_temp_materiais_bq',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.TABELAS_SAP.TMP_MATERIAIS` AS
                    SELECT * FROM `sz-00022-ws.TABELAS_SAP.MATERIAIS`;
                """,
                "useLegacySql": False,
            }
        },
        location='southamerica-east1',
        gcp_conn_id='google_cloud_default'
    )
 
    exportar_materiais_para_gcs = BigQueryToGCSOperator(
        task_id='exportar_materiais_para_gcs',
        source_project_dataset_table='sz-00022-ws.TABELAS_SAP.TMP_MATERIAIS',
        destination_cloud_storage_uris=[
            'gs://airflow_vps/materiais.csv'
        ],
        export_format='CSV',
        field_delimiter='\x1f',
        print_header=True,
        compression='NONE',
        gcp_conn_id='google_cloud_default',
        location='southamerica-east1',
        force_rerun=True,
    )
 
    baixar_csv_materiais = PythonOperator(
        task_id='baixar_csv_materiais',
        python_callable=baixar_csv_para_csv_dir,
        op_args=['airflow_vps', 'materiais.csv','/opt/airflow/csv/materiais.csv']
    )
   
    criar_tabela_temp_materiais_pg = BashOperator(
        task_id='criar_tabela_temp_materiais_pg',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 -c "
    DROP TABLE IF EXISTS materiais_temp;
    CREATE TABLE materiais_temp
    (LIKE materiais INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY INCLUDING STORAGE);
 
    -- remove apenas o índice se existir
    DROP INDEX IF EXISTS idx_materiais ;
"
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )
 
    carregar_csv_materiais_postgres = BashOperator(
        task_id='carregar_csv_materiais_postgres',
        bash_command="""
            cat /opt/airflow/csv/materiais.csv | \
            psql "$PG_CONN" -c "
                COPY materiais_temp (
                    material,
                    texto_material,
                    tipo_material,
                    criticidade,
                    data_criacao,
                    criado_por,
                    num_lista_tecnica
                )
                FROM STDIN
                DELIMITER E'\\x1f' CSV HEADER;
               
            "
        """,
        env={"PG_CONN": os.getenv("PG_CONN")},
    )
 
    swap_tabelas_materiais = BashOperator(
        task_id='swap_tabelas_materiais',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 <<EOF
BEGIN;
 
-- renomeia a tabela original para backup
ALTER TABLE materiais RENAME TO materiais_old;
 
-- renomeia a staging para virar a definitiva
ALTER TABLE materiais_temp RENAME TO materiais;
 
-- cria índice na coluna ordem
CREATE INDEX IF NOT EXISTS idx_materiais_ordem
    ON materiais (material);
 
-- remove a tabela antiga
DROP TABLE materiais_old;
 
COMMIT;
EOF
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )
 
    limpar_csv_local_materiais = BashOperator(
    task_id='limpar_csv_local_materiais',
    bash_command='rm -f /opt/airflow/csv/materiais.csv'
    )
 
    criar_tabela_temp_materiais_bq >> exportar_materiais_para_gcs >> baixar_csv_materiais >> criar_tabela_temp_materiais_pg >> carregar_csv_materiais_postgres >> swap_tabelas_materiais >> limpar_csv_local_materiais
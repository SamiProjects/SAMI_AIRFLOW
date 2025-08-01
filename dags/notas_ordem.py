from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from google.cloud import storage
from datetime import datetime
import os

def baixar_csv_para_csv_dir():
    client = storage.Client()
    bucket = client.get_bucket('airflow_vps')
    blob = bucket.blob('nota_ordem.csv')
    blob.download_to_filename('/opt/airflow/csv/nota_ordem.csv')

with DAG(
    dag_id='exportar_bq_para_gcs_csv',
    schedule_interval='0 7 * * *',
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'exportacao'],
) as dag:

    criar_tabela_temp_bq = BigQueryInsertJobOperator(
        task_id='criar_tabela_temp_bq',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.PLAMI.TMP_NOTAS_ORDENS` AS
                    SELECT * FROM `sz-00022-ws.PLAMI.NOTAS_ORDENS`;
                """,
                "useLegacySql": False,
            }
        },
        location='southamerica-east1',
        gcp_conn_id='google_cloud_default'
    )

    exportar_para_gcs = BigQueryToGCSOperator(
        task_id='exportar_tabela_bq_gcs',
        source_project_dataset_table='sz-00022-ws.PLAMI.TMP_NOTAS_ORDENS',
        destination_cloud_storage_uris=[
            'gs://airflow_vps/nota_ordem.csv'
        ],
        export_format='CSV',
        field_delimiter='\x1f',
        print_header=True,
        compression='NONE',
        gcp_conn_id='google_cloud_default'
    )   

    baixar_csv = PythonOperator(
        task_id='baixar_csv_gcs',
        python_callable=baixar_csv_para_csv_dir,
    )

    criar_tabela_temp_pg = BashOperator(
        task_id='criar_tabela_temp_pg',
        bash_command="""
            psql "$PG_CONN" -c "
                DROP TABLE IF EXISTS notas_ordem_temp;
                CREATE TABLE notas_ordem_temp (LIKE notas_ordem INCLUDING ALL);
            "
        """,
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    carregar_csv_postgres = BashOperator(
        task_id='carregar_csv_postgres',
        bash_command="""
            cat /opt/airflow/csv/nota_ordem.csv | \
            psql "$PG_CONN" -c "
                COPY notas_ordem_temp (
                    nota, tipo_nota, descricao, status_usuario, status_sistema, ordem, tag, desc_tag, tag_desc, criticidade,
                    grupo_planejamento, centro_trabalho, autor, nome_autor, criado_por, nome_criado, criado, modificado_por,
                    nome_modificado, modificado, data_nota, hora_nota, inicio_desejado, hora_inicio_desejado, fim_desejado,
                    hora_fim_desejado, oportunidade, tipo_prioridade, tipo_servico, tipo_impacto, grau_impacto, probabilidade,
                    prioridade, data_mod_prioridade, tipo_ordem, descricao_ordem, status_usuario_ordem, status_sistema_ordem,
                    centro_trabalho_ordem, grupo_planejamento_ordem, inicio_base_ordem, hora_inicio_base_ordem, fim_base_ordem,
                    hora_fim_base_ordem, prioridade_ordem, revisao, tag_ordem, desc_tag_ordem, criado_por_ordem, nome_criado_ordem,
                    data_criacao_ordem, modificado_por_ordem, nome_modificado_ordem, data_modificacao_ordem, objeto_nota, priorizado,
                    planejado, programado, encerrado, seguranca, vazamento, alarme, custo_planejado_ordem, custo_real_ordem,
                    ordem_priorizada_operacao, classificacao_prioridade
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
            psql "$PG_CONN" -c "
                DROP TABLE IF EXISTS notas_ordem_backup;
                ALTER TABLE notas_ordem RENAME TO notas_ordem_backup;
                ALTER TABLE notas_ordem_temp RENAME TO notas_ordem;
                DROP TABLE IF EXISTS notas_ordem_backup;
            "
        """,
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    limpar_csv_local = BashOperator(
    task_id='limpar_csv_local',
    bash_command='rm -f /opt/airflow/csv/nota_ordem.csv /opt/airflow/csv/nota_ordem.csv.gz'
    )   
    criar_tabela_temp_bq >> exportar_para_gcs >> baixar_csv >> criar_tabela_temp_pg >> carregar_csv_postgres >> swap_tabelas >> limpar_csv_local

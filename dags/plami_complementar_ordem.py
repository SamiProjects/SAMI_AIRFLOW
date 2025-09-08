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
    dag_id='exportar_complementar_ordem_plami',
    schedule_interval='20 7 * * *',
    start_date=datetime(2025, 9, 1),
    catchup=False,
    tags=['bigquery', 'gcs', 'exportacao','plami'],
) as dag:

    criar_tabela_temp_bq = BigQueryInsertJobOperator(
        task_id='criar_tabela_temp_bq',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.PLAMI.TMP_OPERACAO_ORDEM_PLAMI` AS
                    SELECT * FROM `sz-00022-ws.TABELAS_SAP.OPERACAO_ORDEM_PLAMI`;
                """,
                "useLegacySql": False,
            }
        },
        location='southamerica-east1',
        gcp_conn_id='google_cloud_default'
    )

    exportar_para_gcs = BigQueryToGCSOperator(
        task_id='exportar_tabela_bq_gcs',
        source_project_dataset_table='sz-00022-ws.PLAMI.TMP_OPERACAO_ORDEM_PLAMI',
        destination_cloud_storage_uris=[
            'gs://airflow_vps/operacao_ordem_plami.csv'
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
        op_args=['airflow_vps', 'operacao_ordem_plami.csv', '/opt/airflow/csv/operacao_ordem_plami.csv']
    )

    criar_tabela_temp_pg = BashOperator(
        task_id='criar_tabela_temp_pg',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 -c "
    DROP TABLE IF EXISTS operacao_ordem_plami_temp;
    CREATE TABLE operacao_ordem_plami_temp
    (LIKE operacao_ordem_plami INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY INCLUDING STORAGE);

    -- remove apenas o índice se existir
    DROP INDEX IF EXISTS idx_operacao_ordem_plami_ordem;
"
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    carregar_csv_postgres = BashOperator(
        task_id='carregar_csv_postgres',
        bash_command="""
            cat /opt/airflow/csv/operacao_ordem_plami.csv | \
            psql "$PG_CONN" -c "
                COPY operacao_ordem_plami_temp (
                    id_ordem_operacao,
                    ordem,
                    operacao,
                    trabalho,
                    trabalho_real,
                    texto_breve_operacao,
                    descricao_ordem,
                    tipo_ordem,
                    oportunidade,
                    centro_trabalho,
                    centro_trabalho_operacao,
                    tag,
                    desc_tag,
                    area,
                    disciplina,
                    qtd_pessoas,
                    data_modificacao,
                    disciplina_operacao,
                    prioridade_nota,
                    duracao,
                    custo_planejado_ordem,
                    custo_Real_ordem,
                    revisao,
                    vazamento,
                    seguranca,
                    alarme,
                    data_criacao_ordem,
                    nota,
                    classificacao_prioridade,
                    criticidade,
                    status_usuario_ordem,
                    status_sistema_ordem,
                    status_sistema_operacao,
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
ALTER TABLE operacao_ordem_plami RENAME TO operacao_ordem_plami_old;

-- renomeia a staging para virar a definitiva
ALTER TABLE operacao_ordem_plami_temp RENAME TO operacao_ordem_plami;

-- cria índice na coluna ordem
CREATE INDEX IF NOT EXISTS idx_operacao_ordem_plami_ordem
    ON operacao_ordem_plami (ordem);

-- remove a tabela antiga
DROP TABLE operacao_ordem_plami_old;

COMMIT;
EOF
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    limpar_csv_local = BashOperator(
    task_id='limpar_csv_local',
    bash_command='rm -f /opt/airflow/csv/operacao_ordem_plami.csv'
    )  


    ##MATERIAIS
    criar_tabela_temp_materiais_bq = BigQueryInsertJobOperator(
        task_id='criar_tabela_temp_materiais_bq',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.PLAMI.TMP_MATERIAIS_ORDENS_GERAL` AS
                    SELECT * FROM `sz-00022-ws.TABELAS_SAP.MATERIAIS_ORDENS`;
                """,
                "useLegacySql": False,
            }
        },
        location='southamerica-east1',
        gcp_conn_id='google_cloud_default'
    )

    exportar_para_materiais_gcs = BigQueryToGCSOperator(
        task_id='exportar_para_materiais_gcs',
        source_project_dataset_table='sz-00022-ws.PLAMI.TMP_MATERIAIS_ORDENS_GERAL',
        destination_cloud_storage_uris=[
            'gs://airflow_vps/materiais_ordens_geral.csv'
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
        op_args=['airflow_vps', 'materiais_ordens_geral.csv', '/opt/airflow/csv/materiais_ordens_geral.csv']
    )

    criar_tabela_temp_materiais_pg = BashOperator(
        task_id='criar_tabela_temp_materiais_pg',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 -c "
    DROP TABLE IF EXISTS materiais_ordem_geral_temp;
    CREATE TABLE materiais_ordem_geral_temp
    (LIKE materiais_ordem_geral INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY INCLUDING STORAGE);

    -- remove apenas o índice se existir
    DROP INDEX IF EXISTS idx_materiais_ordem_plami ;
"
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    carregar_csv_materiais_postgres = BashOperator(
        task_id='carregar_csv_materiais_postgres',
        bash_command="""
            cat /opt/airflow/csv/materiais_ordens_geral.csv | \
            psql "$PG_CONN" -c "
                COPY materiais_ordem_geral_temp (
                    item_ordem,
                    ordem,
                    material,
                    texto_material,
                    quantidade_necessaria,
                    unidade_medida,
                    ctg_item,
                    deposito,
                    operacao,
                    centro,
                    ponto_descarga,
                    recebedor,
                    preco_moeda,
                    unidade_preco,
                    revisao,
                    tipo_material,
                    data_modificacao,
                    qtd_estoque
                    
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
ALTER TABLE materiais_ordem_geral RENAME TO materiais_ordem_geral_old;

-- renomeia a staging para virar a definitiva
ALTER TABLE materiais_ordem_geral_temp RENAME TO materiais_ordem_geral;

-- cria índice na coluna ordem
CREATE INDEX IF NOT EXISTS idx_materiais_ordem_plami
    ON materiais_ordem_geral (ordem);

-- remove a tabela antiga
DROP TABLE materiais_ordem_geral_old;

COMMIT;
EOF
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )
        
    limpar_csv_local_materiais = BashOperator(
    task_id='limpar_csv_local_materiais',
    bash_command='rm -f /opt/airflow/csv/materiais_ordens_geral.csv'
    )
    
    #COLETANDO TODAS AS ORDENS
    criar_tabela_temp_ordens_bq = BigQueryInsertJobOperator(
        task_id='criar_tabela_temp_ordens_bq',
        configuration={
            "query": {
                "query": """
                    CREATE OR REPLACE TABLE `sz-00022-ws.PLAMI.TMP_ORDENS_GERAL` AS
                    SELECT * FROM `sz-00022-ws.TABELAS_SAP.ORDENS`;
                """,
                "useLegacySql": False,
            }
        },
        location='southamerica-east1',
        gcp_conn_id='google_cloud_default'
    )

    exportar_ordens_geral_para_gcs = BigQueryToGCSOperator(
        task_id='exportar_ordens_geral_para_gcs',
        source_project_dataset_table='sz-00022-ws.PLAMI.TMP_ORDENS_GERAL',
        destination_cloud_storage_uris=[
            'gs://airflow_vps/ordens_geral.csv'
        ],
        export_format='CSV',
        field_delimiter='\x1f',
        print_header=True,
        compression='NONE',
        gcp_conn_id='google_cloud_default',
        location='southamerica-east1',
        force_rerun=True,
    )

    baixar_csv_ordens = PythonOperator(
        task_id='baixar_csv_ordens',
        python_callable=baixar_csv_para_csv_dir,
        op_args=['airflow_vps', 'ordens_geral.csv', '/opt/airflow/csv/ordens_geral.csv']
    )
    
    criar_tabela_temp_ordens_pg = BashOperator(
        task_id='criar_tabela_temp_ordens_pg',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 -c "
    DROP TABLE IF EXISTS ordens_geral_temp;
    CREATE TABLE ordens_geral_temp
    (LIKE ordens_geral INCLUDING DEFAULTS INCLUDING GENERATED INCLUDING IDENTITY INCLUDING STORAGE);

    -- remove apenas o índice se existir
    DROP INDEX IF EXISTS idx_ordens_geral_ordem ;
"
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    carregar_csv_ordens_postgres = BashOperator(
        task_id='carregar_csv_ordens_postgres',
        bash_command="""
            cat /opt/airflow/csv/ordens_geral.csv | \
            psql "$PG_CONN" -c "
                COPY ordens_geral_temp (
                    ordem,
                    descricao_ordem,
                    tipo_ordem,
                    oportunidade,
                    centro_trabalho,
                    data_criacao,
                    criado_por,
                    data_modificacao,
                    modificado_por,
                    tag,
                    desc_tag,
                    criticidade,
                    status_usuario_ordem,
                    status_sistema_ordem,
                    revisao,
                    grupo_planejamento_ordem,
                    inicio_base_ordem,
                    hora_inicio_base_ordem,
                    fim_base_ordem,
                    hora_fim_base_ordem,
                    prioridade_ordem,
                    nome_criado_ordem,
                    nome_modificado_ordem,
                    planejado,
                    programado,
                    encerrado,
                    area,
                    custo_planejado,
                    custo_real
                    
                )
                FROM STDIN
                DELIMITER E'\\x1f' CSV HEADER;
                
            "
        """,
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    swap_tabelas_ordens = BashOperator(
        task_id='swap_tabelas_ordens',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 <<EOF
BEGIN;

-- renomeia a tabela original para backup
ALTER TABLE ordens_geral RENAME TO ordens_geral_old;

-- renomeia a staging para virar a definitiva
ALTER TABLE ordens_geral_temp RENAME TO ordens_geral;

-- cria índice na coluna ordem
CREATE INDEX IF NOT EXISTS idx_ordens_geral_ordem
    ON ordens_geral (ordem);

-- remove a tabela antiga
DROP TABLE ordens_geral_old;

COMMIT;
EOF
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    limpar_csv_local_ordens = BashOperator(
    task_id='limpar_csv_local_ordens',
    bash_command='rm -f /opt/airflow/csv/ordens_geral.csv'
    )

    criar_tabela_temp_bq >> exportar_para_gcs >> baixar_csv >> criar_tabela_temp_pg >> carregar_csv_postgres >> swap_tabelas >> limpar_csv_local
    criar_tabela_temp_materiais_bq >> exportar_para_materiais_gcs >> baixar_csv_materiais >> criar_tabela_temp_materiais_pg >> carregar_csv_materiais_postgres >> swap_tabelas_materiais >> limpar_csv_local_materiais 
    criar_tabela_temp_ordens_bq >> exportar_ordens_geral_para_gcs >> baixar_csv_ordens >> criar_tabela_temp_ordens_pg >> carregar_csv_ordens_postgres >> swap_tabelas_ordens >> limpar_csv_local_ordens
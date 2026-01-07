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
    dag_id='exportar_complementar_ordem_plami',
    schedule_interval='5 6-18/3 * * *',
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
            'gs://airflow_vps/operacao_ordem_plami-*.csv'
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
    task_id="baixar_csvs_gcs",
    python_callable=baixar_csvs,
    op_args=["airflow_vps", "operacao_ordem_plami-", "/opt/airflow/csv"],
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
        for f in /opt/airflow/csv/operacao_ordem_plami-*.csv
        do
            echo "Carregando $f ..."
            cat "$f" | psql "$PG_CONN" -c "
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
                    custo_real_ordem,
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
                    valor_descontado
                )
                FROM STDIN
                DELIMITER E'\\x1f' CSV HEADER;
            "
        done

        """,
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    swap_tabelas = BashOperator(
        task_id='swap_tabelas',
        bash_command="""
psql "$PG_CONN" -v ON_ERROR_STOP=1 <<EOF
BEGIN;

-- remove a view se existir (para não travar o DROP/RENAME da tabela base)
DROP VIEW IF EXISTS view_operacao_ordem_aberta;

-- renomeia a tabela original para backup
ALTER TABLE operacao_ordem_plami RENAME TO operacao_ordem_plami_old;

-- renomeia a staging para virar a definitiva
ALTER TABLE operacao_ordem_plami_temp RENAME TO operacao_ordem_plami;

-- cria índice na coluna ordem
CREATE INDEX IF NOT EXISTS idx_operacao_ordem_plami_ordem
    ON operacao_ordem_plami (ordem);

-- remove a tabela antiga
DROP TABLE operacao_ordem_plami_old;

-- recria a view
CREATE OR REPLACE VIEW view_operacao_ordem_aberta AS
SELECT 
    p.ordem,
    p.operacao,
    p.trabalho,
    p.texto_breve_operacao,
    p.descricao_ordem,
    p.tipo_ordem,
    p.oportunidade,
    p.centro_trabalho,
    p.centro_trabalho_operacao,
    p.tag,
    p.area,
    p.disciplina,
    p.qtd_pessoas,
    p.disciplina_operacao,
    p.prioridade_nota,
    p.duracao,
    p.custo_planejado_ordem,
    p.custo_real_ordem,
    CASE 
        WHEN o.ordem IS NOT NULL THEN 'SIM'
        ELSE 'NÃO'
    END AS ordem_priorizada_operacao,
    p.nota,
    p.revisao,
    p.vazamento,
    p.seguranca,
    p.classificacao_prioridade,
    p.alarme
FROM operacao_ordem_plami p
LEFT JOIN ordem_priorizada_operacao o
       ON p.ordem = o.ordem
WHERE p.status_sistema_ordem NOT ILIKE '%ENTE%'
  AND p.status_sistema_ordem NOT ILIKE '%ENCE%'
  AND p.oportunidade <> '3'
  AND p.tipo_ordem IN ('PM02','PM03','PM04','PM05');

COMMIT;
EOF
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    limpar_csv_local = BashOperator(
    task_id='limpar_csv_local',
    bash_command='rm -f /opt/airflow/csv/operacao_ordem_plami*.csv'
    )  

    limpar_csvs_task = PythonOperator(
    task_id="limpar_csvs_gcs",
    python_callable=limpar_arquivos_gcs,
    op_args=["airflow_vps", "operacao_ordem_plami-"],
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
                    qtd_estoque,
                    confirmacao_final
                    
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
        op_args=['airflow_vps', 'ordens_geral.csv','/opt/airflow/csv/ordens_geral.csv']
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

-- remove a view se existir (para não travar o DROP/RENAME da tabela base)
DROP VIEW IF EXISTS view_ordem_geral;

-- renomeia a tabela original para backup
ALTER TABLE ordens_geral RENAME TO ordens_geral_old;

-- renomeia a staging para virar a definitiva
ALTER TABLE ordens_geral_temp RENAME TO ordens_geral;

-- cria índice na coluna ordem
CREATE INDEX IF NOT EXISTS idx_ordens_geral_ordem
    ON ordens_geral (ordem);

-- remove a tabela antiga
DROP TABLE ordens_geral_old;

-- recria a view
CREATE OR REPLACE VIEW view_ordem_geral AS
SELECT g.*,
       CASE 
           WHEN o.ordem IS NOT NULL THEN 'SIM'
           ELSE 'NÃO'
       END AS ordem_priorizada_operacao
FROM ordens_geral g
LEFT JOIN ordem_priorizada_operacao o
       ON g.ordem = o.ordem;

COMMIT;
EOF
""",
        env={"PG_CONN": os.getenv("PG_CONN")},
    )

    limpar_csv_local_ordens = BashOperator(
    task_id='limpar_csv_local_ordens',
    bash_command='rm -f /opt/airflow/csv/ordens_geral.csv'
    )

    criar_tabela_temp_bq >> exportar_para_gcs >> baixar_csv >> criar_tabela_temp_pg >> carregar_csv_postgres >> swap_tabelas >> limpar_csv_local >> limpar_csvs_task
    criar_tabela_temp_materiais_bq >> exportar_para_materiais_gcs >> baixar_csv_materiais >> criar_tabela_temp_materiais_pg >> carregar_csv_materiais_postgres >> swap_tabelas_materiais >> limpar_csv_local_materiais 
    criar_tabela_temp_ordens_bq >> exportar_ordens_geral_para_gcs >> baixar_csv_ordens >> criar_tabela_temp_ordens_pg >> carregar_csv_ordens_postgres >> swap_tabelas_ordens >> limpar_csv_local_ordens
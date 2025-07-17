from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from datetime import datetime
from newsapi import NewsApiClient
import psycopg2
from airflow.sdk import Variable
from airflow.sensors.external_task import ExternalTaskSensor


def camada_silver():
    DATABASE_URL = Variable.get("DATABASE_URL")

    # Conectar ao banco de dados PostgreSQL
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # Criar a tabela silver.noticias_ai se não existir
    cursor.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    create_silver_table_query = """
    CREATE TABLE IF NOT EXISTS silver.noticias_ai (
        id SERIAL PRIMARY KEY,
        titulo TEXT,
        descricao TEXT,
        url TEXT,
        fonte TEXT,
        data_publicacao TIMESTAMP,
        conteudo_completo TEXT
    );
    """
    cursor.execute(create_silver_table_query)

    # Adicionar a restrição de unicidade (se ainda não existirem)
    cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_titulo_data_publicacao 
    ON silver.noticias_ai (titulo, data_publicacao);
    """)

    conn.commit()

    # Remover duplicatas pelo titulo e data_publicacao da bronze
    remove_duplicates_query = """
    SELECT *
        FROM (
            SELECT 
                ROW_NUMBER() OVER (PARTITION BY titulo, data_publicacao ORDER BY id ASC) AS rn,
                *
            FROM bronze.noticias_ai
        ) sub
        WHERE rn = 1;
    """
    cursor.execute(remove_duplicates_query)
    result = cursor.fetchall()
    conn.commit()
    print("Duplicatas removidas da tabela bronze.noticias_ai.")

    # Inserir dados únicos na camada silver
    insert_silver_query = """
    INSERT INTO silver.noticias_ai (titulo, descricao, url, fonte, data_publicacao, conteudo_completo)
    SELECT titulo, descricao, url, fonte, data_publicacao, conteudo_completo
    FROM (
        SELECT 
            ROW_NUMBER() OVER (PARTITION BY titulo, data_publicacao ORDER BY id ASC) AS rn,
            titulo, descricao, url, fonte, data_publicacao, conteudo_completo
        FROM bronze.noticias_ai
    ) sub
    WHERE rn = 1
    ON CONFLICT (titulo, data_publicacao) DO NOTHING;
    """
    cursor.execute(insert_silver_query)
    conn.commit()
    print("Dados únicos inseridos na camada silver.noticias_ai.")

    cursor.close()
    conn.close()


dag = DAG(
    'dag_2_silver_layer',  # Nome da DAG
    # Descrição da DAG
    schedule='* * * * *',  # Executa a cada 20 segundos
    start_date=datetime(2025, 7, 17, 8, 0),  # Data e hora de início da execução
    catchup=False,  # Impede que o Airflow execute a DAG retroativamente
)

# Esperar pela conclusão da DAG anterior
wait_for_extract = ExternalTaskSensor(
    task_id='wait_for_extract',
    external_dag_id='dag_1_extract_data_from_api',  # Nome da DAG externa que executa a extração
    external_task_id='extract_data_api',  # Nome da task que indica que a extração terminou
    mode='poke',
    timeout=300,  # Tempo máximo de espera em segundos
    poke_interval=5,
    dag=dag,
)

silver_task = PythonOperator(
    task_id='silver_layer_task',
    python_callable=camada_silver,
    dag=dag,
)

wait_for_extract >> silver_task

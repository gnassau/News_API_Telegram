from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from datetime import datetime
from newsapi import NewsApiClient
import psycopg2
from airflow.sdk import Variable
from airflow.sensors.external_task import ExternalTaskSensor


def camada_gold():
    DATABASE_URL = Variable.get("DATABASE_URL")

    # Conectar ao banco de dados PostgreSQL
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # Criar a tabela gold.noticias_ai se não existir
    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gold.metricas (
        fonte TEXT,
        qtd INTEGER
    );
    INSERT INTO gold.metricas (fonte, qtd)
    SELECT fonte, COUNT(fonte) as qtd
    FROM silver.noticias_ai
    GROUP BY fonte
    ORDER BY qtd DESC;
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold.termos (
        termo TEXT,
        qtd INTEGER
    );
    INSERT INTO gold.termos (termo, qtd)
    SELECT 
        CASE 
            WHEN LOWER(titulo) LIKE '%google%' THEN 'Google'
            WHEN LOWER(titulo) LIKE '%ia%' THEN 'IA'
            WHEN LOWER(titulo) LIKE '%samsung%' THEN 'Samsung'
            WHEN LOWER(titulo) LIKE '%microsoft%' THEN 'Microsoft'
        END AS termo,
        COUNT(*) AS qtd
    FROM 
        silver.noticias_ai na 
    WHERE
        LOWER(titulo) LIKE '%google%'
        OR LOWER(titulo) LIKE '%ia%'
        OR LOWER(titulo) LIKE '%samsung%'
        OR LOWER(titulo) LIKE '%microsoft%'
    GROUP BY 
        termo
                   """)

    
    conn.commit()
    cursor.close()
    conn.close()


dag = DAG(
    'dag_3_gold_layer',  # Nome da DAG
    # Descrição da DAG
    schedule='* * * * *',  # Executa a cada 20 segundos
    start_date=datetime(2025, 7, 17, 8, 0),  # Data e hora de início da execução
    catchup=False,  # Impede que o Airflow execute a DAG retroativamente
)

# Esperar pela conclusão da DAG anterior
wait_for_extract = ExternalTaskSensor(
    task_id='wait_for_extract',
    external_dag_id='dag_2_silver_layer',  # Nome da DAG externa que executa a extração
    external_task_id='silver_layer_task',  # Nome da task que indica que a extração terminou
    mode='poke',
    timeout=300,  # Tempo máximo de espera em segundos
    poke_interval=5,
    dag=dag,
)

gold_task = PythonOperator(
    task_id='gold_layer_task',
    python_callable=camada_gold,
    dag=dag,
)

wait_for_extract >> gold_task

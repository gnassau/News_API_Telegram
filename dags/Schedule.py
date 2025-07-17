from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from datetime import datetime

from newsapi import NewsApiClient
from dotenv import load_dotenv
import json
import pandas as pd
import psycopg2
import os
load_dotenv()
from airflow.sdk import Variable


# Função que consome a API
def consumir_api():
    # Carregar a chave da API do arquivo .env
    API_KEY = Variable.get("API_KEY")
    DATABASE_URL = Variable.get("DATABASE_URL")

    # Conectar ao banco de dados PostgreSQL
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    print("Conectado ao banco de dados com sucesso!")

    # Função para criar a tabela no banco de dados (caso não exista)
    def criar_tabela():
        cursor.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
        create_table_query = """
        CREATE TABLE IF NOT EXISTS bronze.noticias_ai (
            id SERIAL PRIMARY KEY,
            titulo TEXT,
            descricao TEXT,
            url TEXT,
            fonte TEXT,
            data_publicacao TIMESTAMP,
            conteudo_completo TEXT
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        print("Tabela 'noticias' criada com sucesso na camada bronze.")

    # Obter as principais notícias
    def obter_noticias():
        newsapi = NewsApiClient(api_key=API_KEY)
        everything = newsapi.get_everything(
            q='inteligência artificial'
            , language='pt'
            , sort_by='relevancy')
        return everything


    def salvar_noticias(everything):
        # Loop para processar os artigos
        for article in everything['articles']:
            titulo = article['title']
            descricao = article['description']
            url = article['url']
            fonte = article['source']['name']
            data_publicacao = article['publishedAt']
            conteudo_completo = article.get('content', 'Conteúdo não disponível')

            # Inserir os dados no banco de dados
            inserir_noticia(titulo, descricao, url, fonte, data_publicacao, conteudo_completo)

    # Função para inserir os dados no banco de dados
    def inserir_noticia(titulo, descricao, url, fonte, data_publicacao, conteudo_completo):
        cursor.execute("""
            INSERT INTO bronze.noticias_ai (titulo, descricao, url, fonte, data_publicacao, conteudo_completo)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (titulo, descricao, url, fonte, data_publicacao, conteudo_completo))
        conn.commit()

    # Criar a tabela no banco de dados
    criar_tabela()

    # Obter as notícias
    noticias = obter_noticias()

    # Salvar as notícias no banco de dados
    salvar_noticias(noticias)

    # Fechar a conexão com o banco de dados
    cursor.close()
    conn.close()

    print("Notícias inseridas com sucesso na camada bronze!")


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

def camada_gold():
    DATABASE_URL = Variable.get("DATABASE_URL")

    # Conectar ao banco de dados PostgreSQL
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # Criar a tabela gold.noticias_ai se não existir
    cursor.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS gold.teste (
        id SERIAL PRIMARY KEY,
        nome TEXT,
        valor INTEGER
    );
    """)
    
    conn.commit()
    cursor.close()
    conn.close()


# Definir o DAG
dag = DAG(
    'consumir_api_diariamente',  # Nome da DAG
    # Descrição da DAG
    schedule='* * * * *',  # Executa a cada 20 segundos
    start_date=datetime(2025, 7, 2, 8, 0),  # Data e hora de início da execução
    catchup=False,  # Impede que o Airflow execute a DAG retroativamente
)

consumindo_apis = PythonOperator(
    task_id='consumindo_apis',  # ID da tarefa
    python_callable=consumir_api,  # Função a ser chamada
    dag=dag,  # DAG associada à tarefa
)


sleep_1 = BashOperator(task_id="sleep_1",bash_command="sleep 3",dag=dag)

camada_silver_task = PythonOperator(
    task_id='camada_silver',  # ID da tarefa
    python_callable=camada_silver,  # Função a ser chamada
    dag=dag,  # DAG associada à tarefa
)

camada_gold_task = PythonOperator(
    task_id='camada_gold',  # ID da tarefa
    python_callable=camada_gold,  # Função a ser chamada
    dag=dag,  # DAG associada à tarefa
)

sleep_2 = BashOperator(task_id="sleep_2",bash_command="sleep 3",dag=dag)


consumindo_apis >> sleep_1 >> camada_silver_task >> sleep_2 >> camada_gold_task # Definindo a ordem de execução das tarefas  
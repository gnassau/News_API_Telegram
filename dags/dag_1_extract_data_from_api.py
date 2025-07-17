from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from datetime import datetime
from newsapi import NewsApiClient
import psycopg2
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


dag = DAG(
    'dag_1_extract_data_from_api',  # Nome da DAG
    # Descrição da DAG
    schedule='* * * * *',  # Executa a cada 20 segundos
    start_date=datetime(2025, 7, 17, 8, 0),  # Data e hora de início da execução
    catchup=False,  # Impede que o Airflow execute a DAG retroativamente
)

extract_task = PythonOperator(
    task_id='extract_data_api',
    python_callable=consumir_api,
    dag=dag,
)

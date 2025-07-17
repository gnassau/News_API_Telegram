import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.sdk import Variable
from airflow.sensors.external_task import ExternalTaskSensor
import psycopg2

# Defina o token do seu bot e o chat_id para enviar a mensagem
TELEGRAM_TOKEN = Variable.get("TELEGRAM_TOKEN")
CHAT_ID = Variable.get("CHAT_ID")

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    params = {
        'chat_id': CHAT_ID,
        'text': message,
        'parse_mode': 'Markdown'
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        print("Mensagem enviada com sucesso!")
    else:
        print(f"Falha ao enviar mensagem. Código de status: {response.status_code}")


# Função para realizar a consulta no banco de dados e formatar o resultado
def query_and_send_result():
    # Conectar ao banco de dados PostgreSQL
    DATABASE_URL = Variable.get("DATABASE_URL")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    # Defina a consulta SQL (ajuste conforme necessário)
    query = """
    SELECT titulo, url, data_publicacao
    FROM silver.noticias_ai
    ORDER BY data_publicacao DESC
    LIMIT 5
    """

    # Executar a consulta
    cursor.execute(query)
    results = cursor.fetchall()

    # Formatar os resultados como uma string para enviar via Telegram
    message = "🔔 *Últimas Notícias:*\n\n"
    for row in results:
        titulo = row[0]  # Título da notícia
        url = row[1]  # URL da notícia
        data_publicacao = row[2] # Data de publicação
        data_formatada = data_publicacao.strftime('%d/%m/%Y %H:%M')
        message += f"*Notícia:* {titulo}\n*URL:* {url}\n*Data:* {data_formatada}\n\n"

    # Enviar os resultados via Telegram
    send_telegram_message(message)

    # Fechar a conexão com o banco de dados
    cursor.close()
    conn.close()


# Definindo a DAG
dag = DAG(
    'Send_telegram_message',  # Nome da DAG
    description='Uma DAG para enviar mensagem via Telegram',
    schedule='* * * * *',  # Pode ser agendado para rodar manualmente ou conforme necessidade
    start_date=datetime(2025, 7, 17, 8, 0),  # Data de início
    catchup=False,  # Não executa tarefas passadas
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

# Definindo a tarefa para enviar a mensagem
send_message_task = PythonOperator(
    task_id='send_message_task',
    python_callable=query_and_send_result,
    dag=dag
)


wait_for_extract >> send_message_task


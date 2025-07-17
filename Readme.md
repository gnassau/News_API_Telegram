# News API Telegram

Este projeto utiliza o Airflow para consumir a API NewsAPI, realizando a busca das notícias mais relevantes em vários portais de notícias do Brasil. A busca é feita com base em palavras-chave, como "Inteligência Artificial" e "Big Techs", de acordo com as preferências do usuário. As notícias selecionadas são então enviadas diretamente para o aplicativo do Telegram no horário desejado.

# Tecnologias

1. Airflow
2. Postgres Local
3. Python
4. Telegram API

# Instruções

1. Crie um banco de dados Postgres local.
2. Instale e rode o Airflow na sua máquina e configure as credenciais em Variables.
3. Gere um token da news API em https://newsapi.org/
4. Consiga as credenciais de API do Telegram.
5. Rode as DAGS dentro do Airflow.
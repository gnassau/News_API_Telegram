# News API Telegram

Este projeto usa Airflow para consumir a API NewsAPI, buscando as notícias mais relevantes em diversos portais, baseado em palavras chave, como, "Inteligência Artificial", "Big Techs", conforme preferência.

# Tecnologias

1. Airflow
2. Postgres Local
3. Python
4. Telegram API

# Instruções

1. Crie um banco de dados Postgres local
2. Instale e rode o Airflow na sua máquina e configure as credenciais em Variables.
3. Gere um token da news API em https://newsapi.org/
4. Consiga as credenciais do Telegram
5. Rode as DAGS dentro do Airflow.
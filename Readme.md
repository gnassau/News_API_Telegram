# News API Telegram

Este projeto utiliza o Airflow para consumir a API NewsAPI, realizando a busca das notícias mais relevantes em vários portais de notícias do Brasil. A busca é feita com base em palavras-chave, como "Inteligência Artificial" e "Big Techs", de acordo com as preferências do usuário. As notícias selecionadas são então enviadas diretamente para o aplicativo do Telegram no horário desejado. Além disso, elas são armazenadas nas camadas Bronze, Silver e Gold do Postgres, estrutura conhecida na Engenharia de Dados.

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
5. Clone o repositório para pegar as configurações do container no Docker - instale a lib dentro de requirements.txt.
6. Rode as DAGS dentro do Airflow após configurar os intervalos de tempo desejados.

# Como conseguir as credenciais do Telegram

####  Criar um bot no Telegram:

1. Vá até o [BotFather no Telegram](https://t.me/botfather).

2. Crie um novo bot com o comando /newbot.

3. Depois de criar o bot, o BotFather fornecerá um token de acesso que você usará na sua DAG para enviar mensagens via Telegram.

#### Obter o chat_id:

1. Você pode obter o chat_id de um grupo ou de um chat individual. Uma forma simples de obter o chat_id é enviar uma mensagem ao seu bot e, em seguida, fazer uma requisição HTTP para a API do Telegram.

2. Use esta URL para obter o chat_id:
```bash
https://api.telegram.org/bot<SEU_TOKEN>/getUpdates
```
Isso retornará os dados do chat e você poderá identificar o chat_id.
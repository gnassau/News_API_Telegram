FROM apache/airflow:3.0.2-python3.12

COPY requirements.txt /

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt
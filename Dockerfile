FROM apache/airflow:2.9.1-python3.11

USER root

# Copia o requirements.txt
COPY requirements.txt /requirements.txt

# Instala todos os pacotes da sua venv
RUN pip install --no-cache-dir -r /requirements.txt

USER airflow

FROM apache/airflow:2.9.1-python3.11

ARG AIRFLOW_VERSION=2.9.1
ARG PYTHON_VERSION=3.11
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt

# Instala usando o constraints do Airflow (evita ResolutionImpossible)
RUN pip install --no-cache-dir --constraint "${CONSTRAINT_URL}" -r /opt/airflow/requirements.txt

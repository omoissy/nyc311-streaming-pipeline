FROM apache/airflow:2.10.5-python3.11

COPY requirements.txt /requirements.txt
# Pinning apache-airflow alongside the extras stops pip from silently upgrading it.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

FROM apache/airflow:2.9.3-python3.11

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN grep -v "^apache-airflow" /tmp/requirements.txt > /tmp/pip-requirements.txt \
    && pip install --no-cache-dir -r /tmp/pip-requirements.txt

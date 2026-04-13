FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV PYTHONPATH=/opt/data-pipeline-ai

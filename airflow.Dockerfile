FROM apache/airflow:2.11.0-python3.11

# --- Step 1: pakai root untuk install OS deps (Java dsb.)
USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget curl && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# --- Step 2: kembali ke airflow user (bukan root!)
USER airflow

# Install Python dependencies dengan user airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark

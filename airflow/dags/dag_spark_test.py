from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import requests
import os

DATA_DIR = "/mnt/data"
INPUT_FILE = os.path.join(DATA_DIR, "input.csv")
OUTPUT_DIR = os.path.join(DATA_DIR, "output")

CSV_URL = "https://raw.githubusercontent.com/dataprofessor/data/master/penguins.csv"

def download_csv():
    os.makedirs(DATA_DIR, exist_ok=True)
    r = requests.get(CSV_URL)
    with open(INPUT_FILE, "wb") as f:
        f.write(r.content)
    print(f"Downloaded CSV to {INPUT_FILE}")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 9, 16),
    "retries": 1
}

with DAG(
    "spark_test_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Task 1: Download CSV
    t1_download = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv
    )

    # Task 2: Spark job
    t2_spark = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark/jobs/my_spark_job.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            INPUT_FILE,
            OUTPUT_DIR
        ]
    )

    t1_download >> t2_spark

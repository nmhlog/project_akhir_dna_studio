from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import requests
import os
import pendulum  
import zipfile

def download_file_from_gdrive():
    # Link download langsung dari Google Drive
    file_id = Variable.get("file_id_naufal", default_var="1zVER3BGOcF4URiIebR_nBB8faZnYY4yP")
    output_file_zip = Variable.get("output_file_zip", default_var="dags/data/download_file.zip")
    url = f"https://drive.google.com/uc?export=download&id={file_id}"

    # Pastikan folder tujuan ada
    os.makedirs(os.path.dirname(output_file_zip), exist_ok=True)

    # Download pakai requests
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(output_file_zip, "wb") as f:
            for chunk in response.iter_content(1024):
                f.write(chunk)
        print(f"File berhasil diunduh ke: {output_file_zip}")
    else:
        print(response)
        
def unzip_in_same_folder():
    input_zip = Variable.get("output_file_zip", default_var="dags/data/download_file.zip")

    # folder asal file zip
    output_dir = os.path.dirname(input_zip)

    with zipfile.ZipFile(input_zip, 'r') as zip_ref:
        zip_ref.extractall(output_dir)

    print(f"✅ File {input_zip} berhasil di-unzip ke {output_dir}")
    
def delete_download_file():
    
    input_zip = Variable.get("output_file_zip", default_var="dags/data/download_file.zip")

    if os.path.exists(input_zip):
        os.remove(input_zip)
        print(f"✅ File {input_zip} berhasil dihapus")
    else:
        print(f"⚠️ File {input_zip} tidak ditemukan")   

with DAG(
    dag_id="dag_naufal_download_data_zip",
    start_date=pendulum.datetime(2025, 1, 1, tz=pendulum.timezone("Asia/Jakarta")),  # ✅ pakai pendulum
    schedule_interval=None,
    catchup=False
) as dag:

    download_task = PythonOperator(
        task_id="download_from_gdrive",
        python_callable=download_file_from_gdrive
    )

    unzip_task = PythonOperator(
        task_id="unzip_in_same_folder",
        python_callable=unzip_in_same_folder
    )
    
    
    delete_download_file = PythonOperator(
        task_id="delete_download_file",
        python_callable=delete_download_file
    )

    download_task >> unzip_task>>delete_download_file
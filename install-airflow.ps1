# install-airflow.ps1
$AIRFLOW_VERSION="2.11.0"
$PYTHON_VERSION="3.12"
$CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION/constraints-$PYTHON_VERSION.txt"

Write-Host "Installing Apache Airflow $AIRFLOW_VERSION with Python $PYTHON_VERSION ..."
pip install "apache-airflow==$AIRFLOW_VERSION" --constraint $CONSTRAINT_URL

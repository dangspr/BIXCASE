from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ..from_bard import extract_data_from_api
from ..from_bard import ingest_data_to_snowflake

with DAG(
    dag_id="extract_api",
    start_date=datetime(2024, 3, 2),  # Adjust start date
    schedule_interval="@daily",  # Adjust scheduling as needed
) as dag:

    # Extract data from API
    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_from_api,
    )

    # Ingest data into Snowflake
    ingest_data_task = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data_to_snowflake,
        op_args=[extract_data_task.output],  # Pass output of previous task
    )
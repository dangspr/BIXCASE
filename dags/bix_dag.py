from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from sfconnect import orquestrate
from config import sfacess_infos


with DAG(
    dag_id="extract_api",
    start_date=datetime(2024, 3, 2),  # Adjust start date
    schedule_interval="@daily",  # Adjust scheduling as needed
) as dag:

    # Extract data from API
    extract_data_task = PythonOperator(
        op_kwargs= { "user": os.getenv("USER"),
                    "password": os.getenv("PASSWORD"),
                    "account": os.getenv("ACCOUNT"),
                    "database": os.getenv("DATABASE"),
                    "warehouse": os.getenv("warehouse"),
                    "schema": os.getenv("SCHEMA"),
                    },
        task_id="extract_data",
        python_callable= orquestrate,
    )
    
   
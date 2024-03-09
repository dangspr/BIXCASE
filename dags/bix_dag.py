from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from dags.import_api import orquestrate
from config import sfacess_infos
from dags.import_file import orquestrate_google


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
    
        # Extract data from API
    extract_file_data_task = PythonOperator(
        op_kwargs= { "user": os.getenv("USER"),
                    "password": os.getenv("PASSWORD"),
                    "account": os.getenv("ACCOUNT"),
                    "database": os.getenv("DATABASE_CATEGORIA"),
                    "warehouse": os.getenv("warehouse"),
                    "schema": os.getenv("SCHEMA"),
                    },
        task_id="extract_data",
        python_callable= orquestrate_google,
    )
   
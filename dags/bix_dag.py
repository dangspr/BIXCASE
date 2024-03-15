from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from import_api import orquestrate
from config import sfacess_infos
from import_file import orquestrate_google
from complete_flow import complete_flow


with DAG(
    dag_id="extract_data",
    start_date=datetime(2024, 3, 2),  # Adjust start date
    schedule_interval="@daily",  # Adjust scheduling as needed
) as dag:

    # Extract data from API
    extract_api_task = PythonOperator(
        op_kwargs= { "user": os.getenv("USER"),
                    "password": os.getenv("PASSWORD"),
                    "account": os.getenv("ACCOUNT"),
                    "database": os.getenv("DATABASE"),
                    "warehouse": os.getenv("warehouse"),
                    "schema": os.getenv("SCHEMA"),
                    },
        task_id="extract_data_api",
        python_callable= orquestrate,
    )
    
        # Extract data from Parquet File
    extract_file_data_task = PythonOperator(
        op_kwargs= { "user": os.getenv("USER"),
                    "password": os.getenv("PASSWORD"),
                    "account": os.getenv("ACCOUNT"),
                    "database": os.getenv("DATABASE_CATEGORIA"),
                    "warehouse": os.getenv("warehouse"),
                    "schema": os.getenv("SCHEMA"),
                    },
        task_id="extract_data_parquet",
        python_callable= orquestrate_google,
        op_args=[extract_api_task.output],
    )
    
     # Complete flow
    extract_file_data_task = PythonOperator(
        task_id="complete_flow",
        python_callable= complete_flow,
        op_args=[extract_api_task.output,extract_file_data_task.output],
    )
   
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from snowflake.connector import connect
from datetime import datetime
import os
from ..config import sfacess_infos
from ..config import extract_data_from_api



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
        python_callable= extract_data_from_api,
    )
    
    
    # # Ingest data into Snowflake
    # ingest_data_task = PythonOperator(
    #     task_id="ingest_data",
    #     python_callable=ingest_data_to_snowflake,
    #     op_args=[extract_data_task.output],  # Pass output of previous task
    # )
    ###
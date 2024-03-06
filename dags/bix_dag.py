from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from snowflake.connector import connect
import snowflake.connector
import pandas as pd
from config import sfacess_infos
import requests
import os


# lista que armazena os ID's usados para cada chamada
lista_id = ['1','2','3','4','5','6','7','8','9']
lista_funcionario = []
df = []


# Laco criado para percorrer lista e chamar todos os id's necessarios

for id in lista_id:    
    url = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={}".format(id)
    req = requests.get(url, timeout=3).text
    funcionario = req
    lista_funcionario.append(funcionario)
    print(lista_funcionario)
       
    # Replace with your API call and data processing logic
    # Example using requests library:
    response = req
    data = lista_funcionario
    
    continue
    
def extract_data_from_api():    
    # Process and clean data (example using pandas)
    df = pd.DataFrame(lista_funcionario)
    df = df.dropna()  # Remover linhas com valores nulos
    return df
    

def python_connector():
# Replace with your Snowflake connection details
    connection = snowflake.connector.connect(
    user = os.getenv("USER"),
    password = os.getenv("PASSWORD"),
    account = os.getenv("ACCOUNT"),
    database = os.getenv("DATABASE"),
    warehouse = os.getenv("warehouse"),
    schema = os.getenv("SCHEMA"),
)
# Criar um cursor para executar comandos SQL
    cursor = connection.cursor()

# Preparar a consulta SQL
    consulta_sql = pd.io.sql.write_pandas(df=df, con=connection, index=False, table="BIXAPI.FUNCIONARIO.RAW_DATA", method="copy")

# Executar a consulta SQL
    cursor.execute(consulta_sql)

# Fechar o cursor e a conexão
    cursor.close()
    connection.close()

with DAG(
    dag_id="extract_api",
    start_date=datetime(2024, 3, 2),  # Adjust start date
    schedule_interval="@daily",  # Adjust scheduling as needed
) as dag:

    # Extract data from API
    extract_data_task = PythonOperator(
        task_id="extract_data",
        python_callable= extract_data_from_api,
    )
    
     # Connect into Snowflake
    ingest_data_task = PythonOperator(
        task_id="python_connector",
        python_callable= python_connector,
        op_args=[extract_data_task.output],  # Pass output of previous task
    )

    # # Ingest data into Snowflake
    # ingest_data_task = PythonOperator(
    #     task_id="ingest_data",
    #     python_callable=ingest_data_to_snowflake,
    #     op_args=[extract_data_task.output],  # Pass output of previous task
    # )
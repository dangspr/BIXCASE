from snowflake.connector import connect
import snowflake.connector
import pandas as pd
from config import sfacess_infos
import requests
import os
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


# lista que armazena os ID's usados para cada chamada
lista_id = ['1','2','3','4','5','6','7','8','9']
lista_funcionario = []
df = []


# Laco criado para percorrer lista e chamar todos os id's necessarios
def extract_data_from_api(): 
    for id in lista_id:    
        url = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={}".format(id)
        req = requests.get(url, timeout=5).text
        funcionario = req
        lista_funcionario.append(funcionario)
        print(lista_funcionario)
        print(sfacess_infos)
        
        # Receber e atribuir o processamento da API para data
        response = req
        data = lista_funcionario
        
        continue
    
    # Processar com pandas e inserir num dataframe
    df = pd.DataFrame({
        "ID_FUNCIONARIO": lista_id,
        "NOME_FUNCIONARIO": lista_funcionario    
    })
    df = df.dropna()  # Remover linhas com valores nulos
    nome_funcionario = df
    
    
# Inserir credenciais com variaveis de ambiente
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
    cursor.execute("USE BIXAPI")

# Preparar a consulta SQL
    consulta_sql = write_pandas(auto_create_table=True,database=os.getenv("DATABASE"),schema=os.getenv("SCHEMA"), df=df, conn=connection, index=False, table_name="FUNCIONARIO")

# Executar a consulta SQL
    cursor.execute(consulta_sql)

# Fechar o cursor e a conexão
    cursor.close()
    connection.close()
    return df

extract_data_from_api()
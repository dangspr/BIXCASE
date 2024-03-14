from snowflake.connector import connect
import snowflake.connector
import pandas as pd
from config import sfacess_infos
import requests
import os
from snowflake.connector.pandas_tools import write_pandas


# lista que armazena os ID's usados para cada chamada
lista_id = ['1','2','3','4','5','6','7','8','9']
lista_funcionario = []

# Inserir credenciais com variaveis de ambiente
connection = snowflake.connector.connect(
    user = os.getenv("USER"),
    password = os.getenv("PASSWORD"),
    account = os.getenv("ACCOUNT"),
    database = os.getenv("DATABASE"),
    warehouse = os.getenv("warehouse"),
    schema = os.getenv("SCHEMA"),
)

# Laco criado para percorrer lista e chamar todos os id's necessarios
def extract_data_from_api(): 
    for id in lista_id:    
        url = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={}".format(id)
        req = requests.get(url, timeout=5).text
        funcionario = req
        lista_funcionario.append(funcionario)
        response = req
                
        continue
    
    # Processar dados no pandas
    df = pd.DataFrame({
        "ID_FUNCIONARIO": lista_id,
        "NOME_FUNCIONARIO": lista_funcionario    
    })
    df = df.dropna()  # Remover linhas com valores nulos
    
    return df
  
def validate():          
    # Criar um cursor para executar comandos SQL
    cursor = connection.cursor()
    
    cursor.execute(f"""
        SELECT EXISTS (
        SELECT *
        FROM FUNCIONARIO
        WHERE ID_FUNCIONARIO BETWEEN 1 and 9
        );
        """) 
    
    # Verificação do resultado    
    existe = cursor.fetchone()[0]
    cursor.close()
        
        
    if existe:
        # Fechar o cursor e a conexão
        print(f"Os ID's de Funcionario já estao na tabela.")
        return True
                    

    else:
        print(f"Os ID's de Funcionario ainda nao foram inseridos e serao adicionados a tabela...")
        return False
             
   

def insert_into_snowflake(df): 
    cursor = connection.cursor()
    
    # Preparar a consulta SQL
    consulta_sql = write_pandas(auto_create_table=True,database=os.getenv("DATABASE"),schema=os.getenv("SCHEMA"), df=df, conn=connection, index=False, table_name="FUNCIONARIO")

    # Executar a consulta SQL
    
    try:
        cursor.execute(consulta_sql)
    except:
           pass

    # Fechar o cursor e a conexão
    connection.close()
    connection.close()
    print("Tarefa Concluida")
  
def orquestrate():
        
    df = extract_data_from_api()
    
    if not validate():
        insert_into_snowflake(df)
        


orquestrate()
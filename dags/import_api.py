from snowflake.connector import connect
import snowflake.connector
import pandas as pd
from config import sfacess_infos
import requests
import os
from snowflake.connector.pandas_tools import write_pandas


# list that stores the IDs used for each call
lista_id = ['1','2','3','4','5','6','7','8','9']
lista_funcionario = []

# Insert credentials with environment variables
connection = snowflake.connector.connect(
    user = os.getenv("USER"),
    password = os.getenv("PASSWORD"),
    account = os.getenv("ACCOUNT"),
    database = os.getenv("DATABASE"),
    warehouse = os.getenv("warehouse"),
    schema = os.getenv("SCHEMA"),
)

# Loop created to traverse the list and call all necessary ids
def extract_data_from_api(): 
    for id in lista_id:    
        url = "https://us-central1-bix-tecnologia-prd.cloudfunctions.net/api_challenge_junior?id={}".format(id)
        req = requests.get(url, timeout=5).text
        funcionario = req
        lista_funcionario.append(funcionario)
        response = req
                
        continue
    
# Process data in pandas
    df = pd.DataFrame({
        "ID_FUNCIONARIO": lista_id,
        "NOME_FUNCIONARIO": lista_funcionario    
    })
# Remove rows with null values
    df = df.dropna()  
    
    return df
  
def validate():  
# Open a cursor to execute SQL commands          
    cursor = connection.cursor()
    
    cursor.execute(f"""
        SELECT EXISTS (
        SELECT *
        FROM FUNCIONARIO
        WHERE ID_FUNCIONARIO BETWEEN 1 and 9
        );
        """) 
    
# Checking the result    
    existe = cursor.fetchone()[0]
    cursor.close()
        
        
    if existe:
        print(f"Os ID's de Funcionario já estao na tabela.")
        return True
                    
    else:
        print(f"Os ID's de Funcionario ainda nao foram inseridos e serao adicionados a tabela...")
        return False
           

def insert_into_snowflake(df): 
    cursor = connection.cursor()
    
    # Prepare SQL Query
    consulta_sql = write_pandas(auto_create_table=True,database=os.getenv("DATABASE"),schema=os.getenv("SCHEMA"), df=df, conn=connection, index=False, table_name="FUNCIONARIO")

    # Execute SQL Query
    
    try:
        cursor.execute(consulta_sql)
    except:
           pass

  # Close the cursor and the connection
    connection.close()
    connection.close()
    print("Tarefa Concluida")
  
def orquestrate():
        
    df = extract_data_from_api()
    
    if not validate():
        insert_into_snowflake(df)
        


#orquestrate()
from snowflake.connector import connect
import snowflake.connector
import pandas as pd
from config import sfacess_infos
import requests
import os
from snowflake.connector.pandas_tools import write_pandas

cat = []
nome_cat = []
id_cat = []

# Insert credentials with environment variables
connection = snowflake.connector.connect(
    user = os.getenv("USER"),
    password = os.getenv("PASSWORD"),
    account = os.getenv("ACCOUNT"),
    database = os.getenv("DATABASE_CATEGORIA"),
    warehouse = os.getenv("warehouse"),
    schema = os.getenv("SCHEMA"),
)

def extract_data_from_google():
     url = "https://storage.googleapis.com/challenge_junior/categoria.parquet"
     req = requests.get(url, timeout=5)
     categoria = req
        
     response = requests.get(url)
     
     
     
     print ("Status da requisição:", response.status_code)
     
     if response.status_code == 200:
      with open('categoria.parquet', 'wb') as f:
        f.write(response.content)
        print('Arquivo baixado com sucesso!')
     else:
      print('Erro ao baixar o arquivo:', response.status_code)
      
# Download and Read Parquet file
     
     df = pd.read_parquet('categoria.parquet')
     cat.append(df)
     id_cat = df['id']
     nome_cat = df["nome_categoria"]
         
 
     #cat.info()
     
     df = pd.DataFrame({
        "ID": id_cat,
        "NOME_CATEGORIA": nome_cat  
    })
     
   
     print(df)
     
     return df


def validate_google():          
# Open a cursor to execute SQL commands          
    cursor = connection.cursor()
    
    cursor.execute(f"""
        SELECT EXISTS (
        SELECT *
        FROM CATEGORIA
        WHERE ID BETWEEN 1 and 8
        );
        """) 
    
# Checking the result   
    existe = cursor.fetchone()[0]
    cursor.close()
        
        
    if existe:
        print(f"Os ID's de Categoria já estao na tabela.")
        return True
                    

    else:
        print(f"Os ID's de Categoria ainda nao foram inseridos e serao adicionados a tabela...")
        return False
             

def insert_google_into_snowflake(df):  
# Open a cursor to execute SQL commands  
   cursor = connection.cursor()
     
# Prepare SQL Query 
   consulta_sql = write_pandas(auto_create_table=True,database=os.getenv("DATABASE_CATEGORIA"),schema=os.getenv("SCHEMA"), df=df, conn=connection, index=False, table_name="CATEGORIA") 
   

# Execute SQL Query
    
   try:
      cursor.execute(consulta_sql)
   except:
           pass

   print("Tarefa Concluida")
# Close the cursor and the connection
   connection.close()
   connection.close()   
    
  
def orquestrate_google():
    df = extract_data_from_google()
    
    if not validate_google():
      insert_google_into_snowflake(df)    


#orquestrate_google()
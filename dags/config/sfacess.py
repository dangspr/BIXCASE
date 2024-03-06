import os
from dotenv import load_dotenv
#from .sfconnect import extract_data_from_api

load_dotenv()
#extract_data_from_api()

sfacess_infos = {
    "USER": os.getenv('USER'),
    "PASSWORD": os.getenv('PASSWORD'),
    "ACCOUNT": os.getenv('ACCOUNT'),
    "SESSION": os.getenv('SESSION'),
    "SCHEMA": os.getenv('SCHEMA')

}
import os
from dotenv import load_dotenv

load_dotenv()

sfacess_infos = {
    "USER": os.getenv('USER'),
    "PASSWORD": os.getenv('PASSWORD'),
    "ACCOUNT": os.getenv('ACCOUNT'),
    "SESSION": os.getenv('SESSION'),

}
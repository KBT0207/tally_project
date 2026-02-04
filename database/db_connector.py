
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from urllib.parse import quote_plus



load_dotenv('.env')


class DatabaseConnector:
    def __init__(self, username, password, host, port, database) -> None:
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = create_engine(self.get_db_string(),isolation_level='READ COMMITTED')


    def get_db_string(self):
        encoded_username = quote_plus(self.username) 
        encoded_password = quote_plus(self.password) 
        return f'mysql+pymysql://{encoded_username}:{encoded_password}@{self.host}:{self.port}/{self.database}?autocommit=false'


USERNAME = os.getenv('DB_USERNAME')
PASSWORD = os.getenv('DB_PASSWORD')
HOST = os.getenv('DB_HOST')
PORT = os.getenv('DB_PORT')
DATABASE = os.getenv('KBE_DATABASE')


db_connector = DatabaseConnector(USERNAME, PASSWORD, HOST, PORT, DATABASE)
db_engine = db_connector.engine

db_connection = db_engine.connect()






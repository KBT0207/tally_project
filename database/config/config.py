import os
from dotenv import load_dotenv

load_dotenv(".env")

DB_USERNAME = os.getenv('DB_USERNAME', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'root')
DB_HOST     = os.getenv('DB_HOST', 'localhost')
DB_PORT     = int(os.getenv('DB_PORT', 3306))
DB_NAME     = os.getenv('DB_NAME', 'tally_db')
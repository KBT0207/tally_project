import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from dotenv import load_dotenv
from urllib.parse import quote_plus
from database.models.base import Base

load_dotenv('.env')


class DatabaseConnector:
    def __init__(self, username, password, host, port, database=None) -> None:
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = None
        self.SessionLocal = None

    def get_db_string(self, with_db=True):
        encoded_username = quote_plus(self.username)
        encoded_password = quote_plus(self.password)

        if with_db and self.database:
            return f"mysql+pymysql://{encoded_username}:{encoded_password}@{self.host}:{self.port}/{self.database}"
        else:
            return f"mysql+pymysql://{encoded_username}:{encoded_password}@{self.host}:{self.port}/"

    def create_database_if_not_exists(self):
        engine = create_engine(self.get_db_string(with_db=False))
        with engine.connect() as conn:
            conn.execute(
                text(f"CREATE DATABASE IF NOT EXISTS `{self.database}`")
            )
        engine.dispose()

    def get_engine(self):
        if not self.engine:
            self.engine = create_engine(
                self.get_db_string(),
                isolation_level="READ COMMITTED",
                echo=True
            )
        return self.engine

    def create_tables(self):
        engine = self.get_engine()
        Base.metadata.create_all(engine)
    
    def get_session(self) -> Session:
        """
        Get a new database session
        
        Returns:
            Session: SQLAlchemy session object
        """
        if not self.SessionLocal:
            engine = self.get_engine()
            self.SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=engine
            )
        return self.SessionLocal()
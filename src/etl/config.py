"""Engines SQLAlchemy: origem (leitura) e DW local (escrita)."""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


def get_source_engine():
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}")


def get_dw_engine():
    user = os.getenv("DW_USER", "engenheiro")
    password = os.getenv("DW_PASSWORD", "senha123")
    host = os.getenv("DW_HOST", "localhost")
    port = os.getenv("DW_PORT", "3306")
    database = os.getenv("DW_NAME", "dw_autoprime")
    return create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

"""Conexão com o DW e leitura do fato (sem Streamlit)."""

import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

from src.visualizacao.queries import QUERY_STAR

load_dotenv()


def dw_url() -> str:
    user = os.getenv("DW_USER", "engenheiro")
    password = os.getenv("DW_PASSWORD", "senha123")
    host = os.getenv("DW_HOST", "localhost")
    port = os.getenv("DW_PORT", "3306")
    database = os.getenv("DW_NAME", "dw_autoprime")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"


def fetch_vendas_dataframe() -> pd.DataFrame:
    engine = create_engine(dw_url(), pool_pre_ping=True)
    return pd.read_sql(QUERY_STAR, engine)

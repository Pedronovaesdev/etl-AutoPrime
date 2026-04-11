"""Extração da camada Bronze (origem MySQL)."""

import pandas as pd
from sqlalchemy.engine import Engine

QUERY_BRONZE = """
SELECT * FROM vendas
WHERE ano BETWEEN 2000 AND 2014
"""


def extract_vendas(engine_origem: Engine) -> pd.DataFrame:
    df = pd.read_sql(QUERY_BRONZE, engine_origem)
    print("Dados extraídos da origem:")
    print(df.head(5))
    return df

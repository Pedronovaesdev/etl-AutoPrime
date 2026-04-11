"""Orquestração ETL: extrai da origem, transforma (Silver) e carrega no DW (Gold)."""

from src.etl.config import get_dw_engine, get_source_engine
from src.etl.extract import extract_vendas
from src.etl.load_to_dw import load_gold_layer
from src.etl.transform import transform_to_silver


def run_elt() -> None:
    engine_origem = get_source_engine()
    engine_dw = get_dw_engine()

    df_bronze = extract_vendas(engine_origem)
    df_silver = transform_to_silver(df_bronze)
    load_gold_layer(engine_dw, df_silver)


if __name__ == "__main__":
    run_elt()

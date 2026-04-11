"""Carga na camada Gold (tabelas do DW)."""

import pandas as pd
from sqlalchemy.engine import Engine


def load_gold_layer(engine_dw: Engine, df_silver: pd.DataFrame) -> None:
    print("\nIniciando a carga no Data Warehouse (DW Local)...")

    print("Processando Dim_Tempo_Venda...")
    df_dim_tempo = df_silver[
        [
            "data_venda_dt",
            "ANO_VENDA",
            "MES_VENDA",
            "NOME_DO_MES",
            "DIA_VENDA",
            "TRIMESTRE",
            "SEMESTRE",
            "DIA_DA_SEMANA",
            "INDICADOR_FIM_SEMANA",
        ]
    ].copy()
    df_dim_tempo.columns = [
        "data_completa",
        "ano",
        "mes",
        "nome_mes",
        "dia",
        "trimestre",
        "semestre",
        "dia_semana",
        "indicador_fim_semana",
    ]
    df_dim_tempo["numero_mes"] = df_dim_tempo["mes"]
    df_dim_tempo = df_dim_tempo.drop_duplicates(subset=["data_completa"]).dropna(subset=["data_completa"])
    df_dim_tempo.to_sql("Dim_Tempo_Venda", con=engine_dw, if_exists="append", index=False)

    print("Processando Dim_Loja_Venda...")
    df_dim_loja = df_silver[["seller", "state", "NOME_ESTADO_LOJA", "REGIAO_LOJA"]].copy()
    df_dim_loja.columns = ["nome_loja", "estado_loja", "nome_estado_loja", "regiao_loja"]
    df_dim_loja = df_dim_loja.drop_duplicates(subset=["nome_loja", "estado_loja"]).dropna(
        subset=["nome_loja"]
    )
    df_dim_loja.to_sql("Dim_Loja_Venda", con=engine_dw, if_exists="append", index=False)

    print("Processando Dim_Veiculo...")
    df_dim_veiculo = df_silver[
        [
            "ano",
            "make",
            "model",
            "trim_veiculo",
            "body",
            "vin",
            "idade_veiculo_venda",
            "FAIXA_ODOMETRO",
            "interior",
            "color",
            "CATEGORIA_CARRO",
            "transmission",
        ]
    ].copy()
    df_dim_veiculo.columns = [
        "ano_fabricacao",
        "marca",
        "modelo",
        "versao",
        "tipo_carroceria",
        "chassi",
        "idade_veiculo_no_momento_da_venda",
        "faixa_idade_veiculo",
        "cor_interna",
        "cor_externa",
        "categoria",
        "transmissao",
    ]
    df_dim_veiculo = df_dim_veiculo.drop_duplicates(subset=["chassi"]).dropna(subset=["chassi"])
    df_dim_veiculo["faixa_idade_veiculo"] = df_dim_veiculo["faixa_idade_veiculo"].astype(str)
    df_dim_veiculo.to_sql("Dim_Veiculo", con=engine_dw, if_exists="append", index=False)

    print("Recuperando chaves substitutas (SKs) para a tabela Fato...")
    dim_tempo_db = pd.read_sql("SELECT sk_tempo, data_completa FROM Dim_Tempo_Venda", engine_dw)
    dim_loja_db = pd.read_sql("SELECT sk_loja, nome_loja, estado_loja FROM Dim_Loja_Venda", engine_dw)
    dim_veiculo_db = pd.read_sql("SELECT sk_veiculo, chassi FROM Dim_Veiculo", engine_dw)

    dim_tempo_db["data_completa"] = pd.to_datetime(dim_tempo_db["data_completa"])
    df_silver = df_silver.copy()
    df_silver["data_venda_dt"] = pd.to_datetime(df_silver["data_venda_dt"])

    df_fato = df_silver.merge(
        dim_tempo_db, left_on="data_venda_dt", right_on="data_completa", how="inner"
    )
    df_fato = df_fato.merge(
        dim_loja_db, left_on=["seller", "state"], right_on=["nome_loja", "estado_loja"], how="inner"
    )
    df_fato = df_fato.merge(dim_veiculo_db, left_on="vin", right_on="chassi", how="inner")

    print("Processando Fato_Vendas_Carros...")
    df_fato_final = df_fato[["sk_tempo", "sk_veiculo", "sk_loja", "sellingprice", "mmr"]].copy()
    df_fato_final.columns = [
        "sk_tempo_venda",
        "sk_veiculo",
        "sk_loja",
        "preco_venda",
        "preco_mercado",
    ]
    df_fato_final["quantidade_vendida"] = 1
    df_fato_final = df_fato_final[
        [
            "sk_tempo_venda",
            "sk_veiculo",
            "sk_loja",
            "quantidade_vendida",
            "preco_venda",
            "preco_mercado",
        ]
    ]
    df_fato_final.to_sql("Fato_Vendas_Carros", con=engine_dw, if_exists="append", index=False)

    print("\nCarga finalizada com sucesso! Seu Data Warehouse local está abastecido.")

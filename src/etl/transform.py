"""Transformações da camada Silver."""

import os
import unicodedata
from difflib import SequenceMatcher

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.etl.mapeamentos import MAPA_ESTADOS

load_dotenv()


def limpar_texto(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto).upper()
    texto = unicodedata.normalize("NFKD", texto).encode("ASCII", "ignore").decode("utf-8")
    texto = " ".join(texto.split())
    return texto


def _sem_acento_upper(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("utf-8")
    return s.upper().strip()


def normalizar_transmissao_pdf(val) -> str:
    """Rótulos conforme enunciado: Automático, Manual, Não informado."""
    if pd.isna(val):
        return "Não informado"
    raw = str(val).strip()
    if raw == "":
        return "Não informado"
    t = _sem_acento_upper(raw)
    if t in ("NA", "N/A", "NONE", "NULL", "NAN", "-", "?"):
        return "Não informado"
    if "NAO INFORMADO" in t or t == "DESCONHECIDO":
        return "Não informado"
    if "AUTO" in t or t in ("AUTOMATIC", "AUTOMATICO", "A", "AT"):
        return "Automático"
    if t in ("MANUAL", "M", "MAN"):
        return "Manual"
    return "Não informado"


def deduplicar_sellers_fuzzy(series: pd.Series, threshold: float) -> pd.Series:
    """
    Aproxima lojas com grafia parecida (SequenceMatcher).
    Ordem: strings mais longas primeiro como representante canônico.

    Com milhares de nomes distintos o custo explode (O(n²) no pior caso).
    Por isso há limite SELLER_FUZZY_MAX_UNIQUES e quick_ratio antes de ratio().
    """
    uniques = pd.Series(series.dropna().unique()).astype(str).unique()
    max_u = int(os.getenv("SELLER_FUZZY_MAX_UNIQUES", "350"))
    if len(uniques) > max_u:
        print(
            f"[Silver] SELLER_FUZZY_DEDUP ignorado: {len(uniques)} lojas distintas "
            f"> limite {max_u}. Ajuste SELLER_FUZZY_MAX_UNIQUES só se aceitar demora."
        )
        return series

    canonical: dict[str, str] = {}
    reps: list[str] = []

    for s in sorted(uniques, key=lambda x: (-len(x), x)):
        r_found = None
        for r in reps:
            sm = SequenceMatcher(None, s, r)
            if sm.quick_ratio() < threshold:
                continue
            if sm.ratio() >= threshold:
                r_found = r
                break
        if r_found is not None:
            canonical[s] = r_found
        else:
            canonical[s] = s
            reps.append(s)

    def _map_one(x):
        if pd.isna(x):
            return x
        key = str(x)
        return canonical.get(key, x)

    return series.map(_map_one)


def classificar_categoria(idade, odometro):
    if idade <= 1 and odometro <= 1000:
        return "NOVO"
    if idade <= 3 and odometro <= 40000:
        return "SEMINOVO"
    return "USADO"


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.lower() in ("1", "true", "yes", "on")


def transform_to_silver(df_bronze: pd.DataFrame) -> pd.DataFrame:
    df = df_bronze.copy()
    n0 = len(df)

    colunas_texto = [
        "make",
        "model",
        "trim_veiculo",
        "body",
        "state",
        "color",
        "interior",
        "seller",
    ]
    for col in colunas_texto:
        if col in df.columns:
            df[col] = df[col].apply(limpar_texto)

    if "transmission" in df.columns:
        df["transmission"] = df["transmission"].apply(normalizar_transmissao_pdf)

    if _env_bool("SELLER_FUZZY_DEDUP", False):
        th = float(os.getenv("SELLER_FUZZY_THRESHOLD", "0.88"))
        if "seller" in df.columns:
            df["seller"] = deduplicar_sellers_fuzzy(df["seller"], th)

    df["make"] = df["make"].fillna("NAO INFORMADO")

    df["state"] = df["state"].str.upper()
    df["NOME_ESTADO_LOJA"] = df["state"].map(
        lambda x: MAPA_ESTADOS.get(x, {}).get("nome", "NAO INFORMADO")
    )
    df["REGIAO_LOJA"] = df["state"].map(
        lambda x: MAPA_ESTADOS.get(x, {}).get("regiao", "NAO INFORMADO")
    )

    if "saledate" in df.columns:
        data_limpa = df["saledate"].str.split(" GMT").str[0]
        df["data_venda_dt"] = pd.to_datetime(data_limpa, errors="coerce")
        df["ANO_VENDA"] = df["data_venda_dt"].dt.year
        df = df[(2000 <= df["ANO_VENDA"]) & (df["ANO_VENDA"] < 2015)]
        df["MES_VENDA"] = df["data_venda_dt"].dt.month
        df["DIA_VENDA"] = df["data_venda_dt"].dt.day
        df["TRIMESTRE"] = df["data_venda_dt"].dt.quarter
        df["SEMESTRE"] = np.where(df["MES_VENDA"] <= 6, 1, 2)
        df["NOME_DO_MES"] = df["data_venda_dt"].dt.month_name().str.upper()
        df["DIA_DA_SEMANA"] = df["data_venda_dt"].dt.day_name().str.upper()
        df["INDICADOR_FIM_SEMANA"] = df["data_venda_dt"].dt.dayofweek >= 5

    if "ANO_VENDA" not in df.columns:
        raise ValueError("É necessário o campo saledate na origem para derivar ANO_VENDA e aplicar validações.")

    n1 = len(df)
    df = df[(df["sellingprice"] > 0) & (df["mmr"] > 0) & (df["odometer"] >= 0)]
    if _env_bool("SILVER_LOG_FILTERS", True):
        print(f"[Silver] Preço/MMR/odômetro: removidas {n1 - len(df)} linhas")

    n2 = len(df)
    ano_min = int(os.getenv("PLAUSIBLE_YEAR_MIN", "1980"))
    ano_abs_max = int(os.getenv("PLAUSIBLE_YEAR_ABS_MAX", "2015"))
    idade_max = int(os.getenv("MAX_VEHICLE_AGE_YEARS", "50"))
    mask_ano = (
        df["ano"].notna()
        & df["ANO_VENDA"].notna()
        & (df["ano"] >= ano_min)
        & (df["ano"] <= ano_abs_max)
        & (df["ano"] <= df["ANO_VENDA"])
        & ((df["ANO_VENDA"] - df["ano"]) <= idade_max)
        & ((df["ANO_VENDA"] - df["ano"]) >= 0)
    )
    df = df.loc[mask_ano]
    if _env_bool("SILVER_LOG_FILTERS", True):
        print(f"[Silver] Ano de fabricação plausível: removidas {n2 - len(df)} linhas")

    n3 = len(df)
    ratio = df["sellingprice"] / df["mmr"]
    rmin = float(os.getenv("MMR_PRICE_RATIO_MIN", "0.35"))
    rmax = float(os.getenv("MMR_PRICE_RATIO_MAX", "2.0"))
    df = df.loc[(ratio >= rmin) & (ratio <= rmax)]
    if _env_bool("SILVER_LOG_FILTERS", True):
        print(f"[Silver] Outliers preço/MMR (razão {rmin}–{rmax}): removidas {n3 - len(df)} linhas")

    df["idade_veiculo_venda"] = df["ANO_VENDA"] - df["ano"]
    df["CATEGORIA_CARRO"] = df.apply(
        lambda row: classificar_categoria(row["idade_veiculo_venda"], row["odometer"]),
        axis=1,
    )
    df["FAIXA_ODOMETRO"] = pd.cut(
        df["odometer"],
        bins=[-1, 10000, 50000, 100000, float("inf")],
        labels=["0-10K", "10K-50K", "50K-100K", "100K+"],
    )

    if _env_bool("SILVER_LOG_FILTERS", True):
        print(f"[Silver] Total Bronze {n0} → Silver {len(df)} linhas")

    return df

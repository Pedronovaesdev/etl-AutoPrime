"""Tipagem, colunas derivadas e filtros do painel."""

import pandas as pd

MES_NOMES = {
    1: "Jan",
    2: "Fev",
    3: "Mar",
    4: "Abr",
    5: "Mai",
    6: "Jun",
    7: "Jul",
    8: "Ago",
    9: "Set",
    10: "Out",
    11: "Nov",
    12: "Dez",
}


def preparar_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["data_completa"] = pd.to_datetime(out["data_completa"], errors="coerce")
    out["ano"] = pd.to_numeric(out["ano"], errors="coerce").astype("Int64")
    out["numero_mes"] = pd.to_numeric(out["numero_mes"], errors="coerce").astype("Int64")
    out["trimestre"] = pd.to_numeric(out["trimestre"], errors="coerce").astype("Int64")
    out["semestre"] = pd.to_numeric(out["semestre"], errors="coerce").astype("Int64")
    out["preco_venda"] = pd.to_numeric(out["preco_venda"], errors="coerce")
    out["quantidade_vendida"] = pd.to_numeric(out["quantidade_vendida"], errors="coerce").fillna(1)
    out["receita"] = out["preco_venda"] * out["quantidade_vendida"]
    out["ano_mes"] = out["data_completa"].dt.to_period("M").astype(str)
    out["sku_carro"] = (
        out["marca"].fillna("").astype(str)
        + " "
        + out["modelo"].fillna("").astype(str)
        + " "
        + out["versao"].fillna("").astype(str)
    ).str.strip()
    return out


def aplicar_filtros(
    df: pd.DataFrame,
    anos: list,
    meses: list,
    trimestres: list,
    semestres: list,
    regioes: list,
) -> pd.DataFrame:
    d = df.copy()
    if anos:
        d = d[d["ano"].isin(anos)]
    if meses:
        d = d[d["numero_mes"].isin(meses)]
    if trimestres:
        d = d[d["trimestre"].isin(trimestres)]
    if semestres:
        d = d[d["semestre"].isin(semestres)]
    if regioes:
        d = d[d["regiao_loja"].isin(regioes)]
    return d

import os
import pandas as pd

def verificarPastaPlanilhas():
    if os.path.exists("./planilhas") == False or os.path.isdir("./planilhas") == False:
        os.mkdir("planilhas")
    return "./planilhas"


def exportarParaCsv(df: pd.DataFrame, nome_arquivo: str):
    caminho_pasta = verificarPastaPlanilhas()
    caminho_arquivo = os.path.join(caminho_pasta, f"{nome_arquivo}.csv")

    df.to_csv(caminho_arquivo, index=False, encoding="utf-8-sig")

def exportarParaExcel(dataframes: dict, nome_arquivo: str):
    caminho_pasta = verificarPastaPlanilhas()
    caminho_arquivo = os.path.join(caminho_pasta, f"{nome_arquivo}.xlsx")

    with pd.ExcelWriter(caminho_arquivo, engine="openpyxl") as writer:
        for nome_aba, df in dataframes.items():
            nome_aba_limpo = str(nome_aba)[:31]
            df.to_excel(writer, sheet_name=nome_aba_limpo, index=False)


def exportarDadosCompletos(df_bronze: pd.DataFrame, 
                           df_silver: pd.DataFrame, 
                           df_dim_tempo: pd.DataFrame = None,
                           df_dim_loja: pd.DataFrame = None,
                           df_dim_veiculo: pd.DataFrame = None,
                           df_fato_final: pd.DataFrame = None,
                           formato: str = "ambos") -> dict:
    todos_dfs = {
        "Bronze_Dados_Brutos": df_bronze,
        "Silver_Dados_Tratados": df_silver,
    }

    if df_dim_tempo is not None:
        todos_dfs["Dim_Tempo_Venda"] = df_dim_tempo
    if df_dim_loja is not None:
        todos_dfs["Dim_Loja_Venda"] = df_dim_loja
    if df_dim_veiculo is not None:
        todos_dfs["Dim_Veiculo"] = df_dim_veiculo
    if df_fato_final is not None:
        todos_dfs["Fato_Vendas_Carros"] = df_fato_final


    if formato in ("csv", "ambos"):
        for nome, df in todos_dfs.items():
            exportarParaCsv(df, nome)

    if formato in ("excel", "ambos"):
        exportarParaExcel(todos_dfs, "AutoPrime_ETL_Completo")

"""Ponto de entrada Streamlit: layout, filtros e orquestração dos componentes."""

import streamlit as st

from src.visualizacao.database import fetch_vendas_dataframe
from src.visualizacao.graficos_enunciado import render_perguntas_professor
from src.visualizacao.graficos_extras import render_analises_adicionais
from src.visualizacao.kpis import render_cards_negocio, render_metricas_resumo
from src.visualizacao.preparacao import MES_NOMES, aplicar_filtros, preparar_dataframe


@st.cache_data(ttl=120, show_spinner="Carregando dados do DW…")
def carregar_vendas():
    return fetch_vendas_dataframe()


def main() -> None:
    st.set_page_config(
        page_title="Vendas — DW Autoprime",
        page_icon="📊",
        layout="wide",
    )
    st.title("Análise de vendas — Data Warehouse")
    st.caption(
        "Fonte: `dw_autoprime` (MySQL no Docker). Filtros temporais aplicam-se a todo o painel."
    )

    try:
        df = carregar_vendas()
    except Exception as e:
        st.error(
            f"Não foi possível conectar ao MySQL ou executar a consulta.\n\n**Erro:** `{e}`\n\n"
            "Confirme: `docker compose up -d`, `.env` com credenciais do DW, "
            "e que o schema + ETL já rodaram "
            "(`python -m src.carregamento.load_dw_schema` e `python -m src.etl.elt`)."
        )
        st.code(
            "DW_HOST=localhost\nDW_PORT=3306\nDW_USER=engenheiro\nDW_PASSWORD=senha123\nDW_NAME=dw_autoprime",
            language="properties",
        )
        return

    if df.empty:
        st.warning(
            "O DW não retornou linhas. Execute a carga (módulo carregamento + `python -m src.etl.elt`)."
        )
        return

    df = preparar_dataframe(df)

    anos_all = sorted(df["ano"].dropna().astype(int).unique().tolist())
    meses_all = sorted(df["numero_mes"].dropna().astype(int).unique().tolist())
    trim_all = sorted(df["trimestre"].dropna().astype(int).unique().tolist())
    sem_all = sorted(df["semestre"].dropna().astype(int).unique().tolist())
    regioes_all = sorted(df["regiao_loja"].dropna().unique().tolist())

    with st.sidebar:
        st.header("Filtros de negócio")
        sel_anos = st.multiselect("Ano", options=anos_all, default=anos_all)
        sel_meses = st.multiselect(
            "Mês",
            options=meses_all,
            default=meses_all,
            format_func=lambda m: f"{m} — {MES_NOMES.get(m, m)}",
        )
        sel_trim = st.multiselect("Trimestre", options=trim_all, default=trim_all)
        sel_sem = st.multiselect("Semestre", options=sem_all, default=sem_all)
        sel_regioes = st.multiselect("Região da loja", options=regioes_all, default=regioes_all)
        st.divider()
        top_marcas = st.slider("Top N marcas (análises adicionais)", 5, 25, 10)

    if not sel_anos:
        sel_anos = anos_all
    if not sel_meses:
        sel_meses = meses_all
    if not sel_trim:
        sel_trim = trim_all
    if not sel_sem:
        sel_sem = sem_all
    if not sel_regioes:
        sel_regioes = regioes_all

    dff = aplicar_filtros(df, sel_anos, sel_meses, sel_trim, sel_sem, sel_regioes)

    if dff.empty:
        st.warning("Nenhum registro com os filtros escolhidos.")
        return

    render_metricas_resumo(dff)
    render_cards_negocio(dff)
    render_perguntas_professor(dff)
    render_analises_adicionais(dff, top_marcas)

    with st.expander("Pré-visualização dos dados (amostra)"):
        st.dataframe(dff.head(50), use_container_width=True)


if __name__ == "__main__":
    main()

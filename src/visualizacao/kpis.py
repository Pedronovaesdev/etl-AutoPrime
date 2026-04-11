"""Métricas e cards de negócio."""

import streamlit as st


def render_metricas_resumo(dff) -> None:
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Receita (US$)", f"{dff['receita'].sum():,.0f}")
    with c2:
        st.metric("Ticket médio (US$)", f"{dff['preco_venda'].mean():,.0f}")
    with c3:
        st.metric("Marcas distintas", dff["marca"].nunique())
    with c4:
        st.metric("Lojas distintas", dff["nome_loja"].nunique())


def render_cards_negocio(dff) -> None:
    st.subheader("Indicadores (filtros aplicados)")
    n_trans = len(dff)
    qtd_unidades = int(dff["quantidade_vendida"].sum())
    top_marca = (
        dff.groupby("marca", as_index=False).size().sort_values("size", ascending=False).head(1)
    )
    top_mod = (
        dff.groupby(["marca", "modelo"], as_index=False)
        .size()
        .sort_values("size", ascending=False)
        .head(1)
    )
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total de vendas (transações)", f"{n_trans:,}")
    with c2:
        st.metric("Quantidade de unidades vendidas", f"{qtd_unidades:,}")
    with c3:
        if not top_marca.empty:
            m = top_marca.iloc[0]["marca"]
            n = int(top_marca.iloc[0]["size"])
            st.metric("Marca que mais vende", str(m), delta=f"{n} vendas")
        else:
            st.metric("Marca que mais vende", "—")
    with c4:
        if not top_mod.empty:
            row = top_mod.iloc[0]
            st.metric(
                "Modelo que mais vende",
                f"{row['marca']} {row['modelo']}",
                delta=f"{int(row['size'])} vendas",
            )
        else:
            st.metric("Modelo que mais vende", "—")

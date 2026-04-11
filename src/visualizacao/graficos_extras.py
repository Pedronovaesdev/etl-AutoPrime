"""Gráficos exploratórios adicionais."""

import plotly.express as px
import streamlit as st


def render_analises_adicionais(dff, top_marcas: int) -> None:
    st.divider()
    st.header("Análises adicionais")

    st.subheader("Receita por ano")
    receita_ano = dff.groupby("ano", as_index=False)["receita"].sum()
    fig1 = px.bar(
        receita_ano,
        x="ano",
        y="receita",
        text_auto=".2s",
        labels={"ano": "Ano", "receita": "Receita (US$)"},
    )
    fig1.update_traces(textposition="outside")
    fig1.update_layout(xaxis_type="category", height=400)
    st.plotly_chart(fig1, use_container_width=True)

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Volume de vendas por trimestre")
        vol_trim = dff.groupby(["ano", "trimestre"], as_index=False).size()
        vol_trim = vol_trim.rename(columns={"size": "qtd_vendas"})
        vol_trim["periodo"] = (
            vol_trim["ano"].astype(int).astype(str) + " T" + vol_trim["trimestre"].astype(int).astype(str)
        )
        fig2 = px.bar(
            vol_trim,
            x="periodo",
            y="qtd_vendas",
            labels={"periodo": "Ano / trimestre", "qtd_vendas": "Quantidade de vendas"},
        )
        fig2.update_layout(xaxis_tickangle=-45, height=400)
        st.plotly_chart(fig2, use_container_width=True)

    with col_b:
        st.subheader("Receita por região da loja")
        reg = dff.groupby("regiao_loja", as_index=False)["receita"].sum().sort_values("receita", ascending=True)
        fig3 = px.bar(
            reg,
            x="receita",
            y="regiao_loja",
            orientation="h",
            labels={"regiao_loja": "Região", "receita": "Receita (US$)"},
        )
        fig3.update_layout(height=400)
        st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Top marcas por receita")
    marca_rec = (
        dff.groupby("marca", as_index=False)["receita"]
        .sum()
        .sort_values("receita", ascending=False)
        .head(top_marcas)
    )
    fig4 = px.bar(
        marca_rec,
        x="marca",
        y="receita",
        labels={"marca": "Marca", "receita": "Receita (US$)"},
    )
    fig4.update_layout(xaxis_tickangle=-35, height=420)
    st.plotly_chart(fig4, use_container_width=True)

    col_c, col_d = st.columns(2)
    with col_c:
        st.subheader("Distribuição por categoria (pizza)")
        cat = dff.groupby("categoria", as_index=False).size().rename(columns={"size": "qtd"})
        fig5 = px.pie(cat, names="categoria", values="qtd", hole=0.35)
        fig5.update_layout(height=400)
        st.plotly_chart(fig5, use_container_width=True)

    with col_d:
        st.subheader("Vendas por transmissão")
        tr = dff.groupby("transmissao", as_index=False).size().rename(columns={"size": "qtd"})
        fig6 = px.bar(
            tr,
            x="transmissao",
            y="qtd",
            labels={"transmissao": "Transmissão", "qtd": "Quantidade"},
        )
        fig6.update_layout(height=400)
        st.plotly_chart(fig6, use_container_width=True)

    st.subheader("Preço médio de venda × MMR por ano")
    preco_ano = dff.groupby("ano", as_index=False).agg(
        preco_venda_medio=("preco_venda", "mean"),
        preco_mercado_medio=("preco_mercado", "mean"),
    )
    preco_long = preco_ano.melt(
        id_vars="ano",
        value_vars=["preco_venda_medio", "preco_mercado_medio"],
        var_name="metrica",
        value_name="valor",
    )
    preco_long["metrica"] = preco_long["metrica"].map(
        {
            "preco_venda_medio": "Preço médio de venda",
            "preco_mercado_medio": "Preço médio MMR",
        }
    )
    fig7 = px.bar(
        preco_long,
        x="ano",
        y="valor",
        color="metrica",
        barmode="group",
        labels={"ano": "Ano", "valor": "US$ (média)"},
    )
    fig7.update_layout(xaxis_type="category", height=450)
    st.plotly_chart(fig7, use_container_width=True)

    st.subheader("Receita por estado (Top 15)")
    est = (
        dff.groupby("nome_estado_loja", as_index=False)["receita"]
        .sum()
        .sort_values("receita", ascending=False)
        .head(15)
    )
    fig8 = px.bar(
        est,
        x="nome_estado_loja",
        y="receita",
        labels={"nome_estado_loja": "Estado", "receita": "Receita (US$)"},
    )
    fig8.update_layout(xaxis_tickangle=-40, height=420)
    st.plotly_chart(fig8, use_container_width=True)

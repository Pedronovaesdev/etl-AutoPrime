"""Gráficos das perguntas de negócio do enunciado acadêmico."""

import matplotlib.pyplot as plt
import plotly.express as px
import seaborn as sns
import streamlit as st


def render_perguntas_professor(dff) -> None:
    st.divider()
    st.header("Perguntas de negócio (enunciado)")

    st.markdown("##### 1. Evolução do preço (mesma marca e versão) — série temporal (linha)")
    dff_mv = dff.dropna(subset=["marca", "versao", "data_completa", "preco_venda"])
    dff_mv = dff_mv[
        (dff_mv["marca"].astype(str).str.len() > 0) & (dff_mv["versao"].astype(str).str.len() > 0)
    ]
    if dff_mv.empty:
        st.info("Sem dados para marca/versão após os filtros.")
    else:
        combos = (
            dff_mv.assign(chave=dff_mv["marca"].astype(str) + " | " + dff_mv["versao"].astype(str))
            .groupby("chave")
            .size()
            .loc[lambda s: s >= 3]
            .sort_values(ascending=False)
            .head(80)
        )
        if combos.empty:
            st.info("Nenhuma combinação marca+versão com volume mínimo (3+) para uma linha estável.")
        else:
            sel_mv = st.selectbox("Marca | versão", options=combos.index.tolist(), key="sel_mv")
            part = dff_mv[
                (dff_mv["marca"].astype(str) + " | " + dff_mv["versao"].astype(str)) == sel_mv
            ]
            serie = (
                part.groupby("ano_mes", as_index=False)["preco_venda"].mean().sort_values("ano_mes")
            )
            fig = px.line(
                serie,
                x="ano_mes",
                y="preco_venda",
                markers=True,
                labels={"ano_mes": "Mês", "preco_venda": "Preço médio de venda (US$)"},
            )
            fig.update_layout(xaxis_tickangle=-45, height=400)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("##### 2. Comparação de preços entre lojas — mesmo carro (marca + modelo + versão)")
    dff_sku = dff.dropna(subset=["nome_loja", "preco_venda"])
    sku_counts = dff_sku.groupby("sku_carro").size().loc[lambda s: s >= 2].sort_values(ascending=False).head(60)
    if sku_counts.empty:
        st.info("Não há carros vendidos em mais de uma loja com os filtros atuais.")
    else:
        sel_sku = st.selectbox("Carro (marca modelo versão)", options=sku_counts.index.tolist(), key="sel_sku")
        lojas = (
            dff_sku[dff_sku["sku_carro"] == sel_sku]
            .groupby("nome_loja", as_index=False)
            .agg(preco_medio=("preco_venda", "mean"), n_vendas=("sk_venda", "count"))
            .sort_values("preco_medio", ascending=False)
        )
        fig_loja = px.bar(
            lojas,
            x="nome_loja",
            y="preco_medio",
            text="n_vendas",
            labels={"nome_loja": "Loja", "preco_medio": "Preço médio (US$)"},
        )
        fig_loja.update_traces(texttemplate="n=%{text}", textposition="outside")
        fig_loja.update_layout(xaxis_tickangle=-40, height=420)
        st.plotly_chart(fig_loja, use_container_width=True)

    col_d1, col_d2 = st.columns(2)
    with col_d1:
        st.markdown("##### 3. Distribuição por faixa de preço (histograma — Seaborn)")
        precos = dff["preco_venda"].dropna()
        if len(precos) < 2:
            st.info("Poucos preços para histograma.")
        else:
            fig_h, ax = plt.subplots(figsize=(8, 4))
            sns.histplot(precos, bins=24, kde=True, ax=ax, color="steelblue")
            ax.set_xlabel("Preço de venda (US$)")
            ax.set_ylabel("Quantidade de veículos")
            ax.set_title("Distribuição de veículos por faixa de preço")
            plt.tight_layout()
            st.pyplot(fig_h, clear_figure=True)
            plt.close(fig_h)

    with col_d2:
        st.markdown("##### 4. Ranking das categorias mais ofertadas (volume de vendas)")
        rank_cat = (
            dff.groupby("categoria", as_index=False)
            .size()
            .rename(columns={"size": "qtd_vendas"})
            .sort_values("qtd_vendas", ascending=True)
        )
        fig_rank = px.bar(
            rank_cat,
            x="qtd_vendas",
            y="categoria",
            orientation="h",
            labels={"categoria": "Categoria", "qtd_vendas": "Quantidade de vendas"},
        )
        fig_rank.update_layout(height=360)
        st.plotly_chart(fig_rank, use_container_width=True)

    st.markdown("##### 5. Top 10 maiores preços de venda por categoria e por ano")
    anos_disp = sorted(dff["ano"].dropna().astype(int).unique().tolist())
    if not anos_disp:
        st.info("Sem anos nos dados filtrados.")
    else:
        sel_ano_top = st.selectbox("Ano para o ranking", options=anos_disp, key="ano_top10")
        sub_ano = dff[dff["ano"] == sel_ano_top]
        categorias = sorted(sub_ano["categoria"].dropna().unique())
        tabs = st.tabs([str(c) for c in categorias])
        for tab, cat in zip(tabs, categorias):
            with tab:
                top10 = (
                    sub_ano[sub_ano["categoria"] == cat]
                    .nlargest(10, "preco_venda")[
                        ["preco_venda", "marca", "modelo", "versao", "nome_loja", "data_completa"]
                    ]
                    .reset_index(drop=True)
                )
                top10.index = top10.index + 1
                if top10.empty:
                    st.caption("Sem registros nesta categoria.")
                else:
                    st.dataframe(top10, use_container_width=True)
                    top10_plot = top10.iloc[::-1].copy()
                    top10_plot["label_veiculo"] = (
                        top10_plot["marca"].astype(str) + " " + top10_plot["modelo"].astype(str)
                    ).str.slice(0, 40)
                    fig_t = px.bar(
                        top10_plot,
                        x="preco_venda",
                        y="label_veiculo",
                        orientation="h",
                        labels={"label_veiculo": "Marca / modelo", "preco_venda": "Preço (US$)"},
                    )
                    fig_t.update_layout(height=min(400, 80 + 28 * len(top10)))
                    st.plotly_chart(fig_t, use_container_width=True)

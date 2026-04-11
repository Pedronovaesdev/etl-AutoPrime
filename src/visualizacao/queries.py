"""SQL do star schema consumido pelo dashboard."""

QUERY_STAR = """
SELECT
    f.sk_venda,
    f.preco_venda,
    f.preco_mercado,
    f.quantidade_vendida,
    t.data_completa,
    t.ano,
    t.numero_mes,
    t.nome_mes,
    t.trimestre,
    t.semestre,
    t.dia_semana,
    t.indicador_fim_semana,
    l.nome_loja,
    l.regiao_loja,
    l.estado_loja,
    l.nome_estado_loja,
    v.marca,
    v.modelo,
    v.versao,
    v.categoria,
    v.transmissao,
    v.faixa_idade_veiculo
FROM Fato_Vendas_Carros f
JOIN Dim_Tempo_Venda t ON f.sk_tempo_venda = t.sk_tempo
JOIN Dim_Loja_Venda l ON f.sk_loja = l.sk_loja
JOIN Dim_Veiculo v ON f.sk_veiculo = v.sk_veiculo
"""

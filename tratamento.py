"""No banco só tem a tabela de vendas, com as seguintes colunas:

Colunas na tabela vendas:
['id', 'ano', 'make', 'model', 'trim_veiculo', 'body', 'transmission', 'vin', 'state', 'condicao', 'odometer', 'color', 'interior', 'seller', 'mmr', 'sellingprice', 'saledate']
"""

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import unicodedata

load_dotenv()

# =====================================================================
# 1. CONFIGURAÇÃO DAS CONEXÕES (EXTRAÇÃO E CARGA)
# =====================================================================

# Credenciais da Origem (Remoto / Leitura)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine_origem = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Credenciais do Destino (Docker Local / Escrita)
DW_USER = "engenheiro"
DW_PASSWORD = "senha123"
DW_HOST = "localhost" 
DW_PORT = "3306"
DW_NAME = "dw_autoprime"

engine_dw = create_engine(f"mysql+pymysql://{DW_USER}:{DW_PASSWORD}@{DW_HOST}:{DW_PORT}/{DW_NAME}")


# =====================================================================
# 2. EXTRAÇÃO (CAMADA BRONZE)
# =====================================================================
# Extração já filtrando os anos exigidos na origem para otimizar memória
query = """
SELECT * FROM vendas
WHERE ano BETWEEN 2000 AND 2014
"""
df_bronze = pd.read_sql(query, engine_origem) # Usando o banco de origem!

print("Dados extraídos da origem:")
print(df_bronze.head(5))

df_silver = df_bronze.copy()

# =====================================================================
# 3. TRANSFORMAÇÃO (CAMADA SILVER)
# =====================================================================

## Padronizar texto
def limpar_texto(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto).upper()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = ' '.join(texto.split())
    return texto

# Aplicar limpeza em todas as colunas de texto
colunas_texto = ['make', 'model', 'trim_veiculo', 'body', 'transmission', 'state', 'color', 'interior', 'seller']
for col in colunas_texto:
    if col in df_silver.columns:
        df_silver[col] = df_silver[col].apply(limpar_texto)

## Tratar dados nulos
df_silver['make'] = df_silver['make'].fillna('NAO INFORMADO')
df_silver['transmission'] = df_silver['transmission'].fillna('NAO INFORMADO')
df_silver['transmission'] = df_silver['transmission'].replace({
    'AUTOMATIC': 'AUTOMATICO',
    'MANUAL': 'MANUAL'
})

## Tratamento Geográfico
df_silver['state'] = df_silver['state'].str.upper()

mapa_estados = {
    'AL': {'nome': 'ALABAMA', 'regiao': 'SOUTH'}, 'AK': {'nome': 'ALASKA', 'regiao': 'WEST'},
    'AZ': {'nome': 'ARIZONA', 'regiao': 'WEST'}, 'AR': {'nome': 'ARKANSAS', 'regiao': 'SOUTH'},
    'CA': {'nome': 'CALIFORNIA', 'regiao': 'WEST'}, 'CO': {'nome': 'COLORADO', 'regiao': 'WEST'},
    'CT': {'nome': 'CONNECTICUT', 'regiao': 'NORTHEAST'}, 'DE': {'nome': 'DELAWARE', 'regiao': 'SOUTH'},
    'FL': {'nome': 'FLORIDA', 'regiao': 'SOUTH'}, 'GA': {'nome': 'GEORGIA', 'regiao': 'SOUTH'},
    'HI': {'nome': 'HAWAII', 'regiao': 'WEST'}, 'ID': {'nome': 'IDAHO', 'regiao': 'WEST'},
    'IL': {'nome': 'ILLINOIS', 'regiao': 'MIDWEST'}, 'IN': {'nome': 'INDIANA', 'regiao': 'MIDWEST'},
    'IA': {'nome': 'IOWA', 'regiao': 'MIDWEST'}, 'KS': {'nome': 'KANSAS', 'regiao': 'MIDWEST'},
    'KY': {'nome': 'KENTUCKY', 'regiao': 'SOUTH'}, 'LA': {'nome': 'LOUISIANA', 'regiao': 'SOUTH'},
    'ME': {'nome': 'MAINE', 'regiao': 'NORTHEAST'}, 'MD': {'nome': 'MARYLAND', 'regiao': 'SOUTH'},
    'MA': {'nome': 'MASSACHUSETTS', 'regiao': 'NORTHEAST'}, 'MI': {'nome': 'MICHIGAN', 'regiao': 'MIDWEST'},
    'MN': {'nome': 'MINNESOTA', 'regiao': 'MIDWEST'}, 'MS': {'nome': 'MISSISSIPPI', 'regiao': 'SOUTH'},
    'MO': {'nome': 'MISSOURI', 'regiao': 'MIDWEST'}, 'MT': {'nome': 'MONTANA', 'regiao': 'WEST'},
    'NE': {'nome': 'NEBRASKA', 'regiao': 'MIDWEST'}, 'NV': {'nome': 'NEVADA', 'regiao': 'WEST'},
    'NH': {'nome': 'NEW HAMPSHIRE', 'regiao': 'NORTHEAST'}, 'NJ': {'nome': 'NEW JERSEY', 'regiao': 'NORTHEAST'},
    'NM': {'nome': 'NEW MEXICO', 'regiao': 'WEST'}, 'NY': {'nome': 'NEW YORK', 'regiao': 'NORTHEAST'},
    'NC': {'nome': 'NORTH CAROLINA', 'regiao': 'SOUTH'}, 'ND': {'nome': 'NORTH DAKOTA', 'regiao': 'MIDWEST'},
    'OH': {'nome': 'OHIO', 'regiao': 'MIDWEST'}, 'OK': {'nome': 'OKLAHOMA', 'regiao': 'SOUTH'},
    'OR': {'nome': 'OREGON', 'regiao': 'WEST'}, 'PA': {'nome': 'PENNSYLVANIA', 'regiao': 'NORTHEAST'},
    'RI': {'nome': 'RHODE ISLAND', 'regiao': 'NORTHEAST'}, 'SC': {'nome': 'SOUTH CAROLINA', 'regiao': 'SOUTH'},
    'SD': {'nome': 'SOUTH DAKOTA', 'regiao': 'MIDWEST'}, 'TN': {'nome': 'TENNESSEE', 'regiao': 'SOUTH'},
    'TX': {'nome': 'TEXAS', 'regiao': 'SOUTH'}, 'UT': {'nome': 'UTAH', 'regiao': 'WEST'},
    'VT': {'nome': 'VERMONT', 'regiao': 'NORTHEAST'}, 'VA': {'nome': 'VIRGINIA', 'regiao': 'SOUTH'},
    'WA': {'nome': 'WASHINGTON', 'regiao': 'WEST'}, 'WV': {'nome': 'WEST VIRGINIA', 'regiao': 'SOUTH'},
    'WI': {'nome': 'WISCONSIN', 'regiao': 'MIDWEST'}, 'WY': {'nome': 'WYOMING', 'regiao': 'WEST'},
    'DC': {'nome': 'DISTRICT OF COLUMBIA', 'regiao': 'SOUTH'}, 'PR': {'nome': 'PUERTO RICO', 'regiao': 'TERRITORY'}
}

df_silver['NOME_ESTADO_LOJA'] = df_silver['state'].map(lambda x: mapa_estados.get(x, {}).get('nome', 'NAO INFORMADO'))
df_silver['REGIAO_LOJA'] = df_silver['state'].map(lambda x: mapa_estados.get(x, {}).get('regiao', 'NAO INFORMADO'))

## Tratamento da Data
if 'saledate' in df_silver.columns:
    data_limpa = df_silver['saledate'].str.split(' GMT').str[0]
    df_silver['data_venda_dt'] = pd.to_datetime(data_limpa, errors='coerce')
    df_silver['ANO_VENDA'] = df_silver['data_venda_dt'].dt.year
    df_silver['MES_VENDA'] = df_silver['data_venda_dt'].dt.month
    df_silver['DIA_VENDA'] = df_silver['data_venda_dt'].dt.day
    df_silver['TRIMESTRE'] = df_silver['data_venda_dt'].dt.quarter
    df_silver['SEMESTRE'] = np.where(df_silver['MES_VENDA'] <= 6, 1, 2)
    df_silver['NOME_DO_MES'] = df_silver['data_venda_dt'].dt.month_name().str.upper()
    df_silver['DIA_DA_SEMANA'] = df_silver['data_venda_dt'].dt.day_name().str.upper()
    df_silver['INDICADOR_FIM_SEMANA'] = df_silver['data_venda_dt'].dt.dayofweek >= 5

## Regras de negócio do Slide
df_silver = df_silver[
    (df_silver['sellingprice'] > 0) &
    (df_silver['mmr'] > 0) &
    (df_silver['odometer'] >= 0)
]

df_silver['idade_veiculo_venda'] = df_silver['ANO_VENDA'] - df_silver['ano']

def classificar_categoria(idade, odometro):
    if idade <= 1 and odometro <= 1000:
        return 'NOVO'
    elif idade <= 3 and odometro <= 40000:
        return 'SEMINOVO'
    else:
        return 'USADO'

df_silver['CATEGORIA_CARRO'] = df_silver.apply(
    lambda row: classificar_categoria(row['idade_veiculo_venda'], row['odometer']), axis=1
)

df_silver['FAIXA_ODOMETRO'] = pd.cut(
    df_silver['odometer'],
    bins=[-1, 10000, 50000, 100000, float('inf')],
    labels=['0-10K', '10K-50K', '50K-100K', '100K+']
)


# =====================================================================
# 4. CARGA (CAMADA GOLD - DATA WAREHOUSE)
# =====================================================================

print("\nIniciando a carga no Data Warehouse (DW Local)...")

print("Processando Dim_Tempo_Venda...")
df_dim_tempo = df_silver[['data_venda_dt', 'ANO_VENDA', 'MES_VENDA', 'NOME_DO_MES', 
                          'DIA_VENDA', 'TRIMESTRE', 'SEMESTRE', 'DIA_DA_SEMANA', 
                          'INDICADOR_FIM_SEMANA']].copy()

df_dim_tempo.columns = ['data_completa', 'ano', 'mes', 'nome_mes', 'dia', 
                        'trimestre', 'semestre', 'dia_semana', 'indicador_fim_semana']
df_dim_tempo['numero_mes'] = df_dim_tempo['mes']

df_dim_tempo = df_dim_tempo.drop_duplicates(subset=['data_completa']).dropna(subset=['data_completa'])
# USANDO engine_dw AQUI PARA ESCREVER NO DESTINO
df_dim_tempo.to_sql('Dim_Tempo_Venda', con=engine_dw, if_exists='append', index=False)


print("Processando Dim_Loja_Venda...")
df_dim_loja = df_silver[['seller', 'state', 'NOME_ESTADO_LOJA', 'REGIAO_LOJA']].copy()
df_dim_loja.columns = ['nome_loja', 'estado_loja', 'nome_estado_loja', 'regiao_loja']

df_dim_loja = df_dim_loja.drop_duplicates(subset=['nome_loja', 'estado_loja']).dropna(subset=['nome_loja'])
# USANDO engine_dw
df_dim_loja.to_sql('Dim_Loja_Venda', con=engine_dw, if_exists='append', index=False)


print("Processando Dim_Veiculo...")
df_dim_veiculo = df_silver[['ano', 'make', 'model', 'trim_veiculo', 'body', 'vin', 
                            'idade_veiculo_venda', 'FAIXA_ODOMETRO', 'interior', 
                            'color', 'CATEGORIA_CARRO', 'transmission']].copy()

df_dim_veiculo.columns = ['ano_fabricacao', 'marca', 'modelo', 'versao', 'tipo_carroceria', 
                          'chassi', 'idade_veiculo_no_momento_da_venda', 'faixa_idade_veiculo', 
                          'cor_interna', 'cor_externa', 'categoria', 'transmissao']

df_dim_veiculo = df_dim_veiculo.drop_duplicates(subset=['chassi']).dropna(subset=['chassi'])
df_dim_veiculo['faixa_idade_veiculo'] = df_dim_veiculo['faixa_idade_veiculo'].astype(str)
# USANDO engine_dw
df_dim_veiculo.to_sql('Dim_Veiculo', con=engine_dw, if_exists='append', index=False)


print("Recuperando chaves substitutas (SKs) para a tabela Fato...")
# USANDO engine_dw PARA LER AS CHAVES DO DW
dim_tempo_db = pd.read_sql("SELECT sk_tempo, data_completa FROM Dim_Tempo_Venda", engine_dw)
dim_loja_db = pd.read_sql("SELECT sk_loja, nome_loja, estado_loja FROM Dim_Loja_Venda", engine_dw)
dim_veiculo_db = pd.read_sql("SELECT sk_veiculo, chassi FROM Dim_Veiculo", engine_dw)

dim_tempo_db['data_completa'] = pd.to_datetime(dim_tempo_db['data_completa'])
df_silver['data_venda_dt'] = pd.to_datetime(df_silver['data_venda_dt'])

df_fato = df_silver.merge(dim_tempo_db, left_on='data_venda_dt', right_on='data_completa', how='inner')
df_fato = df_fato.merge(dim_loja_db, left_on=['seller', 'state'], right_on=['nome_loja', 'estado_loja'], how='inner')
df_fato = df_fato.merge(dim_veiculo_db, left_on='vin', right_on='chassi', how='inner')


print("Processando Fato_Vendas_Carros...")
df_fato_final = df_fato[['sk_tempo', 'sk_veiculo', 'sk_loja', 'sellingprice', 'mmr']].copy()

df_fato_final.columns = ['sk_tempo_venda', 'sk_veiculo', 'sk_loja', 'preco_venda', 'preco_mercado']
df_fato_final['quantidade_vendida'] = 1
df_fato_final = df_fato_final[['sk_tempo_venda', 'sk_veiculo', 'sk_loja', 'quantidade_vendida', 'preco_venda', 'preco_mercado']]

# USANDO engine_dw PARA ESCREVER A FATO NO DESTINO
df_fato_final.to_sql('Fato_Vendas_Carros', con=engine_dw, if_exists='append', index=False)

print("\nCarga finalizada com sucesso! Seu Data Warehouse local está abastecido.")
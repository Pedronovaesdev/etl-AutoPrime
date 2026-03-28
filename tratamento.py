"""No banco só tem a tabela de vendas, com as seguintes colunas:


Colunas na tabela vendas:


['id', 'ano', 'make', 'model', 'trim_veiculo', 'body', 'transmission', 'vin', 'state', 'condicao', 'odometer', 'color', 'interior', 'seller', 'mmr', 'sellingprice', 'saledate']


"""

import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import unicodedata


load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")

engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Extração já filtrando os anos exigidos na origem para otimizar memória
query = """
SELECT * FROM vendas
WHERE ano BETWEEN 2000 AND 2014
"""
df_bronze = pd.read_sql(query, engine)

print(df_bronze.head(10))

df_silver = df_bronze.copy()

"""## Padronizar texto"""

def limpar_texto(texto):
    if pd.isna(texto):
        return texto
    texto = str(texto).upper()
    texto = unicodedata.normalize('NFKD', texto).encode('ASCII', 'ignore').decode('utf-8')
    texto = ' '.join(texto.split())
    return texto

# Aplicar limpeza em todas as colunas de texto (ajuste os nomes conforme sua extração)
colunas_texto = ['make', 'model', 'trim', 'body', 'transmission', 'state', 'color', 'interior', 'seller']
for col in colunas_texto:
    if col in df_silver.columns:
        df_silver[col] = df_silver[col].apply(limpar_texto)

"""## Tratar dados nulos"""

df_silver['make'] = df_silver['make'].fillna('NAO INFORMADO')

# Transmissão: preencher ausente e padronizar
df_silver['transmission'] = df_silver['transmission'].fillna('NAO INFORMADO')
# Ajustando variações para o padrão exigido
df_silver['transmission'] = df_silver['transmission'].replace({
    'AUTOMATIC': 'AUTOMATICO',
    'MANUAL': 'MANUAL'
})

"""## Tratamento Geogrático"""

df_silver['state'] = df_silver['state'].str.upper()

mapa_estados = {
    'AL': {'nome': 'ALABAMA', 'regiao': 'SOUTH'},
    'AK': {'nome': 'ALASKA', 'regiao': 'WEST'},
    'AZ': {'nome': 'ARIZONA', 'regiao': 'WEST'},
    'AR': {'nome': 'ARKANSAS', 'regiao': 'SOUTH'},
    'CA': {'nome': 'CALIFORNIA', 'regiao': 'WEST'},
    'CO': {'nome': 'COLORADO', 'regiao': 'WEST'},
    'CT': {'nome': 'CONNECTICUT', 'regiao': 'NORTHEAST'},
    'DE': {'nome': 'DELAWARE', 'regiao': 'SOUTH'},
    'FL': {'nome': 'FLORIDA', 'regiao': 'SOUTH'},
    'GA': {'nome': 'GEORGIA', 'regiao': 'SOUTH'},
    'HI': {'nome': 'HAWAII', 'regiao': 'WEST'},
    'ID': {'nome': 'IDAHO', 'regiao': 'WEST'},
    'IL': {'nome': 'ILLINOIS', 'regiao': 'MIDWEST'},
    'IN': {'nome': 'INDIANA', 'regiao': 'MIDWEST'},
    'IA': {'nome': 'IOWA', 'regiao': 'MIDWEST'},
    'KS': {'nome': 'KANSAS', 'regiao': 'MIDWEST'},
    'KY': {'nome': 'KENTUCKY', 'regiao': 'SOUTH'},
    'LA': {'nome': 'LOUISIANA', 'regiao': 'SOUTH'},
    'ME': {'nome': 'MAINE', 'regiao': 'NORTHEAST'},
    'MD': {'nome': 'MARYLAND', 'regiao': 'SOUTH'},
    'MA': {'nome': 'MASSACHUSETTS', 'regiao': 'NORTHEAST'},
    'MI': {'nome': 'MICHIGAN', 'regiao': 'MIDWEST'},
    'MN': {'nome': 'MINNESOTA', 'regiao': 'MIDWEST'},
    'MS': {'nome': 'MISSISSIPPI', 'regiao': 'SOUTH'},
    'MO': {'nome': 'MISSOURI', 'regiao': 'MIDWEST'},
    'MT': {'nome': 'MONTANA', 'regiao': 'WEST'},
    'NE': {'nome': 'NEBRASKA', 'regiao': 'MIDWEST'},
    'NV': {'nome': 'NEVADA', 'regiao': 'WEST'},
    'NH': {'nome': 'NEW HAMPSHIRE', 'regiao': 'NORTHEAST'},
    'NJ': {'nome': 'NEW JERSEY', 'regiao': 'NORTHEAST'},
    'NM': {'nome': 'NEW MEXICO', 'regiao': 'WEST'},
    'NY': {'nome': 'NEW YORK', 'regiao': 'NORTHEAST'},
    'NC': {'nome': 'NORTH CAROLINA', 'regiao': 'SOUTH'},
    'ND': {'nome': 'NORTH DAKOTA', 'regiao': 'MIDWEST'},
    'OH': {'nome': 'OHIO', 'regiao': 'MIDWEST'},
    'OK': {'nome': 'OKLAHOMA', 'regiao': 'SOUTH'},
    'OR': {'nome': 'OREGON', 'regiao': 'WEST'},
    'PA': {'nome': 'PENNSYLVANIA', 'regiao': 'NORTHEAST'},
    'RI': {'nome': 'RHODE ISLAND', 'regiao': 'NORTHEAST'},
    'SC': {'nome': 'SOUTH CAROLINA', 'regiao': 'SOUTH'},
    'SD': {'nome': 'SOUTH DAKOTA', 'regiao': 'MIDWEST'},
    'TN': {'nome': 'TENNESSEE', 'regiao': 'SOUTH'},
    'TX': {'nome': 'TEXAS', 'regiao': 'SOUTH'},
    'UT': {'nome': 'UTAH', 'regiao': 'WEST'},
    'VT': {'nome': 'VERMONT', 'regiao': 'NORTHEAST'},
    'VA': {'nome': 'VIRGINIA', 'regiao': 'SOUTH'},
    'WA': {'nome': 'WASHINGTON', 'regiao': 'WEST'},
    'WV': {'nome': 'WEST VIRGINIA', 'regiao': 'SOUTH'},
    'WI': {'nome': 'WISCONSIN', 'regiao': 'MIDWEST'},
    'WY': {'nome': 'WYOMING', 'regiao': 'WEST'},
    'DC': {'nome': 'DISTRICT OF COLUMBIA', 'regiao': 'SOUTH'},
    'PR': {'nome': 'PUERTO RICO', 'regiao': 'TERRITORY'}
}

df_silver['NOME_ESTADO_LOJA'] = df_silver['state'].map(lambda x: mapa_estados.get(x, {}).get('nome', 'NAO INFORMADO')) # [cite: 86]
df_silver['REGIAO_LOJA'] = df_silver['state'].map(lambda x: mapa_estados.get(x, {}).get('regiao', 'NAO INFORMADO')) # [cite: 86]

"""## Tratamento da Data"""

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

"""## Regras de negócio do Slide"""

# Filtros de qualidade (removendo registros inválidos)
df_silver = df_silver[
    (df_silver['sellingprice'] > 0) &
    (df_silver['mmr'] > 0) &
    (df_silver['odometer'] >= 0)
]

# Calculando a idade do veículo no momento da venda
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

# Faixa de idade do veículo baseada no odômetro
df_silver['FAIXA_ODOMETRO'] = pd.cut(
    df_silver['odometer'],
    bins=[-1, 10000, 50000, 100000, float('inf')],
    labels=['0-10K', '10K-50K', '50K-100K', '100K+']
)

print(df_silver.head(10))


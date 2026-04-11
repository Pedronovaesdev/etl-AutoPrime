# 🚗 Projeto Laboratório de Banco de Dados - Processamento de Vendas de Veículos

## O que é este Projeto?

Este é um **projeto de laboratório de banco de dados** focado no processamento, limpeza e transformação de dados de vendas de veículos. O projeto implementa uma arquitetura de **Data Lake** com camadas de dados (Bronze e Silver), onde os dados brutos são extraídos de um banco de dados MySQL e submetidos a um rigoroso processo de limpeza, validação e enriquecimento.

### Objetivo Principal

O objetivo principal deste projeto é transformar dados brutos de vendas de veículos em informações confiáveis e padronizadas, prontas para análise e tomada de decisão. O projeto demonstra boas práticas de:

- **Extração de dados** de bancos de dados relacionais
- **Limpeza e padronização** de dados textuais e numéricos
- **Enriquecimento de dados** com informações geográficas
- **Transformação de datas** e criação de dimensões temporais
- **Aplicação de regras de negócio** e classificação de produtos
- **Tratamento de valores nulos** e validação de dados

## 📊 Estrutura dos Dados

### Tabela Original (Bronze Layer)

A tabela de origem `vendas` contém as seguintes colunas:

```
['id', 'ano', 'make', 'model', 'trim_veiculo', 'body', 'transmission', 'vin', 
 'state', 'condicao', 'odometer', 'color', 'interior', 'seller', 'mmr', 
 'sellingprice', 'saledate']
```

**Descrição das colunas principais:**
- `id`: Identificador único da venda
- `ano`: Ano de fabricação do veículo
- `make`: Marca/fabricante do veículo
- `model`: Modelo do veículo
- `trim_veiculo`: Versão/trim do veículo
- `body`: Tipo de carroçaria
- `transmission`: Tipo de transmissão (Manual/Automática)
- `vin`: Número de identificação do veículo
- `state`: Estado americano onde foi realizada a venda
- `condicao`: Condição do veículo
- `odometer`: Quilometragem do veículo
- `color`: Cor do veículo
- `interior`: Material/cor do interior
- `seller`: Vendedor responsável
- `mmr`: Valor de mercado de referência
- `sellingprice`: Preço de venda
- `saledate`: Data da venda

### Período de Dados

O projeto processa **vendas realizadas entre 2000 e 2014**, filtrando os dados na origem para otimizar o consumo de memória.

## Estrutura do repositório (`src/`)

O código está organizado em três áreas: **carregamento** do schema no MySQL, **ETL** (extrair → transformar → carregar no DW) e **visualização** (dashboard Streamlit).

```
projeto-laboratorio/
├── docker-compose.yaml          # MySQL do DW (porta 3306)
├── requirements.txt
├── .env                         # Credenciais (não versionar)
├── load.py                      # Atalho → cria tabelas no DW
├── tratamento.py                # Atalho → pipeline ETL
├── dashboard_dw.py              # Atalho → inicia o Streamlit
├── README.md
└── src/
    ├── carregamento/            # DDL / tabelas dimensionais e fato
    │   └── load_dw_schema.py    # criar_data_warehouse()
    ├── etl/                     # Pipeline Bronze → Silver → Gold (pandas + SQLAlchemy)
    │   ├── config.py            # Engines SQLAlchemy (origem + DW)
    │   ├── extract.py           # Bronze — leitura na origem
    │   ├── mapeamentos.py       # Mapa estado → região
    │   ├── transform.py         # Silver — pandas
    │   ├── load_to_dw.py        # Gold — to_sql no DW
    │   └── elt.py               # Orquestra extract → transform → load_to_dw
    └── visualizacao/            # Dashboard
        ├── queries.py           # SQL do star schema
        ├── database.py          # Conexão e leitura do fato
        ├── preparacao.py        # Tipos, colunas derivadas, filtros
        ├── kpis.py              # Métricas e cards
        ├── graficos_enunciado.py
        ├── graficos_extras.py
        └── app.py               # Entrada Streamlit (layout e orquestração)
```

Equivalente por módulo:

| Pasta | Função |
|--------|--------|
| `src/carregamento` | Cria `Dim_*` e `Fato_Vendas_Carros` no MySQL local (Docker). |
| `src/etl` | Lê a origem (`DB_*`) com **pandas/SQLAlchemy**, transforma e grava no DW (`DW_*`). |
| `src/visualizacao` | Lê o DW e exibe o painel interativo. |

## 🛠️ Pré-requisitos de Instalação

Antes de executar o projeto, você precisa ter instalado:

### 1. Python 3.7+
Verifique se o Python está instalado:
```bash
python --version
```

### 2. Docker (MySQL do DW)

O `docker-compose.yaml` sobe o MySQL onde ficam as tabelas do data warehouse. É necessário ter **Docker Desktop** (ou Docker Engine) instalado.

### 3. Dependências Python

Principais pacotes (lista completa em `requirements.txt`): **pandas**, **SQLAlchemy**, **PyMySQL**, **python-dotenv**, **mysql-connector-python** (DDL), **streamlit**, **plotly**, **matplotlib**, **seaborn**.

## 📦 Instalação e Configuração

### Passo 1: Clonar ou Preparar o Projeto

Navegue até o diretório do projeto:
```bash
cd projeto-laboratorio
```

### Passo 2: Instalar Dependências

Execute o comando abaixo para instalar todas as bibliotecas necessárias:
```bash
pip install -r requirements.txt
```

(Veja `requirements.txt` para versões fixas de todos os pacotes.)

### Passo 3: Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto.

**MySQL no Docker** (usado pelo Compose — ajuste senhas conforme sua máquina):

```env
MYSQL_ROOT_PASSWORD=sua_senha_root
MYSQL_DATABASE=dw_autoprime
MYSQL_USER=engenheiro
MYSQL_PASSWORD=senha123
```

**Banco de origem** (onde está a tabela `vendas`; o ETL lê daqui):

```env
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
DB_HOST=host_do_mysql_origem
DB_PORT=3306
DB_NAME=nome_do_banco
```

**DW** (opcional; se omitir, usam-se os mesmos padrões dos scripts: `localhost`, `engenheiro`, `senha123`, `dw_autoprime`):

```env
DW_HOST=localhost
DW_PORT=3306
DW_USER=engenheiro
DW_PASSWORD=senha123
DW_NAME=dw_autoprime
```

**Regras extras na transformação (opcional — padrões já alinham ao enunciado):**

```env
PLAUSIBLE_YEAR_MIN=1980
PLAUSIBLE_YEAR_ABS_MAX=2015
MAX_VEHICLE_AGE_YEARS=50
MMR_PRICE_RATIO_MIN=0.35
MMR_PRICE_RATIO_MAX=2.0
SELLER_FUZZY_DEDUP=false
SELLER_FUZZY_THRESHOLD=0.88
SELLER_FUZZY_MAX_UNIQUES=350
SILVER_LOG_FILTERS=true
```

`SELLER_FUZZY_DEDUP` fica **desligado** por padrão: com muitas lojas distintas o algoritmo fica muito lento. Ligue só com `true` em bases menores (ou aumente `SELLER_FUZZY_MAX_UNIQUES` com cautela).

Os módulos carregam o `.env` com `load_dotenv()`.

## Como rodar o projeto

Execute os passos **na raiz do repositório** (`projeto-laboratorio`), com o ambiente virtual ativado se estiver usando um.

Opcional — criar e ativar um `venv` (PowerShell):

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 1. Subir o MySQL (DW)

```bash
docker compose up -d
```

Aguarde o container ficar pronto antes de criar tabelas ou rodar o ETL.

### 2. Instalar dependências

```bash
pip install -r requirements.txt
```

### 3. Criar as tabelas no DW

```bash
python load.py
```

Ou, equivalente:

```bash
python -m src.carregamento.load_dw_schema
```

### 4. Extrair, transformar e carregar no DW (ETL)

Garanta que o MySQL de **origem** (`DB_*`) está acessível e contém a tabela `vendas`. O pipeline usa **pandas** e **SQLAlchemy** (`read_sql` / `to_sql`).

```bash
python tratamento.py
```

Ou:

```bash
python -m src.etl.elt
```

### 5. Abrir o dashboard (Streamlit)

Use o arquivo da **raiz** para o Python encontrar o pacote `src`:

```bash
streamlit run dashboard_dw.py
```

O navegador abre em `http://localhost:8501`.

**Ordem resumida:** Docker → `pip install` → `load.py` → `tratamento.py` → `streamlit run dashboard_dw.py`.

### Saída esperada no ETL

O terminal exibe uma amostra dos dados extraídos e mensagens de progresso da carga nas dimensões e na fato. No dashboard, os gráficos dependem de haver dados já carregados no DW.

## Transformações e Lógica de Negócio

### 1. Limpeza de Texto

A função `limpar_texto()` padroniza todas as colunas de texto:

- Converte para MAIÚSCULAS
- Remove acentuação e caracteres especiais
- Normaliza espaços em branco

**Colunas processadas:** make, model, trim, body, transmission, state, color, interior, seller

### 2. Tratamento de Valores Nulos

- **make**: Preenchido com "NAO INFORMADO"
- **transmission**: Preenchido com "NAO INFORMADO" e padronizado (AUTOMÁTICO/MANUAL)
- **state**: Convertido para maiúsculas

### 3. Enriquecimento Geográfico

Os códigos de estado americano (AL, AK, AZ, etc.) são expandidos para:

- **NOME_ESTADO_LOJA**: Nome completo do estado (ex: ALABAMA)
- **REGIAO_LOJA**: Região geográfica (SOUTH, WEST, NORTHEAST, MIDWEST, TERRITORY)

**Mapa de regiões implementado:**
- **SOUTH**: AL, AR, DE, FL, GA, KY, LA, MD, MS, NC, OK, SC, TN, TX, VA, WV, DC, PR
- **WEST**: AK, AZ, CA, CO, HI, ID, MT, NV, NM, OR, UT, WA, WY
- **NORTHEAST**: CT, MA, ME, NH, NJ, NY, PA, RI, VT
- **MIDWEST**: IL, IN, IA, KS, MI, MN, MO, NE, ND, OH, SD, WI

### 4. Processamento de Datas

A coluna `saledate` é processada para criar múltiplas dimensões temporais:

- **data_venda_dt**: Data/hora processada
- **ANO_VENDA**: Ano extraído
- **MES_VENDA**: Mês (1-12)
- **DIA_VENDA**: Dia do mês
- **TRIMESTRE**: Trimestre (1-4)
- **SEMESTRE**: Semestre (1-2)
- **NOME_DO_MES**: Nome completo do mês em inglês
- **DIA_DA_SEMANA**: Nome do dia da semana
- **INDICADOR_FIM_SEMANA**: Boolean (True se sábado ou domingo)

### 5. Validação de Qualidade

Registros são removidos se atenderem qualquer um dos critérios:

```python
- sellingprice <= 0
- mmr <= 0
- odometer < 0
```

### 6. Classificação de Veículos

Os veículos são classificados automaticamente em três categorias:

```
NOVO: idade <= 1 ano E quilometragem <= 1.000 km
SEMINOVO: idade <= 3 anos E quilometragem <= 40.000 km
USADO: todas as outras combinações
```

### 7. Faixa de Odômetro

Os veículos são agrupados por quilometragem:

- **0-10K**: Até 10.000 km
- **10K-50K**: De 10.000 a 50.000 km
- **50K-100K**: De 50.000 a 100.000 km
- **100K+**: Acima de 100.000 km

# Observações Importantes

### Segurança
- **Nunca commit o arquivo `.env`** com credenciais reais no repositório
- Use um arquivo `.env.example` como referência para outros desenvolvedores
- Mantenha as credenciais do banco de dados seguras

### Performance
- O script filtra os dados na origem para otimizar consumo de memória
- A extração é limitada ao período 2000-2014
- Considere usar índices no banco de dados para melhor performance

### Validação de Dados
- Sempre verifique os dados após importação
- Use `df.info()` e `df.describe()` para análise exploratória
- Valide o mapeamento de estados antes de usar em produção


## 📄 Licença

Este projeto é fornecido como material educacional e de laboratório.

---

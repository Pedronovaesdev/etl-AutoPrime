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

## 🛠️ Pré-requisitos de Instalação

Antes de executar o projeto, você precisa ter instalado:

### 1. Python 3.7+
Verifique se o Python está instalado:
```bash
python --version
```

### 2. Dependências do Projeto

As seguintes bibliotecas são necessárias:
- **pandas**: Manipulação e análise de dados
- **sqlalchemy**: ORM e engine para conexão com banco de dados
- **pymysql**: Driver MySQL para Python
- **python-dotenv**: Carregamento de variáveis de ambiente

Todas as dependências estão listadas em `requirements.txt`.

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

Este comando instala:
```
pymysql
pandas
sqlalchemy
python-dotenv
```

### Passo 3: Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```
DB_USER = "seu_usuario"
DB_PASSWORD = "sua_senha"
DB_HOST = "seu_host_mysql"
DB_PORT = "3306"
DB_NAME = "nome_do_banco"
TABLE_NAME = "vendas"
```

### Passo 4: Verificar Conectividade

O script carregará automaticamente as variáveis do arquivo `.env` usando a função `load_dotenv()`.

## 🚀 Como Usar

### Executar o Script Principal

Para executar o processamento completo de dados, execute:

```bash
python projeto_laboratorio_de_banco.py
```

Este comando irá:

1. **Carregar variáveis de ambiente** do arquivo `.env`
2. **Conectar ao banco de dados MySQL** usando as credenciais fornecidas
3. **Extrair dados** da tabela `vendas` para o período 2000-2014
4. **Criar o DataFrame Bronze** - dados brutos extraídos
5. **Processar a camada Silver** - dados limpos e transformados
6. **Aplicar transformações**:
   - Padronização de texto
   - Tratamento de valores nulos
   - Enriquecimento geográfico
   - Processamento de datas
   - Validação e classificação de veículos

### Saída Esperada

O script irá imprimir as primeiras 10 linhas do DataFrame nas fases Bronze e Silver:

```
   id  ano     make  model  ...  CATEGORIA_CARRO FAIXA_ODOMETRO
0   1  2010   HONDA  CIVIC  ...            USADO      10K-50K
1   2  2005   FORD  FOCUS  ...            USADO      50K-100K
...
```

## 📝 Transformações e Lógica de Negócio

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

## 📂 Estrutura de Arquivos

```
projeto-laboratorio/
├── projeto_laboratorio_de_banco.py   # Script principal
├── requirements.txt                   # Dependências Python
├── .env                              # Variáveis de ambiente (não versionado)
├── .env.example                      # Exemplo de arquivo .env
└── README.md                         # Este arquivo
```

## ⚠️ Observações Importantes

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

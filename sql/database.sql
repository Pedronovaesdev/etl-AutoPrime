CREATE DATABASE IF NOT EXISTS dw_autoprime;
USE dw_autoprime;

-- Tabela: Dim_Tempo_Venda
CREATE TABLE IF NOT EXISTS Dim_Tempo_Venda (
    sk_tempo INT AUTO_INCREMENT PRIMARY KEY,
    data_completa DATE,
    ano INT,
    mes VARCHAR(20),
    nome_mes VARCHAR(50),
    numero_mes INT,
    dia INT,
    trimestre INT,
    semestre INT,
    dia_semana VARCHAR(50),
    indicador_fim_semana BOOLEAN
);

-- Tabela: Dim_Veiculo
CREATE TABLE IF NOT EXISTS Dim_Veiculo (
    sk_veiculo INT AUTO_INCREMENT PRIMARY KEY,
    ano_fabricacao INT,
    marca VARCHAR(100),
    modelo VARCHAR(100),
    versao VARCHAR(100),
    tipo_carroceria VARCHAR(50),
    chassi VARCHAR(50),
    idade_veiculo_no_momento_da_venda INT,
    faixa_idade_veiculo VARCHAR(50),
    cor_interna VARCHAR(50),
    cor_externa VARCHAR(50),
    categoria VARCHAR(50),          
    transmissao VARCHAR(50)        
);

-- Tabela: Dim_Loja_Venda
CREATE TABLE IF NOT EXISTS Dim_Loja_Venda (
    sk_loja INT AUTO_INCREMENT PRIMARY KEY,
    nome_loja VARCHAR(150),
    estado_loja VARCHAR(10),
    nome_estado_loja VARCHAR(100),
    regiao_loja VARCHAR(50)
);

-- Tabela: Fato_Vendas_Carros
CREATE TABLE IF NOT EXISTS Fato_Vendas_Carros (
    sk_venda INT AUTO_INCREMENT PRIMARY KEY,
    sk_tempo_venda INT,
    sk_veiculo INT,
    sk_loja INT,
    
    -- Medidas
    quantidade_vendida INT DEFAULT 1,
    preco_venda DECIMAL(12, 2),
    preco_mercado DECIMAL(12, 2),
    
    -- Relacionamentos (Chaves Estrangeiras)
    CONSTRAINT fk_fato_tempo FOREIGN KEY (sk_tempo_venda) 
        REFERENCES Dim_Tempo_Venda(sk_tempo) ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_fato_veiculo FOREIGN KEY (sk_veiculo) 
        REFERENCES Dim_Veiculo(sk_veiculo) ON DELETE RESTRICT ON UPDATE CASCADE,
        
    CONSTRAINT fk_fato_loja FOREIGN KEY (sk_loja) 
        REFERENCES Dim_Loja_Venda(sk_loja) ON DELETE RESTRICT ON UPDATE CASCADE
);
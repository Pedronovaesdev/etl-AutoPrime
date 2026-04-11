"""Criação das tabelas dimensionais e de fato no MySQL (DW)."""

import os

import mysql.connector
from mysql.connector import Error


def _dw_connection_params():
    return {
        "host": os.getenv("DW_HOST", "localhost"),
        "database": os.getenv("DW_NAME", "dw_autoprime"),
        "user": os.getenv("DW_USER", "engenheiro"),
        "password": os.getenv("DW_PASSWORD", "senha123"),
    }


def criar_data_warehouse() -> None:
    try:
        conexao = mysql.connector.connect(**_dw_connection_params())
        if not conexao.is_connected():
            return
        cursor = conexao.cursor()
        print("Conectado ao MySQL com sucesso.\n")

        sql_dim_tempo = """
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
            """
        sql_dim_veiculo = """
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
            """
        sql_dim_loja = """
            CREATE TABLE IF NOT EXISTS Dim_Loja_Venda (
                sk_loja INT AUTO_INCREMENT PRIMARY KEY,
                nome_loja VARCHAR(150),
                estado_loja VARCHAR(10),
                nome_estado_loja VARCHAR(100),
                regiao_loja VARCHAR(50)
            );
            """
        sql_fato_vendas = """
            CREATE TABLE IF NOT EXISTS Fato_Vendas_Carros (
                sk_venda INT AUTO_INCREMENT PRIMARY KEY,
                sk_tempo_venda INT,
                sk_veiculo INT,
                sk_loja INT,
                quantidade_vendida INT DEFAULT 1,
                preco_venda DECIMAL(12, 2),
                preco_mercado DECIMAL(12, 2),
                CONSTRAINT fk_tempo FOREIGN KEY (sk_tempo_venda) REFERENCES Dim_Tempo_Venda(sk_tempo),
                CONSTRAINT fk_veiculo FOREIGN KEY (sk_veiculo) REFERENCES Dim_Veiculo(sk_veiculo),
                CONSTRAINT fk_loja FOREIGN KEY (sk_loja) REFERENCES Dim_Loja_Venda(sk_loja)
            );
            """

        tabelas_sql = {
            "Dim_Tempo_Venda": sql_dim_tempo,
            "Dim_Veiculo": sql_dim_veiculo,
            "Dim_Loja_Venda": sql_dim_loja,
            "Fato_Vendas_Carros": sql_fato_vendas,
        }

        for nome_tabela, query in tabelas_sql.items():
            print(f"Criando tabela {nome_tabela}...")
            cursor.execute(query)
            print(f"Tabela {nome_tabela} verificada/criada com sucesso.")

        conexao.commit()
        print("\nModelagem Dimensional criada com sucesso no Data Warehouse 'dw_autoprime'!")

    except Error as e:
        print(f"Erro ao conectar ou executar script no MySQL: {e}")
    finally:
        if "conexao" in locals() and conexao.is_connected():
            cursor.close()
            conexao.close()
            print("Conexão ao MySQL encerrada.")


if __name__ == "__main__":
    criar_data_warehouse()

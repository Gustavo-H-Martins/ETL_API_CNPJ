# libs
import pandas as pd
import numpy as np
import datetime
import warnings
import os
import sqlite3
import logging
warnings.filterwarnings("ignore")
pd.option_context(10,5)

def atualiza_database():
    #define o caminho do diretório atual
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_logs = current_dir.replace(r"dados\utils",r"logs\atualiza_database.log")
    dir_dados = current_dir.replace(r"utils", r"csv")

    # configurando o registro de logs
    logging.basicConfig(level=logging.DEBUG, filename=file_logs,encoding="utf-8", format="%(asctime)s - %(levelname)s - %(message)s")

    # pega o caminho do arquivo gerado
    arquivos = list(filter(lambda arquivo: arquivo.endswith('.csv'), os.listdir(dir_dados)))
    BASE_RFB = os.path.join(dir_dados, arquivos[0])

    # qual cabeçalho nós usamos mesmo?
    cabecalho = ["CNPJ","RAZAO_SOCIAL","NOME_FANTASIA",
        "SITUACAO_CADASTRAL","DATA_SITUACAO_CADASTRAL",
        "DATA_INICIO_ATIVIDADE","CNAE_PRINCIPAL","ENDERECO",
        "BAIRRO","CIDADE","UF","CEP","TELEFONE","CNAE_DESCRICAO", "EMAIL"]

    # carregada os dados no dataframe pandas aqui, simples né?
    dados  = pd.read_csv(BASE_RFB, sep=";", dtype="string")

    # a parte de transform de fato está toda aqui, bem simples:
    # com quaanto de dadps começou?
    logging.info(f"Tinham: {dados.shape[0]} dados")
    # Remove os dados duplicados, estranho que sempre aparecem
    dados.fillna("", inplace=True)
    dados.drop_duplicates(inplace=True)
    dados["SITUACAO_CADASTRAL"] = dados["SITUACAO_CADASTRAL"].apply(lambda valor: str(valor).zfill(2))
    # coloca tudo em uppercase
    dados["CNAE_DESCRICAO"] = dados["CNAE_DESCRICAO"].str.upper()
    dados["ENDERECO"] = dados["ENDERECO"].str.strip()

    # conta quando de dados sobrou
    logging.info(f"Ficaram: {dados.shape[0]} dados")

    #Criar uma conexão com o banco de dados sqlite
    db_file = current_dir.replace(r"rfb\dados\utils", r"app\files\database.db")
    conn = sqlite3.connect(database=db_file)

    conn.execute("DROP TABLE IF EXISTS tb_cache")

    #Converter o dataframe em uma tabela no banco de dados
    """
    O parâmetro if_exists=`append` verifica se a tabela já existe e incrementa os dados
    O parâmetro index=False evita que o índice do dataframe seja inserido na tabela
    O parâmetro dtype define o tipo de cada coluna na tabela
    """
    dados.to_sql("tb_rfb", conn, 
                if_exists="replace", index=False, 
                dtype={"CNPJ": "TEXT", #PRIMARY KEY", 
                        "RAZAO_SOCIAL": "TEXT", "NOME_FANTASIA": "TEXT", 
                        "ENDERECO": "TEXT", "BAIRRO": "TEXT", "CIDADE": "TEXT", 
                        "UF": "TEXT", "CEP": "TEXT", 
                        "TELEFONE": "TEXT", "EMAIL": "TEXT", 
                        "CNAE_PRINCIPAL": "TEXT", "CNAE_DESCRICAO": "TEXT",
                        "CNAE_SECUNDARIO": "TEXT",
                        "SITUACAO_CADASTRAL" : "TEXT", "DATA_SITUACAO_CADASTRAL" : "TEXT",
                        "DATA_INICIO_ATIVIDADE" : "TEXT"})
    # Finaliza a transação
    conn.commit()
    # Executa o comando VACUUM para compactar o banco de dados
    conn.execute("VACUUM")

    # Fechar a conexão com o banco de dados
    conn.close()

    # Salva tudo novamente desta vez com um csv e no banco de dados, a galera gosta de "variedades"
    dados.to_csv(BASE_RFB ,sep=";", index=False, encoding="utf-8")

    logging.info("Dados atualizados no database tabela tb_rfb")

if __name__ == '__main__':
    atualiza_database()
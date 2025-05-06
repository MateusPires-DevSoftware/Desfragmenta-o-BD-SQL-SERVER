import os
import logging
import pyodbc
from functions import manutencao, cria_funcao

CONFIG_FILE = r"C:\AtualizaIndexe\config.txt"


def carregar_configuracao():
    """Carrega as configurações do banco a partir do config.txt."""
    if not os.path.exists(CONFIG_FILE):
        print(f"❌ ERRO: Arquivo de configuração não encontrado em {CONFIG_FILE}")
        exit(1)

    config = {}
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            chave_valor = line.strip().split("=")
            if len(chave_valor) == 2:
                chave, valor = chave_valor
                config[chave.strip()] = valor.strip()

    # Depuração: Exibir valores carregados
    print("🔹 Configurações carregadas:", config)

    if not config.get("SERVER") or not config.get("DATABASE"):
        print("❌ ERRO: Configuração inválida! Verifique o arquivo config.txt.")
        exit(1)

    return config

def get_connection(config):
    """Tenta conectar ao banco de dados usando as configurações do config.txt."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={'ODBC Driver 17 for SQL Server'};"
            f"SERVER={config.get('SERVER')};"
            f"DATABASE={config.get('DATABASE')};"
            f"UID={'USER-BD'};"
            f"PWD={'SENHA-USER-BD'};"
        )
        return conn
    except Exception as e:
        print(f"❌ ERRO: Sessão não encontrada. Não foi possível conectar ao banco.\nDetalhes: {e}")
        logging.error(f"Erro de conexão: {e}", exc_info=True)
        exit(1)

def main():
    logging.basicConfig(filename="app.log", level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    print("🔹 Carregando configurações do banco...")
    config = carregar_configuracao()

    print(f"🔹 Tentando conectar ao banco {config.get('DATABASE')} no servidor {config.get('SERVER')}...")
    conn = get_connection(config)

    logging.info("✅ Conexão estabelecida com sucesso.")
    print("✅ Conexão estabelecida com sucesso.")

    try:
        cria_funcao()
        manutencao()
        print("✅ Indexes atualizados!")
        logging.info("✅ Indexes atualizados!")
    except Exception as e:
        print(f"❌ Erro na atualização de indexes: {e}")
        logging.error(f"Erro na atualização de indexes: {e}", exc_info=True)
    
    conn.close()

if __name__ == "__main__":
    main()

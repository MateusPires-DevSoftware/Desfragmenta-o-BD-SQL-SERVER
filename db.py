import os
import pyodbc

CONFIG_FILE = r"C:\AtualizaIndexe\config.txt"

def carregar_configuracao():
    """Carrega as configurações do banco a partir do config.txt."""
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError(f"❌ ERRO: Arquivo de configuração não encontrado em {CONFIG_FILE}")

    config = {}
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            chave_valor = line.strip().split("=")
            if len(chave_valor) == 2:
                chave, valor = chave_valor
                config[chave.strip()] = valor.strip()

    # Validações básicas
    for key in ["SERVER", "DATABASE"]:
        if not config.get(key):
            raise ValueError(f"❌ ERRO: Configuração '{key}' não encontrada ou inválida! Verifique o arquivo config.txt.")

    return config

def get_connection():
    """Tenta conectar ao banco de dados usando as configurações do config.txt."""
    config = carregar_configuracao()

    try:
        conn = pyodbc.connect(
            f"DRIVER={'ODBC Driver 17 for SQL Server'};"
            f"SERVER={config.get('SERVER')};"
            f"DATABASE={config.get('DATABASE')};"
            f"UID={'itprovider'};"
            f"PWD={'passitpxyz'};"
        )
        return conn
    except Exception as e:
        raise ConnectionError(f"❌ ERRO: Não foi possível conectar ao banco.\nDetalhes: {e}")


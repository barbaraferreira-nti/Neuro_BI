import json
import psycopg2
from pathlib import Path

ARQUIVO_ACESSOS = Path(__file__).parent / "bancos.json"

with open(ARQUIVO_ACESSOS, "r", encoding="utf-8") as f:
    BANCOS = json.load(f)


class PGSQL:
    @staticmethod
    def conexao(nome_banco):
        config = BANCOS.get(nome_banco)

        if config is None:
            raise ValueError(f"Banco '{nome_banco}' não encontrado em {ARQUIVO_ACESSOS}.")

        return psycopg2.connect(**config)
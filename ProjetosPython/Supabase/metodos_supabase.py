## Arquivo que cria os métodos para buscar dados da API

import os, json
import requests
from supabase import create_client
import pandas as pd


class api:
    @staticmethod
    def auth(banco):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configSupabase.json")

        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)
            url = config.get(banco, {}).get("url")
            key = config.get(banco, {}).get("key")

            if not url or not key:
                raise ValueError(f"Configuração inválida para o banco: {banco}")

        return create_client(url, key)
    
    @staticmethod
    def get_data(banco,tabela):
        response = (
            api.auth(banco=banco)
            .table(tabela)
            .select("*")
            .execute()
            )


    @staticmethod
    def insert_data(banco,tabela, dados):
        response = (
            api.auth(banco=banco).
            table(tabela).
            insert(dados).
            execute()
        )
    
    @staticmethod
    def delete_data(banco, tabela):
        response = (
            api.auth(banco).table(tabela).delete().execute()
        )

    @staticmethod
    def upsert_data(banco, tabela, dados, chave):
        response = api.auth(banco).table(tabela).upsert(dados, on_conflict=chave).execute()

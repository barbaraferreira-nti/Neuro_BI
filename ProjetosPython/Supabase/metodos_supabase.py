## Arquivo que cria os métodos para buscar dados da API
from config import Config
from supabase import create_client
import pandas as pd


class api:
    @staticmethod
    def auth(banco):
        configs = {
            "Guru_DB": {
                "url": Config.Supabase.URL_GURU_DB,
                "key": Config.Supabase.TOKEN_GURU_DB
            },
            "Octadesk_DB": {
                "url": Config.Supabase.URL_OCTADESK_DB,
                "key": Config.Supabase.TOKEN_OCTADESK_DB
            },
            "Formularios_Neuroescola":{
                "url": Config.Supabase.URL_FORMULARIOS_NEUROESCOLA,
                "key": Config.Supabase.TOKEN_FORMULARIOS_NEUROESCOLA
            },
            "ClickUp_DB":{
                "url": Config.Supabase.URL_CLICKUP_DB,
                "key": Config.Supabase.TOKEN_CLICKUP_DB
            },
            "Clint_DB": {
                "url": Config.Supabase.URL_CLINT_DB,
                "key": Config.Supabase.TOKEN_CLINT_DB
            },
            "Meta_DB":{
                "url": Config.Supabase.URL_META_DB,
                "key": Config.Supabase.TOKEN_META_DB               
            }
        }

        config = configs.get(banco)

        if not config:
            raise ValueError(f"Banco '{banco}' não configurado.")

        url = config["url"]
        key = config["key"]

        if not url:
            raise ValueError(f"URL não configurada para o banco: {banco}")

        if not key:
            raise ValueError(f"TOKEN não configurado para o banco: {banco}")

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

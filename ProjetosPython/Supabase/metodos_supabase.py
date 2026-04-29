## Arquivo que cria os métodos para buscar dados da API
from config import Config
import os, json
import requests
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
            }
        }

        config = configs.get(banco)

        if not config:
            raise ValueError(f"Banco '{banco}' não configurado.")

        url = config["url"]
        key = config["key"]

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

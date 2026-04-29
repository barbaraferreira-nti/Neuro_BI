import os, json
from pathlib import Path
import hashlib
from typing import List, Dict, Any
from config import Config
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import numpy as np
import math

class api:
    @staticmethod
    def encontrar_raiz_projeto() -> Path:
        caminho = Path(__file__).resolve()

        for parent in [caminho.parent, *caminho.parents]:
            if parent.name == "ProjetosPython":
                return parent

        raise FileNotFoundError("Não encontrei a pasta ProjetosPython no caminho do script.")


    @staticmethod
    def carregar_config_planilhas(nome_arquivo) -> list[dict]:
        raiz_projeto = api.encontrar_raiz_projeto()
        caminho_json = raiz_projeto / "Google" / f"{nome_arquivo}"

        with open(caminho_json, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        planilhas = []

        for nome_municipio, dados in config.items():
            planilhas.append({
                "nome_municipio": nome_municipio,
                "id_planilha": dados["id"],
                "spreadsheet_id": dados["spreadsheet_id"],
                "aba": dados["aba"],
                "id_aba": dados["gid"],
                "url": dados.get("url")
            })

        return planilhas
    
    @staticmethod
    def carregar_config_planilha_nps(nome_arquivo) -> list[dict]:
        raiz_projeto = api.encontrar_raiz_projeto()
        caminho_json = raiz_projeto / "Google" / f"{nome_arquivo}"

        with open(caminho_json, "r", encoding="utf-8") as f:
            config = json.load(f)

        return {
            "id_planilha": config["id"],
            "spreadsheet_id": config["spreadsheet_id"],
            "aba": config["aba"],
            "id_aba": config["gid"],
            "url": config.get("url")
        }

        

    @staticmethod
    def get_sheets_service(SCOPES):
        raiz_projeto = api.encontrar_raiz_projeto()
        caminho_service_account = raiz_projeto / "Google" / "configGoogle.json"

        credentials = service_account.Credentials.from_service_account_file(
            caminho_service_account,
            scopes=SCOPES
        )

        service = build("sheets", "v4", credentials=credentials)

        return service

    @staticmethod
    def read_sheet_values(service, spreadsheet_id: str, aba: str) -> List[List[Any]]:
        """
        Lê todos os valores da aba.
        """
        range_name = f"'{aba}'!A:ZZ"
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )

        return result.get("values", [])

    @staticmethod
    def values_to_dataframe(values: List[List[Any]]) -> pd.DataFrame:
        """
        Primeira linha = cabeçalho
        Demais linhas = dados
        """
        if not values or len(values) < 2:
            return pd.DataFrame()

        headers = values[0]
        rows = values[1:]

        # garante mesmo número de colunas
        max_cols = len(headers)
        normalized_rows = []

        for row in rows:
            if len(row) < max_cols:
                row = row + [None] * (max_cols - len(row))
            else:
                row = row[:max_cols]
            normalized_rows.append(row)

        columns = [f"col_{i+1}" for i in range(max_cols)]

        return pd.DataFrame(normalized_rows, columns=columns)

    @staticmethod
    def generate_row_id(id_planilha: str, id_aba: str, numero_linha: int) -> str:
        """
        Gera um hash único por linha.
        """
        raw_key = f"{id_planilha}|{id_aba}|{numero_linha}"
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    @staticmethod
    def prepare_dataframe(df: pd.DataFrame, id_planilha: str, id_aba: str) -> pd.DataFrame:
        """
        Adiciona metadados sem alterar o conteúdo das respostas.
        """
        if df.empty:
            return df

        df = df.copy()
        df.reset_index(drop=True, inplace=True)

        # linha real da planilha: cabeçalho está na linha 1, então os dados começam na 2
        df["numero_linha"] = df.index + 2
        df["id_planilha"] = id_planilha
        df["id_aba"] = id_aba
        df["id_linha"] = df["numero_linha"].apply(
            lambda n: api.generate_row_id(id_planilha, id_aba, int(n))
        )

        return df

    @staticmethod
    def chunk_list(items: List[Dict[str, Any]], chunk_size: int = 500):
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]

    @staticmethod
    def limpar_valor_json(valor):
        if valor is None:
            return None

        if isinstance(valor, float) and (math.isnan(valor) or math.isinf(valor)):
            return None

        if pd.isna(valor):
            return None

        return valor
    

    @staticmethod
    def dataframe_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
        if df.empty:
            return []

        records = df.to_dict(orient="records")
        records_limpos = []

        for record in records:
            record_limpo = {
                chave: api.limpar_valor_json(valor)
                for chave, valor in record.items()
            }
            records_limpos.append(record_limpo)

        return records_limpos

    @staticmethod
    def limpar_quebras_linha(valor):
        if isinstance(valor, str):
            return valor.replace("\r", " ").replace("\n", " ").strip()
        return valor








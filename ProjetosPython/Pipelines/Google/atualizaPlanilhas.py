
from typing import List, Dict, Any
from Supabase import metodos_supabase
from Google import metodos_google
import pandas as pd


SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly"
]


tabela_onboarding = "formulario_onboarding"
tabela_kickoff = "formulario_kickoff"
tabela_nps = "formulario_nps_csat"
banco = "Formularios_Neuroescola"
service = metodos_google.api.get_sheets_service(SCOPES)

MAPA_COLUNAS_ONBOARDING = {
    "col_1": "data",
    "col_2": "documento",
    "col_3": "funcao",
    "col_4": "q1",
    "col_5": "q2",
    "col_6": "q3",
    "col_7": "q4",
    "col_8": "q5"
}

MAPA_COLUNAS_ONBOARDING_ORDEM = {
    "col_1": "data",
    "col_2": "documento",
    "col_3": "q1",
    "col_4": "q2",
    "col_5": "q3",
    "col_6": "q4",
    "col_7": "q5"
}

MAPA_COLUNAS_ONBOARDING_EXTRAS = {
    "col_1": "data",
    "col_2": "nome",
    "col_3": "documento",
    "col_4": "funcao",
    "col_5": "q1",
    "col_6": "q2",
    "col_7": "q3",
    "col_8": "q4",
    "col_9": "q5"
}

MAPA_COLUNAS_KICKOFF = {
    "col_1": "data",
    "col_2": "documento",
    "col_3": "q1",
    "col_4": "q2",
    "col_5": "q3",
    "col_6": "q4",
    "col_7": "q5"
}

MAPA_COLUNAS_NPS = {
     "col_1": "data",
     "col_2": "municipio",
     "col_3": "email",
     "col_4": "q1",
     "col_5": "q2"
}

def processar_planilha(service, config: Dict[str, str], banco, tabela, mapa) -> int:
        nome_municipio = config["nome_municipio"]
        id_planilha = config["id_planilha"]
        spreadsheet_id = config["spreadsheet_id"]
        aba = config["aba"]
        id_aba = config["id_aba"]

        values = metodos_google.api.read_sheet_values(service, spreadsheet_id, aba)
        df = metodos_google.api.values_to_dataframe(values)

        if df.empty:
            return 0
        df = df.rename(columns=mapa)
        if "data" in df.columns:
            df["data"] = pd.to_datetime(
                df["data"],
                format="%d/%m/%Y %H:%M:%S",
                errors="coerce"
            )

            df["data"] = df["data"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df_prepared = metodos_google.api.prepare_dataframe(df, id_planilha, id_aba)
        df_prepared["nome_municipio"] = nome_municipio
        df_prepared = df_prepared.where(pd.notnull(df_prepared), None)
    
        records = metodos_google.api.dataframe_to_records(df_prepared)

        metodos_supabase.api.upsert_data(banco,tabela,records,chave="id_linha")

        return len(records)

def processar_planilha_nps(service, config: Dict[str, str], banco, tabela, mapa) -> int:
        id_planilha = config["id_planilha"]
        spreadsheet_id = config["spreadsheet_id"]
        aba = config["aba"]
        id_aba = config["id_aba"]

        values = metodos_google.api.read_sheet_values(service, spreadsheet_id, aba)
        df = metodos_google.api.values_to_dataframe(values)

        if df.empty:
            return 0
        df = df.rename(columns=mapa)
        if "data" in df.columns:
            df["data"] = pd.to_datetime(
                df["data"],
                format="%d/%m/%Y %H:%M:%S",
                errors="coerce"
            )

            df["data"] = df["data"].dt.strftime("%Y-%m-%d %H:%M:%S")

        df_prepared = metodos_google.api.prepare_dataframe(df, id_planilha, id_aba)
        df_prepared = df_prepared.where(pd.notnull(df_prepared), None)
    
        records = metodos_google.api.dataframe_to_records(df_prepared)

        metodos_supabase.api.upsert_data(banco,tabela,records,chave="id_linha")

        return len(records)


## 01. Onboarding
planilhas_onboarding = metodos_google.api.carregar_config_planilhas("planilhas_neuroescola_onboarding.json")

total_onboarding = 0
municipios_mapa_ordem = {"Venda Nova", "Grandes Rios"}
municipios_mapa_extra = {"Guaíba", "Araras"}

for planilha in planilhas_onboarding:
    if planilha["nome_municipio"] in municipios_mapa_ordem:
        mapa = MAPA_COLUNAS_ONBOARDING_ORDEM
    elif planilha["nome_municipio"] in municipios_mapa_extra:
        mapa = MAPA_COLUNAS_ONBOARDING_EXTRAS

    else:
        mapa = MAPA_COLUNAS_ONBOARDING

    qtd = processar_planilha(
        service,
        planilha,
        banco,
        tabela_onboarding,
        mapa
    )

    print(f"{planilha['nome_municipio']}: {qtd} linhas processadas")

    total_onboarding += qtd

print(f"Total linhas inseridas na tabela {tabela_onboarding}: {total_onboarding}")


## 02. Kickoff
planilhas_kickoff = metodos_google.api.carregar_config_planilhas("planilhas_neuroescola_kickoff.json")

total_kickoff = 0

for planilha in planilhas_kickoff:
    qtd = processar_planilha(service, planilha, banco, tabela_kickoff, MAPA_COLUNAS_KICKOFF)
    
    print(f"{planilha['nome_municipio']}: {qtd} linhas processadas")
    
    total_kickoff += qtd

print(f"Total linhas inseridas na tabela {tabela_kickoff}: {total_kickoff}")

## 03. NPS e CSAT
planilha_nps = metodos_google.api.carregar_config_planilha_nps("planilha_neuroescola_nps.json")
qtd = processar_planilha_nps(service,planilha_nps, banco, tabela_nps, MAPA_COLUNAS_NPS)
print(f"Total linhas inseridas na tabela {tabela_nps}: {qtd}")
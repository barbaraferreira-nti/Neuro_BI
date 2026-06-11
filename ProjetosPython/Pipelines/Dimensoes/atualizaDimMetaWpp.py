from MetaAds import metodos_meta
from Supabase import metodos_supabase
from ETL import metodos_etl
import datetime
import pandas as pd
import math

waba_ids_neurosaber = {
    "Neurosaber OPC": {
        "waba_id": "1533047911689726",
        "phone_number": ["555559316711"]},
    "NeuroSaber Notificações": 
        {"waba_id":"2129625144468722",
         "phone_number": ["555558527874"]},
    "NeuroSaber": {
        "waba_id": "1377246404206163",
        "phone_number": ["554391462497"]},
    "NeuroSaber Oficial": {
        "waba_id":"2103307913808127",
        "phone_number": ["554391403936", "5558725072"]},
    "Instituto Neurosaber Palestras": {
        "waba_id": "429566746915963",
        "phone_number": ["554391615373"]},
    "NeuroSaber Comercial": {
        "waba_id": "996131145623350",
        "phone_number": ["555557702866"]},
    "NeuroSaber Comercial 2": {
        "waba_id": "3957199931167965",
        "phone_number": ["551152866595"]}
}

waba_ids_sinahpse = {
    "NeuroSaber": {
        "waba_id": "101910505855540",
        "phone_number": ["554391852129"]},
    "NeuroSaber 2": 
        {"waba_id":"435531316312233",
         "phone_number": ["554388572523"]},
    "Sinahpse": {
        "waba_id": "253683307837895",
        "phone_number": ["554391852129"]},
    "Sinahpse 2": {
        "waba_id":"327889960401891",
        "phone_number": []},
    "Sinahpse - OCTADESK": {
        "waba_id": "287112521158452",
        "phone_number": []},
    "Sinahpse - Octa": {
        "waba_id": "316997841490913",
        "phone_number": []},
    "Sinahpse 3": {
        "waba_id": "277390872132740",
        "phone_number": []}
}

## Dados da conta 'NeuroSaber'
for nome, info in waba_ids_neurosaber.items():
    waba_id = info["waba_id"]

    dados = metodos_meta.api.getWhatsAppTemplates(ambiente="neurosaber", waba_id=waba_id, campos=["id", "name", "status", "category", "correct_category", 
                                                                                                    "previous_category", "last_updated_time"])
    
    if dados.empty:
        print(f"Nenhum template encontrado para a WABA {nome} ({waba_id}).")
        continue

    dados["waba_id"] = waba_id
    rows = dados.to_dict(orient='records')
    rows = metodos_etl.Etl.normalizar_rows(rows)
    upsert = metodos_supabase.api.upsert_data(banco='Meta_DB', tabela='dm_wpp_templates', dados=rows, chave='id,waba_id')
 
    print(f"Templates atualizados na tabela da WABA {nome} ({waba_id}).")

## Dados da conta 'Sinahpse'
for nome, info in waba_ids_sinahpse.items():
    waba_id = info["waba_id"]

    dados = metodos_meta.api.getWhatsAppTemplates(ambiente="sinahpse", waba_id=waba_id, campos=["id", "name", "status", "category", "correct_category", 
                                                                                                    "previous_category", "last_updated_time"])
    
    if dados.empty:
        print(f"Nenhum template encontrado para a WABA {nome} ({waba_id}).")
        continue

    dados["waba_id"] = waba_id
    rows = dados.to_dict(orient='records')
    rows = metodos_etl.Etl.normalizar_rows(rows)
    upsert = metodos_supabase.api.upsert_data(banco='Meta_DB', tabela='dm_wpp_templates', dados=rows, chave='id,waba_id')
 
    print(f"Templates atualizados na tabela da WABA {nome} ({waba_id}).")
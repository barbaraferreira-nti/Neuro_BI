from MetaAds import metodos_meta
from Supabase import metodos_supabase
from ETL import metodos_etl
import datetime
import pandas as pd
import math

contas_wpp_neurosaber = metodos_meta.api.getContasMeta("Neurosaber", "ContasWpp")
contas_wpp_sinahpse = metodos_meta.api.getContasMeta("Sinahpse", "ContasWpp")

## Dados da conta 'NeuroSaber'
for nome, info in contas_wpp_neurosaber.items():
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
for nome, info in contas_wpp_sinahpse.items():
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
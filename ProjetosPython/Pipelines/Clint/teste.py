import Clint.metodos_clint
from Supabase import metodos_supabase
import pandas as pd
import traceback
import numpy as np
from datetime import datetime, timezone

## Atualizar as tabelas de dimensão do ClickUp
BANCO = "Clint_DB"
BATCH_SIZE = 1000


def tratar_nan_json(df):
    df = df.copy()

    df = df.replace({
        np.nan: None,
        pd.NaT: None
    })

    return df

def data_para_iso(data_str, final_do_dia=False):

    dt = datetime.strptime(data_str, "%Y-%m-%d")

    if final_do_dia:
        dt = dt.replace(hour=23, minute=59, second=59)
    else:
        dt = dt.replace(hour=0, minute=0, second=0)

    return dt.isoformat()

TABELA_ORIGINS = 'dim_origin'
TABELA_STAGES = 'dim_origin_stage'
TABELA_TAGS = 'dim_tag'
TABELA_USERS = 'dim_user'
TABELA_CONTACTS = 'dim_contact'
TABELA_CONTACT_TAGS = 'contact_tags'
TABELA_LOST_STATUS = 'dim_lost_status'
TABELA_GROUPS = 'dim_group'

params = {
    "limit": "1000",
    "offset": "0"
    }




## Atualiza Dim_Origins, Dim_Stages
# try:
#     df_origins, df_stages = Clint.metodos_clint.api.getOriginsDF(params=params)
    
#     df_origins = tratar_nan_json(df_origins)
#     df_stages = tratar_nan_json(df_stages)

#     rows_origins = df_origins.to_dict(orient='records')
#     rows_stages = df_stages.to_dict(orient='records')

#     total_rows_origins = len(rows_origins)
#     total_rows_stages = len(rows_stages)

#     print(f"Total de registros para upsert na tabela {TABELA_ORIGINS}: {total_rows_origins}")
#     for i in range(0, total_rows_origins, BATCH_SIZE):
#         lote_origins = rows_origins[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_ORIGINS, dados=lote_origins, chave="origin_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_origins)} | Tabela: {TABELA_ORIGINS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_origins)} | Tabela: {TABELA_ORIGINS}")
#             print(str(e))
#             print(traceback.format_exc())

#     print(f"Total de registros para upsert na tabela {TABELA_STAGES}: {total_rows_stages}")
#     for i in range(0, total_rows_stages, BATCH_SIZE):
#         lote_stages = rows_stages[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_STAGES, dados=lote_stages, chave="stage_id,origin_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_stages)} | Tabela: {TABELA_STAGES}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_stages)} | Tabela: {TABELA_STAGES}")
#             print(str(e))
#             print(traceback.format_exc())
        
# except Exception as e:
#     print(f"Erro ao fazer upsert nas tabelas '{TABELA_ORIGINS}' e '{TABELA_STAGES}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Origins e Stages atualizados com sucesso!")


## Atualiza Dim_Tags, Dim_Lost_Status e Dim_Groups

# try:
#     df_tags = Clint.metodos_clint.api.getTagsDF(params=params)
#     df_lost_status = Clint.metodos_clint.api.getStatusLostDF(params=params)
#     df_groups = Clint.metodos_clint.api.getGroupsDF(params=params)

#     df_tags = tratar_nan_json(df_tags)
#     df_lost_status = tratar_nan_json(df_lost_status)
#     df_groups = tratar_nan_json(df_groups)

#     rows_tags = df_tags.to_dict(orient='records')
#     rows_lost_status = df_lost_status.to_dict(orient='records')
#     rows_groups = df_groups.to_dict(orient='records')

#     total_rows_tags = len(rows_tags)
#     total_rows_lost_status = len(rows_lost_status)
#     total_rows_groups = len(rows_groups)

#     print(f"Total de registros para upsert na tabela {TABELA_TAGS}: {total_rows_tags}")
#     for i in range(0, total_rows_tags, BATCH_SIZE):
#         lote_tags = rows_tags[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_TAGS, dados=lote_tags, chave="tag_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_tags)} | Tabela: {TABELA_TAGS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_tags)} | Tabela: {TABELA_TAGS}")
#             print(str(e))
#             print(traceback.format_exc())
    
#     print(f"Total de registros para upsert na tabela {TABELA_LOST_STATUS}: {total_rows_lost_status}")
#     for i in range(0, total_rows_lost_status, BATCH_SIZE):
#         lote_lost_status = rows_lost_status[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_LOST_STATUS, dados=lote_lost_status, chave="status_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_lost_status)} | Tabela: {TABELA_LOST_STATUS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_lost_status)} | Tabela: {TABELA_LOST_STATUS}")
#             print(str(e))
#             print(traceback.format_exc())

#     print(f"Total de registros para upsert na tabela {TABELA_GROUPS}: {total_rows_groups}")
#     for i in range(0, total_rows_groups, BATCH_SIZE):
#         lote_groups = rows_groups[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_GROUPS, dados=lote_groups, chave="group_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_groups)} | Tabela: {TABELA_GROUPS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_groups)} | Tabela: {TABELA_GROUPS}")
#             print(str(e))
#             print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert nas tabelas '{TABELA_TAGS}', '{TABELA_LOST_STATUS}' e '{TABELA_GROUPS}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Tags, Lost_Status e Groups atualizados com sucesso!")

## Atualiza Dim_Users

# try:
#     df_users = Clint.metodos_clint.api.getUsersDF(params=params)

#     df_users = tratar_nan_json(df_users)

#     rows_users = df_users.to_dict(orient='records')

#     total_rows_users = len(rows_users)

#     print(f"Total de registros para upsert na tabela {TABELA_USERS}: {total_rows_users}")
#     for i in range(0, total_rows_users, BATCH_SIZE):
#         lote_users = rows_users[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_USERS, dados=lote_users, chave="user_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_users)} | Tabela: {TABELA_USERS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_users)} | Tabela: {TABELA_USERS}")
#             print(str(e))
#             print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert na tabela '{TABELA_USERS}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Users atualizados com sucesso!")          


## Atualizar Dim_Contacts, Dim_Contact_Tags
# try:
#     df_contacts, df_contact_tags = Clint.metodos_clint.api.getContactsDF(params=params)

#     df_contacts = tratar_nan_json(df_contacts)
#     df_contact_tags = tratar_nan_json(df_contact_tags)

#     rows_contacts = df_contacts.to_dict(orient='records')
#     rows_contact_tags = df_contact_tags.to_dict(orient='records')

#     total_rows_contacts = len(rows_contacts)
#     total_rows_contact_tags = len(rows_contact_tags)

#     print(f"Total de registros para upsert na tabela {TABELA_CONTACTS}: {total_rows_contacts}")
#     for i in range(0, total_rows_contacts, BATCH_SIZE):
#         lote_contacts = rows_contacts[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_CONTACTS, dados=lote_contacts, chave="contact_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_contacts)} | Tabela: {TABELA_CONTACTS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_contacts)} | Tabela: {TABELA_CONTACTS}")
#             print(str(e))
#             print(traceback.format_exc())

#     print(f"Total de registros para upsert na tabela {TABELA_CONTACT_TAGS}: {total_rows_contact_tags}")
#     for i in range(0, total_rows_contact_tags, BATCH_SIZE):
#         lote_contact_tags = rows_contact_tags[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_CONTACT_TAGS, dados=lote_contact_tags, chave="tag_id,contact_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_contact_tags)} | Tabela: {TABELA_CONTACT_TAGS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_contact_tags)} | Tabela: {TABELA_CONTACT_TAGS}")
#             print(str(e))
#             print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert nas tabelas '{TABELA_CONTACTS}' e '{TABELA_CONTACT_TAGS}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Contacts e Contact_Tags atualizados com sucesso!")

## Atualizar DEALS

# TABELA_DEALS = "fact_deals"
# dataI = "2026-01-01"
# dataF = "2026-01-31"

# params_deals = {
#     "created_at_start": data_para_iso(dataI),
#     "created_at_end": data_para_iso(dataF)
# }

# try:
#     df_deals = Clint.metodos_clint.api.getDealsDF(params_deals)
#     df_deals = tratar_nan_json(df_deals)

#     rows_deals = df_deals.to_dict(orient="records")

#     total_rows_deals = len(rows_deals)
#     print(f"Total de registros para upsert na tabela {TABELA_DEALS}: {total_rows_deals}")
#     for i in range(0, total_rows_deals, BATCH_SIZE):
#         lote_deals = rows_deals[i:i + BATCH_SIZE]
#         try:
#             upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_DEALS, dados=lote_deals, chave="deal_id")
#             print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_deals)} | Tabela: {TABELA_DEALS}")
#         except Exception as e:
#             print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote_deals)} | Tabela: {TABELA_DEALS}")
#             print(str(e))
#             print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert nas tabelas '{TABELA_DEALS}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da fato 'Deals' atualizados com sucesso!")

    

params_t = {
    "limit": "1000",
    "offset": "0",
    "origin_id": "633dc282-1846-4b8e-b1da-ff1ea1531572"
    }

df_contacts, df_tags = Clint.metodos_clint.api.getContactsDF(params_t)
df_contacts = pd.DataFrame(df_contacts)
df_contacts.to_csv(r"C:\Users\Barbara\Downloads\teste_contatos.csv", index=False)

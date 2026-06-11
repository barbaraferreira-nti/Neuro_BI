import os
import time
from datetime import datetime
import requests
import pandas as pd
from Supabase import metodos_supabase
import ClickUp.metodos_clickup
import traceback
import numpy as np

## Atualizar as tabelas de dimensão do ClickUp
BANCO = "ClickUp_DB"
BATCH_SIZE = 1000


def tratar_nan_json(df):
    df = df.copy()

    df = df.replace({
        np.nan: None,
        pd.NaT: None
    })

    # Timestamp -> string ISO
    for col in df.columns:

        if pd.api.types.is_datetime64_any_dtype(df[col]):

            df[col] = df[col].astype(str)

            df[col] = df[col].replace({
                "NaT": None
            })

    return df


def data_para_ms(data_str):
    """
    Converte YYYY-MM-DD para Unix timestamp em milissegundos
    """

    dt = datetime.strptime(data_str, "%Y-%m-%d")

    return int(dt.timestamp() * 1000)

## 01. Atualizar Workspace
TABELA_WORKSPACE = "dim_workspace"
# workspaces = ClickUp.metodos_clickup.api.get_workspaces()
# all_workspaces = []
# try:
#     for workspace in workspaces:
#         all_workspaces.append({
#             "workspace_id": workspace.get("id"),
#             "workspace_name": workspace.get("name")
#         })

#     df_workspaces = pd.DataFrame(all_workspaces)
#     rows_workspaces = df_workspaces.to_dict(orient="records")
#     total_rows = len(rows_workspaces)
#     print(f"Total de registros para upsert na tabela {TABELA_WORKSPACE}: {total_rows}")
#     for i in range(0, total_rows, BATCH_SIZE):
#             lote = rows_workspaces[i:i + BATCH_SIZE]
#             try:
#                 upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_WORKSPACE, dados=lote, chave="workspace_id")
#                 print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#             except Exception as e:
#                 print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#                 print(str(e))
#                 print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert na tabela {TABELA_WORKSPACE}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Workspaces atualizados com sucesso!")


## 02. Atualizar Space
TABELA_SPACE = "dim_space"
# all_spaces = []
# workspaces = ClickUp.metodos_clickup.api.get_workspaces()
# try:
#     for workspace in workspaces:
#         workspace_id = workspace["id"]
#         spaces = ClickUp.metodos_clickup.api.get_spaces(workspace_id)
#         for space in spaces:
#             all_spaces.append({
#                 "space_id": space.get("id"),
#                 "space_name": space.get("name"),
#                 "workspace_id": workspace_id,
#                 "is_private": space.get("private")
#             })
#     df_spaces = pd.DataFrame(all_spaces)
#     rows_spaces = df_spaces.to_dict(orient="records")
#     total_rows = len(rows_spaces)
#     print(f"Total de registros para upsert na tabela {TABELA_SPACE}: {total_rows}")
#     for i in range(0, total_rows, BATCH_SIZE):
#         lote = rows_spaces[i:i + BATCH_SIZE]
#         try:
#                 upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_SPACE, dados=rows_spaces, chave="space_id")
#                 print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#         except Exception as e:
#                 print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#                 print(str(e))
#                 print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert na tabela {TABELA_SPACE}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Spaces atualizados com sucesso!")


## 03. Atualizar Folder
TABELA_FOLDER = "dim_folder"
# all_folders = []
# workspaces = ClickUp.metodos_clickup.api.get_workspaces()
# try:
#     for workspace in workspaces:
#         workspace_id = workspace["id"]
#         spaces = ClickUp.metodos_clickup.api.get_spaces(workspace_id)
#         for space in spaces:
#              space_id = space["id"]
#              folders = ClickUp.metodos_clickup.api.get_folders(space_id)
#              for folder in folders:
#                   all_folders.append({
#                        "folder_id": folder.get("id"),
#                        "folder_name": folder.get("name"),
#                        "space_id": space_id,
#                        "hidden": folder.get("hidden")
#                   })
#     df_folders = pd.DataFrame(all_folders)
#     rows_folders = df_folders.to_dict(orient="records")
#     total_rows = len(rows_folders)
#     print(f"Total de registros para upsert na tabela {TABELA_FOLDER}: {total_rows}")
#     for i in range(0, total_rows, BATCH_SIZE):
#         lote = rows_folders[i:i + BATCH_SIZE]
#         try:
#                 upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_FOLDER, dados=rows_folders, chave="folder_id")
#                 print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#         except Exception as e:
#                 print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#                 print(str(e))
#                 print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert na tabela {TABELA_FOLDER}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Folders atualizados com sucesso!")


## 04. Atualizar List
TABELA_LIST = "dim_list"
# all_listas = []
# workspaces = ClickUp.metodos_clickup.api.get_workspaces()
# try:
#     for workspace in workspaces:
#         workspace_id = workspace["id"]
#         spaces = ClickUp.metodos_clickup.api.get_spaces(workspace_id)
#         for space in spaces:
#              space_id = space["id"]
#              folders = ClickUp.metodos_clickup.api.get_folders(space_id)
#              for folder in folders:
#                   folder_id = folder["id"]
#                   listas = ClickUp.metodos_clickup.api.get_lists_from_folder(folder_id)
#                   for lista in listas:
#                        all_listas.append({
#                             "list_id": lista.get("id"),
#                             "list_name": lista.get("name"),
#                             "folder_id": folder_id,
#                             "space_id": space_id,
#                             "task_count": lista.get("task_count"),
#                             "archived": lista.get("archived")
#                        })
#     df_listas = pd.DataFrame(all_listas)
#     rows_listas = df_listas.to_dict(orient="records")
#     total_rows = len(rows_listas)
#     print(f"Total de registros para upsert na tabela {TABELA_LIST}: {total_rows}")
#     for i in range(0, total_rows, BATCH_SIZE):
#         lote = rows_listas[i:i + BATCH_SIZE]
#         try:
#                 upsert = metodos_supabase.api.upsert_data(banco=BANCO, tabela=TABELA_LIST, dados=rows_listas, chave="list_id")
#                 print(f"Upsert concluído com sucesso. Lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#         except Exception as e:
#                 print(f"Erro no lote {i//BATCH_SIZE + 1} | Registros {i+1} até {i+len(lote)}")
#                 print(str(e))
#                 print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert na tabela {TABELA_LIST}' no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Listas atualizados com sucesso!")

## 05. Atualizar User
TABELA_USER = "dim_user"
# try:
#     workspaces = ClickUp.metodos_clickup.api.get_workspaces()

#     for workspace in workspaces:
#         workspace_id = workspace["id"]
#         df_users = ClickUp.metodos_clickup.api.get_users(workspace_id)
#         df_users = tratar_nan_json(df_users)
#         rows_users = df_users.to_dict(orient="records")
#         total_rows = len(rows_users)
#         print(f"Total de registros para upsert na tabela {TABELA_USER}: {total_rows}")
#         for i in range(0, total_rows, BATCH_SIZE):
#             lote = rows_users[i:i + BATCH_SIZE]
#             try:
#                 metodos_supabase.api.upsert_data(banco=BANCO,tabela=TABELA_USER,dados=lote,chave="user_id")
#                 print(f"Upsert concluído com sucesso. Lote {i // BATCH_SIZE + 1} | Registros {i + 1} até {i + len(lote)}")
#             except Exception as e:
#                 print(f"Erro no lote {i // BATCH_SIZE + 1} | Registros {i + 1} até {i + len(lote)}")
#                 print(str(e))
#                 print(traceback.format_exc())
# except Exception as e:
#     print(f"Erro ao fazer upsert na tabela {TABELA_USER} no banco '{BANCO}'.")
#     print(str(e))
#     print(traceback.format_exc())
# print("Dados da dimensão Usuários atualizados com sucesso!")


### 05. Atualizar Tasks
dataI = "2026-05-01"
dataF = "2026-05-24"
params = {
        "include_closed": "true",
        "subtasks": "true",
        "include_timl": "true",
        "date_created_gt": data_para_ms(dataI),
        "date_created_lt": data_para_ms(dataF)
    }

TABELA_TASK = "fact_task"
try:
        workspace_id = '3134018'
        spaces = ClickUp.metodos_clickup.api.get_spaces(workspace_id)
        for space in spaces:
             space_id = space["id"]
             folders = ClickUp.metodos_clickup.api.get_folders(space_id)
             for folder in folders:
                  folder_id = folder["id"]
                  listas = ClickUp.metodos_clickup.api.get_lists_from_folder(folder_id)
                  for lista in listas:
                       list_id = lista.get("id")
                       df_tasks = ClickUp.metodos_clickup.api.get_tasks_from_list(list_id, params)
                       df_tasks = ClickUp.metodos_clickup.api.tratar_tasks(df_tasks)

                       if df_tasks.empty:
                            continue
                       
                       df_tasks = tratar_nan_json(df_tasks)
                       rows_tasks = df_tasks.to_dict(orient="records")
                       total_rows = len(rows_tasks)
                       print(f"Total de registros para upsert na tabela {TABELA_TASK}: {total_rows}")
                       for i in range(0, total_rows, BATCH_SIZE):
                            lote = rows_tasks[i:i + BATCH_SIZE]
                            try:
                                metodos_supabase.api.upsert_data(banco=BANCO,tabela=TABELA_TASK,dados=lote,chave="task_id")
                                print(f"Upsert concluído com sucesso. Lote {i // BATCH_SIZE + 1} | Registros {i + 1} até {i + len(lote)}")
                            except Exception as e:
                                print(f"Erro no lote {i // BATCH_SIZE + 1} | Registros {i + 1} até {i + len(lote)}")
                                print(str(e))
                                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {TABELA_TASK} no banco '{BANCO}'.")
    print(str(e))
    print(traceback.format_exc())
print("Dados da fato Tarefas atualizados com sucesso!")




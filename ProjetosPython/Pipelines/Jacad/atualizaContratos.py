from Jacad import metodos_jacad
from config import Config
from Supabase import metodos_supabase
import traceback
import numpy as np
import pandas as pd

BANCO = "Jacad_DB"
TABELA_ALUNOS = "dim_alunos"
TABELA_CONTRATOS = "fact_contratos"
TABELA_PLANOS_PAGAMENTO = "dim_planos_pagamento"
TABELA_CURSOS = "dim_cursos"

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


## ATUALIZAR TABELA ALUNOS
# try:
#     df_alunos = metodos_jacad.api.tratarAlunos(params={"idOrg": "0"})

#     if df_alunos.empty:
#         print("Sem dados na chamada da API.")
    
#     df_alunos = tratar_nan_json(df_alunos)
    
#     rows_alunos = df_alunos.to_dict(orient='records')
    
#     metodos_supabase.api.upsert_data(banco=BANCO,
#                                      tabela=TABELA_ALUNOS,
#                                      dados=rows_alunos,
#                                      chave="idAluno")
# except Exception as e:
#     print(f"Erro ao atualizar a tabela {TABELA_ALUNOS} no banco.")
#     print(str(e))
#     print(traceback.format_exc())
# print(f"Dados atualizados com sucesso!")

## ATUALIZAR TABELA CURSOS
# try:
#     df_cursos = metodos_jacad.api.tratarCursos(params={"idOrg": "0"})

#     if df_cursos.empty:
#         print("Sem dados na chamada da API.")
    

#     df_cursos = tratar_nan_json(df_cursos)
    
#     rows_cursos = df_cursos.to_dict(orient='records')
    
#     metodos_supabase.api.upsert_data(banco=BANCO,
#                                      tabela=TABELA_CURSOS,
#                                      dados=rows_cursos,
#                                      chave="id_curso")
# except Exception as e:
#     print(f"Erro ao atualizar a tabela {TABELA_CURSOS} no banco.")
#     print(str(e))
#     print(traceback.format_exc())
# print(f"Dados atualizados com sucesso!")


## ATUALIZAR TABELA PLANOS PAGAMENTO
# try:
#     df_planos = metodos_jacad.api.tratarPlanosPagamento(params={"idOrg": "0"})

#     if df_planos.empty:
#         print("Sem dados na chamada da API.")
    

#     df_planos = tratar_nan_json(df_planos)
    
#     rows_planos = df_planos.to_dict(orient='records')
    
#     metodos_supabase.api.upsert_data(banco=BANCO,
#                                      tabela=TABELA_PLANOS_PAGAMENTO,
#                                      dados=rows_planos,
#                                      chave="id_plano_pagamento")
# except Exception as e:
#     print(f"Erro ao atualizar a tabela {TABELA_PLANOS_PAGAMENTO} no banco.")
#     print(str(e))
#     print(traceback.format_exc())
# print(f"Dados atualizados com sucesso!")

## ATUALIZAR TABELA CONTRATOS
try:
    df_contratos= metodos_jacad.api.tratarContratos(params={"idOrg": "0"})

    if df_contratos.empty:
        print("Sem dados na chamada da API.")
    

    df_contratos = tratar_nan_json(df_contratos)
    
    rows_contratos = df_contratos.to_dict(orient='records')
    
    metodos_supabase.api.upsert_data(banco=BANCO,
                                     tabela=TABELA_CONTRATOS,
                                     dados=rows_contratos,
                                     chave="id_contrato")
except Exception as e:
    print(f"Erro ao atualizar a tabela {TABELA_CONTRATOS} no banco.")
    print(str(e))
    print(traceback.format_exc())
print(f"Dados atualizados com sucesso!")
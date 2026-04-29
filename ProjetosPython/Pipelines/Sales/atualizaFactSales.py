from Principia import metodosPrincipia
from Supabase import metodos_supabase
from Guru import metodos_guru
import datetime
import traceback
import pandas as pd
import math

dataI = (datetime.date.today() - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
dataF = datetime.date.today().strftime("%Y-%m-%d")

banco = "Guru_DB"
tabela = "fact_sales"
batch_size = 1000

def normalizar_json(valor):
    # 1. nulos 
    if pd.isna(valor):
        return None
    if isinstance(valor, float) and math.isnan(valor):
        return None

    if pd.isna(valor):
        return None

    # 2. datas 
    if isinstance(valor, pd.Timestamp):
        return valor.isoformat()

    if isinstance(valor, (datetime.datetime, datetime.date)):
        return valor.isoformat()

    # 3. floats que são inteiros (ex: 6.0 → 6)
    if isinstance(valor, float):
        if valor.is_integer():
            return int(valor)
        return float(valor)

    # 4. strings
    if isinstance(valor, str):
        valor = valor.replace("\x00", "")
        valor = valor.replace("\u0000", "")
        return valor.strip()
    
    return valor

def normalizar_rows(rows):
    rows_tratadas = []

    for row in rows:
        row_tratada = {}
        for chave, valor in row.items():
            row_tratada[chave] = normalizar_json(valor)
        rows_tratadas.append(row_tratada)

    return rows_tratadas

### 1. Atualizar as vendas da Guru

try:
    print(f"Buscando dados de vendas da Guru para o período de {dataI} a {dataF}...")

    df = metodos_guru.api.getTransactionsDF(periodo=[dataI,dataF])

    if df.empty:
        print("Aviso: nenhum dado retornado.")
    else:
        if "id" not in df.columns:
            raise ValueError("A coluna 'id' não existe no DataFrame.")
        
        if df["id"].isnull().any():
            raise ValueError("Existem registros com id nulo.")
        
        rows = df.to_dict(orient="records")
        rows = normalizar_rows(rows)
        total_rows = len(rows)

        print(f"Total de registros para upsert: {total_rows}")

        for i in range(0, total_rows, batch_size):
            lote = rows[i:i + batch_size]
            try:
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela, dados=rows, chave="id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())

print("Dados de vendas da Guru atualizados com sucesso!")


### 2. Atualizar as vendas da Principia
try:
    print(f"Buscando dados de vendas da Principia para o período de {dataI} a {dataF}...")

    df = metodosPrincipia.api.getSalesDF(periodo=[dataI, dataF], ambiente="url_prod")

    if df.empty:
        print("Aviso: nenhum dado retornado.")
    else:
        if "id" not in df.columns:
            raise ValueError("A coluna 'id' não existe no DataFrame.")
        if df["id"].isnull().any():
            raise ValueError("Existem registros com id nulo.")
        
        rows = df.to_dict(orient="records")
        rows = normalizar_rows(rows)
        total_rows = len(rows)
        print(f"Total de registros para upsert: {total_rows}")

        for i in range(0, total_rows, batch_size):
            lote = rows[i:i + batch_size]
            try:
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela, dados=rows, chave="id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())
    
print("Dados de vendas da Principia atualizados com sucesso!")


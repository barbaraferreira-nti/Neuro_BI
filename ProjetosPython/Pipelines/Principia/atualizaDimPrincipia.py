from ProjetosPython.Principia import metodosPrincipia
from ProjetosPython.Supabase import metodos_supabase
import datetime
import traceback
import pandas as pd
import math

banco = "Guru_DB"
tabela_produdo = "dim_product"
tabela_offer = "dim_offer"
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

# Atualizar a tabela 'dim_product'
try:
    df = metodosPrincipia.api.getCoursesDF(app="PrincipiaApi", ambiente="url_prod")

    if df.empty:
        print("Aviso: nenhum dado retornado.")
    else:
        if "product_id" not in df.columns:
            raise ValueError("A coluna 'product_id' não existe no DataFrame.")
        if df["product_id"].isnull().any():
            raise ValueError("Existem registros com product_id nulo.")
        
        rows = df.to_dict(orient="records")
        rows = normalizar_rows(rows)
        total_rows = len(rows)
        print(f"Total de registros para upsert: {total_rows}")

        for i in range(0, total_rows, batch_size):
            lote = rows[i:i + batch_size]
            try:
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela_produdo, dados=rows, chave="product_id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela_produdo}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())


# atualiza a tabela 'dim_offer' 

offers = {
        "p0001": {
            "offer_name": "[LANCAMENTO][PROLEIA][PRINCIPIA]",
            "product_id": "64591"
            },
        "p0002": {
            "offer_name": "[RECOVERY][PROLEIA][PRINCIPIA]",
            "product_id":"64591"
            },
        "p0003": {
            "offer_name": "[PROLEIA][PRINCIPIA]",
            "product_id":"53855"
            },
        "p0004": {
            "offer_name": "[PENNSA][PRINCIPIA]",
            "product_id":"54078"
            },
        "p0005": {
            "offer_name": "[CERTIFICACAO-PROLEIA][PRINCIPIA]",
            "product_id":"55271"
            },
        "p0006": {
            "offer_name": "[PROMAIS][PRINCIPIA]",
            "product_id":"60215"
            },
        "p0007": {
            "offer_name": "[PENNSA][PRINCIPIA]",
            "product_id":"65379"
            }                    
}

def preparar_dim_offer(offers_dict):
    rows = []

    for offer_id, dados in offers_dict.items():
        rows.append({
            "offer_id": offer_id,
            "offer_name": dados.get("offer_name"),
            "product_internal_id": dados.get("product_id")
        })

    return rows

rows_offer = preparar_dim_offer(offers)

upsert = metodos_supabase.api.upsert_data(
    banco=banco,
    tabela=tabela_offer,
    dados=rows_offer,
    chave="offer_id"
)
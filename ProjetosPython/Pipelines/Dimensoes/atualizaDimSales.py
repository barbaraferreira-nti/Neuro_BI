from Guru import metodos_guru
from Supabase import metodos_supabase
from Principia import metodosPrincipia
import pandas as pd
import datetime
import math
import traceback


dataI = (datetime.datetime.today() - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
dataF = datetime.datetime.today().strftime("%Y-%m-%d")

banco = "Guru_DB"
tabela_dim_product = "dim_product"
tabela_dim_contact = "dim_contact"
tabela_dim_offer = "dim_offer"
batch_size = 1000

def normalizar_json(valor):
    # nulos do pandas
    if pd.isna(valor):
        return None

    # pandas Timestamp
    if isinstance(valor, pd.Timestamp):
        return valor.isoformat()

    # datetime/date Python
    if isinstance(valor, (datetime.datetime, datetime.date)):
        return valor.isoformat()

    # float nan
    if isinstance(valor, float) and math.isnan(valor):
        return None

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

### 01. Atualizar as dimensões (product, contact e offer) da Guru no Supabase

# Atualizando a dimensão 'product' 
try: 
    print("Iniciando atualização da dimensão 'product' da Guru...")
    df_product = metodos_guru.api.getProductsDF(app="GuruApi")
    
    if df_product.empty:
        print("Aviso: nenhum dado retornado no endpoint de produtos.")
    else:
        if "product_id" not in df_product.columns:
            raise ValueError("A coluna 'produc_id' não existe no DataFrame 'df_product'.")
        rows_products = df_product.to_dict(orient="records")
        rows_products = normalizar_rows(rows_products)
        total_rows_products = len(rows_products)

        print(f"Total de registros para upsert na tabela {tabela_dim_product}: {total_rows_products}")
        for i in range(0, total_rows_products, batch_size):
            lote = rows_products[i:i + batch_size]
            try:
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela_dim_product, dados=rows_products, chave="product_id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela_dim_product}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())


# Atualizando a dimensão 'contacts' 

try: 
    print("Iniciando atualização da dimensão 'contacts' da Guru...")
    df_contact = metodos_guru.api.getContactsDF(app="GuruApi", periodo=[dataI,dataF])
    
    if df_contact.empty:
        print("Aviso: nenhum dado retornado no endpoint de contatos.")
    else:
        if "contact_id" not in df_contact.columns:
            raise ValueError("A coluna 'contact_id' não existe no DataFrame 'df_contact'.")
        rows_contacts = df_contact.to_dict(orient="records")
        rows_contacts = normalizar_rows(rows_contacts)
        total_rows_contacts = len(rows_contacts)

        print(f"Total de registros para upsert na tabela {tabela_dim_contact}: {total_rows_contacts}")
        for i in range(0, total_rows_contacts, batch_size):
            lote = rows_contacts[i:i + batch_size]
            try:
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela_dim_contact, dados=rows_contacts, chave="contact_id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela_dim_product}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())

# Atualizando a dimensão 'offers' 
try: 
    print("Iniciando atualização da dimensão 'offers' da Guru...")
    df_offers = metodos_guru.api.getOffersDF(app="GuruApi")
    
    if df_offers.empty:
        print("Aviso: nenhum dado retornado no endpoint de produtos/offers.")
    else:
        if "offer_id" not in df_offers.columns:
            raise ValueError("A coluna 'offer_id' não existe no DataFrame 'df_offers'.")
        rows_offers = df_offers.to_dict(orient="records")
        rows_offers = normalizar_rows(rows_offers)
        total_rows_offers = len(rows_offers)

        print(f"Total de registros para upsert na tabela {tabela_dim_offer}: {total_rows_offers}")
        for i in range(0, total_rows_offers, batch_size):
            lote = rows_offers[i:i + batch_size]
            try:
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela_dim_offer, dados=rows_offers, chave="offer_id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela_dim_offer}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())


### 02. Atualizar as dimensões (product e offer) da Principia no Supabase

# Atualizar a tabela 'dim_product'
try:
    print("Iniciando atualização da dimensão 'product' da Principia...")
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
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela_dim_product, dados=rows, chave="product_id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela_dim_product}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())


# Atualizar a tabela 'dim_offer'

offers_principia = {
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

try:
    print("Iniciando atualização da dimensão 'offer' da Principia...")
    rows_offer = preparar_dim_offer(offers_principia)

    upsert = metodos_supabase.api.upsert_data(
        banco=banco,
        tabela=tabela_dim_offer,
        dados=rows_offer,
        chave="offer_id"
    )
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela_dim_offer}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())

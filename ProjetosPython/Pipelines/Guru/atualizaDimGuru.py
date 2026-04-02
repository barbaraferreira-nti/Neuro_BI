from ProjetosPython.Guru import metodos_guru
from ProjetosPython.Supabase import metodos_supabase
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


# Atualizando a dimensão 'product' 
try: 
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
                upsert = metodos_supabase.api.upsert_data(banco=banco, tabela=tabela_dim_offer, dados=rows_offers, chave="offer_id, product_internal_id")
                print(f"Upsert concluído com sucesso. Lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
            except Exception as e:
                print(f"Erro no lote {i//batch_size + 1} | Registros {i+1} até {i+len(lote)}")
                print(str(e))
                print(traceback.format_exc())
except Exception as e:
    print(f"Erro ao fazer upsert na tabela {tabela_dim_offer}' no banco '{banco}'.")
    print(str(e))
    print(traceback.format_exc())
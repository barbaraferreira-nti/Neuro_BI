from Supabase import metodos_supabase
import psycopg2
import os, json
from copy import deepcopy
import pandas as pd
import numpy as np
from datetime import datetime, date
from decimal import Decimal

# Pipeline para espelhar vendas com oferta de Livro incluso

conexao = psycopg2.connect(
        host="aws-1-us-east-1.pooler.supabase.com",
        database="postgres",
        user="postgres.yswgoojqqpwlfmxxuink",
        password="Aveces16.1612",
        port="5432"
    )

def espelhar_venda(venda, novo_product_id, novo_product_id_guru):
    nova_venda = deepcopy(venda)

    id_original = venda.get("id")

    nova_venda["id"] = f"{id_original}_espelho_{novo_product_id}"

    nova_venda["product_id"] = novo_product_id
    nova_venda["product_guru_id"] =novo_product_id_guru
    nova_venda["payment_net"] = 0.0
    nova_venda["payment_discount_value"] = 0.0
    nova_venda["payment_gross"] = 0.0
    nova_venda["installments_qty"] = 0
    nova_venda["installments_value"] = 0.0
    nova_venda["installments_interest"] = 0.0
    nova_venda["product_total_value"] = 0.0
    nova_venda["coupon_value"] = 0.0
    nova_venda["shipping_value"] = 0.0

    for chave, valor in nova_venda.items():
        if isinstance(valor, float) and np.isnan(valor):
            nova_venda[chave] = None

    return nova_venda

def normalizar_json(df):
    def tratar_valor(x):
        if x is None:
            return None

        try:
            if pd.isna(x):
                return None
        except Exception:
            pass

        if isinstance(x, pd.Timestamp):
            return x.isoformat()

        if isinstance(x, (datetime, date)):
            return x.isoformat()

        if isinstance(x, Decimal):
            return float(x)

        if isinstance(x, str):
            return x.replace("\x00", "")

        return x

    return df.astype(object).apply(lambda col: col.map(tratar_valor))

def limpar_nan_para_json(obj):
    if obj is None:
        return None

    if isinstance(obj, float) and np.isnan(obj):
        return None

    if isinstance(obj, (np.floating,)):
        if np.isnan(obj):
            return None
        return float(obj)

    if isinstance(obj, (np.integer,)):
        return int(obj)

    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    if isinstance(obj, Decimal):
        return float(obj)

    if isinstance(obj, str):
        return obj.replace("\x00", "")

    if isinstance(obj, dict):
        return {k: limpar_nan_para_json(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [limpar_nan_para_json(v) for v in obj]

    try:
        if pd.isna(obj):
            return None
    except Exception:
        pass

    return obj

# Lista dos lançamentos que deseja atualizar
nomes_arquivos = ["POS_MATEMATICA_0626"]

### TRAZENDO OS DADOS DO JSON
scriptDir = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(scriptDir, "ofertas_livros.json")

with open(configPath, "r", encoding="utf-8") as f:
    config = json.load(f)

### LOOP PRINCIPAL
for nomeArquivo in nomes_arquivos:

    if nomeArquivo not in config:
        print(f"[AVISO] {nomeArquivo} não encontrado no JSON")
        continue

    print(f"\n--- Processando {nomeArquivo} ---")

    dados = config[nomeArquivo]

    idLancamento = dados["id_lancamento"]
    inicioLanc = dados["inicio_lancamento"]
    fimLanc = dados["fim_lancamento"]
    id_produtos = dados["id_produtos_lancamento"][0]
    id_offers = dados["id_offer_lancamento"]
    id_produto_livro = dados["id_produto_livro"][0]
    id_produto_guru_livro = dados["id_produto_guru_livro"][0]

    try:
        cur = conexao.cursor()

        cur.execute("""
            SELECT *
            FROM public.fact_sales
            WHERE (ordered_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date = %s 
            AND product_id = %s
            AND status = 'approved'
        """, (inicioLanc, id_produtos))

        rows = cur.fetchall()
        colunas = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=colunas)

        if df.empty:
            print(f"Nenhuma venda encontrada para {nomeArquivo}")
            continue

        #df = normalizar_json(df)

        vendas_originais = df.to_dict(orient="records")

        vendas_espelhadas = []

        for venda in vendas_originais:
            venda_espelhada = espelhar_venda(
                venda,
                id_produto_livro,
                id_produto_guru_livro
            )

            venda_espelhada = limpar_nan_para_json(venda_espelhada)

            vendas_espelhadas.append(venda_espelhada)

        df_espelhadas = pd.DataFrame(vendas_espelhadas)
        df_espelhadas = df_espelhadas.replace({np.nan: None})
        #df_espelhadas = normalizar_json(df_espelhadas)

        vendas_espelhadas = df_espelhadas.to_dict(orient="records")
        for i, venda in enumerate(vendas_espelhadas):
            try:
                json.dumps(venda, allow_nan=False)
            except Exception as e:
                print(f"Venda com erro na posição {i}: {e}")
                for chave, valor in venda.items():
                    try:
                        json.dumps(valor, allow_nan=False)
                    except Exception:
                        print(chave, valor, type(valor))
                raise

        upsert = metodos_supabase.api.upsert_data(
            banco="Guru_DB",
            tabela="fact_sales",
            dados=vendas_espelhadas,
            chave="id"
        )

        print(f"{len(vendas_espelhadas)} vendas espelhadas para {nomeArquivo}")

    except Exception as e:
        conexao.rollback()
        print(f"Erro no {nomeArquivo}: {e}")

    finally:
        cur.close()
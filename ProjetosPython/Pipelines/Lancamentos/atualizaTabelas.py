import os, json, datetime
from Supabase import metodos_supabase
import psycopg2
from psycopg2.extras import execute_values



"""
ARQUIVO QUE ATUALIZARÁ AS TABELAS NECESSÁRIAS PARA O DASHBOARD DE
ACOMPANHAMENTO DOS LANÇAMENTOS (PAGOS E GRÁTIS)
"""

# Lista dos lançamentos que deseja atualizar
nomes_arquivos = [
    "PROLEIA_0426",
    "POS_MANEJO_0426"
]

conexao = psycopg2.connect(
        host="aws-1-us-east-1.pooler.supabase.com",
        database="postgres",
        user="postgres.yswgoojqqpwlfmxxuink",
        password="Aveces16.1612",
        port="5432"
    )


### TRAZENDO OS DADOS DO JSON
scriptDir = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(scriptDir, "lancamentos.json")

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
    inicioCapt = dados["inicio_captacao"]
    fimCapt = dados["fim_captacao"]
    inicioLanc = dados["inicio_lancamento"]
    fimLanc = dados["fim_lancamento"]
    campanhaSendFlow = dados["campanha_sendflow"]
    contaMeta = dados["contaMeta"]
    id_produtos_captacao = dados["id_produtos_captacao"]
    id_produtos_lancamento = dados["id_produtos_lancamento"]
    id_offer_captacao = dados["id_offer_captacao"]
    id_offer_lancamento = dados["id_offer_lancamento"]

    try:
        cur = conexao.cursor()

        # META
        cur.execute("""
            UPDATE public.fact_meta
            SET id_lancamento = %s
            WHERE date_start BETWEEN %s AND %s
              AND account_id = %s
        """, (idLancamento, inicioCapt, fimCapt, contaMeta))

        print(f"Meta: {cur.rowcount} linhas")

        # SENDFLOW
        cur.execute("""
            UPDATE public.fact_sendflow
            SET id_lancamento = %s
            WHERE created_at::date BETWEEN %s AND %s
              AND name_camp = %s
        """, (idLancamento, inicioCapt, fimLanc, campanhaSendFlow))

        print(f"SendFlow: {cur.rowcount} linhas")

        # GURU CAPTAÇÃO
        cur.execute("""
            UPDATE public.fact_sales
            SET id_lancamento = %s
            WHERE ordered_at::date BETWEEN %s AND %s
              AND product_guru_id = ANY(%s)
              AND offer_id = ANY(%s)
        """, (idLancamento, inicioCapt, fimCapt, id_produtos_captacao, id_offer_captacao))

        print(f"Guru Captação: {cur.rowcount} linhas")

        # GURU LANÇAMENTO
        cur.execute("""
            UPDATE public.fact_sales
            SET id_lancamento = %s
            WHERE ordered_at::date BETWEEN %s AND %s
              AND product_guru_id = ANY(%s)
              AND offer_id = ANY(%s)
        """, (idLancamento, inicioLanc, fimLanc, id_produtos_lancamento, id_offer_lancamento))

        print(f"Guru Lançamento: {cur.rowcount} linhas")

        conexao.commit()

    except Exception as e:
        conexao.rollback()
        print(f"Erro no {nomeArquivo}: {e}")

    finally:
        cur.close()
    
conexao.close()
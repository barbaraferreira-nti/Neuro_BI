import os, json, datetime
from ProjetosPython.Supabase import metodos_supabase
import psycopg2
from psycopg2.extras import execute_values

""" ARQUIVO QUE ATUALIZARÁ AS TABELAS NECESSÁRIAS PARA O DASHBOARD DE 
    ACOMPANHAMENTO DOS LANÇAMENTOS (PAGOS E GRÁTIS)
"""

nomeArquivo = "PROLEIA_0126"
banco_supabase = "Guru_DB"
BATCH_INSERT_SUPABASE = 500

## TRAZENDO OS DADOS DO JSON
scriptDir = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(scriptDir, "lancamentos.json")
with open(configPath, "r", encoding="utf-8") as f:
    config = json.load(f)

idLancamento = config[nomeArquivo]["id_lancamento"]
nomeLancamento = config[nomeArquivo]["nome_lancamento"]
inicioCapt = config[nomeArquivo]["inicio_captacao"]
fimCapt = config[nomeArquivo]["fim_captacao"]
inicioLanc = config[nomeArquivo]["inicio_lancamento"]
fimLanc = config[nomeArquivo]["fim_lancamento"]
campanhaSendFlow = config[nomeArquivo]["campanha_sendflow"]
contaMeta = config[nomeArquivo]["contaMeta"]
id_produtos_captacao = config[nomeArquivo]["id_produtos_captacao"]
id_produtos_lancamento = config[nomeArquivo]["id_produtos_lancamento"]
id_offer_captacao = config[nomeArquivo]["id_offer_captacao"]
id_offer_lancamento = config[nomeArquivo]["id_offer_lancamento"]

conn_fact = psycopg2.connect(
    host="aws-1-us-east-1.pooler.supabase.com",
    database="postgres",
    user="postgres.yswgoojqqpwlfmxxuink",
    password="Aveces16.1612",
    port="5432"
)


try:
    cur = conn_fact.cursor()
    query_captacao = """
        UPDATE public.fact_transactions
        SET id_lancamento = %s
        WHERE ordered_at::date BETWEEN %s AND %s
          AND product_id = ANY(%s)
          AND offer_id = ANY(%s)

    """

    query_lancamento = """
        UPDATE public.fact_transactions
        SET id_lancamento = %s
        WHERE ordered_at::date BETWEEN %s AND %s
          AND product_id = ANY(%s) 
          AND offer_id = ANY(%s)


    """
    cur.execute(query_captacao, (idLancamento, inicioCapt, fimCapt, id_produtos_captacao, id_offer_captacao))
    linhas_captacao = cur.rowcount
    cur.execute(query_lancamento, (idLancamento, inicioLanc, fimLanc, id_produtos_lancamento, id_offer_lancamento))
    linhas_lancamento = cur.rowcount

    conn_fact.commit()
    print(f"Captação: {linhas_captacao} linhas atualizadas.")
    print(f"Lançamento: {linhas_lancamento} linhas atualizadas.")
    

except Exception as e:
    conn_fact.rollback()
    print("Erro ao atualizar fact_transactions:", e)

finally:
    cur.close()
    conn_fact.close()
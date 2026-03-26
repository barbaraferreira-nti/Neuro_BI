import os, json, datetime
from Supabase import metodos_supabase
import psycopg2
from psycopg2.extras import execute_values


""" ARQUIVO QUE ATUALIZARÁ AS TABELAS NECESSÁRIAS PARA O DASHBOARD DE 
    ACOMPANHAMENTO DOS LANÇAMENTOS (PAGOS E GRÁTIS)
"""

nomeArquivo = "PROLEIA_0326"


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



## Conexão com o banco de dados
conexao = psycopg2.connect(
    host="aws-1-us-east-1.pooler.supabase.com",
    database="postgres",
    user="postgres.yswgoojqqpwlfmxxuink",
    password="Aveces16.1612",
    port="5432"
)


### Atualiza a tabela "fact_meta"

try:
    cur_meta = conexao.cursor()
    query_captacao_meta = """
        UPDATE public.fact_meta
        SET id_lancamento = %s
        WHERE date_start BETWEEN %s AND %s
          AND account_id = %s
    """

    cur_meta.execute(query_captacao_meta, (idLancamento, inicioCapt, fimCapt, contaMeta))
    linhas_captacao_meta = cur_meta.rowcount

    conexao.commit()
    print(f"Captação Meta: {linhas_captacao_meta} linhas atualizadas.")
#   print(f"Lançamento: {linhas_lancamento} linhas atualizadas.")
    

except Exception as e:
    conexao.rollback()
    print("Erro ao atualizar fact_meta:", e)

finally:
    cur_meta.close()



### Atualiza a tabela "fact_sendflow"

try:
    cur_sf = conexao.cursor()
    query_captacao_sf = """
        UPDATE public.fact_sendflow
        SET id_lancamento = %s
        WHERE created_at::date BETWEEN %s AND %s
          AND name_camp = %s
    """

    cur_sf.execute(query_captacao_sf, (idLancamento, inicioCapt, fimLanc, campanhaSendFlow))
    linhas_captacao_sf = cur_sf.rowcount

    conexao.commit()
    print(f"Captação SendFlow: {linhas_captacao_sf} linhas atualizadas.")
#   print(f"Lançamento: {linhas_lancamento} linhas atualizadas.")
    

except Exception as e:
    conexao.rollback()
    print("Erro ao atualizar fact_sendflow:", e)

finally:
    cur_sf.close()


## Atualizando a tabela 'fact_transactions'

try:
    cur_guru = conexao.cursor()
    query_captacao_guru = """
        UPDATE public.fact_transactions
        SET id_lancamento = %s
        WHERE ordered_at::date BETWEEN %s AND %s
          AND product_id = ANY(%s)
          AND offer_id = ANY(%s)

    """

    query_lancamento_guru = """
        UPDATE public.fact_transactions
        SET id_lancamento = %s
        WHERE ordered_at::date BETWEEN %s AND %s
          AND product_id = ANY(%s) 
          AND offer_id = ANY(%s)


    """
    cur_guru.execute(query_captacao_guru, (idLancamento, inicioCapt, fimCapt, id_produtos_captacao, id_offer_captacao))
    linhas_captacao_guru = cur_guru.rowcount
    cur_guru.execute(query_lancamento_guru, (idLancamento, inicioLanc, fimLanc, id_produtos_lancamento, id_offer_lancamento))
    linhas_lancamento_guru = cur_guru.rowcount

    conexao.commit()
    print(f"Captação Guru: {linhas_captacao_guru} linhas atualizadas.")
    print(f"Lançamento Guru: {linhas_lancamento_guru} linhas atualizadas.")
    

except Exception as e:
    conexao.rollback()
    print("Erro ao atualizar fact_transactions:", e)

finally:
    cur_guru.close()
    conexao.close()
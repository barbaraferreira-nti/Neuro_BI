import os, json
import psycopg2
from psycopg2.extras import execute_values
from PostgreSQL import metodos_postgresql

"""
ARQUIVO QUE ATUALIZARÁ AS TABELAS NECESSÁRIAS PARA O DASHBOARD DE
ACOMPANHAMENTO DOS LANÇAMENTOS (PAGOS E GRÁTIS)
"""

# Lista dos lançamentos que deseja atualizar
nomes_arquivos = ['PROLEIA_0626', 'POS_MATEMATICA_0626']
conexao_guru = metodos_postgresql.PGSQL.conexao("Guru_DB")
conexao_meta = metodos_postgresql.PGSQL.conexao("Meta_DB")

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
        cur_guru = conexao_guru.cursor()
        cur_meta = conexao_meta.cursor()

        # META
        cur_meta.execute("""
            UPDATE public.fact_fb_account f
            SET id_lancamento = %s
            WHERE f.date_start BETWEEN %s AND %s
              AND f.account_id = %s
              AND EXISTS (
                    SELECT 1
                    FROM dm_fb_campaigns c
                    WHERE c.campaign_id = f.campaign_id
                        AND c.campaign_name LIKE '%%VENDA-INGRESSO%%'
                )
        """, (idLancamento, inicioCapt, fimCapt, contaMeta))

        print(f"Meta: {cur_meta.rowcount} linhas")

        # SENDFLOW
        cur_guru.execute("""
            UPDATE public.fact_sendflow
            SET id_lancamento = %s
            WHERE created_at::date BETWEEN %s AND %s
              AND name_camp = %s
        """, (idLancamento, inicioCapt, fimLanc, campanhaSendFlow))

        print(f"SendFlow: {cur_guru.rowcount} linhas")

        # GURU CAPTAÇÃO
        cur_guru.execute("""
            UPDATE public.fact_sales
            SET id_lancamento = %s
            WHERE (ordered_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s AND %s
              AND product_id = ANY(%s)
              AND offer_id = ANY(%s)
        """, (idLancamento, inicioCapt, fimCapt, id_produtos_captacao, id_offer_captacao))

        print(f"Guru Captação: {cur_guru.rowcount} linhas")

        # GURU LANÇAMENTO
        cur_guru.execute("""
            UPDATE public.fact_sales
            SET id_lancamento = %s
            WHERE (ordered_at AT TIME ZONE 'utc' AT TIME ZONE 'America/Sao_Paulo')::date BETWEEN %s AND %s
              AND product_id = ANY(%s)
              AND offer_id = ANY(%s)
        """, (idLancamento, inicioLanc, fimLanc, id_produtos_lancamento, id_offer_lancamento))

        print(f"Guru Lançamento: {cur_guru.rowcount} linhas")

        conexao_guru.commit()
        conexao_meta.commit()

    except Exception as e:
        conexao_guru.rollback()
        conexao_meta.rollback()
        print(f"Erro no {nomeArquivo}: {e}")

    finally:
        cur_guru.close()
        cur_meta.close()
    
conexao_guru.close()
conexao_meta.close()
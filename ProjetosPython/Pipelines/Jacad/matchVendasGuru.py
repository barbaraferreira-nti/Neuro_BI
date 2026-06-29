from Supabase import metodos_supabase
import psycopg2
import pandas as pd
import datetime
from supabase import create_client
from config import Config

SUPABASE_JACAD_URL = Config.Supabase.URL_JACAD_DB
SUPABASE_JACAD_KEY = Config.Supabase.TOKEN_JACAD_DB

supabase_jacad = create_client(SUPABASE_JACAD_URL, SUPABASE_JACAD_KEY)

JANELA_DIAS = 60

CONN_GURU = psycopg2.connect(
        host="aws-1-us-east-1.pooler.supabase.com",
        database="postgres",
        user="postgres.yswgoojqqpwlfmxxuink",
        password="Aveces16.1612",
        port="5432"
    )

CONN_JACAD = psycopg2.connect(
        host="aws-1-sa-east-1.pooler.supabase.com",
        database="postgres",
        user="postgres.trxanwvrbitdndlrkisn",
        password="wA7rRn6ZEz3nkUMC",
        port="5432"
    )

def query_to_df(conn, sql, params=None):
    cur = conn.cursor()

    cur.execute(sql, params)

    dados = cur.fetchall()

    colunas = [desc[0] for desc in cur.description]

    cur.close()

    return pd.DataFrame(dados, columns=colunas)
    
def chunks(lista, tamanho):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]

def atualizar_contratos_supabase(df_matches):
    dados_update = []

    for _, row in df_matches.iterrows():
        dados_update.append({
            "id_contrato": row["id_contrato"],
            "id_transacao_guru": row["id_transacao_guru"]
        })

    for item in dados_update:
        (
            supabase_jacad
            .table("fact_contratos")
            .update({
                "id_transacao_guru": item["id_transacao_guru"]
            })
            .eq("id_contrato", item["id_contrato"])
            .execute()
        )

# =====================================================
# 1. BUSCAR CONTRATOS JACAD + CPF + PRODUTOS GURU
# =====================================================

sql_contratos = """
    SELECT
        fc.id_contrato,
        fc.id_aluno,
        fc.id_curso,
        fc.data_contrato,
        a.cpf,
        cg.id_produto_guru
    FROM public.fact_contratos fc
    INNER JOIN public.dim_alunos a
        ON a."idAluno" = fc.id_aluno
    INNER JOIN public.dim_cursos_guru cg
        ON cg.id_curso = fc.id_curso
    WHERE fc.id_transacao_guru IS NULL
      AND a.cpf IS NOT NULL
      AND cg.id_produto_guru IS NOT NULL
"""
df_base = query_to_df(CONN_JACAD, sql_contratos)

if df_base.empty:
    print("Nenhum contrato pendente de match.")
    raise SystemExit

df_base["data_contrato"] = pd.to_datetime(df_base["data_contrato"]).dt.date

df_base = df_base.dropna(subset=["cpf", "data_contrato", "id_produto_guru"])

if df_base.empty:
    print("Nenhum contrato válido após normalização.")
    raise SystemExit

# =====================================================
# 2. DEFINIR CPFS E JANELA GERAL DE BUSCA
# =====================================================

cpfs = df_base["cpf"].dropna().unique().tolist()

data_min = df_base["data_contrato"].min() - datetime.timedelta(days=JANELA_DIAS)
data_max = df_base["data_contrato"].max() + datetime.timedelta(days=JANELA_DIAS)

print(f"Contratos candidatos: {df_base['id_contrato'].nunique()}")
print(f"CPFs únicos: {len(cpfs)}")
print(f"Buscando vendas Guru entre {data_min} e {data_max}")


# =====================================================
# 3. BUSCAR VENDAS GURU POR CPF + PERÍODO
# =====================================================

sql_vendas = """
    SELECT
        id,
        contact_doc,
        product_id,
        ordered_at,
        payment_net,
        status
    FROM public.fact_sales
    WHERE contact_doc = ANY(%s)
      AND status IN ('approved', 'made_effective', 'Aprovada')
      AND ordered_at::date BETWEEN %s AND %s
"""
dfs_vendas = []

for lote_cpfs in chunks(cpfs, 500):
    df_lote = query_to_df(
        CONN_GURU,
        sql_vendas,
        params=(lote_cpfs, data_min, data_max)
    )

    if not df_lote.empty:
        dfs_vendas.append(df_lote)

if not dfs_vendas:
    print("Nenhuma venda Guru encontrada.")
    raise SystemExit

df_vendas = pd.concat(dfs_vendas, ignore_index=True)

df_vendas["ordered_at"] = pd.to_datetime(df_vendas["ordered_at"])

# =====================================================
# 4. MATCH CONTRATO X VENDA
# =====================================================

matches = []

for id_contrato, grupo_contrato in df_base.groupby("id_contrato"):
    contrato = grupo_contrato.iloc[0]

    cpf = contrato["cpf"]
    data_contrato = contrato["data_contrato"]
    produtos_possiveis = grupo_contrato["id_produto_guru"].dropna().unique().tolist()

    data_inicio = pd.Timestamp(data_contrato - datetime.timedelta(days=JANELA_DIAS))
    data_fim = pd.Timestamp(data_contrato + datetime.timedelta(days=JANELA_DIAS))

    vendas_candidatas = df_vendas[
        (df_vendas["contact_doc"] == cpf) &
        (df_vendas["product_id"].isin(produtos_possiveis)) &
        (df_vendas["ordered_at"] >= data_inicio) &
        (df_vendas["ordered_at"] <= data_fim)
    ].copy()

    if vendas_candidatas.empty:
        continue

    vendas_candidatas["diff_dias"] = vendas_candidatas["ordered_at"].dt.date.apply(
        lambda d: abs((d - data_contrato).days)
    )

    venda_escolhida = (
        vendas_candidatas
        .sort_values(["diff_dias", "ordered_at"])
        .iloc[0]
    )

    matches.append({
        "id_contrato": id_contrato,
        "id_transacao_guru": venda_escolhida["id"],
        "id_aluno": contrato["id_aluno"],
        "id_curso": contrato["id_curso"],
        "cpf": cpf,
        "data_contrato": data_contrato,
        "id_produto_guru": venda_escolhida["product_id"],
        "data_venda": venda_escolhida["ordered_at"],
        "valor_venda": venda_escolhida["payment_net"],
        "diff_dias": venda_escolhida["diff_dias"]
    })

df_matches = pd.DataFrame(matches)

if df_matches.empty:
    print("Nenhum match encontrado.")
    raise SystemExit

print(f"Matches encontrados: {len(df_matches)}")


# =====================================================
# 5. ATUALIZAR FACT_CONTRATOS NO PROJETO JACAD
# =====================================================

atualizar_contratos_supabase(df_matches)

print("Atualização finalizada.")
print(f"Contratos atualizados: {len(df_matches)}")
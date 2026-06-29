from MetaAds import metodos_meta
from Supabase import metodos_supabase
from ETL import metodos_etl
import datetime, requests
import pandas as pd

contas_wpp_neurosaber = metodos_meta.api.getContasMeta("Neurosaber", "ContasWpp")
contas_wpp_sinahpse = metodos_meta.api.getContasMeta("Sinahpse", "ContasWpp")

def data_para_ms(data_str, final_do_dia=False):
    """
    Converte YYYY-MM-DD para Unix timestamp em segundos
    """
    dt = datetime.datetime.strptime(data_str, "%Y-%m-%d")

    if final_do_dia:
        dt = dt.replace(hour=23, minute=59, second=59)
    else:
        dt = dt.replace(hour=0, minute=0, second=0)
    
    return int(dt.timestamp())

def gerar_dias(data_inicio, data_fim):
    atual = datetime.datetime.strptime(data_inicio, "%Y-%m-%d").date()
    fim = datetime.datetime.strptime(data_fim, "%Y-%m-%d").date()

    while atual <= fim:
        yield atual.strftime("%Y-%m-%d")
        atual += datetime.timedelta(days=1)

def chunks(lista, tamanho=None):
    for i in range(0, len(lista), tamanho):
        yield lista[i:i + tamanho]

#dataI = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
#dataF = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

dataI = '2026-05-30'
dataF = '2026-05-31'

colunas_metricas = [
    "sent",
    "delivered",
    "read",
    "amount_spent",
    "click_total",
    "click_unique",
    "click_reply_button"
]

## Atualiza Dados para as contas da NeuroSaber

for nome, info in contas_wpp_neurosaber.items():
    waba_id = info["waba_id"]

    df_templates = metodos_meta.api.getWhatsAppTemplates(
        ambiente="neurosaber",
        waba_id=waba_id,
        campos=[
            "id", "name", "status", "category"]
    )

    if df_templates.empty:
        print(f"{nome}: nenhum template encontrado.")
        continue

    if "status" not in df_templates.columns:
        print(f"{nome}: coluna 'status' não encontrada.")
        continue

    df_templates = df_templates[(df_templates["status"] == "APPROVED")]

    template_ids = df_templates["id"].astype(str).tolist()

    if not template_ids:
        print(f"Nenhum template válido para {nome}.")
        continue

    resultados = []

    for data in gerar_dias(dataI, dataF):
        for lote_templates in chunks(template_ids, tamanho=10):

            df_dados = metodos_meta.api.getWhatsAppTemplateAnalytics(
                ambiente="neurosaber",
                waba_id=waba_id,
                template_ids=lote_templates,
                start=data,
                end=data
            )

            if df_dados.empty:
                continue

            df_dados = df_dados[
                df_dados[colunas_metricas]
                .fillna(0)
                .sum(axis=1)
                .gt(0)
            ]

            if df_dados.empty:
                continue

            df_dados["data"] = data
            df_dados["waba_id"] = waba_id

            rows = metodos_etl.Etl.normalizar_rows(
                df_dados.to_dict(orient="records")
            )

            metodos_supabase.api.upsert_data(
                banco="Meta_DB",
                tabela="fact_wpp_templates",
                dados=rows,
                chave="template_id,data,waba_id"
            )

            print(
                f"{nome} | {data} | {len(lote_templates)} templates consultados | "
                f"{len(rows)} linhas inseridas/atualizadas"
            )


## Atualiza Dados para as contas da Sinahpse

for nome, info in contas_wpp_sinahpse.items():
    waba_id = info["waba_id"]

    df_templates = metodos_meta.api.getWhatsAppTemplates(
        ambiente="sinahpse",
        waba_id=waba_id,
        campos=["id", "name", "status", "category"]
    )

    if df_templates.empty:
        print(f"{nome}: nenhum template encontrado.")
        continue

    if "status" not in df_templates.columns:
        print(f"{nome}: coluna 'status' não encontrada.")
        continue

    df_templates = df_templates[(df_templates["status"] == "APPROVED")]

    template_ids = df_templates["id"].astype(str).tolist()

    if not template_ids:
        print(f"Nenhum template válido para {nome}.")
        continue

    for data in gerar_dias(dataI, dataF):
        for lote_templates in chunks(template_ids, tamanho=10):
            try:
                df_dados = metodos_meta.api.getWhatsAppTemplateAnalytics(
                    ambiente="sinahpse",
                    waba_id=waba_id,
                    template_ids=lote_templates,
                    start=data,
                    end=data
                )

            except requests.exceptions.HTTPError as e:
                print(f"Erro no lote | WABA {waba_id} | Data {data}")
                print("Tentando template por template...")

                for template_id in lote_templates:
                    try:
                        df_dados = metodos_meta.api.getWhatsAppTemplateAnalytics(
                            ambiente="sinahpse",
                            waba_id=waba_id,
                            template_ids=[template_id],
                            start=data,
                            end=data
                        )

                        if df_dados.empty:
                            continue

                        df_dados = df_dados[
                            df_dados[colunas_metricas]
                            .fillna(0)
                            .sum(axis=1)
                            .gt(0)
                        ]

                        if df_dados.empty:
                            continue

                        df_dados["data"] = data
                        df_dados["waba_id"] = waba_id

                        rows = metodos_etl.Etl.normalizar_rows(
                            df_dados.to_dict(orient="records")
                        )

                        metodos_supabase.api.upsert_data(
                            banco="Meta_DB",
                            tabela="fact_wpp_templates",
                            dados=rows,
                            chave="template_id,data,waba_id"
                        )

                        print(
                            f"{nome} | {data} | {len(lote_templates)} templates consultados | "
                            f"{len(rows)} linhas inseridas/atualizadas"
                        )

                    except requests.exceptions.HTTPError:
                        print(f"Template ignorado com erro: {template_id}")
                        continue
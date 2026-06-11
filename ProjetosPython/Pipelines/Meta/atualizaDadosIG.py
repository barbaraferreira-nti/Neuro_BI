from MetaAds import metodos_meta
import pandas as pd
from Supabase import metodos_supabase
import datetime


ig_accounts_neurosaber = {
    "@faculdadeneurosaber": "17841447825761704",
    "@lubritesoficial": "17841401590092952",
    "@neuroescolaoficial": "17841473063465957"
}

ig_accounts_sinahpse = {
    "@entendendoautismo": "17841403967093719",
    "@neurosaberoficial": "17841403637747125"
}

def gerar_dias(data_inicio, data_fim):
    atual = datetime.datetime.strptime(data_inicio, "%Y-%m-%d").date()
    fim = datetime.datetime.strptime(data_fim, "%Y-%m-%d").date()

    while atual <= fim:
        yield atual.strftime("%Y-%m-%d")
        atual += datetime.timedelta(days=1)

def data_para_unix(data_str):
    data = datetime.datetime.strptime(data_str, "%Y-%m-%d")
    return int(data.timestamp())

def data_para_unix_dia_seguinte(data):
    return int(
        (
            datetime.datetime.strptime(
                data,
                "%Y-%m-%d"
            )
            + datetime.timedelta(days=1)
        ).timestamp()
    )

def proximo_dia(data):
    data_obj = datetime.datetime.strptime(data, "%Y-%m-%d")
    return data_obj + datetime.timedelta(days=1)

dataI = "2026-05-01"
dataF = "2026-05-10"

## Atualizar dados contas IG
resultados = []
for nome, ig_account_id in ig_accounts_neurosaber.items():
    for data in gerar_dias(dataI, dataF):
        try:
            dados_meta = metodos_meta.api.getIGAccountInsights(
                ambiente="neurosaber",
                ig_account=ig_account_id,
                metric_type="total_value",
                dataI=data_para_unix(data),
                dataF=data_para_unix_dia_seguinte(data),
                metricas=["accounts_engaged","comments","likes","reach","replies", "reposts", "saves", "shares", "total_interactions", "views"]
            )

            if dados_meta is None or dados_meta.empty:
                print(f"Conta {nome} sem dados.")
                continue
            

            # rows = dados_meta.to_dict("records")

            # upsert = metodos_supabase.api.upsert_data(
            #     banco="Meta_DB",
            #     tabela="dm_ig_accounts",
            #     dados=rows,
            #     chave="id_account"
            # )
            row = metodos_meta.api.tratar_insights_conta(dados_meta, ig_account_id, data)
            resultados.append(row)

            #print(f"Conta {nome} atualizada com sucesso.")
        except Exception as e:
            print(f"Erro ao atualizar os dados da conta {nome} na tabela 'dm_ig_accounts'.")
            print(str(e))
            continue

df = pd.concat(resultados, ignore_index=True)
df.to_csv(r"C:\Users\Barbara\Downloads\insights_contas.csv", index= False)
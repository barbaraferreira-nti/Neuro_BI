from MetaAds import metodos_meta
from Supabase import metodos_supabase
import datetime

contas_fb_neurosaber = metodos_meta.api.getContasMeta("Neurosaber", "ContasAnuncio")
contas_fb_sinahpse = metodos_meta.api.getContasMeta("Sinahpse", "ContasAnuncio")

dataI = (datetime.date.today() - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
dataF = datetime.date.today().strftime("%Y-%m-%d")

def gerar_dias(data_inicio, data_fim):
    atual = datetime.datetime.strptime(data_inicio, "%Y-%m-%d").date()
    fim = datetime.datetime.strptime(data_fim, "%Y-%m-%d").date()

    while atual <= fim:
        yield atual.strftime("%Y-%m-%d")
        atual += datetime.timedelta(days=1)

# Atualizando os dados da 'NeuroSaber'
try:
    for nome, id in contas_fb_neurosaber.items():
        for data in gerar_dias(dataI, dataF):
            try:
                dados_meta = metodos_meta.api.getDadosConta(ambiente="neurosaber",
                                                            periodo=[data, data], 
                                                            campos=["account_id", "campaign_id", "ad_id", "impressions", "reach", "clicks", "spend", "actions", 
                                                                    "action_values", "video_play_actions", "video_avg_time_watched_actions", 
                                                                    "video_p25_watched_actions","video_p50_watched_actions", "video_p75_watched_actions", "video_p100_watched_actions"], 
                                                            nivel="ad", 
                                                            contaAnuncio=id
                                                            )
                if not dados_meta:
                    print(f"Conta {nome} sem dados em {data}.")
                    continue
                
                dados_supabase = metodos_meta.api.transformarDadosSupabase(dados_meta)

                metodos_supabase.api.upsert_data(banco="Meta_DB", tabela="fact_fb_account", dados=dados_supabase, chave="account_id, campaign_id, ad_id, date_start")
                print(f"{nome} | {data} atualizado.")
            except Exception as e:
                print(f"Erro na conta '{nome}' no dia {data}.")
                print(str(e))
except Exception as e:
    print(f"Erro ao atualizar os dados da conta '{nome}' na tabela 'fact_fb_account'.")
    print(str(e))


# # Atualizando os dados da 'Sinahpse'
try:
    for nome, id in contas_fb_sinahpse.items():
        for data in gerar_dias(dataI, dataF):
            try:
                dados_meta = metodos_meta.api.getDadosConta(ambiente="sinahpse",
                                                            periodo=[data, data], 
                                                            campos=["account_id", "campaign_id", "ad_id", "impressions", "reach", "clicks", "spend", "actions", 
                                                                    "action_values", "video_play_actions", "video_avg_time_watched_actions", 
                                                                    "video_p25_watched_actions","video_p50_watched_actions", "video_p75_watched_actions", "video_p100_watched_actions"], 
                                                            nivel="ad", 
                                                            contaAnuncio=id
                                                            )
                if not dados_meta:
                    print(f"Conta {nome} sem dados em {data}.")
                    continue
                

                dados_supabase = metodos_meta.api.transformarDadosSupabase(dados_meta)

                metodos_supabase.api.upsert_data(banco="Meta_DB", tabela="fact_fb_account", dados=dados_supabase, chave="account_id, campaign_id, ad_id, date_start")
                print(f"{nome} | {data} atualizado.")
            except Exception as e:
                print(f"Erro na conta '{nome}' no dia {data}.")
                print(str(e))  
except Exception as e:
    print(f"Erro ao atualizar os dados da conta '{nome}' na tabela 'fact_fb_account'.")
    print(str(e))


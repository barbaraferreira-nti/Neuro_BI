from ProjetosPython.MetaAds import metodos_meta
from ProjetosPython.Supabase import metodos_supabase
import datetime


# Arquio que irá atualizar as três dimensões (dim_contas, dim_campanhas, dim_anuncios) no Supabase

dic_contas_meta = {
    "Venda Direta": "1669866780628047",
    "Neuromeeting": "839591808350412",
    "Lu Brites": "877278007142846",
    "Pos-Graduacao": "410083191312739",
    "Numeracia": "260935725436002",
    "PENNSA": "208577594523171",
    "NeuroSaber Geral": "346919473719117",
    "Corredor Polonês/Conteúdos": "2772430283027189",
    "Ecommerce": "1884841205352699",
    "CA - Neurosaber (Terceiros)": "623426096445795"
}

dataI = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
dataF = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

supabase = metodos_supabase.api.auth("Guru_DB")

## Atualizando tabela dim_campanhas
allCampaigns = []

for nome, id in dic_contas_meta.items():
        dados_campanhas = metodos_meta.api.getDadosConta(app="Time_NTI", 
                                                periodo=["2026-03-01","2026-03-27"], 
                                                campos=["account_id", "account_name","campaign_id", "campaign_name"], 
                                                nivel="campaign", 
                                                contaAnuncio=id
                                                )
        if not dados_campanhas:
            print(f"Conta {nome} sem dados.")
            continue
        
        allCampaigns.extend(dados_campanhas)

campanhas_unicas = {}
for camp in allCampaigns:
    campaign_id = camp.get("campaign_id")

    if campaign_id:
        campanhas_unicas[campaign_id] = {
            "campaign_id": campaign_id,
            "campaign_name": camp.get("campaign_name"),
            "account_id":camp.get("account_id"),
            "account_name": camp.get("account_name")
        }

dados_campanhas_finais = list(campanhas_unicas.values())


response_campaign = (
    supabase
    .table("dim_meta_campanhas")
    .upsert(dados_campanhas_finais, on_conflict="campaign_id")
    .execute()
)

print(f"{len(dados_campanhas_finais)} campanhas processadas com upsert.")


# Atualizando a tabela dim_anuncios

allAds = []

for nome, id in dic_contas_meta.items():
        dados_meta = metodos_meta.api.getDadosConta(app="Time_NTI", 
                                               periodo=["2026-03-24","2026-03-26"], 
                                               campos=["ad_id", "ad_name"], 
                                               nivel="ad", 
                                               contaAnuncio=id
                                               )
        if not dados_meta:
            print(f"Conta {nome} sem dados.")
            continue
        
        allAds.extend(dados_meta)

ads_unicos = {}
for ad in allAds:
    ad_id = ad.get("ad_id")

    if ad_id:
        ads_unicos[ad_id] = {
            "ad_id": ad_id,
            "ad_name": ad.get("ad_name")
        }

dados_ads_finais = list(ads_unicos.values())

response_ad = (
    supabase
    .table("dim_meta_anuncios")
    .upsert(dados_ads_finais, on_conflict="ad_id")
    .execute()
)

print(f"{len(dados_ads_finais)} anúncios processados com upsert.")




        

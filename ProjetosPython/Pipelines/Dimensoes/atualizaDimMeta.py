from MetaAds import metodos_meta
from Supabase import metodos_supabase
import datetime


# Arquivo que irá atualizar as três dimensões (dim_contas, dim_campanhas, dim_anuncios) no Supabase
tabela_dim_campanhas = "dim_meta_campanhas" 
tabela_dim_anuncios = "dim_meta_anuncios"

banco = "Guru_DB"

dic_contas_meta_sinahpse = {
    "Alfabetização": "2758913401060356",
    "TDAH": "1022966454801060",
    "PROLEIA": "618694088749786",
    "Perpétuos": "411919152706872",
    "CA - 01 - Teste Perpétuos NS": "2357097777954786",
    "Matemagica": "531329858353944",
    "TDAH na Escola": "2354015068250801",
    "LANÇAMENTOS": "386267435573477"
}

dic_contas_meta_neurosaber= {
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

supabase = metodos_supabase.api.auth(banco=banco)

# 001.Atualizando os dados da 'NeuroSaber'
    
# Atualizando tabela dim_campanhas
allCampaigns = []

print("Iniciando atualização da dimensão 'campanhas' da Meta das contas da Neurosaber...")

for nome, id in dic_contas_meta_neurosaber.items():
        dados_campanhas = metodos_meta.api.getDadosConta(app="Time_NTI", ambiente="neurosaber",
                                                periodo=[dataI, dataF], 
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

if dados_campanhas_finais:
    response_campaign = (
        supabase
        .table(tabela_dim_campanhas)
        .upsert(dados_campanhas_finais, on_conflict="campaign_id")
        .execute()
    )

    print(f"{len(dados_campanhas_finais)} campanhas da Neurosaber processadas com upsert.")
else:
    print("Nenhum anúncio válido para upsert.")

print("Campanhas da Neurosaber atualizadas com sucesso!")


# Atualizando a tabela dim_anuncios
print("Iniciando atualização da dimensão 'anúncios' da Meta das contas da Neurosaber...")

allAds = []

for nome, id in dic_contas_meta_neurosaber.items():
        dados_meta = metodos_meta.api.getDadosConta(app="Time_NTI", ambiente="neurosaber",
                                               periodo=[dataI, dataF], 
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
print(f"Total final de anúncios válidos: {len(dados_ads_finais)}")

if dados_ads_finais:
    response_ad = (
        supabase
        .table(tabela_dim_anuncios)
        .upsert(dados_ads_finais, on_conflict="ad_id")
        .execute()
    )

    print(f"{len(dados_ads_finais)} anúncios da Neursosaber processados com upsert.")
else:
    print("Nenhum anúncio válido para upsert.")

print("Anúncios da Neurosaber atualizados com sucesso!")


# 002.Atualizando os dados da 'Sinahpse'

# Atualizando tabela dim_campanhas
print("Iniciando atualização da dimensão 'campanhas' da Meta das contas da Sinahpse...")
allCampaigns = []

for nome, id in dic_contas_meta_sinahpse.items():
        dados_campanhas = metodos_meta.api.getDadosConta(app="Time_NTI", ambiente="sinahpse",
                                                periodo=[dataI, dataF], 
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

if dados_campanhas_finais:
    response_campaign = (
        supabase
        .table(tabela_dim_campanhas)
        .upsert(dados_campanhas_finais, on_conflict="campaign_id")
        .execute()
    )

    print(f"{len(dados_campanhas_finais)} campanhas da Sinahpse processadas com upsert.")
else:
    print("Nenhum anúncio válido para upsert.")

print("Campanhas da Sinahpse atualizadas com sucesso!")

# Atualizando a tabela dim_anuncios
print("Iniciando atualização da dimensão 'anúncios' da Meta das contas da Sinahpse...")
allAds = []

for nome, id in dic_contas_meta_sinahpse.items():
        dados_meta = metodos_meta.api.getDadosConta(app="Time_NTI", ambiente="sinahpse",
                                               periodo=[dataI, dataF], 
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

if dados_ads_finais:
    response_ad = (
        supabase
        .table(tabela_dim_anuncios)
        .upsert(dados_ads_finais, on_conflict="ad_id")
        .execute()
    )

    print(f"{len(dados_ads_finais)} anúncios da Sinahpse processados com upsert.")
else:
    print("Nenhum anúncio válido para upsert.")
    
print("Anúncios da Sinahpse atualizados com sucesso!")
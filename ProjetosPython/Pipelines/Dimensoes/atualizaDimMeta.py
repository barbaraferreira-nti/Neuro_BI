from MetaAds import metodos_meta
from Supabase import metodos_supabase
import datetime

# Arquivo que irá atualizar as três dimensões (dim_contas, dim_campanhas, dim_anuncios) no Supabase
tabela_dim_campanhas = "dm_fb_campaigns" 
tabela_dim_anuncios = "dm_fb_ads"
banco = "Meta_DB"
contas_fb_neurosaber = metodos_meta.api.getContasMeta("Neurosaber", "ContasAnuncio")
contas_fb_sinahpse = metodos_meta.api.getContasMeta("Sinahpse", "ContasAnuncio")
contas_ig_neurosaber = metodos_meta.api.getContasMeta("Neurosaber", "ContasInstagram")
contas_ig_sinahpse = metodos_meta.api.getContasMeta("Sinahpse", "ContasInstagram")

dataI = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
dataF = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

supabase = metodos_supabase.api.auth(banco=banco)

# 001.Atualizando os dados da 'NeuroSaber'
# Atualizando tabela dim_campanhas
allCampaigns = []

print("Iniciando atualização da dimensão 'campanhas' da Meta das contas da Neurosaber...")

for nome, id in contas_fb_neurosaber.items():
        dados_campanhas = metodos_meta.api.getDadosConta(ambiente="neurosaber",
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

for nome, id in contas_fb_neurosaber.items():
        dados_meta = metodos_meta.api.getDadosConta(ambiente="neurosaber",
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

for nome, id in contas_fb_sinahpse.items():
        dados_campanhas = metodos_meta.api.getDadosConta(ambiente="sinahpse",
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

for nome, id in contas_fb_sinahpse.items():
        dados_meta = metodos_meta.api.getDadosConta(ambiente="sinahpse",
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

## Atualiza dim IG accounts
for nome, ig_account_id in contas_ig_neurosaber.items():
    try:
        dados_meta = metodos_meta.api.getIGAccounts(
            ambiente="neurosaber",
            ig_account=ig_account_id
        )

        if dados_meta is None or dados_meta.empty:
            print(f"Conta {nome} sem dados.")
            continue
        

        rows = dados_meta.to_dict("records")

        upsert = metodos_supabase.api.upsert_data(
            banco="Meta_DB",
            tabela="dm_ig_accounts",
            dados=rows,
            chave="id_account"
        )

        print(f"Conta {nome} atualizada com sucesso.")

    except Exception as e:
        print(f"Erro ao atualizar os dados da conta {nome} na tabela 'dm_ig_accounts'.")
        print(str(e))
        continue

for nome, ig_account_id in contas_ig_sinahpse.items():
    try:
        dados_meta = metodos_meta.api.getIGAccounts(
            ambiente="sinahpse",
            ig_account=ig_account_id
        )

        if dados_meta is None or dados_meta.empty:
            print(f"Conta {nome} sem dados.")
            continue
        

        rows = dados_meta.to_dict("records")

        upsert = metodos_supabase.api.upsert_data(
            banco="Meta_DB",
            tabela="dm_ig_accounts",
            dados=rows,
            chave="id_account"
        )

        print(f"Conta {nome} atualizada com sucesso.")

    except Exception as e:
        print(f"Erro ao atualizar os dados da conta {nome} na tabela 'dm_ig_accounts'.")
        print(str(e))
        continue


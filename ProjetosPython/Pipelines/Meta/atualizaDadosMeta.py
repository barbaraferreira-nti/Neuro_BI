from MetaAds import metodos_meta
from Supabase import metodos_supabase
import datetime

dic_contas_meta_sinahpse = {
    "Alfabetização": "2758913401060356",
    "TDAH": "1022966454801060",
    "PROLEIA": "618694088749786",
    "CA - 01 - Teste Perpétuos NS": "2357097777954786",
    "Matemagica": "531329858353944",
    "TDAH na Escola": "2354015068250801",
    "LANÇAMENTOS": "386267435573477",
    "Perpetuos": "411919152706872"
}

dic_contas_meta_neurosaber = {
    "Venda Direta": "1669866780628047",
    "Neuromeeting": "839591808350412",
    "Lu Brites": "877278007142846",
    "Pos-Graduacao": "410083191312739",
    "Numeracia": "260935725436002",
    "PENNSA": "208577594523171",
    "NeuroSaber Geral": "346919473719117",
    "Corredor Polonês/Conteúdos": "2772430283027189",
    "Ecommerce": "1884841205352699",
    "CA - Neurosaber (Terceiros)": "623426096445795",
    
}


dataI = (datetime.date.today() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
dataF = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


# Atualizando os dados da 'NeuroSaber'
try:
    for nome, id in dic_contas_meta_neurosaber.items():

        dados_meta = metodos_meta.api.getDadosConta(ambiente="neurosaber",
                                                    periodo=[dataI, dataF], 
                                                    campos=["account_id", "account_name", "campaign_id", "campaign_name", "ad_id", "ad_name", "impressions", "reach", "clicks", "spend",
                                                            "frequency", "actions", "action_values", "video_play_actions"], 
                                                    nivel="ad", 
                                                    contaAnuncio=id
                                                    )
        if not dados_meta:
            print(f"Conta {nome} sem dados.")
            continue
        
        dados_supabase = metodos_meta.api.transformarDadosSupabase(dados_meta)


        insert = metodos_supabase.api.insert_data(banco="Guru_DB", tabela="fact_meta", dados=dados_supabase)
except Exception as e:
    print(f"Erro ao atualizar os dados da conta 'Neurosaber' na tabela 'fact_meta'.")
    print(str(e))


# # Atualizando os dados da 'Sinahpse'
try:
    for nome, id in dic_contas_meta_sinahpse.items():

        dados_meta = metodos_meta.api.getDadosConta(ambiente="sinahpse",
                                                    periodo=[dataI, dataF], 
                                                    campos=["account_id", "account_name", "campaign_id", "campaign_name", "ad_id", "ad_name", "impressions", "reach", "clicks", "spend",
                                                            "frequency", "actions", "action_values", "video_play_actions"], 
                                                    nivel="ad", 
                                                    contaAnuncio=id
                                                    )
        if not dados_meta:
            print(f"Conta {nome} sem dados.")
            continue
        
        dados_supabase = metodos_meta.api.transformarDadosSupabase(dados_meta)


        insert = metodos_supabase.api.insert_data(banco="Guru_DB", tabela="fact_meta", dados=dados_supabase)
except Exception as e:
    print(f"Erro ao atualizar os dados da conta 'Sinahpse' na tabela 'fact_meta'.")
    print(str(e))


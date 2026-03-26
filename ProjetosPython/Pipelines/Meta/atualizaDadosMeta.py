from MetaAds import metodos_meta
from Supabase import metodos_supabase
import datetime

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


for nome, id in dic_contas_meta.items():

    dados_meta = metodos_meta.api.getDadosConta(app="Time_NTI", 
                                                periodo=["2026-03-22", "2026-03-23"], 
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

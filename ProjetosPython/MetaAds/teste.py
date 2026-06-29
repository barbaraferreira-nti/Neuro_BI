from MetaAds import metodos_meta

dataI = '2026-06-11'
dataF = '2026-06-13'

dados_meta = metodos_meta.api.getDadosConta(ambiente="neurosaber",
                                                    periodo=[dataI, dataF], 
                                                    campos=["account_id", "campaign_id", "ad_id", "impressions", "reach", "clicks", "spend", "actions", 
                                                            "action_values", "video_play_actions", "video_avg_time_watched_actions", 
                                                            "video_p25_watched_actions","video_p50_watched_actions", "video_p75_watched_actions", "video_p100_watched_actions"], 
                                                    nivel="ad", 
                                                    contaAnuncio='260935725436002'
                                                    )


print(dados_meta)
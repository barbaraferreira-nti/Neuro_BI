import pandas as pd
import requests
import os, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
from config import Config
import datetime

class api:
    @staticmethod
    def auth(ambiente):
        if ambiente=="neurosaber":
            token = Config.Meta.TOKEN_NEUROSABER
        else:
            token = Config.Meta.TOKEN_SINAHPSE
        return token
    
    @staticmethod
    def make_session():
        s = requests.Session()

        retry = Retry(
            total=0,  # a gente controla o retry no nosso loop, pra ter mais controle
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        s.mount("https://", adapter)
        s.mount("http://", adapter)
        return s
    
    @staticmethod
    def RetryRequest(session, url, headers=None, params=None, retries=11, timeout=(10, 120)):
        """
        Retry para:
        - erros de rede (ConnectTimeout, ReadTimeout, ConnectionError)
        - HTTP 5xx
        - HTTP 429 (rate limit), respeitando Retry-After se existir
        """
        last_exc = None

        for attempt in range(1, retries + 1):
            try:
                r = session.get(url, headers=headers, params=params, timeout=timeout)

                # 429: tente respeitar Retry-After
                if r.status_code == 429:
                    ra = r.headers.get("Retry-After")
                    if ra:
                        sleep_time = min(int(ra), 120)
                    else:
                        sleep_time = min(2 ** attempt, 60) + random.uniform(0, 0.5)
                    time.sleep(sleep_time)
                    continue

                # 5xx: instabilidade do servidor
                if r.status_code in (500, 502, 503, 504):
                    sleep_time = min(2 ** attempt, 60) + random.uniform(0, 0.5)
                    time.sleep(sleep_time)
                    continue

                # 400/401: normalmente é erro definitivo (parâmetros/token)
                if r.status_code in (400, 401):
                    # Se quiser: tentar detectar "is_transient" no payload do erro
                    try:
                        err = r.json().get("error", {})
                        if err.get("is_transient") is True:
                            sleep_time = min(2 ** attempt, 60) + random.uniform(0, 0.5)
                            time.sleep(sleep_time)
                            continue
                    except Exception:
                        pass

                    r.raise_for_status()
                
                if r.status_code >= 400:
                    print("STATUS:", r.status_code)
                    print("RESPOSTA meta:", r.text)
                    
                r.raise_for_status()
                return r.json()

            except (requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError) as e:
                last_exc = e
                sleep_time = min(2 ** attempt, 60) + random.uniform(0, 0.5)
                time.sleep(sleep_time)
                continue

        raise Exception(f"Erro persistente na URL: {url}. Última exceção: {last_exc}")

    @staticmethod
    def getDadosConta(ambiente=None, periodo=[], campos=[], nivel=None, contaAnuncio=None):
        periodo = periodo or []
        camposList = campos or []
        if len(periodo) != 2:
            raise ValueError("período deve ser [dataInicio, dataFim] no formato YYYY-MM-DD")
        
        baseEndPoint = Config.Meta.URL_FACEBOOK
        tokens = {
            "neurosaber": Config.Meta.TOKEN_NEUROSABER,
            "sinahpse": Config.Meta.TOKEN_SINAHPSE
        }

        token = tokens.get(ambiente)

        headers = {"Authorization": f"Bearer {token}"}
        url = f"{baseEndPoint}act_{contaAnuncio}/insights"
        dataInicio, dataFim = periodo[0], periodo[1]

        params = {
            "fields": ",".join(camposList),
            "time_range": json.dumps({"since": dataInicio, "until": dataFim}), 
            "time_increment": 1,
            "limit": 100,
            "level": nivel
        }

        resultados = []
        with requests.Session() as session:

            next_url = url
            next_params = params

            while next_url:

                payload = api.RetryRequest(
                    session=session,
                    url=next_url,
                    headers=headers,
                    params=next_params
                )

                resultados.extend(payload.get("data", []))

                # Paginação
                next_url = payload.get("paging", {}).get("next")
                next_params = None  

        return resultados
    
    @staticmethod
    def tratarDados(dados, colunas=None):
        df = pd.DataFrame(dados)

        if colunas:
            df = df[colunas]  # seleciona apenas as colunas desejadas

        return df

    @staticmethod
    def transformarDadosSupabase(rows):
        ALLOWED = [
            "account_id",
            "campaign_id", 
            "ad_id",
            "impressions", "reach", "clicks", "spend", "frequency","date_start"
        ]

        ACTIONS_MAP = {
            "link_click": "link_click",
            "page_engagement": "page_engagement",
            "landing_page_view": "landing_page_view",
            "post_engagement": "post_engagement",
            "comment": "post_comment",
            "post": "post_share",
            "post_reaction": "post_reaction",
            "onsite_conversion.post_save": "post_save",
            "offsite_conversion.fb_pixel_add_to_cart": "add_to_cart",
            "offsite_conversion.fb_pixel_purchase": "results",
            "offsite_conversion.fb_pixel_initiate_checkout": "initiate_checkout",
            "video_view": "video_view"

        }

        ACTION_VALUES_MAP = {
            "offsite_conversion.fb_pixel_purchase": "results_value",
            "offsite_conversion.fb_pixel_add_to_cart": "add_to_cart_value"
        }

        VIDEO_MAP = {
            "video_play_actions": "video_p_total",
            "video_avg_time_watched_actions": "video_avg_time_watched",
            "video_p25_watched_actions": "video_p25_total",
            "video_p50_watched_actions": "video_p50_total",
            "video_p75_watched_actions": "video_p75_total",
            "video_p100_watched_actions": "video_p100_total"
        }

        def to_int(x):
            return int(float(x)) if x not in (None, "") else 0

        def to_float(x):
            return float(x) if x not in (None, "") else 0.0
        
        def extract_value(lista):
            if not lista:
                return 0

            for item in lista:
                if item.get("action_type") == "video_view":
                    return to_int(item.get("value"))

            return 0
        
        out = []

        for r in rows:
            row = {k: r.get(k) for k in ALLOWED}

            row["impressions"] = to_int(row.get("impressions"))
            row["reach"] = to_int(row.get("reach"))
            row["clicks"] = to_int(row.get("clicks"))
            row["spend"] = to_float(row.get("spend"))
            row["frequency"] = to_float(row.get("frequency"))

            # Inicializa colunas com zero
            for col in ACTIONS_MAP.values():
                row[col] = 0

            for col in ACTION_VALUES_MAP.values():
                row[col] = 0.0
            
            for col in VIDEO_MAP.values():
                row[col] = 0

            # Actions
            for action in r.get("actions", []):
                action_type = action.get("action_type")
                if action_type in ACTIONS_MAP:
                    row[ACTIONS_MAP[action_type]] = to_int(action.get("value"))

            # Action values
            for action_value in r.get("action_values", []):
                action_type = action_value.get("action_type")
                if action_type in ACTION_VALUES_MAP:
                    row[ACTION_VALUES_MAP[action_type]] = to_float(action_value.get("value"))

            for origem, destino in VIDEO_MAP.items():
                row[destino] = extract_value(r.get(origem))
            
            out.append(row)

        return out
    
    @staticmethod
    def enableTemplateInsights(ambiente, waba_id=None):
        """
        Habilita analytics de templates no WhatsApp Business Account
        ATENÇÃO: irreversível
        """


        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)

        endPoint = f"{baseEndPoint}/{waba_id}"

        params = {
            "is_enabled_for_insights": True,
            "access_token": token
        }

        response = requests.post(endPoint, data=params)
        dados = response.json()

        return dados
    
    @staticmethod
    def getWhatsAppTemplateAnalytics(
        ambiente=None,
        waba_id=None,
        template_ids=[],
        start=None,
        end=None,
        granularity="daily",
        metric_types=["sent", "delivered", "read", "clicked", "cost"]
    ):

        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)

        metrics = ",".join(metric_types)
        templates = "[" + ",".join(map(str, template_ids)) + "]"

        endPoint = f"{baseEndPoint}{waba_id}/template_analytics"

        params = {
            "start": start,
            "end": end,
            "granularity": granularity,
            "metric_types": metrics,
            "template_ids": templates,
            "use_waba_timezone": "true",
            "access_token": token
        }

        resultados = []

        while endPoint:
            response = requests.get(endPoint, params=params)
            response.raise_for_status()
            dados = response.json()

            if "error" in dados:
                raise Exception(dados["error"])
            

            resultados.extend(dados.get("data", []))
            endPoint = dados.get("paging", {}).get("next")
            params = {}

        # Normalização
        registros = []

        for bloco in resultados:
            for ponto in bloco.get("data_points", []):
                registro = {
                    "template_id": ponto.get("template_id"),
                    "start": f"{start}T00:00:00-03:00",
                    "end": f"{end}T23:59:59-03:00",
                    "sent": ponto.get("sent"),
                    "delivered": ponto.get("delivered"),
                    "read": ponto.get("read"),
                    "amount_spent": 0,
                    "click_total": 0,
                    "click_unique": 0,
                    "click_reply_button": 0
                }

                # Cliques
                for c in ponto.get("clicked", []):
                    tipo = c.get("type")
                    count = c.get("count", 0)

                    if tipo == "quick_reply_button":
                        registro["click_reply_button"] += count

                    elif tipo == "unique_url_button":
                        registro["click_unique"] += count

                    elif tipo == "url_button":
                        registro["click_total"] += count
                
                # Custo
                for c in ponto.get("cost", []):
                    if c.get("type") == "amount_spent":
                        registro["amount_spent"] = c.get("value", 0)
                        break

                registros.append(registro)
        df = pd.DataFrame(registros)

        if not df.empty:
            df["data"] = pd.to_datetime(df["start"]).dt.date

            df = (
                df.groupby(["template_id", "data"], as_index=False)
                .agg({
                    "start": "min",
                    "end": "max",
                    "sent": "max",
                    "delivered": "max",
                    "read": "max",
                    "amount_spent": "max",
                    "click_total": "max",
                    "click_unique": "max",
                    "click_reply_button": "max"
                })
            )

        return df
    
    @staticmethod
    def getWhatsAppTemplatesCost(
        ambiente=None,
        waba_id=None,
        start=None,
        end=None,
        phone_numbers=[],
        dimensions=["phone","pricing_category", "pricing_type"],
        granularity="daily",
        metric_types=["cost", "volume"]):

        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)

        endPoint = f"{baseEndPoint}{waba_id}/pricing_analytics"
        metrics = ",".join(metric_types)
        numbers = "[" + ",".join(map(str, phone_numbers)) + "]"
        dimensions = ",".join(dimensions)
        
        params = {
            "start": start,
            "end": end,
            "granularity": granularity,
            "dimensions": dimensions,
            "metric_types": metrics,
            "phone_numbers": numbers,
            "waba_id": waba_id,
            "access_token": token
        }

        resultados = []
        while endPoint:
            response = requests.get(endPoint, params=params)
            response.raise_for_status()
            dados = response.json()

            if "error" in dados:
                raise Exception(dados["error"])
            
            
            resultados.extend(dados.get("data", []))
            endPoint = dados.get("paging", {}).get("next")
            params = {}

            # Normalização
            registros = []

            for bloco in resultados:
                for ponto in bloco.get("data_points", []):
                    base = {
                        "template_id": ponto.get("template_id"),
                        "start": pd.to_datetime(ponto.get("start"), unit="s"),
                        "end": pd.to_datetime(ponto.get("end"), unit="s"),
                        "phone": ponto.get("phone"),
                        "pricing_type": ponto.get("pricing_type"),
                        "pricing_category": ponto.get("pricing_category"),
                        "volume": ponto.get("volume"),
                        "cost": ponto.get("cost")
    
                    } 
                    registros.append(base)
            
            df = pd.DataFrame(registros)
            df["utm_source"] = "WhatsApp"

            return df
    
    @staticmethod
    def getWhatsAppTemplates(ambiente=None, waba_id=None, campos=[]):

        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)

        endPoint = f"{baseEndPoint}{waba_id}/message_templates"

        params = {
            "fields":  ",".join(campos),
            "access_token": token
        }

        resultados = []

        while endPoint:
            response = requests.get(endPoint, params=params)
            response.raise_for_status()
            dados = response.json()

            if "error" in dados:
                raise Exception(dados["error"])
            
            resultados.extend(dados.get("data", []))
            endPoint = dados.get("paging", {}).get("next")
            params = {}

        df = pd.json_normalize(resultados)

        return df
    
    @staticmethod
    def getIGAccounts(ambiente=None, ig_account=None):
        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)
        endPoint = f"{baseEndPoint}{ig_account}"
        
        metricas = ["id","name","username","profile_picture_url"]

        params = {
            "fields": ",".join(metricas),
            "access_token": token
        }

        response = requests.get(endPoint, params=params)
        response.raise_for_status()
        dados = response.json()

        if "error" in dados:
            raise Exception(dados["error"])
        
        df = pd.json_normalize(dados)

        df = df.rename(columns={
            "id": "id_account",
            "name": "account_name",
            "username": "account_ig_name",
            "profile_picture_url": "profile_picture_url"
            })

        return df

    @staticmethod
    def getIGAccountMidias(ambiente=None, ig_account=None, dataI=None, dataF=None):
        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)
        endPoint = f"{baseEndPoint}{ig_account}/media"
        
        metricas = ["id","media_type", "media_url","permalink","thumbnail_url", "timestamp"]

        params = {
            "fields": ",".join(metricas),
            "since": dataI,
            "until": dataF,
            "access_token": token
        }
        resultados = []

        while endPoint:
            response = requests.get(endPoint, params=params, timeout=30)
            response.raise_for_status()

            dados = response.json()

            if "error" in dados:
                raise Exception(dados["error"])

            resultados.extend(dados.get("data", []))

            endPoint = dados.get("paging", {}).get("next")
            params = {}

        df = pd.json_normalize(resultados)

        df["id_account"] = ig_account

        return df

    @staticmethod
    def getIGAccountInsights(ambiente=None, ig_account=None, dataI=None, dataF=None,breakdown=None, metric_type=None,metricas=[]):
        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)

        endPoint = f"{baseEndPoint}{ig_account}/insights"

        params = {
            "metric": ",".join(metricas),
            "period": "day",
            "since": dataI,
            "until": dataF,
            "access_token": token
        }

        if metric_type:
            params["metric_type"] = metric_type

        if breakdown:
            params["breakdown"] = breakdown

        response = requests.get(endPoint, params=params, timeout=30)
        response.raise_for_status()

        dados = response.json()

        if "error" in dados:
            raise Exception(dados["error"])

        df = pd.json_normalize(dados.get("data", []))

        return df
    
    @staticmethod
    def getIGMidiaInsights(ambiente=None, midia_id=None, dataI=None, dataF=None, metricas=[]):
        baseEndPoint = Config.Meta.URL_FACEBOOK
        token = api.auth(ambiente=ambiente)

        endPoint = f"{baseEndPoint}{midia_id}/insights"

        params = {
            "metric": ",".join(metricas),
            "period": "day",
            "since": dataI,
            "unitl": dataF,
            "access_token": token
        }
        resultados = []

        while endPoint:
            response = requests.get(endPoint, params=params)
            response.raise_for_status()

            dados = response.json()

            if "error" in dados:
                raise Exception(dados["error"])
            
            resultados.extend(dados.get("data", []))
            endPoint = dados.get("paging", {}).get("next")
            params = {}

        df = pd.json_normalize(resultados)
        return df
    
    @staticmethod
    def tratar_insights_conta(dados, id_account=None, data=None):
        row = {
            "id_account": id_account,
            "data": data
        }

        for _, linha in dados.iterrows():
            metrica = linha["name"]
            valor = linha.get("total_value.value")

            row[metrica] = valor

        return pd.DataFrame([row])


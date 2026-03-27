## Arquivo que cria os métodos para buscar dados da API

import pandas as pd
import requests
import os, json, time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random

class api:
    @staticmethod
    def auth(app):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configMeta.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config.get(app, {}).get("token")
    
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
    def getDadosConta(app=None, periodo=[], campos=[], nivel=None, contaAnuncio=None):
        periodo = periodo or []
        camposList = campos or []
        if len(periodo) != 2:
            raise ValueError("período deve ser [dataInicio, dataFim] no formato YYYY-MM-DD")
        
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configMeta.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        baseEndPoint = config[app]["apiEndPoints"]["facebook"]
        token = api.auth(app=app)

        headers = {"Authorization": f"Bearer {token}"}
        url = f"{baseEndPoint}act_{contaAnuncio}/insights"
        dataInicio, dataFim = periodo[0], periodo[1]

        params = {
            "fields": ",".join(camposList),
            "time_range": json.dumps({"since": dataInicio, "until": dataFim}), 
            "time_increment": 1,
            "level": nivel,
            "action_breakdowns": "action_video_type"
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
                "account_id", "account_name",
                "campaign_id", "campaign_name",
                "ad_id", "ad_name",
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

            }

            ACTION_VALUES_MAP = {
                "offsite_conversion.fb_pixel_add_payment_info": "results_value",
                "offsite_conversion.fb_pixel_add_to_cart": "add_to_cart_value",
            }

            VIDEO_ACTION_MAP = {
                "total": "video_p3s"
            }

            VIDEO_PLAY_MAP = {
                "total": "video_p_total"
            }

            VIDEO_METRIC_COLS = [
                "video_p25_total","video_p50_total","video_p75_total","video_p100_total"
            ]

            def to_int(x):
                return int(float(x)) if x not in (None, "") else 0

            def to_float(x):
                return float(x) if x not in (None, "") else 0.0

            def extract_video_metric(items):
                out = {
                    "total": 0
                }

                for item in items or []:
                    video_type = item.get("action_video_type")
                    value = item.get("value")

                    if video_type in out:
                        out[video_type] = to_int(value)

                return out
            

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
                
                for col in VIDEO_ACTION_MAP.values():
                    row[col] = 0
                
                for col in VIDEO_PLAY_MAP.values():
                    row[col] = 0
                
                for col in VIDEO_METRIC_COLS:
                    row[col] = 0

                # Actions
                for action in r.get("actions", []):
                    action_type = action.get("action_type")
                    if action_type in ACTIONS_MAP:
                        row[ACTIONS_MAP[action_type]] = to_int(action.get("value"))
                    if action_type == "video_view":
                        video_type = action.get("action_video_type")

                        if video_type in VIDEO_ACTION_MAP:
                            row[VIDEO_ACTION_MAP[video_type]] = to_int(action.get("value"))

                # Action values
                for action_value in r.get("action_values", []):
                    action_type = action_value.get("action_type")
                    if action_type in ACTION_VALUES_MAP:
                        row[ACTION_VALUES_MAP[action_type]] = to_float(action_value.get("value"))

                # Video Play values
                for video_value in r.get("video_play_actions", []):
                    action_type = video_value.get("action_video_type")
                    if action_type in VIDEO_PLAY_MAP:
                        row[VIDEO_PLAY_MAP[action_type]] = to_int(video_value.get("value"))

                
                video_p25 = extract_video_metric(r.get("video_p25_watched_actions"))
                row["video_p25_total"] = video_p25["total"]

                video_p50 = extract_video_metric(r.get("video_p50_watched_actions"))
                row["video_p50_total"] = video_p50["total"]

                video_p75 = extract_video_metric(r.get("video_p75_watched_actions"))
                row["video_p75_total"] = video_p75["total"]

                video_p100 = extract_video_metric(r.get("video_p100_watched_actions"))
                row["video_p100_total"] = video_p100["total"]

                out.append(row)

            return out
    


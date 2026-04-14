import os, json, requests, time


class api:
    @staticmethod
    def auth(app, endpoint, payload, method):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configVTurb.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        urlbase = config[app]["url"]
        url = f"{urlbase}{endpoint}"
        token = config[app]["token"]

        headers = {
            "X-Api-Token": token,
            "X-Api-Version": "v1",
            "Content-Type": "application/json"
        }
        payload = payload

        if method == "POST":
            response = requests.post(url, json=payload, headers=headers, timeout=30)
        elif method == "GET":
            response = requests.get(url, json=payload, headers=headers, timeout=30)
        else:
            raise ValueError(f"Método HTTP não suportado: {method}")
        
        response.raise_for_status()

        return response.json()
    
    @staticmethod
    def getPlayers(app):
        endpoint = 'players/list'
        dados = api.auth(app=app, endpoint=endpoint, payload=None, method="GET")

        return {
                p["id"]: {
                    "name": p.get("name"),
                    "pitch_time": p.get("pitch_time"),
                    "duration": p.get("duration"),
                    "created_at": p.get("created_at")
                }
                for p in dados if p.get("id")
            }
        
    
    @staticmethod
    def getMetrics(app, start_date, end_date):
        endpoint = 'sessions/stats_by_day'

        players = api.getPlayers(app=app)

        all_data = []

        for player_id in players:
            payload = {
                "start_date": f"{start_date} 00:00:00",
                "end_date": f"{end_date} 23:59:59",
                "player_id": player_id,    
                "timezone": "America/Sao_Paulo"
            }
            try:
                dados = api.auth(app=app, endpoint=endpoint, payload=payload, method="POST")
                all_data.append({
                    "player_id": player_id,
                    "dados": dados
                })
                time.sleep(0.2)
            except Exception as e:
                print(f"Erro no player {player_id}: {e}")

        return all_data
    
    @staticmethod
    def getMetricsDF(app, start_date, end_date):
        all_data = api.getMetrics(app=app, start_date=start_date, end_date=end_date)
        rows = []
        for item in all_data:
            player_id = item.get("player_id")
            dados = item.get("dados", [])
    

teste = api.getMetrics(app="VTurbApi", start_date="2026-04-07", end_date="2026-04-08")
print(teste)

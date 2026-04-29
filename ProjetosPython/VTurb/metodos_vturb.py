import os, json, requests, time
from config import Config

class api:
    @staticmethod
    def auth(endpoint, payload, method):
 

        urlbase = Config.VTurb.URL
        url = f"{urlbase}{endpoint}"
        token = Config.VTurb.TOKEN

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
    def getPlayers():
        endpoint = 'players/list'
        dados = api.auth(endpoint=endpoint, payload=None, method="GET")

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
    def getMetrics(start_date, end_date):
        endpoint = 'sessions/stats_by_day'

        players = api.getPlayers()

        all_data = []

        for player_id in players:
            payload = {
                "start_date": f"{start_date} 00:00:00",
                "end_date": f"{end_date} 23:59:59",
                "player_id": player_id,    
                "timezone": "America/Sao_Paulo"
            }
            try:
                dados = api.auth(endpoint=endpoint, payload=payload, method="POST")
                all_data.append({
                    "player_id": player_id,
                    "dados": dados
                })
                time.sleep(0.2)
            except Exception as e:
                print(f"Erro no player {player_id}: {e}")

        return all_data
    
    @staticmethod
    def getMetricsDF(start_date, end_date):
        all_data = api.getMetrics(start_date=start_date, end_date=end_date)
        rows = []
        for item in all_data:
            player_id = item.get("player_id")
            dados = item.get("dados", [])
    

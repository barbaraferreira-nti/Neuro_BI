from datetime import datetime,date, timedelta, timezone
import time
import requests
import psycopg2
from config import Config
import os
from Supabase import metodos_supabase
import pandas as pd
import numpy as np

script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(script_dir, "log_execucao.txt")

with open(log_path, "a", encoding="utf-8") as f:
    f.write(f"Iniciado em {datetime.now()}\n")

# =========================
# CONFIGURAÇÕES
# =========================

token = Config.Octadesk.TOKEN
URL = Config.Octadesk.URL_CHAT

HEADERS = {
    "x-api-key": token,
    "Accept": "application/json"
}


LIMIT = 100
MAX_PAGES = 1000
REQUEST_TIMEOUT = 30
REQUEST_RETRIES = 3
REQUEST_SLEEP = 1
PAGE_SLEEP = 0.5
COMMIT_EVERY = 500

BR_TZ = timezone(timedelta(hours=-3))

# Atualiza sempre os últimos 5 dias
DATA_FIM = (date.today() + timedelta(days=1))
DATA_INICIO = (date.today() - timedelta(days=3))

# =========================
# FUNÇÕES AUXILIARES
# =========================

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return (
            datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
            .astimezone(BR_TZ)
        )
    except (ValueError, TypeError, AttributeError):
        return None


def safe_get(obj, *path):
    current = obj
    for key in path:
        if current is None:
            return None

        if isinstance(key, int):
            if isinstance(current, list) and len(current) > key:
                current = current[key]
            else:
                return None
        else:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
    return current

def get_main_phone(chat_item):
    """
    Busca o primeiro telefone válido em contact.phoneContacts[].number
    """
    phone_contacts = safe_get(chat_item, "contact", "phoneContacts")
    if isinstance(phone_contacts, list):
        for phone in phone_contacts:
            if isinstance(phone, dict) and phone.get("number"):
                return phone.get("number")
    return None

def connect_db():
    return psycopg2.connect(
        host="aws-0-us-west-2.pooler.supabase.com",
        database="postgres",
        user="postgres.oucfnlmlzssddpifqifs",
        password="Aveces16.1612",
        port=5432,
        sslmode="require"
    )

def reconnect_db(conn=None, cursor=None):
    try:
        if cursor:
            cursor.close()
    except Exception:
        pass

    try:
        if conn:
            conn.close()
    except Exception:
        pass

    print("🔌 Reconectando ao Supabase...")
    conn = connect_db()
    cursor = conn.cursor()
    return conn, cursor

def fetch_page(page, data_inicio, data_fim):
    params = {
        "page": page,
        "limit": LIMIT,

        "filters[0][operator]": "gt",
        "filters[0][property]": "createdAt",
        "filters[0][value]": data_inicio.strftime("%Y-%m-%d"),

        "filters[1][operator]": "lt",
        "filters[1][property]": "createdAt",
        "filters[1][value]": data_fim.strftime("%Y-%m-%d")
    }

    for attempt in range(REQUEST_RETRIES):
        try:
            print(f"🔄 Buscando página {page} (tentativa {attempt + 1}/{REQUEST_RETRIES})...")

            resp = requests.get(
                URL,
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            resp.raise_for_status()
            payload = resp.json()

            if isinstance(payload, list):
                return payload

            if isinstance(payload, dict):
                return payload.get("data", [])

            return []

        except requests.exceptions.RequestException as e:
            print(f"❌ Erro API página {page}: {e}")

            if attempt < REQUEST_RETRIES - 1:
                time.sleep(REQUEST_SLEEP)
            else:
                print(f"⚠️ Falha após {REQUEST_RETRIES} tentativas na página {page}.")
                return None

def build_record(it):
    rows = []

    for it in items:
        tags_list = safe_get(it, "tags", [])

        rows.append({
            "id": safe_get(it, "id"),
            "number": safe_get(it, "number"),
            "channel": safe_get(it, "channel"),
            "contact_name": safe_get(it, "contact", "name"),
            "contact_email": safe_get(it, "contact", "email"),
            "contact_phone": get_main_phone(it),
            "agent_name": safe_get(it, "agent", "name"),
            "last_message_date": parse_date(safe_get(it, "lastMessageDate")),
            "status": safe_get(it, "status"),
            "created_at": parse_date(safe_get(it, "createdAt")),
            "updated_at": parse_date(safe_get(it, "updatedAt")),
            "closed_at": parse_date(safe_get(it, "closedAt")),
            "group_name": safe_get(it, "group", "name"),
            "tags": ",".join([str(t) for t in tags_list if t]) if isinstance(tags_list, list) else None,
            "origin": safe_get(it, "origin"),
            "status_detail": safe_get(it, "statusDetail"),
            "agent_first_message_date": parse_date(safe_get(it, "agentFirstMessageDate")),
            "bot_assigned_date": parse_date(safe_get(it, "bot", "assignedAt")),
            "assigned_to_agent_date": parse_date(safe_get(it, "assignedToAgentDate")),
            "survey_response": safe_get(it, "survey", "response"),
            "survey_comment": safe_get(it, "survey", "comment"),
            "inserted_at": datetime.now(BR_TZ)
        })

    return pd.DataFrame(rows)

def page_date_stats(items):
    dates = []
    for it in items:
        dt = parse_date(safe_get(it, "createdAt"))
        if dt:
            dates.append(dt)

    if not dates:
        return None, None

    return min(dates), max(dates)

def tratar_nan_json(df):
    df = df.copy()

    def converter_valor(x):
        if x is None:
            return None
        
        if isinstance(x, str) and x in ["NaT", "nan", "None"]:
            return None

        if isinstance(x, float) and np.isnan(x):
            return None

        if isinstance(x, pd.Timestamp):
            return x.isoformat()

        if isinstance(x, datetime):
            return x.isoformat()

        if pd.isna(x):
            return None

        return x

    for col in df.columns:
        df[col] = df[col].astype(object).map(converter_valor)

    return df

# =========================
# INÍCIO DO PROCESSO
# =========================

start_time = time.time()

print("▶️ Iniciando carga Octadesk")
print(f"📌 Período de atualização: {DATA_INICIO} até {DATA_FIM}")


page = 1
total_received = 0
total_upserted = 0
total_errors = 0

while page <= MAX_PAGES:
    try:
        print(f"\n🔄 Processando página {page}...")

        items = fetch_page(page, DATA_INICIO, DATA_FIM)

        if items is None:
            msg = f"⛔ Página {page}: falha de comunicação com a API. Encerrando execução."
            print(msg)
            total_errors += 1
            break

        if not items:
            print(f"✅ Página {page}: sem registros. Fim da paginação.")
            break

        total_received += len(items)
        print(f"📄 Página {page}: {len(items)} registros recebidos da API.")

        df = build_record(items)

        if df is None:
            msg = f"❌ Página {page}: build_record retornou None."
            print(msg)
            total_errors += 1
            break

        if df.empty:
            print(f"⚠️ Página {page}: nenhum registro válido após tratamento.")
            page += 1
            continue

        df = tratar_nan_json(df)

        registros = df.to_dict(orient="records")

        for registro in registros:
            for chave, valor in registro.items():
                if valor in ["NaT", "nan", "None"]:
                    registro[chave] = None
                elif isinstance(valor, float) and np.isnan(valor):
                    registro[chave] = None

        if not registros:
            print(f"⚠️ Página {page}: lista de registros vazia após conversão.")
            page += 1
            continue

        try:
            metodos_supabase.api.upsert_data(
                banco="Octadesk_DB",
                tabela="octadesk_chats",
                dados=registros,
                chave="id"
            )

            total_upserted += len(registros)
            print(f"✅ Página {page}: {len(registros)} linhas atualizadas no Supabase.")

        except Exception as e:
            total_errors += 1
            print(f"❌ Página {page}: erro ao realizar upsert no Supabase.")
            print(f"   Erro: {e}")

            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"Erro no upsert da página {page}: {e}\n")

    except Exception as e:
        total_errors += 1
        print(f"❌ Erro inesperado na página {page}.")
        print(f"   Erro: {e}")

        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"Erro inesperado na página {page}: {e}\n")

        break

    page += 1
    time.sleep(PAGE_SLEEP)

elapsed = time.time() - start_time

print("\n===================================")
print("🏁 Carga Octadesk finalizada")
print(f"📥 Total recebido da API: {total_received}")
print(f"✅ Total atualizado no Supabase: {total_upserted}")
print(f"❌ Total de erros: {total_errors}")
print(f"⏱️ Tempo total: {elapsed:.2f} segundos")

with open(log_path, "a", encoding="utf-8") as f:
    f.write(f"Finalizado em {datetime.now()}\n\n")
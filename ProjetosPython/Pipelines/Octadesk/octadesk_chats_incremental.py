from datetime import datetime
import time
import requests 
import psycopg2
import os, json
from datetime import datetime, timedelta, timezone

with open("C:\\Users\\Administrator\\Neuro_BI\\ProjetosPython\\Pipelines\\Octadesk\\log_execucao.txt", "a", encoding="utf-8") as f:
    f.write(f"Iniciado em {datetime.now()}\n")

# =========================
# CONFIGURAÇÕES
# =========================
scriptDir = os.path.dirname(os.path.abspath(__file__))
configPath = os.path.join(scriptDir, "configOctadesk.json")
with open(configPath, "r", encoding="utf-8") as f:
    config = json.load(f)

app = "OctadeskApi"
token = config[app]["token"]
URL = config[app]["url"]

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


def fetch_page(page):
    params = {"page": page, "limit": LIMIT}

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
    tags_list = safe_get(it, "tags", [])

    return {
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
    }


def page_date_stats(items):
    dates = []
    for it in items:
        dt = parse_date(safe_get(it, "createdAt"))
        if dt:
            dates.append(dt)

    if not dates:
        return None, None

    return min(dates), max(dates)


# =========================
# INÍCIO DO PROCESSO
# =========================

start_time = time.time()
print("▶️ Iniciando carga incremental Octadesk")

conn = connect_db()
cursor = conn.cursor()

cursor.execute("SELECT MAX(created_at) FROM octadesk_chats")
last_created = cursor.fetchone()[0]

if last_created is not None and getattr(last_created, "tzinfo", None) is not None:
    last_created = last_created.astimezone(BR_TZ)

print(f"📌 Última data na base: {last_created}")

page = 1
total_received = 0
total_candidates = 0
total_upserted = 0
seen_ids = set()

upsert_sql = """
INSERT INTO octadesk_chats (
    id, number, channel,
    contact_name, contact_email, contact_phone,
    agent_name, last_message_date, status,
    created_at, updated_at, closed_at,
    group_name, tags, origin, status_detail,
    agent_first_message_date, bot_assigned_date,
    assigned_to_agent_date,
    survey_response, survey_comment, inserted_at
)
VALUES (
    %s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s,%s,%s,%s,%s,%s,
    %s,%s,%s,%s
)
ON CONFLICT (id) DO UPDATE SET
    number = EXCLUDED.number,
    channel = EXCLUDED.channel,
    contact_name = EXCLUDED.contact_name,
    contact_email = EXCLUDED.contact_email,
    contact_phone = EXCLUDED.contact_phone,
    agent_name = EXCLUDED.agent_name,
    last_message_date = EXCLUDED.last_message_date,
    status = EXCLUDED.status,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    closed_at = EXCLUDED.closed_at,
    group_name = EXCLUDED.group_name,
    tags = EXCLUDED.tags,
    origin = EXCLUDED.origin,
    status_detail = EXCLUDED.status_detail,
    agent_first_message_date = EXCLUDED.agent_first_message_date,
    bot_assigned_date = EXCLUDED.bot_assigned_date,
    assigned_to_agent_date = EXCLUDED.assigned_to_agent_date,
    survey_response = EXCLUDED.survey_response,
    survey_comment = EXCLUDED.survey_comment,
    inserted_at = EXCLUDED.inserted_at
"""

while page <= MAX_PAGES:
    items = fetch_page(page)

    if items is None:
        print("⛔ Encerrando por falha de comunicação com a API.")
        break

    if not items:
        print("✅ Página vazia. Fim da paginação.")
        break

    total_received += len(items)

    page_min_dt, page_max_dt = page_date_stats(items)
    print(f"📄 Pg {page}: {len(items)} registros | created_at min={page_min_dt} | max={page_max_dt}")

    parar_paginacao = True
    considered_this_page = 0

    for it in items:
        record = build_record(it)

        if not record["id"]:
            continue

        created_at = record["created_at"]
        if not created_at:
            continue

        if last_created and created_at <= last_created:
            continue

        parar_paginacao = False

        if record["id"] in seen_ids:
            continue
        seen_ids.add(record["id"])

        total_candidates += 1
        considered_this_page += 1

        params = (
            record["id"],
            record["number"],
            record["channel"],
            record["contact_name"],
            record["contact_email"],
            record["contact_phone"],
            record["agent_name"],
            record["last_message_date"],
            record["status"],
            record["created_at"],
            record["updated_at"],
            record["closed_at"],
            record["group_name"],
            record["tags"],
            record["origin"],
            record["status_detail"],
            record["agent_first_message_date"],
            record["bot_assigned_date"],
            record["assigned_to_agent_date"],
            record["survey_response"],
            record["survey_comment"],
            record["inserted_at"]
        )

        try:
            cursor.execute(upsert_sql, params)
            total_upserted += 1

            if total_upserted % COMMIT_EVERY == 0:
                conn.commit()
                print(f"💾 Commit parcial: {total_upserted} registros processados")

        except psycopg2.OperationalError:
            conn, cursor = reconnect_db(conn, cursor)
            try:
                cursor.execute(upsert_sql, params)
                total_upserted += 1
            except Exception as e:
                print(f"❌ Erro ao reinserir/upsert registro {record['id']}: {e}")

        except Exception as e:
            print(f"❌ Erro no upsert do registro {record['id']}: {e}")

    print(f"   ↳ considerados para carga: {considered_this_page}")

    try:
        conn.commit()
    except psycopg2.OperationalError:
        conn, cursor = reconnect_db(conn, cursor)

    if parar_paginacao:
        print("⛔ Página totalmente antiga. Encerrando paginação.")
        break

    page += 1
    time.sleep(PAGE_SLEEP)

try:
    cursor.close()
except Exception:
    pass

try:
    conn.close()
except Exception:
    pass

elapsed = time.time() - start_time

print("===================================")
print("🏁 Carga finalizada com sucesso")
print("📥 Total recebido da API:", total_received)
print("🎯 Total separado para carga:", total_candidates)
print("✅ Total processado no Supabase:", total_upserted)
print(f"⏱️ Tempo total: {elapsed:.2f} segundos")

with open("C:\\Users\\Administrator\\Neuro_BI\\ProjetosPython\\Pipelines\\Octadesk\\log_execucao.txt", "a", encoding="utf-8") as f:
    f.write(f"Finalizado em {datetime.now()}\n\n")
from config import Config
import requests
import time
import json, re
import pandas as pd

class api:
    @staticmethod
    def request_clint(endpoint, params=None):
        TOKEN = Config.Clint.TOKEN
        BASE_URL = Config.Clint.URL

        HEADERS = {
            "api-token": TOKEN,
            "Content-Type": "application/json"
        }

        url = f"{BASE_URL}{endpoint}"

        while True:
            response = requests.get(url, headers=HEADERS, params=params)

            if response.status_code == 429:
                time.sleep(10)
                continue

            response.raise_for_status()
            return response.json()
        
    @staticmethod
    def get_origins(params=None):
        data = api.request_clint("/origins", params)
        return data.get("data", [])
    
    @staticmethod
    def get_tags(params=None):
        data = api.request_clint("/tags", params)
        return data.get("data", [])
    
    @staticmethod
    def get_lost_status(params=None):
        data = api.request_clint("/lost-status", params)
        return data.get("data", [])
    
    @staticmethod
    def get_groups(params=None):
        data = api.request_clint("/groups", params)
        return data.get("data", [])
    
    @staticmethod
    def get_contact(contact_id):
        data = api.request_clint(f"/contacts/{contact_id}")
        return data.get("data", [])
    
    @staticmethod
    def get_contacts(params=None):
        all_contacts = []
        page = 1

        while True:
            params_page = params.copy() if params else {}
            params_page["page"] = page

            data = api.request_clint("/contacts", params=params_page)

            contacts = data.get("data", [])

            if not contacts:
                break

            all_contacts.extend(contacts)
            page += 1

        return all_contacts
      
    
    @staticmethod
    def get_deals(params=None):
        all_deals = []
        page = 1

        while True:
            params_page = params.copy() if params else {}
            params_page["page"] = page

            data = api.request_clint("/deals", params=params_page)

            deals = data.get("data", [])

            if not deals:
                break

            all_deals.extend(deals)
            page += 1

        return all_deals
    
    @staticmethod
    def get_users(params=None):
        all_users = []
        page = 1

        while True:
            params_page = params.copy() if params else {}
            params_page["page"] = page

            data = api.request_clint("/users", params=params_page)

            users = data.get("data", [])

            if not users:
                break

            all_users.extend(users)
            page += 1

        return all_users
         
    @staticmethod
    def tratar_origins(origins):
        rows_origins = []
        rows_stages = []

        for origin in origins:
            origin_id = origin.get("id")
            group = origin.get("group", {}) or {}

            rows_origins.append({
                "origin_id": origin_id,
                "origin_name": origin.get("name"),
                "group_id": group.get("id"),
                "group_name": group.get("name"),
                "archived_at": origin.get("archived_at"),
                "archived_by": origin.get("archived_by")
                })
            
            for stage in origin.get("stages", []):
                rows_stages.append({
                    "stage_id": stage.get("id"),
                    "stage_name": stage.get("label"),
                    "stage_order": stage.get("order"),
                    "stage_type": stage.get("type"),
                    "origin_id": origin_id
                })

        return pd.DataFrame(rows_origins), pd.DataFrame(rows_stages)

    @staticmethod
    def tratar_tags(tags):
        rows = []

        for tag in tags:
            rows.append({
                "tag_id": tag.get("id"),
                "tag_name": tag.get("name"),
                "tag_color": tag.get("color")
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def tratar_groups(groups):
        rows = []

        for group in groups:
            rows.append({
                "group_id": group.get("id"),
                "group_name": group.get("name"),
                "archived_at": group.get("archived_at"),
                "archived_by": group.get("archived_by")
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def limpar_caracteres(cpf):
        if not cpf:
            return None

        # remove tudo que não for número
        cpf = re.sub(r"\D", "", str(cpf))

    @staticmethod
    def tratar_contacts(contacts):
        rows_contacts = []
        rows_tags = []

        for contact in contacts:
            contact_id = contact.get("id")

            fields = contact.get("fields") or {}
            if not isinstance(fields, dict):
                fields = {}

            address = api.parse_json_dict(fields.get("address"))

            doc = (
                fields.get("doc")
                or fields.get("cpf")
            )

            rows_contacts.append({
                "contact_id": contact_id,
                "created_at": contact.get("created_at"),
                "updated_at": contact.get("updated_at"),
                "contact_name": api.normalizar_none(contact.get("name")),
                "contact_email": api.normalizar_none(contact.get("email")),
                "contact_phone": api.limpar_caracteres(contact.get("fullPhone")),
                "doc": api.normalizar_none(fields.get("doc")),
                "produto": api.normalizar_none(fields.get("produto")),
                "assinado_data": api.normalizar_none(fields.get("assinado_data")),
                "assinatura_status": api.normalizar_none(fields.get("assinatura_status")),
                "assinatura_visualizada": api.normalizar_none(fields.get("assinatura_visualiza")),
                "motivo_para_assinatura": api.normalizar_none(fields.get("motivo_para_assinatu")),
                "address_street": api.normalizar_none(address.get("street")),
                "address_number": api.normalizar_none(address.get("number")),
                "address_complement": api.normalizar_none(address.get("comp")),
                "address_district": api.normalizar_none(address.get("district")),
                "address_city": api.normalizar_none(address.get("city")),
                "address_state": api.normalizar_none(address.get("state")),
                "address_country": api.normalizar_none(address.get("country")),
                "address_zip_code": api.limpar_caracteres(address.get("zipcode"))
    
            })

            for tag in contact.get("tags") or []:
                rows_tags.append({
                    "contact_id": contact_id,
                    "tag_id": tag.get("id"),
                    "tag_name": tag.get("name")
                })


        return (
            pd.DataFrame(rows_contacts),
            pd.DataFrame(rows_tags))
    

    @staticmethod
    def tratar_deals(deals):
        rows = []

        for deal in deals:
            contact = deal.get("contact", {}) or {}
            user = deal.get("user", {}) or {}

            rows.append({
                "deal_id": deal.get("id"),
                "origin_id": deal.get("origin_id"),
                "user_id": user.get("id"),
                "contact_id": contact.get("id"),
                "created_at": deal.get("created_at"),
                "stage_id": deal.get("stage_id"),
                "updated_stage_at": deal.get("updated_stage_at"),
                "status": deal.get("status"),
                "won_at": deal.get("won_at"),
                "won_by": deal.get("won_by"),
                "lost_status_id": deal.get("lost_status_id"),
                "lost_at": deal.get("lost_at")
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def tratar_users(users):
        rows = []

        for user in users:
            rows.append({
                "user_id": user.get("id"),
                "user_name": user.get("first_name") + " " + user.get("last_name"),
                "user_email": user.get("email")
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def tratar_lost_status(status):
        rows = []

        for s in status:
            rows.append({
                "status_id": s.get("id"),
                "status_name": s.get("name")
            })
        
        return pd.DataFrame(rows)
    
    @staticmethod
    def getOriginsDF(params=None):
        dados = api.get_origins(params)
        df_origins, df_stages = api.tratar_origins(dados)

        return df_origins, df_stages
    
    @staticmethod
    def getTagsDF(params=None):
        dados = api.get_tags(params)
        df_tags = api.tratar_tags(dados)

        return df_tags
    
    @staticmethod
    def getUsersDF(params=None):
        dados = api.get_users(params)
        df_users = api.tratar_users(dados)

        return df_users
    
    @staticmethod
    def getContactsDF(params=None):
        dados = api.get_contacts(params)
        df_contacts, df_tags = api.tratar_contacts(dados)

        return df_contacts, df_tags
    
    @staticmethod
    def getGroupsDF(params=None):
        dados = api.get_groups(params)
        df_groups = api.tratar_groups(dados)

        return df_groups
    
    @staticmethod
    def getStatusLostDF(params=None):
        dados = api.get_lost_status(params)
        df_status = api.tratar_lost_status(dados)

        return df_status
    
    @staticmethod
    def getDealsDF(params=None):
        dados = api.get_deals(params)
        df_deals = api.tratar_deals(dados)

        return df_deals
    
    @staticmethod
    def parse_json_dict(valor):
        valor = api.normalizar_none(valor)

        if valor is None:
            return {}

        if isinstance(valor, dict):
            return valor

        if isinstance(valor, str):
            try:
                convertido = json.loads(valor)
                if isinstance(convertido, dict):
                    return convertido
            except Exception:
                return {}

        return {}
    
    @staticmethod
    def normalizar_none(valor):
        if valor in [None, "", "None", "null", "NULL"]:
            return None
        return valor
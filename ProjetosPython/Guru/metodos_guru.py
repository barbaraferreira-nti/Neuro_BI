import os, json, time, random
from datetime import datetime, timezone
import requests
import pandas as pd
import re

class api:
    @staticmethod
    def auth(app):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configGuru.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)
        return config.get(app, {}).get("token")
    
    @staticmethod
    def RetryRequest(session, url, headers=None, params=None, retries=11, timeout=60):
            last_exc = None

            for attempt in range(1, retries + 1):
                try:
                    r = session.get(url, headers=headers, params=params, timeout=timeout)

                    if r.status_code == 429:
                        ra = r.headers.get("Retry-After")
                        sleep_time = min(int(ra), 120) if ra else min(2 ** attempt, 60) + random.uniform(0, 0.5)
                        time.sleep(sleep_time)
                        continue

                    if r.status_code in (500, 502, 503, 504):
                        sleep_time = min(2 ** attempt, 60) + random.uniform(0, 0.5)
                        time.sleep(sleep_time)
                        continue

                    r.raise_for_status()

                    if not r.text.strip():
                        raise Exception(f"Resposta vazia na URL: {url}")

                    try:
                        return r.json()
                    except requests.exceptions.JSONDecodeError:
                        raise Exception(
                            f"Resposta não é JSON válido. "
                            f"status={r.status_code} | body={repr(r.text[:500])}"
                        )

                except (
                    requests.exceptions.ConnectTimeout,
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError,
                    requests.exceptions.HTTPError,
                    Exception
                ) as e:
                    last_exc = e

                    if attempt == retries:
                        break

                    sleep_time = min(2 ** attempt, 60) + random.uniform(0, 0.5)
                    time.sleep(sleep_time)

            raise Exception(f"Falha após {retries} tentativas. Último erro: {last_exc}")

    @staticmethod
    def unix_to_datetime(value):
        if value in (None, "", 0):
            return None
        if value > 1e12:
            value = value/1000
        return datetime.fromtimestamp(value, tz=timezone.utc)

    @staticmethod
    def tratar_telefone(value):
        if not value:
            return None

        value = re.sub(r"\D", "", str(value))

        # DDD + telefone fixo
        if len(value) == 10:
            value = "55" + value

        # DDD + celular
        elif len(value) == 11:
            value = "55" + value

        # 55 + DDD + telefone fixo
        elif len(value) == 12:
            value = value[:4] + "9" + value[4:]

        return value

    @staticmethod
    def getTransactions(app, periodo, per_page=50, max_pages=600):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configGuru.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        baseEndPoint = config[app]["url"]["transactions"]
        token = api.auth(app=app)
        headers = {"Authorization": f"Bearer {token}"}
        dataI, dataF = periodo[0], periodo[1]

        page_count = 0
        cursor = None 

        all_transactions = []
        with requests.Session() as session:
            while True:
                page_count += 1
                params = {
                    "ordered_at_ini": dataI,
                    "ordered_at_end": dataF,
                    "per_page": per_page
                }
                if cursor:
                    params['cursor'] = cursor

                
                payload = api.RetryRequest(
                    session=session,
                    url=baseEndPoint,
                    headers=headers,
                    params=params
                )

                transactions = payload.get("data", []) or []
                
                for transaction in transactions:
                    row = api.tratarTransaction(transaction)
                    all_transactions.append(row)

                cursor = payload.get("next_cursor")

                if not cursor:
                    break

                if page_count >= max_pages:
                    print(f"⚠️ Limite de {max_pages} páginas atingido.")
                    break

        return all_transactions

    @staticmethod
    def getIdTransaction(app, invoice_id):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configGuru.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        baseEndPoint = config[app]["url"]["transactions"]
        token = api.auth(app=app)

        headers = {"Authorization": f"Bearer {token}"}
        url = f"{baseEndPoint}{invoice_id}"

        with requests.Session() as session:
            payload = api.RetryRequest(
                session=session,
                url=url,
                headers=headers
            )

        return payload

    @staticmethod
    def getContacts(app, periodo, per_page=50, max_pages=600):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configGuru.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        baseEndPoint = config[app]["url"]["contacts"]
        token = api.auth(app=app)
        headers = {"Authorization": f"Bearer {token}"}
        dataI, dataF = periodo[0], periodo[1]

        page_count = 0
        cursor = None 

        all_contacts = []
        with requests.Session() as session:
            while True:
                page_count += 1
                params = {
                    "created_at_ini": dataI,
                    "created_at_end": dataF,
                    "per_page": per_page
                }
                if cursor:
                    params['cursor'] = cursor

                
                payload = api.RetryRequest(
                    session=session,
                    url=baseEndPoint,
                    headers=headers,
                    params=params
                )

                contacts = payload.get("data", []) or []
                
                for contact in contacts:
                    row = api.tratarContact(contact)
                    all_contacts.append(row)

                cursor = payload.get("next_cursor")

                if not cursor:
                    break

                if page_count >= max_pages:
                    print(f"⚠️ Limite de {max_pages} páginas atingido.")
                    break

        return all_contacts

    @staticmethod
    def getProducts(app, per_page=50, max_pages=600):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configGuru.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        baseEndPoint = config[app]["url"]["products"]
        token = api.auth(app=app)
        headers = {"Authorization": f"Bearer {token}"}
        
        page_count = 0
        cursor = None 

        all_products = []
        with requests.Session() as session:
            while True:
                page_count += 1
                params = {
                    "per_page": per_page
                }
                if cursor:
                    params['cursor'] = cursor

                
                payload = api.RetryRequest(
                    session=session,
                    url=baseEndPoint,
                    headers=headers,
                    params=params
                )

                products = payload.get("data", []) or []

                for product in products:
                    row = api.tratarProduct(product)
                    if row:
                        all_products.append(row)

                
                cursor = payload.get("next_cursor")

                if not cursor:
                    break

                if page_count >= max_pages:
                    print(f"⚠️ Limite de {max_pages} páginas atingido.")
                    break

        return all_products

    @staticmethod
    def getProductOffers(app, product_id, session, headers):
    
        url = f"https://digitalmanager.guru/api/v2/products/{product_id}/offers"

        payload = api.RetryRequest(
            session=session,
            url=url,
            headers=headers
        )

        offers = payload.get("data", []) or []

      
        if not offers:
            return {
                "offer_id": None,
                "offer_name": None
            }

        # como você informou que cada product_id tem uma única offer
        offer = offers[0]

        return {
            "offer_id": offer.get("id"),
            "offer_name": offer.get("name")
        }
                       
    @staticmethod
    def getOffers(app, per_page=50, max_pages=600):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configGuru.json")
        
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        baseEndPoint = config[app]["url"]["products"]
        token = api.auth(app=app)
        headers = {"Authorization": f"Bearer {token}"}

        page_count = 0
        cursor = None
        all_offers = []

        with requests.Session() as session:
            while True:
                page_count += 1

                params = {
                    "per_page": per_page
                }
                if cursor:
                    params["cursor"] = cursor

                payload = api.RetryRequest(
                    session=session,
                    url=baseEndPoint,
                    headers=headers,
                    params=params
                )

                products = payload.get("data", []) or []

                for product in products:
                    product_id = product.get("id")

                    if not product_id:
                        continue

                    url_offers = f"https://digitalmanager.guru/api/v2/products/{product_id}/offers"

                    payload_offers = api.RetryRequest(
                        session=session,
                        url=url_offers,
                        headers=headers
                    )

                    offers = payload_offers.get("data", []) or []

                    if not offers:
                        continue

                    for offer in offers:
                        all_offers.append({
                            "offer_id": offer.get("id"),
                            "offer_name": offer.get("name"),
                            "product_internal_id": product_id
                        })

                cursor = payload.get("next_cursor")

                if not cursor:
                    break

                if page_count >= max_pages:
                    print(f"Limite de páginas atingido: {max_pages}")
                    break

        return all_offers

    @staticmethod
    def tratarTransaction(payload):
        contact = payload.get("contact", {}) or {}
        dates = payload.get("dates", {}) or {}
        product = payload.get("product", {}) or {}
        payment = payload.get("payment", {}) or {}
        trackings = payload.get("trackings", {}) or {}
        installments = payment.get("installments", {}) or {}

        subscription_raw = payload.get("subscription", [])
        if isinstance(subscription_raw, list):
            subscription = subscription_raw[0] if subscription_raw else {}
        elif isinstance(subscription_raw, dict):
            subscription = subscription_raw
        else:
            subscription = {}

        offer = product.get("offer", {}) or {}
        coupon = payment.get("coupon", {}) or {}

        return {
            "id": payload.get("id"),
            "status": payload.get("status"),
        
            # timestamptz no banco
            "created_at": api.unix_to_datetime(dates.get("created_at")),
            "updated_at": api.unix_to_datetime(dates.get("updated_at")),
            "ordered_at": api.unix_to_datetime(dates.get("ordered_at")),
            "confirmed_at": api.unix_to_datetime(dates.get("confirmed_at")),
            "canceled_at": api.unix_to_datetime(dates.get("canceled_at")),

            "product_id": product.get("internal_id"),
            "product_guru_id": product.get("id"),
            "offer_id": offer.get("id"),
            
            "payment_gross": payment.get("gross"),
            "payment_discount_value": payment.get("discount_value"),
            "payment_net": payment.get("net"),
            "payment_method": payment.get("method"),
            "installments_qty": installments.get("qty"),
            "installments_value": installments.get("value"),
            "installments_interest": installments.get("interest"),
            "payment_currency": payment.get("currency"),
        
            "trackings_utm_source": trackings.get("utm_source"),
            "trackings_utm_campaign": trackings.get("utm_campaign"),
            "trackings_utm_medium": trackings.get("utm_medium"),
            "trackings_utm_content": trackings.get("utm_content"),
            "trackings_utm_term": trackings.get("utm_term"),

            "has_order_bump": payload.get("has_order_bump"),
            "is_order_bump": payload.get("is_order_bump"),

            "product_qty": product.get("qty"),
            "product_total_value": product.get("total_value"),
            "payment_refuse_reason": payment.get("refuse_reason"),

            "contact_doc": contact.get("doc"),
            "contact_name": contact.get("name"),
            "contact_email": contact.get("email"),
            "contact_phone": api.tratar_telefone(contact.get("phone_number")),
            "contact_guru_id": contact.get("id"),
            "contact_address_zipcode": contact.get("address_zip_code"),
            "contact_address_state": contact.get("address_state"),
            "contact_address_city": contact.get("address_city"),
            "contact_address_district": contact.get("address_district"),
            "contact_address_street": contact.get("address"),
            "contact_address_number": contact.get("address_number"),
            "contact_address_complement": contact.get("address_comp"),

            "plataforma": "Guru",
            "coupon_id": coupon.get("id"),
            "coupon_code": coupon.get("coupon_code"),
            "coupon_value": coupon.get("final_value")
        }

    @staticmethod
    def tratarContact(payload):

        return {
            "contact_id": payload.get("id"),
            "name": payload.get("name"),
            "company_name": payload.get("company_name"),
            "email": payload.get("email"),
            "doc": payload.get("doc"),
            "phone_number": api.tratar_telefone(payload.get("phone_number")),
            "phone_local_code": payload.get("phone_local_code"),
            "address": payload.get("address"),
            "address_number": payload.get("address_number"),
            "address_comp": payload.get("address_comp"),
            "address_district": payload.get("address_district"),
            "address_city": payload.get("address_city"),
            "address_state": payload.get("address_state"),
            "address_state_full_name": payload.get("address_state_full_name"),
            "address_country": payload.get("address_country"),
            "address_zip_code": payload.get("address_zip_code"),

            # timestamptz no banco
            "created_at": api.unix_to_datetime(payload.get("created_at")),
            "updated_at": api.unix_to_datetime(payload.get("updated_at"))
    }

    @staticmethod    
    def tratarProduct(payload):
        group = payload.get("group") or {}

        return {
            "product_id": payload.get("id"),
            "internal_id": payload.get("internal_id"),
            "name": payload.get("name"),
            "marketplace_name": payload.get("marketplace_name"),
            "product_group_id": group.get("id"),
            "product_group_name": group.get("name"),
            
            # timestamptz no banco
            "created_at": api.unix_to_datetime(payload.get("created_at")),
            "updated_at": api.unix_to_datetime(payload.get("updated_at")),
            "is_hidden": payload.get("is_hidden"),
            "plataforma": "Guru"
        }
    
    @staticmethod
    def getTransactionsDF(app, periodo):
        rows = api.getTransactions(
            app=app,
            periodo=periodo
        )

        df = pd.DataFrame(rows)
        return df
    
    @staticmethod
    def getContactsDF(app, periodo):
        rows = api.getContacts(
            app=app,
            periodo=periodo)

        df = pd.DataFrame(rows)
        return df
    
    @staticmethod
    def getProductsDF(app):
        rows = api.getProducts(
            app=app
        )

        df = pd.DataFrame(rows)
        return df
    
    @staticmethod
    def getOffersDF(app):
        rows = api.getOffers(
            app=app
            )
        df = pd.DataFrame(rows)
        return df

    @staticmethod
    def getCoupons(app, max_pages=600):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configGuru.json")
        
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        baseEndPoint = config[app]["url"]["coupons"]
        token = api.auth(app=app)
        headers = {"Authorization": f"Bearer {token}"}

        page_count = 0
        cursor = None
        all_coupons = []

        with requests.Session() as session:
            while True:
                page_count += 1

                params = {
                    "is_active": "all",
                    "has_transactions": "all",
                    "validate_by ": "document"

                }
                if cursor:
                    params["cursor"] = cursor

                payload = api.RetryRequest(
                    session=session,
                    url=baseEndPoint,
                    headers=headers,
                    params=params
                )

                coupons = payload.get("data", []) or []
                
                for coupon in coupons:
                    row = api.tratarCoupon(coupon)
                    if row:
                        all_coupons.append(row)

                
                cursor = payload.get("next_cursor")

                if not cursor:
                    break

                if page_count >= max_pages:
                    print(f"⚠️ Limite de {max_pages} páginas atingido.")
                    break

        return all_coupons
    
    @staticmethod
    def tratarCoupon(payload):
    
        return {
            "coupon_id": payload.get("id"),
            "name": payload.get("coupon_code"),
            
            # timestamptz no banco
            "date_ini": api.unix_to_datetime(payload.get("date_ini")),
            "date_end": api.unix_to_datetime(payload.get("date_end")),

            "incidence_field": payload.get("incidence_field"),
            "incidence_type": payload.get("incidence_type"),
            "incidence_value": payload.get("incidence_value"),
            "is_active": payload.get("is_active"),
            "plataforma": "Guru"
        }
  
    @staticmethod
    def getCouponsDF(app):
        rows = api.getCoupons(
            app=app
        )

        df = pd.DataFrame(rows)
        return df



import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    class Guru:
        TOKEN = os.getenv("GURU_API_TOKEN")
        URL_TRANSACTIONS = os.getenv("GURU_API_URL_TRANSACTIONS")
        URL_CONTACTS = os.getenv("GURU_API_URL_CONTACTS")
        URL_PRODUCTS = os.getenv("GURU_API_URL_PRODUCTS")
        URL_COUPONS = os.getenv("GURU_API_URL_COUPONS")

    class ActiveCampaign:
        TOKEN = os.getenv("ACTIVE_CAMPAIGN_API_TOKEN")
        URL = os.getenv("ACTIVE_CAMPAIGN_URL")

    class Manychat:
        TOKEN = os.getenv("MANYCHAT_API_TOKEN")

    class Meta:
        TOKEN_NEUROSABER = os.getenv("META_NEUROSABER_API_TOKEN")
        TOKEN_SINAHPSE = os.getenv("META_SINAHPSE_API_TOKEN")
        URL_FACEBOOK = os.getenv("META_API_URL_FACEBOOK")
        URL_INSTAGRAM = os.getenv("META_API_URL_INSTAGRAM")

    class Principia:
        TOKEN = os.getenv("PRINCIPIA_API_TOKEN")
        URL_PROD = os.getenv("PRINCIPIA_API_URL_PROD")
        URL_DEV = os.getenv("PRINCIPIA_API_URL_DEV")

    class Supabase:
        TOKEN_GURU_DB = os.getenv("SUPABASE_API_TOKEN_GURU_DB")
        TOKEN_OCTADESK_DB = os.getenv("SUPABASE_API_TOKEN_OCTADESK_DB")
        TOKEN_FORMULARIOS_NEUROESCOLA = os.getenv("SUPABASE_API_TOKEN_FORMULARIOS_NEUROESCOLA")
        TOKEN_CLICKUP_DB = os.getenv("SUPABASE_API_TOKEN_CLICKUP_DB")
        TOKEN_CLINT_DB = os.getenv("SUPABASE_API_TOKEN_CLINT_DB")
        TOKEN_META_DB = os.getenv("SUPABASE_API_TOKEN_META_DB")
        URL_GURU_DB = os.getenv("SUPABASE_API_URL_GURU_DB")
        URL_OCTADESK_DB = os.getenv("SUPABASE_API_URL_OCTADESK_DB")
        URL_FORMULARIOS_NEUROESCOLA = os.getenv("SUPABASE_API_URL_FORMULARIOS_NEUROESCOLA")
        URL_CLICKUP_DB = os.getenv("SUPABASE_API_URL_CLICKUP_DB")
        URL_CLINT_DB = os.getenv("SUPABASE_API_URL_CLINT_DB")
        URL_META_DB = os.getenv("SUPABASE_API_URL_META_DB")

    class VTurb:
        TOKEN = os.getenv("VTURB_API_TOKEN")
        URL = os.getenv("VTURB_API_URL")
    
    class Octadesk:
        TOKEN = os.getenv("OCTADESK_API_TOKEN")
        URL_CHAT = os.getenv("OCTADESK_API_URL_CHAT")

    class ClickUp:
        TOKEN_BARBARA = os.getenv("CLICKUP_API_TOKEN_BARBARA")
        URL = os.getenv("CLICKUP_API_URL")
    class Shopify:
        CLIENT_ID = os.getenv("SHOPIFY_CLIENT_ID")
        CLIENT_SECRET = os.getenv("SHOPIFY_CLIENT_SECRET")
        APP_TOKEN = os.getenv("SHOPIFY_APP_TOKEN")
        SHOP = os.getenv("SHOPIFY_SHOP")

    class Clint:
        TOKEN = os.getenv("CLINT_API_TOKEN")
        URL = os.getenv("CLINT_API_URL")

    class Autentique:
        TOKEN = os.getenv("AUTENTIQUE_API_TOKEN")

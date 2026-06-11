from config import Config
import shopify
from Shopify.queries_shopify import queries
import json, requests, time, re
from pprint import pprint
from shopifyql import ShopifyQLClient
import pandas as pd
from datetime import datetime, timezone


SHOP = Config.Shopify.SHOP
CLIENT_ID = Config.Shopify.CLIENT_ID
CLIENT_SECRET = Config.Shopify.CLIENT_SECRET
token = None
token_expires_at = 0.0

class api:
    @staticmethod
    def get_token():
        global token, token_expires_at
        if token and time.time() < token_expires_at - 60:
            return token

        response = requests.post(
            f"https://{SHOP}.myshopify.com/admin/oauth/access_token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        token = data["access_token"]
        token_expires_at = time.time() + data["expires_in"]
        return token

    @staticmethod
    def graphql(query):
        response = requests.post(
            f"https://{SHOP}.myshopify.com/admin/api/2026-04/graphql.json",
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": api.get_token(),
            },
            json={"query": query},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise RuntimeError(payload["errors"])
        return payload["data"]

    @staticmethod
    def extrair_id_shopify(valor):
        if not valor:
            return None
        return str(valor).split("/")[-1]

    @staticmethod
    def extrair_cpf(localized_fields):
        """
        Busca o CPF/CNPJ dentro de localizedFields.edges
        """
        if not localized_fields:
            return None

        edges = localized_fields.get("edges", [])

        for item in edges:
            node = item.get("node", {})
            if node.get("key") == "TAX_CREDENTIAL_BR":
                return node.get("value")

        return None

    @staticmethod
    def limpar_caracteres(cpf):
        if not cpf:
            return None

        # remove tudo que não for número
        cpf = re.sub(r"\D", "", str(cpf))

        return cpf
    
    @staticmethod
    def get_desconto_item(item):
        allocations = item.get("discountAllocations") or []
        total = 0

        for alloc in allocations:
            valor = (
                alloc.get("allocatedAmount", {})
                .get("amount")
            )

            total += float(valor or 0)

        return round(total, 2)
    
    @staticmethod
    def tem_desconto_frete(pedido):
        applications = (
            pedido.get("discountApplications", {})
            .get("nodes", [])
        )

        for app in applications:
            app.get("targetType") == "SHIPPING_LINE"

        return app
    

    @staticmethod
    def tratar_orders_shopify(dados_api):
        registros = []

        orders = dados_api["orders"]["edges"]

        for order_edge in orders:
            pedido = order_edge.get("node", {})

            id_pedido = api.extrair_id_shopify(pedido.get("id"))
            created_at = pedido.get("createdAt")
            updated_at = pedido.get("updatedAt")
            processed_at = pedido.get("processedAt")
            cancelled_at = pedido.get("cancelledAt")

            customer = pedido.get("customer") or {}
            nome_cliente = customer.get("displayName")
            email_cliente = customer.get("email")
            telefone_cliente = api.limpar_caracteres(customer.get("phone"))

            cpf_cliente = api.limpar_caracteres(api.extrair_cpf(pedido.get("localizedFields")))
            forma_pagamento = ", ".join(pedido.get("paymentGatewayNames", []))
            discount_code = pedido.get("discountCode")
            display_financial_status = pedido.get("displayFinancialStatus")
            endereco = pedido.get("shippingAddress", {}) or {}
            rua = endereco.get("address1")
            bairro = endereco.get("address2")
            cidade = endereco.get("city")
            uf = endereco.get("provinceCode")
            cep = api.limpar_caracteres(endereco.get("zip"))
            entrega = pedido.get("shippingLine", {}) or {}
            tipo_entrega = entrega.get("title")
            valor_entrega = float(entrega.get("price") or 0)
            utm_source = pedido.get("referrerDisplayText")
            line_items = pedido.get("lineItems", {}).get("edges", [])
            total_itens = qtd_linhas = len(line_items)
            if total_itens > 0:
                frete_por_item = round(valor_entrega / qtd_linhas, 5)
            else: frete_por_item = 0
            
            for item_edge in line_items:
                item = item_edge.get("node", {})
                line_item_id = api.extrair_id_shopify(item.get("id"))
                produto = item.get("product") or {}
                produto_id = api.extrair_id_shopify(produto.get("id"))
                valor_original = float(item.get("discountedTotal") or 0)
                quantidade = int(item.get("quantity") or 0)
                valor_desconto = api.get_desconto_item(item)

                valor_pago_produto = round(valor_original - valor_desconto, 2)

                valor_liquido = round(valor_pago_produto + frete_por_item, 2)


                registro = {
                    "id": f"{id_pedido}_{line_item_id}",
                    "order_id_shopify": id_pedido,
                    "status": display_financial_status,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "confirmed_at": processed_at,
                    "canceled_at": cancelled_at,
                    "ordered_at": created_at,
                    "product_id": produto_id,
                    "offer_id": api.get_offer_id(produto_id),
                    "product_qty": quantidade,
                    "payment_gross": valor_original,
                    "payment_net": valor_liquido,
                    "payment_discount_value": valor_desconto,
                    "payment_method": forma_pagamento,
                    "coupon_id": discount_code,
                    "trackings_utm_source": utm_source,
                    "contact_doc": cpf_cliente,
                    "contact_name": nome_cliente,
                    "contact_email": email_cliente,
                    "contact_phone": telefone_cliente,
                    "contact_address_zipcode": cep,
                    "contact_address_state": uf,
                    "contact_address_city": cidade,
                    "contact_address_district": bairro,
                    "contact_address_street": rua,
                    "plataforma": "Shopify",
                    "shipping_name": tipo_entrega,
                    "shipping_value": frete_por_item,
                    "upsert_time": datetime.now(timezone.utc)
                }
                registros.append(registro)

        df = pd.DataFrame(registros)

        colunas_numericas = [
        "payment_gross",
        "payment_net",
        "payment_discount_value",
        "shipping_value"
    ]

        for col in colunas_numericas:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df
    
    @staticmethod
    def status_produto(status):
        return status != 'ACTIVE'
        
    @staticmethod
    def tratar_produtos_shopify(dados_api):

        registros = []

        products = dados_api.get("data", dados_api)\
                            .get("products", {})\
                            .get("edges", [])

        for product_edge in products:

            produto = product_edge.get("node", {})

            categoria = produto.get("category") or {}
            produto_id = api.extrair_id_shopify(produto.get("id"))

            registro = {
                "product_id": produto_id,
                "name": produto.get("title"),
                "category_id": api.extrair_id_shopify(categoria.get("id")),
                "category_name": categoria.get("name"),
                "created_at": produto.get("createdAt"),
                "updated_at": produto.get("updatedAt"),

                "is_hidden": api.status_produto(produto.get("status")),
                "product_type": produto.get("productType"),

                "offer_id": api.get_offer_id(produto_id),
                "id_produto": produto_id
            }

            registros.append(registro)

        df = pd.DataFrame(registros)

        return df
    
    @staticmethod
    def getOrdersDF(dataI, dataF):
        dados = api.graphql(query=queries.query_orders(dataI, dataF))
        df = api.tratar_orders_shopify(dados)

        return df

    @staticmethod
    def get_offer_id(product_id):
        if product_id is None:
            return {}

        product_id = str(product_id)

        regras = {
            "10533666554159": "sf_0001",
            "10533686968623": "sf_0002",
            "10533688312111": "sf_0003",
            "10533689458991": "sf_0004",
            "10533691719983": "sf_0005",
            "10535596425519": "sf_0006",
            "10544985833775": "sf_0007",
            "10546406850863": "sf_0008",
            "10546414190895": "sf_0009",
            "10546416615727": "sf_0010",
            "10546419007791": "sf_0011",
            "10546422776111": "sf_0012",
            "10546434900271": "sf_0013",
            "10546660802863": "sf_0014",
            "10546665193775": "sf_0015",
            "10546670240047": "sf_0016",
            "10548538048815": "sf_0017",
            "10548544307503": "sf_0018",
            "10548560265519": "sf_0019",
            "10548577009967": "sf_0020",
            "10548595523887": "sf_0021",
            "10548651426095": "sf_0022",
            "10549514600751": "sf_0023",
            "10549528199471": "sf_0024",
            "10549537800495": "sf_0025",
            "10549548712239": "sf_0026",
            "10549553496367": "sf_0027",
            "10549589344559": "sf_0028",
            "10549599568175": "sf_0029",
            "10549606252847": "sf_0030",
            "10549607268655": "sf_0031",
            "10549691711791": "sf_0032",
            "10549707997487": "sf_0033",
            "10577684300079": "sf_0034",
            "10718089281839": "sf_0035",
            "10726376079663": "sf_0036",
            "10761030795567": "sf_0037",
            "10762816782639": "sf_0038",
            "10768058253615": "sf_0039",
            "10790194544943": "sf_0040",
            "10792158167343": "sf_0041",
            "10799200764207": "sf_0042",
            "10799517925679": "sf_0043",
            "10799520350511": "sf_0044",
            "11595279499567": "sf_0045",
            "11595504091439": "sf_0046",
            "11595635982639": "sf_0047",
            "11595698733359": "sf_0048",
            "11596054855983": "sf_0049",
            "11596787908911": "sf_0050",
            "12002471575855": "sf_0051",
            "12219527037231": "sf_0052",
            "13574553567535": "sf_0053"
        }

        return regras.get(str(product_id))



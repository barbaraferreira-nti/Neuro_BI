import os, json, requests, re
import pandas as pd


class api:
    @staticmethod
    def requisicao(app, ambiente, endpoint, metodo="GET", dados=None, params=None):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configPrincipia.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        baseEndPoint = config[app][f"{ambiente}"]
        token = config[app]["token"]

        headers = {
                "accept": "application/json",
                "Api-Token": token
            }
        
        url = f"{baseEndPoint}{endpoint}"

        response = requests.request(
            metodo,
            url,
            headers=headers,
            json=dados,
            params=params
            )
        
        response.raise_for_status()
        return response.json()
        
    

    @staticmethod
    def getSales(app, periodo=None,ambiente=None,endpoint='sales'):
        dataI, dataF = periodo[0], periodo[1]

        params = {
            "startDate": dataI,
            "endDate": dataF,
            "page": 100,
            "limit": 50
        }

        json = api.requisicao(app=app, ambiente=ambiente, endpoint=endpoint)

        return json
    
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
    def tratar_value(value):
        if value is None:
            return None
        
        return round(float(value) / 100, 2)

    @staticmethod
    def tratarSales(payload):
        address = payload.get("address", {}) or {}
        courses = payload.get("courses", [])
        if isinstance(courses, list):
            course = courses[0] if courses else {}
        elif isinstance(courses, dict):
            course = courses
        else:
            course = {}

        return {

        "id": payload.get("id"),
        "status": payload.get("resumeStatus"),
        "created_at": payload.get("createdAt"),
        "updated_at": payload.get("updatedAt"),
        "signed_at": payload.get("signedDate"),
        "first_installment_paid": payload.get("firstInstallmentPaid"),
        "installments_qty": payload.get("installmentsToApply"),
        "total_value": api.tratar_value(payload.get("totalValue")),
        "up_front_value": payload.get("upfrontValue"),
        "origin": payload.get("origin"),
        "product_type": payload.get("ProductType"),
        "product_id": course.get("id"),
        "product_name": course.get("name"),
        "product_value": api.tratar_value(course.get("price")),
        "product_course_class_id": course.get("courseClassId"),
        "product_course_class_name": course.get("courseClassName"),
        "product_course_class_value": api.tratar_value(course.get("courseClassPrice")),
        "contact_name": payload.get("fullName"),
        "contact_doc": payload.get("CPF"),
        "contact_email": payload.get("email"),
        "contact_phone": api.tratar_telefone(payload.get("phone")),
        "contact_address_zipcode": address.get("zipcode"),
        "contact_address_state": address.get("state"),
        "contact_address_city": address.get("city"),
        "contact_address_district": address.get("district"),
        "contact_address_street": address.get("street"),
        "contact_address_number": address.get("number"),
        "contact_address_complement": address.get("complement"),
        "provi_comission": payload.get("proviComission"),
        "creator": payload.get("creator")

        }
    
    @staticmethod
    def getSalesDF(app, periodo, ambiente):
        response = api.getSales(
            app=app,
            periodo=periodo,
            ambiente=ambiente
        )

        # ajuste aqui conforme a estrutura real da API
        rows = response.get("content", [])

        rows_tratadas = [api.tratarSales(row) for row in rows]

        df = pd.DataFrame(rows_tratadas)
        return df
        

teste = api.getSalesDF(app="PrincipiaApi", periodo=["2025-03-01", "2025-03-25"], ambiente="url_prod")
teste.to_csv(r"C:\Users\Barbara\Downloads\vendas_principia.csv", index=False)
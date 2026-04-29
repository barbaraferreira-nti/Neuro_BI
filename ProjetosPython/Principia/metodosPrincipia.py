import os, json, requests, re
import pandas as pd
from datetime import datetime, timezone
from config import Config


class api:
    @staticmethod
    def requisicao(ambiente, endpoint, metodo="GET", dados=None, params=None):

        if ambiente == "prod":
            baseEndPoint = Config.Principia.URL_PROD
        else:
            baseEndPoint = Config.Principia.URL_DEV
        token = Config.Principia.TOKEN

        headers = {
                "accept": "application/json",
                "Api-Token": token
            }
        
        url = f"{baseEndPoint}{endpoint}"

        response = requests.request(
            method=metodo,
            url=url,
            headers=headers,
            json=dados,
            params=params
            )
        
        response.raise_for_status()
        return response.json()
        
    @staticmethod
    def getSales(periodo=None,ambiente=None,endpoint='sales'):
        dataI, dataF = periodo[0], periodo[1]

        pagina = 1
        limite = 50
        todas_vendas = []

        while True:
            params = {
                "startDate": dataI,
                "endDate": dataF,
                "page": pagina,
                "limit": limite
            }

            response = api.requisicao(
                ambiente=ambiente, 
                endpoint=endpoint, 
                params=params
                )

            content = response.get("content", [])
            paging = response.get("paging", {})

            todas_vendas.extend(content)
            next_page = paging.get("nextPage")

            if not next_page:
                break

            pagina = next_page

        return todas_vendas
    
    @staticmethod
    def getCourses(ambiente=None,endpoint='courses'):
        pagina = 1
        limite = 50
        todos_cursos = []

        while True:
            params = {
                "page": pagina,
                "limit": limite
            }

            response = api.requisicao(
                ambiente=ambiente, 
                endpoint=endpoint, 
                params=params
                )

            content = response.get("content", [])
            paging = response.get("paging", {})

            todos_cursos.extend(content)
            next_page = paging.get("nextPage")

            if not next_page:
                break

            pagina = next_page

        return todos_cursos
    
    @staticmethod
    def getCourseClasses(ambiente):
        pagina = 1
        limite = 50
        todas_classes = []

        while True:
            payload = api.requisicao(
                ambiente=ambiente,
                endpoint="courses",
                params={
                    "page": pagina,
                    "limit": limite
                }
            )

            cursos = payload.get("content", [])

            for curso in cursos:
                course_id = curso.get("id")

                if not course_id:
                    continue

                payload_classes = api.requisicao(
                    ambiente=ambiente,
                    endpoint="course-classes",
                    params={"CourseId": course_id}
                )

                classes = payload_classes.get("content", [])

                for classe in classes:
                    todas_classes.append({
                        "product_id": course_id,
                        "product_name": curso.get("name"),
                        "product_course_class_id": classe.get("id"),
                        "product_course_class_name": classe.get("name"),
                        "product_course_class_active": classe.get("active"),
                        "product_course_class_visible": classe.get("visible"),
                        "product_course_class_price": api.tratar_value(classe.get("priceInCents")),
                        "product_course_class_created_at": api.unix_to_datetime(classe.get("createdAt")),
                        "product_course_class_updated_at": api.unix_to_datetime(classe.get("updatedAt"))
                    })

            paging = payload.get("paging", {})
            next_page = paging.get("nextPage")

            if not next_page:
                break

            pagina = next_page

        return todas_classes

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
    def tratar_value2(value):
        if value is None:
            return None
        
        return round(float(value) / 10, 2)
    
    @staticmethod
    def unix_to_datetime(value):
        if value is None:
            return None

        if isinstance(value, str):
            value = value.strip()
            if value == "":
                return None
            try:
                value = float(value)
            except ValueError:
                return value

        if not isinstance(value, (int, float)):
            return None

        if value > 1e12:
            value = value / 1000

        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()

    @staticmethod
    def padronizar_nome_curso(nome):
        if not nome:
            return None

        nome_lower = nome.lower()
        if "certificação proleia" in nome_lower:
            return "[CURSO] - CERTIFICAÇÃO PROLEIA +"
        elif "proleia" in nome_lower:
            return "[CURSO] - PROLEIA: PROGRAMA LEITURA ESCRITA INTERPRETAÇÃO E APRENDIZAGEM"
        elif "pennsa" in nome_lower:
            return "[CURSO] - PENNSA: PROGRAMA ESPECIALIZADO EM NEUROAPRENDIZAGEM NEUROSABER"
        elif "promais" in nome_lower:
            return "[CURSO] - PROMAIS: PROGRAMA DE RACIOCÍNIO E APRENDIZAGEM MATEMÁTICA"
        return nome

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

        product_id = course.get("id")
        valor_product = payload.get("totalValue")

        regras = api.get_regras_produto(product_id, valor_product)

        return {

        "id": payload.get("id"),
        "status": payload.get("resumeStatus"),
        "created_at": payload.get("createdAt"),
        "updated_at": payload.get("updatedAt"),
        "ordered_at": payload.get("createdAt"),
        "confirmed_at": payload.get("signedDate"),
        "product_id": product_id,
        "offer_id": regras.get("offer_id"),
        "payment_gross": api.tratar_value2(payload.get("upfrontValue")),
        "payment_net": api.tratar_value(payload.get("totalValue")),
        "payment_method": "Boleto Parcelado",
        "installments_qty": payload.get("installmentsToApply"),
        "product_total_value": api.tratar_value(course.get("price")),
        "contact_doc": payload.get("CPF"),
        "contact_name": payload.get("fullName"),
        "contact_email": payload.get("email"),
        "contact_phone": api.tratar_telefone(payload.get("phone")),
        "contact_address_zipcode": address.get("zipcode"),
        "contact_address_state": address.get("state"),
        "contact_address_city": address.get("city"),
        "contact_address_district": address.get("district"),
        "contact_address_street": address.get("street"),
        "contact_address_number": address.get("number"),
        "contact_address_complement": address.get("complement"),
        "principia_course_class_id": course.get("courseClassId"),
        "principia_creator": payload.get("creator"),

        "plataforma": "Principia"
        }
   
    @staticmethod
    def tratarActive(active):
        if active is None:
            return None

        if isinstance(active, str):
            active = active.lower() == "true"

        return not active

    @staticmethod
    def tratarCourses(payload):
        nome = payload.get("name")

        return {
        "product_id": payload.get("id"),
        "name": api.padronizar_nome_curso(nome),
        "marketplace_name": payload.get("ProductType"),
        "created_at": api.unix_to_datetime(payload.get("createdAt")),
        "updated_at": api.unix_to_datetime(payload.get("updatedAt")),
        "is_hidden": api.tratarActive(payload.get("active")),
        "plataforma": "Principia"
        }
    
    @staticmethod
    def getSalesDF(periodo, ambiente):
        rows = api.getSales(
            periodo=periodo,
            ambiente=ambiente
        )

        rows_tratadas = [api.tratarSales(row) for row in rows]

        df = pd.DataFrame(rows_tratadas)
        return df
        

    @staticmethod
    def getCoursesDF(ambiente=None):
        rows = api.getCourses(ambiente=ambiente)

        rows_tratadas = [api.tratarCourses(row) for row in rows]

        df = pd.DataFrame(rows_tratadas)
        return df

    @staticmethod
    def getCourseClassesDF(ambiente):
        rows = api.getCourseClasses(ambiente=ambiente)
        df = pd.DataFrame(rows)
        return df
    
    @staticmethod
    def get_regras_produto(product_id, payment_net):
        if product_id is None or payment_net is None:
            return {}

        product_id = str(product_id)

        regras = {
            "64591": {
                "product_name_padrao": "[CURSO] - PROLEIA: PROGRAMA LEITURA ESCRITA INTERPRETAÇÃO E APRENDIZAGEM",
                "offers": {
                    149700: {
                        "offer_id": "p0001"
                    },
                    129700: {
                        "offer_id": "p0002"
                    }
                }
            },
            "53855": {
                "product_name_padrao": "[CURSO] - PROLEIA: PROGRAMA LEITURA ESCRITA INTERPRETAÇÃO E APRENDIZAGEM",
                "offers": {
                    142700: {
                        "offer_id": "p0003"
                    }
                }
            },
            "54078":{
                "product_name_padrao": "[CURSO] - PENNSA: PROGRAMA ESPECIALIZADO EM NEUROAPRENDIZAGEM NEUROSABER",
                "offers":{
                    94700:{
                        "offer_id":"p0004"
                    }
                }
            },
            "55271":{
                "product_name_padrao": "[CURSO] - CERTIFICAÇÃO PROLEIA +",
                "offers":{
                    499700:{
                        "offer_id":"p0005"
                    }
                }
            },
            "60215":{
                "product_name_padrao": "[CURSO] - PROMAIS: PROGRAMA DE RACIOCÍNIO E APRENDIZAGEM MATEMÁTICA",
                "offers":{
                    99700:{
                        "offer_id":"p0006"
                    }
                }
            },
            "65379":{
                "product_name_padrao": "[CURSO] - PENNSA: PROGRAMA ESPECIALIZADO EM NEUROAPRENDIZAGEM NEUROSABER",
                "offers":{
                    99700:{
                        "offer_id":"p0007"
                    }
                }
            }
        }

        produto = regras.get(product_id, {})

        if not produto:
            return {}

        offer = produto.get("offers", {}).get(payment_net, {})

        return {
            "product_name_padrao": produto.get("product_name_padrao"),
            "offer_id": offer.get("offer_id"),
            "offer_name": offer.get("offer_name")
        }
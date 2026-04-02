import os, json, requests, re
import pandas as pd
from datetime import datetime, timezone


class api:
    @staticmethod
    def requisicao(app, ambiente, endpoint, metodo="GET", dados=None, params=None):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configPrincipia.json")
        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)
        
        baseEndPoint = config[app][ambiente]
        token = config[app]["token"]

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
    def getSales(app, periodo=None,ambiente=None,endpoint='sales'):
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
                app=app, 
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
    def getCourses(app,ambiente=None,endpoint='courses'):
        pagina = 1
        limite = 50
        todos_cursos = []

        while True:
            params = {
                "page": pagina,
                "limit": limite
            }

            response = api.requisicao(
                app=app, 
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
    def getCourseClasses(app, ambiente):
        pagina = 1
        limite = 50
        todas_classes = []

        while True:
            payload = api.requisicao(
                app=app,
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
                    app=app,
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
        if value in (None, "", 0):
            return None
        if value > 1e12:
            value = value/1000
        return datetime.fromtimestamp(value, tz=timezone.utc)

    @staticmethod
    def padronizar_nome_curso(nome):
        if not nome:
            return None

        nome_lower = nome.lower()
        if "certificação proleia" in nome_lower:
            return "[CURSO] - CERTIFICAÇÃO PROLEIA +"
        elif "proleia" in nome_lower:
            return "[CURSO] - PROLEIA: PROGRAMA LEITURA ESCRITA INTERPRETAÇÃO E APRENDIZAGEM"
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

        return {

        "id": payload.get("id"),
        "status": payload.get("resumeStatus"),
        "created_at": payload.get("createdAt"),
        "updated_at": payload.get("updatedAt"),
        "signed_at": payload.get("signedDate"),
        "first_installment_paid": payload.get("firstInstallmentPaid"),
        "installments_qty": payload.get("installmentsToApply"),
        "total_value": api.tratar_value(payload.get("totalValue")),
        "up_front_value": api.tratar_value2(payload.get("upfrontValue")),
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
    def tratarCourses(payload):
        nome = payload.get("name")

        return {

        "product_id": payload.get("id"),
        "product_name": nome,
        "active": payload.get("active"),
        "created_at": api.unix_to_datetime(payload.get("createdAt")),
        "updated_at": api.unix_to_datetime(payload.get("updatedAt")),
        "product_type": payload.get("ProductType"),
        "creator": payload.get("creator"),
        "product_name_padrao": api.padronizar_nome_curso(nome)

        }
    
    @staticmethod
    def getSalesDF(app, periodo, ambiente):
        rows = api.getSales(
            app=app,
            periodo=periodo,
            ambiente=ambiente
        )


        rows_tratadas = [api.tratarSales(row) for row in rows]

        df = pd.DataFrame(rows_tratadas)
        return df
        

    @staticmethod
    def getCourseClassesDF(app, ambiente):
        rows = api.getCourseClasses(
            app=app,
            ambiente=ambiente
        )
        df = pd.DataFrame(rows)
        return df
    
    @staticmethod
    def updateCourse(app, ambiente, course_id, body):
        scriptDir = os.path.dirname(os.path.abspath(__file__))
        configPath = os.path.join(scriptDir, "configPrincipia.json")

        with open(configPath, "r", encoding="utf-8") as f:
            config = json.load(f)

        baseEndPoint = config[app][ambiente]
        token = config[app]["token"]

        headers = {
            "accept": "application/json",
            "Api-Token": token,
            "Content-Type": "application/json"
        }

        url = f"{baseEndPoint}courses/{course_id}"

        
        response = requests.request(
            method="PUT",
            url=url,
            headers=headers,
            json=body,
            timeout=30
        )

        response.raise_for_status()

        return response.json()

    
#teste = api.getCourseClassesDF(app="PrincipiaApi", ambiente="url_prod")
#teste.to_csv(r"C:\Users\Barbara\Downloads\classes_principia.csv", index=False)

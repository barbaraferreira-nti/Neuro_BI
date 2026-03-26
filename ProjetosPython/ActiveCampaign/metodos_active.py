import requests
import os, json, time
import re

class api:
    @staticmethod
    def requisicao(servico, endpoint, metodo='GET', dados=None, params=None, timeout=60, retries=4):
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            config_path = os.path.join(script_dir, "configActive.json")

            with open(config_path, 'r', encoding="utf-8") as f:
                config = json.load(f)

            config_servico = config.get(servico)
            if not config_servico:
                raise ValueError(f"Serviço {servico} não encontrado no configActive.json")

            # Montar a URL e headers
            url_base = config_servico.get("url", "")
            headers_config = config_servico.get("headers", {})
            url = url_base + endpoint

            # Montar o headers padrão
            headers = {
                "accept": headers_config.get("accept", "application/json"),
                "Api-Token": headers_config.get("token", "")
            }

            for i in range(retries):
                try:
                    # Requisição
                    response = requests.request(
                        metodo.upper(),
                        url,
                        headers=headers,
                        json=dados,
                        params=params
                    )

                    response.raise_for_status()
                    return response.json()
                
                except requests.exceptions.Timeout:
                    print(f"Timeout na tentativa {i + 1} para a URL: {url}")

                    if i < retries - 1:
                        time.sleep(2 ** i)
                        continue
                    return None
                
                except requests.exceptions.RequestException as e:
                    print(f"Erro HTTP na tentativa {i + 1}: {e}")

                    if i < retries - 1:
                        time.sleep(2 ** i)
                        continue
                    return None
        except Exception as e:
            print(f'Erro ao conectar com API: {e}')
            return None

    @staticmethod
    def limparTelefone(phone):

        if not phone:
            return None

        phone = re.sub(r'\D', '', phone)  # remove tudo que não for número

        return phone

    @staticmethod
    def getContacts(listid=None, listname=None):
        endpoint = "contacts"

        allContacts = []
        offset = 0
        limit = 100

        while True:
            params = {
                "limit": limit,
                "offset": offset,
                "listid": listid
                }

            response = api.requisicao(servico="ApiActive", endpoint=endpoint, params=params)

            if response is None:
                print((f"Falha ao buscar contatos no offset {offset}."))
                break

            contacts = response.get("contacts", [])

            if not contacts:
                break

            for c in contacts:
                contact_id =  c.get("id")
                sdate = api.getDataEntradaLista(contact_id=contact_id, listid=listid)
                phone = api.limparTelefone(c.get("phone"))

                contato = {
                    "contact_id": contact_id,
                    "email": c.get("email"),
                    "phone": phone,
                    "firstname": c.get("firstName"),
                    "lastname": c.get("lastName"),
                    "listid": listid,
                    "listname": listname,
                    "data": sdate,
                    "source": "Email"
                }

                allContacts.append(contato)
                

            offset += limit

        return allContacts

    @staticmethod
    def getMapContactId(listid=None):
        contact_ids = api.getContacts(listid=listid)

        mapa_id = []

        for cl in contact_ids:
            contact_id = str(cl.get("contact_id")) if cl.get("contact_id") is not None else None
            mapa_id.append(contact_id)


        return mapa_id
        

    @staticmethod
    def getDataEntradaLista(contact_id, listid):

        endpoint = f"contacts/{contact_id}/contactLists"

        response = api.requisicao(
            servico="ApiActive",
            endpoint=endpoint
            )

        if response is None:
            print(f"Falha ao buscar contactLists para o contact_id {contact_id}.")
        contact_lists_response = response.get("contactLists", [])

        if not contact_lists_response:
            print(f"Response sem dados.")

        for cl in contact_lists_response:

            if str(cl.get("list")) != str(listid):
                continue

            sdate = cl.get("sdate")

        return sdate
        
    



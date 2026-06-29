import requests, json
import pandas as pd
from config import Config
import time


class api:
    @staticmethod
    def gerar_token_jacad():
        url = f"{Config.Jacad.URL}/auth/token"

        response = requests.post(
            url,
            headers={"token": Config.Jacad.TOKEN}
        )

        response.raise_for_status()
        return response.json()["token"]
    
    @staticmethod
    def request_jacad(metodo=None, endpoint=None, params=None, payload=None):
        TOKEN = api.gerar_token_jacad()
        BASE_URL = Config.Jacad.URL.rstrip("/")

        headers = {
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        }

        endpoint = endpoint.lstrip("/")
        url = f"{BASE_URL}/{endpoint}"

        if metodo == "GET":
            response = requests.get(url, headers=headers, params=params)

        elif metodo == "POST":
            response = requests.post(url, headers=headers, params=params, json=payload)

        else:
            raise ValueError("Método inválido.")

        if response.status_code != 200:
            print("Status:", response.status_code)
            print("URL:", response.url)
            print("Resposta:", response.text)

        response.raise_for_status()
        return response.json()
        
    @staticmethod
    def getContratos(params=None):
        all_contratos = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 100

            data = api.request_jacad(
                metodo="GET",
                endpoint="financeiro/contratos-matricula",
                params=params_page
            )

            contratos = data.get("elements", [])
            all_contratos.extend(contratos)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(f"Página {current_page + 1}/{total_pages} - {len(contratos)} contratos")

            if not contratos or current_page + 1 >= total_pages:
                break

            page += 1

        return all_contratos
    
    @staticmethod
    def tratarContratos(params=None):
        dados = api.getContratos(params=params)

        rows = []

        for i in dados:
            rows.append({
                "id_contrato": i.get("idContrato"),
                "id_aluno": i.get("idAluno"),
                "id_curso": i.get("idCursoBase"),
                "id_turma": i.get("idTurma"),
                "id_matricula": i.get("idMatricula"),
                "status_matricula": i.get("statusMatricula"),
                "id_periodo_letivo": i.get("idPeriodoLetivo"),
                "status_contrato": i.get("status"),
                "id_tabela_preco": i.get("idTabelaPreco"),
                "id_plano_pagamento": i.get("idPlanoPagamento"),
                "desconto": i.get("descontoConcedido"),
                "data_incio_vigencia": i.get("dataVigenciaInicio"),
                "data_fim_vigencia": i.get("dataVigenciaTermino"),
                "data_contrato": i.get("dataContrato"),
                "data_aceite_contrato": i.get("dataAceiteContrato"),
                "id_org": i.get("idOrg")
            })
        
        return pd.DataFrame(rows)
    
    @staticmethod
    def getPlanosPagamento(params=None):
        data = api.request_jacad(
                metodo="GET",
                endpoint="financeiro/planos-pagamento",
                params=params
            )
        
        return data

    @staticmethod
    def tratarPlanosPagamento(params=None):
        dados = api.getPlanosPagamento(params=params)
        rows = []
        if not dados:
            return pd.DataFrame(rows)
        
        for plano in dados:
            servicos = plano.get("servicos", []) or [None]

            servico_mensalidade = next(
                (
                    servico for servico in servicos
                    if str(servico.get("idServico")) == "3"
                ),
                None
                )
            if not servico_mensalidade:
                continue

            rows.append({
                    "id_plano_pagamento": plano.get("idPlanoPagamento"),
                    "descricao": plano.get("descricao"),
                    "status": plano.get("status"),
                    "id_plano_pagamento_servico": servico_mensalidade.get("idPlanoPagamentoServico"),
                    "id_servico": servico_mensalidade.get("idServico"),
                    "numero_parcelas": servico_mensalidade.get("numeroParcelas"),
                    "valor_total": servico_mensalidade.get("valor")
                })

        return pd.DataFrame(rows)

    @staticmethod
    def getAlunos(params=None):
        all_alunos = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 500

            data = api.request_jacad(
                metodo="GET",
                endpoint="academico/alunos",
                params=params_page
            )

            alunos = data.get("elements", [])
            all_alunos.extend(alunos)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(f"Página {current_page + 1}/{total_pages} - {len(alunos)} alunos")

            if not alunos or current_page + 1 >= total_pages:
                break

            page += 1

        return all_alunos
    
    @staticmethod
    def tratarAlunos(params=None):
        dados = api.getAlunos(params=params)

        if not dados:
            return pd.DataFrame()

        df = pd.DataFrame(dados)

        if df.empty:
            return df
                
        return df[
            [   "idAluno",
                "idPerfil",
                "nome",
                "ra",
                "cpf",
                "email",
                "sexo",
                "dataNascimento",
                "idOrg",
                "dataCriacao",
                "dataAlteracao"
            ]
        ]
        

    @staticmethod
    def getCursos(params=None):
        all_cursos = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 100

            data = api.request_jacad(
                metodo="GET",
                endpoint="academico/cursos-base/",
                params=params_page
            )

            cursos = data.get("elements", [])
            all_cursos.extend(cursos)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(f"Página {current_page + 1}/{total_pages} - {len(cursos)} cursos")

            if not cursos or current_page + 1 >= total_pages:
                break

            page += 1

        return all_cursos
    
    @staticmethod
    def tratarCursos(params=None):

        dados = api.getCursos(params=params)
        rows = []

        for curso in dados:

            rows.append({
                "id_curso": curso.get("idCursoBase"),
                "nome": curso.get("nomeImpressao"),
                "nome_reduzido": curso.get("nomeReduzido"),
                "status": curso.get("status"),
                "org": curso.get("org"),
                "org_nome_fantasia": curso.get("orgNomeFantasia"),
                "modalidade": curso.get("modalidade")
                })

        return pd.DataFrame(rows)
    
    @staticmethod
    def getPeriodosLetivos(params=None):
        registros = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 100

            data = api.request_jacad(
                metodo="GET",
                endpoint="academico/periodos-letivos/",
                params=params_page
            )

            turmas = data.get("elements", [])
            registros.extend(turmas)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(f"Página {current_page + 1}/{total_pages} - {len(turmas)} turmas")

            if not turmas or current_page + 1 >= total_pages:
                break

            page += 1

        return registros
    
    @staticmethod
    def tratarPeriodosLetivos(params=None):
        dados = api.getPeriodosLetivos(params=params)
        rows = []

        for periodo in dados:
            rows.append({
                "id_periodo": periodo.get("idPeriodoLetivo"),
                "nome": periodo.get("descricao"),
                "id_org": periodo.get("idOrg"),
                "nome_especial": periodo.get("descricaoEspecial"),
                "data_inicio": periodo.get("dataInicio"),
                "data_fim": periodo.get("dataTermino"),
                "status": periodo.get("situacao"),
                "periodo_especial": periodo.get("periodoEspecial"),
                "ano": periodo.get("ano"),
                "periodo_atual": periodo.get("periodoAtual")
            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def getTurmas(params=None):
        all_turmas = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 100

            data = api.request_jacad(
                metodo="GET",
                endpoint="academico/turmas",
                params=params_page
            )

            turmas = data.get("elements", [])
            all_turmas.extend(turmas)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(f"Página {current_page + 1}/{total_pages} - {len(turmas)} turmas")

            if not turmas or current_page + 1 >= total_pages:
                break

            page += 1

        return all_turmas
    
    @staticmethod
    def tratarTurmas(params=None):
        dados = api.getTurmas(params=params)
        rows = []

        for turma in dados:
            rows.append({
            "id_turma": turma.get("idTurma"),
            "nome": turma.get("turmaNome"),
            "nome_reduzido": turma.get("turmaNomeRed"),
            "id_perido_letivo": turma.get("turmaIdPeriodoLetivo"),
            "id_matriz": turma.get("turmaIdMatriz"),
            "id_curso": turma.get("turmaIdCurso"),
            "perido": turma.get("turmaPeriodoItem"),
            "turno": turma.get("turmaTurno"),
            "status": turma.get("turmaStatus"),
            "data_inicio": turma.get("turmaDataInicio"),
            "data_fim": turma.get("turmaDataFim"),
            "id_org": turma.get("idOrg")
            })

            return pd.DataFrame(rows)

    @staticmethod
    def getDisciplinasMatriz(id_curso,params=None):
        registros = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 100

            data = api.request_jacad(
                metodo="GET",
                endpoint=f"academico/matrizes/{id_curso}/disciplinas",
                params=params_page
            )

            turmas = data.get("elements", [])
            registros.extend(turmas)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(f"Página {current_page + 1}/{total_pages} - {len(turmas)} turmas")

            if not turmas or current_page + 1 >= total_pages:
                break

            page += 1

        return registros
    
    @staticmethod
    def tratarDisciplinas(id_curso=None, params=None):
        dados = api.getDisciplinasMatriz(id_curso=id_curso, params=params)
        rows = []

        for matriz in dados:
            rows.append({
                "id_disciplina": matriz.get("idDisciplina"),
                "nome": matriz.get("disciplinaDescricao"),
                "id_disciplina_pai": matriz.get("idDisciplinaPai"),
                "nome_disciplina_pai": matriz.get("disciplinaPaiDescricao"),
                "nome_reduzido": matriz.get("disciplinaNomeReduzido"),
                "nome_reduzido_disciplina_pai": matriz.get("disciplinaPaiNomeReduzido"),
                "id_curso": matriz.get("disciplinaIdCurso"),
                "tipo": matriz.get("disciplinaTipo"),
                "tipo_avaliacao": matriz.get("disciplinaTipoAvaliaca"),
                "modalidade": matriz.get("disciplinaModalidad"),
                "id_periodo": matriz.get("periodoCodigo"),
                "periodo": matriz.get("periodoNome"),
                "carga_horaria": matriz.get("cargaHoraria")
            })

        return pd.DataFrame(rows)

    @staticmethod
    def getCursosIngresso(params=None):
        registros = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 500

            data = api.request_jacad(
                metodo="GET",
                endpoint="academico/cursos-ingressos/",
                params=params_page
            )

            # Caso a API retorne lista diretamente
            if isinstance(data, list):
                registros.extend(data)
                break

            cursos = data.get("elements", [])
            registros.extend(cursos)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(
                f"Página {current_page + 1}/{total_pages} - {len(cursos)} alunos"
            )

            if not cursos or current_page + 1 >= total_pages:
                break

            page += 1

        return registros
    
    @staticmethod
    def tratarCursosIngressos(params=None):
        dados = api.getCursosIngresso(params=params)
        rows = []

        for i in dados:
            rows.append({
                "id_aluno_curso_ingresso": i.get("idAlunoCursoIngresso"),
                "id_aluno": i.get("idAluno"),
                "status_curso": i.get("statusCurso"),
                "turno": i.get("turno"),
                "id_curso": i.get("idCurso"),
                "id_matriz": i.get("idMatriz"),
                "id_turma": i.get("idTurmaBase"),
                "matricula": i.get("numeroMatricula"),
                "data_cadstro": i.get("dataCadastro"),
                "data_inicio": i.get("dataInicioCurso"),
                "data_conclusao": i.get("dataConclusao"),
                "data_colacao": i.get("dataColacao"),
                "data_trancamento": i.get("dataTrancamento"),
                "data_expedicao_diploma": i.get("dataExpedicaoDiploma")

            })

        return pd.DataFrame(rows)
    
    @staticmethod
    def getConcluintes(dataI, dataF, idCurso):

        dados = api.getCursosIngresso(
            params={
                "idCurso": idCurso
            }
        )

        if not dados:
            return pd.DataFrame()

        df = pd.DataFrame(dados)

        if df.empty:
            return df

        df["dataConclusao"] = pd.to_datetime(
            df["dataConclusao"],
            errors="coerce"
        )

        df = df[
            (df["statusCurso"].astype(str).str.lower() == "concluido") &
            (df["dataConclusao"] >= pd.to_datetime(dataI)) &
            (df["dataConclusao"] <= pd.to_datetime(dataF))
        ]

        return df[
            [   "idAluno",
                "idPerfil",
                "nome",
                "ra",
                "cpf",
                "statusCurso",
                "curso",
                "idCurso",
                "dataCadastro",
                "dataConclusao",
                "numeroMatricula",
                "idOrg",
                "status"
            ]
        ]
    
    @staticmethod
    def getMatriculas(params=None):
        registros = []
        page = 0

        while True:
            params_page = params.copy() if params else {}

            params_page["currentPage"] = page
            params_page["pageSize"] = 500

            data = api.request_jacad(
                metodo="GET",
                endpoint="academico/matriculas",
                params=params_page
            )

            # Caso a API retorne lista diretamente
            if isinstance(data, list):
                registros.extend(data)
                break

            cursos = data.get("elements", [])
            registros.extend(cursos)

            page_info = data.get("page", {})
            total_pages = page_info.get("totalPages", 0)
            current_page = page_info.get("currentPage", page)

            print(
                f"Página {current_page + 1}/{total_pages} - {len(cursos)} alunos"
            )

            if not cursos or current_page + 1 >= total_pages:
                break

            page += 1

        return registros  

    @staticmethod
    def tratarMatriculas(params=None):
        dados = api.getMatriculas(params=params)
        rows = []

        for i in dados:
            rows.append({
                "id_matricula": i.get("idMatricula"),
                "id_aluno": i.get("idAluno"),
                "id_aluno_curso_ingresso": i.get("idAlunoCursoIngresso"),
                "id_curso": i.get("idCursoBase"),
                "id_turma": i.get("idTurma"),
                "id_matriz": i.get("idCursoMatriz"),
                "email_institucional": i.get("alunoEmailInstitucional"),
                "data_matricula": i.get("dataMatricula"),
                "data_ativacao": i.get("dataAtivacao"),
                "data_trancamento": i.get("dataTrancamento"),
                "data_cadastro": i.get("dataCadastro"),
                "contrato_entregue": i.get("contratoEntregue"),
                "data_aceite_contrato": i.get("dataAceiteContrato"),
                "id_contrato": i.get("idContrato"),
                "data_criacao": i.get("dataCriacao"),
                "data_alteracao": i.get("dataAlteracao")
            })     

#dados = api.getPlanosPagamento(params={"idOrg": "0", "idPlanoPagamento": "55"})
#print(dados)


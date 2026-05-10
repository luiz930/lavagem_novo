import json
import os
import sqlite3
import tempfile
import time
import unittest
from unittest.mock import patch
import zipfile

from flask import render_template_string, request, session

import app as app_module
from domains import changelog as changelog_domain
from domains.clientes import consultar_sincronizacoes_clientes
from domains.sync_clientes import excluir_sincronizacao_cliente


class PersistentCompatConnection:
    def __init__(self, conn):
        self._conn = conn
        self.backend = "sqlite"

    def cursor(self):
        return app_module.CursorCompat(self._conn.cursor(), self.backend)

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return None

    def __getattr__(self, name):
        return getattr(self._conn, name)


class AppRegressionTests(unittest.TestCase):
    def setUp(self):
        self.client = app_module.app.test_client()
        self.testing_anterior = app_module.app.config.get("TESTING", False)
        app_module.app.config["TESTING"] = True

    def tearDown(self):
        app_module.app.config["TESTING"] = self.testing_anterior

    def test_adicionar_coluna_se_preciso_nao_desfaz_colunas_anteriores_no_sqlite(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        compat = app_module.CursorCompat(conn.cursor(), "sqlite")
        compat.execute("CREATE TABLE usuarios (id INTEGER PRIMARY KEY AUTOINCREMENT)")
        compat.execute("CREATE TABLE clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER)")

        app_module.adicionar_coluna_se_preciso(compat, "usuarios", "empresa_id INTEGER DEFAULT 1")
        app_module.adicionar_coluna_se_preciso(compat, "clientes", "empresa_id INTEGER DEFAULT 1")

        compat.execute("PRAGMA table_info(usuarios)")
        colunas_usuarios = {row["name"] for row in compat.fetchall()}
        compat.execute("PRAGMA table_info(clientes)")
        colunas_clientes = {row["name"] for row in compat.fetchall()}
        conn.close()

        self.assertIn("empresa_id", colunas_usuarios)
        self.assertIn("empresa_id", colunas_clientes)

    def test_excluir_sincronizacao_cliente_marca_como_excluida_sem_apagar(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE sincronizacoes_clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                nome TEXT,
                ativo INTEGER DEFAULT 1,
                proximo_sync_em TEXT,
                ultimo_status TEXT,
                ultima_mensagem TEXT,
                atualizado_em TEXT,
                excluido_em TEXT,
                excluido_por TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE historico_lavagens_sync (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                sync_id INTEGER
            )
            """
        )
        c.execute(
            "INSERT INTO sincronizacoes_clientes (empresa_id, nome, ativo, proximo_sync_em) VALUES (1, 'Planilha', 1, '2026-05-06T10:00:00')"
        )
        sync_id = c.lastrowid
        c.execute("INSERT INTO historico_lavagens_sync (empresa_id, sync_id) VALUES (1, ?)", (sync_id,))

        removidos = excluir_sincronizacao_cliente(c, sync_id, 1, "2026-05-06T20:00:00", "admin")

        self.assertEqual(removidos, 1)
        c.execute("SELECT * FROM sincronizacoes_clientes WHERE id=?", (sync_id,))
        sync = c.fetchone()
        self.assertIsNotNone(sync)
        self.assertEqual(sync["ativo"], 0)
        self.assertEqual(sync["ultimo_status"], "EXCLUIDA")
        self.assertEqual(sync["excluido_em"], "2026-05-06T20:00:00")
        self.assertEqual(sync["excluido_por"], "admin")
        c.execute("SELECT COUNT(*) AS total FROM historico_lavagens_sync WHERE sync_id=?", (sync_id,))
        self.assertEqual(c.fetchone()["total"], 0)
        self.assertEqual(consultar_sincronizacoes_clientes(c, 1), [])
        conn.close()

    def test_excluir_sync_clientes_limpa_cache_da_tela_clientes(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE sincronizacoes_clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                nome TEXT,
                ativo INTEGER DEFAULT 1,
                proximo_sync_em TEXT,
                ultimo_status TEXT,
                ultima_mensagem TEXT,
                atualizado_em TEXT,
                excluido_em TEXT,
                excluido_por TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE historico_lavagens_sync (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                sync_id INTEGER
            )
            """
        )
        c.execute("INSERT INTO sincronizacoes_clientes (empresa_id, nome, ativo) VALUES (1, 'Planilha', 1)")
        sync_id = c.lastrowid
        wrapper = PersistentCompatConnection(conn)
        app_module.CLIENTES_CONTEXT_CACHE["testado_em"] = 9999999999.0
        app_module.CLIENTES_CONTEXT_CACHE["chave"] = "admin|1|"
        app_module.CLIENTES_CONTEXT_CACHE["resultado"] = {"sincronizacoes": [{"id": sync_id}]}

        with app_module.app.test_request_context(f"/clientes/sincronizacao/{sync_id}/excluir", method="POST"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar", return_value=wrapper):
                resposta = app_module.excluir_sync_clientes(sync_id)

        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(app_module.CLIENTES_CONTEXT_CACHE["testado_em"], 0.0)
        self.assertEqual(app_module.CLIENTES_CONTEXT_CACHE["chave"], "")
        self.assertIsNone(app_module.CLIENTES_CONTEXT_CACHE["resultado"])
        conn.close()

    def _criar_banco_admin_memoria(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE configuracao_backup (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                frequencia TEXT,
                tipo_backup TEXT,
                retencao_arquivos INTEGER,
                destino_externo_ativo INTEGER,
                destino_externo_tipo TEXT,
                destino_externo_pasta TEXT,
                destino_externo_drive_folder_id TEXT,
                atualizado_em TEXT
            )
            """
        )
        c.execute(
            "CREATE UNIQUE INDEX idx_configuracao_backup_empresa_unique ON configuracao_backup(empresa_id)"
        )
        c.execute(
            """
            CREATE TABLE manutencao_arquivos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                ultimo_executado_em TEXT,
                ultima_mensagem TEXT,
                ultimo_resultado_json TEXT
            )
            """
        )
        c.execute(
            "CREATE UNIQUE INDEX idx_manutencao_arquivos_empresa_unique ON manutencao_arquivos(empresa_id)"
        )
        c.execute(
            """
            CREATE TABLE integracao_fiscal (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                tipo_integracao TEXT,
                provedor_nome TEXT,
                ambiente TEXT,
                municipio_codigo_ibge TEXT,
                municipio_nome TEXT,
                uf TEXT,
                endpoint_emissao TEXT,
                endpoint_consulta TEXT,
                endpoint_cancelamento TEXT,
                autenticacao_tipo TEXT,
                usuario_api TEXT,
                senha_api TEXT,
                client_id TEXT,
                client_secret TEXT,
                token_api TEXT,
                token_url TEXT,
                certificado_tipo TEXT,
                certificado_arquivo TEXT,
                certificado_senha TEXT,
                serie_rps TEXT,
                serie_nfe TEXT,
                ativo INTEGER,
                ultimo_status TEXT,
                ultima_mensagem TEXT,
                atualizado_em TEXT
            )
            """
        )
        c.execute(
            "CREATE UNIQUE INDEX idx_integracao_fiscal_empresa_unique ON integracao_fiscal(empresa_id)"
        )
        conn.commit()
        return conn

    def _criar_banco_servico_fluxo_memoria(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                status TEXT,
                etapa_atual TEXT,
                etapa_atual_iniciada_em TEXT,
                lavagem_iniciada_em TEXT,
                finalizacao_iniciada_em TEXT,
                lavagem_segundos INTEGER DEFAULT 0,
                finalizacao_segundos INTEGER DEFAULT 0
            )
            """
        )
        conn.commit()
        return conn

    def _criar_banco_fotos_memoria(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE fotos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                servico_id INTEGER NOT NULL,
                tipo TEXT,
                caminho TEXT,
                criado_em TEXT,
                usuario TEXT,
                usuario_nome TEXT,
                tamanho_bytes INTEGER,
                largura INTEGER,
                altura INTEGER,
                arquivo_blob BLOB,
                mime_type TEXT,
                arquivo_nome TEXT
            )
            """
        )
        conn.commit()
        return conn

    def _criar_banco_clientes_notificacoes_memoria(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                nome TEXT NOT NULL,
                telefone TEXT,
                placa_principal TEXT,
                data_nascimento TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE veiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                placa TEXT NOT NULL,
                modelo TEXT,
                cor TEXT,
                cliente_id INTEGER
            )
            """
        )
        c.execute(
            """
            CREATE TABLE notificacoes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER NOT NULL DEFAULT 1,
                mensagem TEXT,
                tipo TEXT,
                categoria TEXT,
                referencia TEXT,
                lida INTEGER DEFAULT 0,
                criada_em TEXT
            )
            """
        )
        conn.commit()
        return conn

    def test_login_missing_fields_returns_error_message(self):
        with patch.object(app_module, "csrf_protection_ativa", return_value=False), \
             patch.object(app_module, "INIT_DB_EXECUTADO", True):
            response = self.client.post("/login", data={"usuario": "", "senha": ""})

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Informe usuario e senha.", response.data)

    def test_login_exibe_versao_configurada_sem_sessao(self):
        with patch.object(app_module, "INIT_DB_EXECUTADO", True), \
             patch.object(app_module, "obter_versao_sistema", return_value="Versao: 0.13.5 - Beta") as versao_mock:
            response = self.client.get("/login")

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Versao: 0.13.5 - Beta", response.data)
        versao_mock.assert_any_call(permitir_sem_sessao=True)

    def test_uploads_de_imagem_nao_forcam_camera_no_mobile(self):
        caminhos = [
            "templates/checklist_finalizacao.html",
            "templates/editar_atendimento.html",
            "templates/index.html",
            "templates/index2.html",
            "templates/painel.html",
            "templates/components/service_card.html",
        ]

        for caminho in caminhos:
            with self.subTest(caminho=caminho):
                with open(caminho, encoding="utf-8") as arquivo:
                    conteudo = arquivo.read()
                self.assertNotIn("capture=", conteudo)

    def test_configuracoes_requires_login(self):
        with patch.object(app_module, "INIT_DB_EXECUTADO", True):
            response = self.client.get("/configuracoes")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers.get("Location", ""))

    def test_base_nao_mostra_busca_global_no_header(self):
        template = (
            '{% extends "base.html" %}'
            '{% block title %}Teste{% endblock %}'
            '{% block titulo %}Teste{% endblock %}'
            '{% block subtitulo %}Config{% endblock %}'
        )
        with app_module.app.test_request_context("/configuracoes/meu-acesso"):
            session["usuario"] = "dev"
            session["usuario_perfil"] = "desenvolvedor"
            session["empresa_id"] = 1
            html_config = render_template_string(template)
        with app_module.app.test_request_context("/clientes"):
            session["usuario"] = "dev"
            session["usuario_perfil"] = "desenvolvedor"
            session["empresa_id"] = 1
            html_clientes = render_template_string(template)

        self.assertNotIn('<form class="global-search"', html_config)
        self.assertNotIn('id="globalSearchInput"', html_config)
        self.assertNotIn('<form class="global-search"', html_clientes)
        self.assertNotIn('id="globalSearchInput"', html_clientes)

    def test_index_avatar_usuario_abre_meu_acesso(self):
        with open(os.path.join(app_module.app.root_path, "templates", "index.html"), encoding="utf-8") as arquivo:
            conteudo = arquivo.read()

        self.assertIn('avatar.href = "/configuracoes/meu-acesso"', conteudo)
        self.assertIn('avatar.setAttribute("aria-label", "Abrir minhas configuracoes de usuario")', conteudo)

    def test_index_carrega_notificacoes_e_agenda_sem_clique(self):
        with open(os.path.join(app_module.app.root_path, "templates", "index.html"), encoding="utf-8") as arquivo:
            conteudo = arquivo.read()

        self.assertIn("function iniciarCarregamentoCabecalhoOperacional()", conteudo)
        self.assertIn("carregarNotificacoes(true);", conteudo)
        self.assertIn("carregarAgendaRetorno(true);", conteudo)
        self.assertIn("setInterval(() => carregarNotificacoes(true), NOTIFICACOES_REFRESH_MS)", conteudo)
        self.assertIn("setInterval(() => carregarAgendaRetorno(true), AGENDA_REFRESH_MS)", conteudo)

    def test_changelog_monta_links_github_automaticos(self):
        commit_hash = "1234567890abcdef1234567890abcdef12345678"

        def git_fake(_repo, *args):
            if args[:3] == ("remote", "get-url", "origin"):
                return "https://github.com/luiz930/lavagem_novo.git"
            if args[:2] == ("branch", "--show-current"):
                return "main"
            if args[:3] == ("rev-parse", "--short", "HEAD"):
                return "1234567"
            if args[:4] == ("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"):
                return "origin/main"
            if args[:3] == ("rev-list", "--left-right", "--count"):
                return "0\t0"
            if args and args[0] == "log":
                return f"{commit_hash}\x1f2026-05-10\x1fRemove busca global das configuracoes"
            return ""

        changelog_domain.CHANGELOG_CACHE["payload"] = None
        with tempfile.TemporaryDirectory(prefix="changelog_git_") as pasta, \
             patch.object(changelog_domain, "_run_git_command", side_effect=git_fake):
            contexto = changelog_domain.carregar_contexto_changelog(pasta, versao_atual="teste")

        self.assertEqual(contexto["resumo"]["github_url"], "https://github.com/luiz930/lavagem_novo")
        self.assertEqual(contexto["resumo"]["github_upstream"], "origin/main")
        self.assertTrue(contexto["resumo"]["github_sincronizado"])
        self.assertEqual(
            contexto["commits_recentes"][0]["url"],
            f"https://github.com/luiz930/lavagem_novo/commit/{commit_hash}",
        )

    def test_configuracoes_site_get_renders_for_logged_admin(self):
        with app_module.app.test_request_context("/configuracoes/site", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 2
            session["senha_alteracao_obrigatoria"] = False
            with patch.object(app_module, "preparar_rotinas_interface_logada"), \
                 patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                 patch.object(app_module, "recurso_liberado_por_plano", return_value=True), \
                 patch.object(app_module, "obter_configuracao_empresa", return_value=app_module.empresa_snapshot_padrao()), \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                response = app_module.configuracoes_site()

        self.assertEqual(response, "ok")
        render_mock.assert_called_once()

    def test_status_sistema_renderiza_para_admin_sem_consultas_pesadas(self):
        status = {
            "gerado_em": "2026-05-06T10:00:00",
            "itens": [],
            "ultimo_erro": {},
            "resumo": {"ok": True, "falhas": []},
        }

        with app_module.app.test_request_context("/status-sistema", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                 patch.object(app_module, "montar_status_sistema_dono", return_value=status) as montar, \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                response = app_module.pagina_status_sistema()

        self.assertEqual(response, "ok")
        montar.assert_called_once()
        render_mock.assert_called_once()

    def test_auto_suporte_limpa_caches_com_acao_segura(self):
        app_module.CLIENTES_CONTEXT_CACHE["testado_em"] = 9999999999.0
        app_module.CLIENTES_CONTEXT_CACHE["chave"] = "admin|1|"
        app_module.CLIENTES_CONTEXT_CACHE["resultado"] = {"clientes": [1]}

        with app_module.app.test_request_context("/api/auto-suporte/acao", method="POST", json={"acao": "limpar_caches"}):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                 patch.object(app_module, "registrar_auditoria", return_value=None), \
                 patch.object(app_module, "salvar_notificacao", return_value=True), \
                 patch.object(app_module, "registrar_evento_telemetria_app", return_value=True), \
                 patch.object(app_module, "status_auto_suporte", return_value={"ok": True, "falhas": []}):
                response = app_module.api_auto_suporte_acao()

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(app_module.CLIENTES_CONTEXT_CACHE["testado_em"], 0.0)
        self.assertIsNone(app_module.CLIENTES_CONTEXT_CACHE["resultado"])

    def test_auto_suporte_rejeita_acao_nao_permitida(self):
        with app_module.app.test_request_context("/api/auto-suporte/acao", method="POST", json={"acao": "editar_codigo"}):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True):
                response = app_module.api_auto_suporte_acao()

        resposta, status_code = response
        self.assertEqual(status_code, 400)
        self.assertIn("nao permitida", resposta.get_json()["erro"])

    def test_auto_suporte_exige_confirmacao_para_acao_sensivel(self):
        with app_module.app.test_request_context("/api/auto-suporte/acao", method="POST", json={"acao": "limpar_todos_erros"}):
            session["usuario"] = "dev"
            session["usuario_perfil"] = "desenvolvedor"
            session["empresa_id"] = 1
            with patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True):
                response = app_module.api_auto_suporte_acao()

        resposta, status_code = response
        self.assertEqual(status_code, 400)
        self.assertIn("Confirmacao obrigatoria", resposta.get_json()["erro"])

    def test_auto_suporte_admin_nao_executa_acao_tecnica_sensivel(self):
        with app_module.app.test_request_context(
            "/api/auto-suporte/acao",
            method="POST",
            json={"acao": "limpar_todos_erros", "confirmacao": "LIMPAR TODOS OS ERROS"},
        ):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            session["empresa_id"] = 1
            response = app_module.api_auto_suporte_acao()

        resposta, status_code = response
        self.assertEqual(status_code, 400)
        self.assertIn("somente para o desenvolvedor", resposta.get_json()["erro"])

    def test_auto_suporte_executa_acao_sensivel_com_confirmacao_correta(self):
        with tempfile.TemporaryDirectory(prefix="auto_suporte_confirma_") as pasta:
            caminho_erros = os.path.join(pasta, "erros.json")
            caminho_historico = os.path.join(pasta, "historico.json")
            with app_module.app.test_request_context(
                "/api/auto-suporte/acao",
                method="POST",
                json={"acao": "limpar_todos_erros", "confirmacao": "LIMPAR TODOS OS ERROS"},
            ):
                session["usuario"] = "dev"
                session["usuario_perfil"] = "desenvolvedor"
                session["empresa_id"] = 1
                with patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                     patch.object(app_module, "ERROS_PRODUCAO_ARQUIVO", caminho_erros), \
                     patch.object(app_module, "AUTO_SUPORTE_HISTORICO_ARQUIVO", caminho_historico), \
                     patch.object(app_module, "registrar_incidente_auto_suporte"), \
                     patch.object(app_module, "status_auto_suporte", return_value={"ok": True, "falhas": []}):
                    app_module.salvar_erros_producao([{"id": "erro", "resolvido": False}])
                    response = app_module.api_auto_suporte_acao()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["detalhes"]["erros_removidos"], 1)

    def test_pre_deploy_json_retorna_checklist_sem_500(self):
        checklist = [{"nome": "HTTPS ativo", "ok": True, "detalhe": "OK", "acao": "OK"}]

        with app_module.app.test_request_context("/pre-deploy.json", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                 patch.object(app_module, "montar_pre_deploy_checklist", return_value=checklist):
                response = app_module.pre_deploy_json()

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["checklist"][0]["nome"], "HTTPS ativo")

    def test_erro_global_html_exibe_pagina_amigavel(self):
        with app_module.app.test_request_context("/rota-com-falha", method="GET"):
            response, status = app_module.tratar_erro_inesperado_producao(RuntimeError("falha teste"))

        self.assertEqual(status, 500)
        self.assertIn("Nao foi possivel concluir", response)
        self.assertEqual(app_module.ULTIMO_ERRO_PRODUCAO["tipo"], "RuntimeError")

    def test_paginas_menu_desabilitadas_bloqueiam_acesso_comum(self):
        with patch.object(app_module, "INIT_DB_EXECUTADO", True), \
             patch.object(app_module, "obter_paginas_menu_desabilitadas", return_value={"painel"}):
            with self.client.session_transaction() as sess:
                sess["usuario"] = "operador"
                sess["usuario_perfil"] = "funcionario"
                sess["usuario_id"] = 2

            response = self.client.get("/painel")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/configuracoes", response.headers.get("Location", ""))

    def test_salvar_paginas_menu_configuracao_form_persiste_desabilitadas(self):
        with app_module.app.test_request_context(
            "/configuracoes/paginas",
            method="POST",
            data={"paginas_habilitadas": ["painel", "clientes"]},
        ):
            with patch.object(app_module, "salvar_campos_configuracao_empresa") as salvar, \
                 patch.object(app_module, "limpar_caches_interface"):
                desabilitadas = app_module.salvar_paginas_menu_configuracao_form(request.form)

        self.assertNotIn("painel", desabilitadas)
        self.assertNotIn("clientes", desabilitadas)
        self.assertIn("nota_fiscal", desabilitadas)
        payload = salvar.call_args.args[0]
        self.assertIn("paginas_menu_desabilitadas_json", payload)
        self.assertIn("nota_fiscal", json.loads(payload["paginas_menu_desabilitadas_json"]))

    def test_salvar_configuracao_auto_teste_preserva_token_existente(self):
        atual = app_module.empresa_snapshot_padrao()
        atual["auto_teste_telegram_bot_token"] = "123456:token-atual"

        with app_module.app.test_request_context(
            "/configuracoes/auto-teste",
            method="POST",
            data={
                "auto_teste_ativo": "1",
                "auto_teste_site_url": "https://wagenestetica.duckdns.org/",
                "auto_teste_intervalo_horas": "2",
                "auto_teste_telegram_bot_nick": "wagenesteticabot",
                "auto_teste_telegram_chat_id": "999",
            },
        ):
            with patch.object(app_module, "obter_configuracao_empresa", return_value=atual), \
                 patch.object(app_module, "salvar_campos_configuracao_empresa") as salvar, \
                 patch.object(app_module, "limpar_cache_configuracao_empresa"):
                config = app_module.salvar_configuracao_auto_teste_form(request.form)

        payload = salvar.call_args.args[0]
        self.assertEqual(payload["auto_teste_telegram_bot_token"], "123456:token-atual")
        self.assertEqual(payload["auto_teste_telegram_bot_nick"], "@wagenesteticabot")
        self.assertEqual(payload["auto_teste_site_url"], "https://wagenestetica.duckdns.org")
        self.assertEqual(payload["auto_teste_ativo"], 1)
        self.assertIn("auto_teste_ativo", config)

    def test_executar_auto_teste_envia_relatorio_e_salva_chat_resolvido(self):
        config = app_module.empresa_snapshot_padrao()
        config.update(
            {
                "auto_teste_site_url": "https://wagenestetica.duckdns.org",
                "auto_teste_telegram_bot_token": "123456:token",
                "auto_teste_telegram_chat_id": "",
            }
        )

        resultado_check = type("Resultado", (), {
            "name": "Login",
            "ok": True,
            "status": 200,
            "elapsed_ms": 100,
            "message": "HTTP 200",
        })()

        with patch.object(app_module, "has_request_context", return_value=False), \
             patch.object(app_module, "run_site_checks", return_value=[resultado_check]), \
             patch.object(app_module, "build_site_monitor_report", return_value="relatorio ok"), \
             patch.object(app_module, "check_auto_teste_banco_online", return_value=resultado_check), \
             patch.object(app_module, "resolver_chat_id_telegram", return_value="777"), \
             patch.object(app_module, "send_site_monitor_telegram_message") as enviar, \
             patch.object(app_module, "salvar_resultado_auto_teste_seguro") as salvar_resultado:
            resultado = app_module.executar_auto_teste_site(config, enviar_telegram=True)

        self.assertTrue(resultado["ok"])
        self.assertEqual(resultado["chat_id"], "777")
        enviar.assert_called_once_with("123456:token", "777", "relatorio ok", 15)
        salvar_resultado.assert_called_once_with("ok", "relatorio ok", chat_id="777")

    def test_executar_auto_teste_em_request_nao_chama_http_do_proprio_site(self):
        config = app_module.empresa_snapshot_padrao()
        config.update(
            {
                "auto_teste_site_url": "https://wagenestetica.duckdns.org",
                "auto_teste_telegram_bot_token": "123456:token",
                "auto_teste_telegram_chat_id": "777",
            }
        )

        resultado_check = app_module.SiteMonitorCheckResult(
            name="Login",
            ok=True,
            status=200,
            elapsed_ms=10,
            message="HTTP 200",
        )

        with app_module.app.test_request_context("/configuracoes/auto-teste/testar", method="POST"):
            with patch.object(app_module, "run_site_checks") as externo, \
                 patch.object(app_module, "run_site_checks_interno", return_value=[resultado_check]) as interno, \
                 patch.object(app_module, "build_site_monitor_report", return_value="relatorio ok"), \
                 patch.object(app_module, "check_auto_teste_banco_online", return_value=resultado_check), \
                 patch.object(app_module, "send_site_monitor_telegram_message"), \
                 patch.object(app_module, "salvar_resultado_auto_teste_seguro"):
                resultado = app_module.executar_auto_teste_site(config, enviar_telegram=True)

        self.assertTrue(resultado["ok"])
        interno.assert_called_once()
        externo.assert_not_called()

    def test_salvar_campos_configuracao_empresa_garante_colunas_auto_teste(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE configuracao_empresa (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                atualizado_em TEXT
            )
            """
        )
        conn.execute("INSERT INTO configuracao_empresa (empresa_id) VALUES (1)")
        conn.commit()
        wrapper = PersistentCompatConnection(conn)

        with app_module.app.test_request_context("/configuracoes/auto-teste", method="POST"):
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar", return_value=wrapper):
                app_module.salvar_campos_configuracao_empresa(
                    {
                        "auto_teste_ativo": 1,
                        "auto_teste_site_url": "https://wagenestetica.duckdns.org",
                    }
                )

        colunas = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(configuracao_empresa)").fetchall()
        }
        row = conn.execute("SELECT auto_teste_ativo, auto_teste_site_url FROM configuracao_empresa WHERE empresa_id=1").fetchone()

        self.assertIn("auto_teste_telegram_bot_token", colunas)
        self.assertEqual(row["auto_teste_ativo"], 1)
        self.assertEqual(row["auto_teste_site_url"], "https://wagenestetica.duckdns.org")
        conn.close()

    def test_salvar_configuracao_hud_usuario_isola_por_usuario(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE usuarios (
                id INTEGER PRIMARY KEY,
                usuario TEXT,
                hud_config_json TEXT
            )
            """
        )
        conn.execute("INSERT INTO usuarios (id, usuario, hud_config_json) VALUES (1, 'admin', NULL)")
        conn.execute("INSERT INTO usuarios (id, usuario, hud_config_json) VALUES (2, 'dev', NULL)")
        conn.commit()
        wrapper = PersistentCompatConnection(conn)

        with app_module.app.test_request_context(
            "/configuracoes/hud",
            method="POST",
            data={
                "hud_ativo": "1",
                "hud_itens_habilitados": ["data_hora", "financeiro"],
            },
        ):
            session["usuario_id"] = 1
            with patch.object(app_module, "conectar", return_value=wrapper), \
                 patch.object(app_module, "limpar_caches_interface"):
                config = app_module.salvar_configuracao_hud_usuario_form(request.form)

        row_admin = conn.execute("SELECT hud_config_json FROM usuarios WHERE id=1").fetchone()
        row_dev = conn.execute("SELECT hud_config_json FROM usuarios WHERE id=2").fetchone()
        payload = json.loads(row_admin["hud_config_json"])

        self.assertTrue(config["hud_ativo"])
        self.assertEqual(payload["itens_habilitados"], ["data_hora", "financeiro"])
        self.assertIsNone(row_dev["hud_config_json"])
        conn.close()

    def test_manifesto_site_renderiza_com_branding_dinamico(self):
        with app_module.app.test_request_context("/site.webmanifest", method="GET"):
            with patch.object(
                app_module,
                "carregar_contexto_produto",
                return_value={
                    "site_title": "Minha Operacao",
                    "brand_name": "Minha Marca",
                    "brand_favicon_url": "/branding/favicon",
                    "brand_background_color": "#010203",
                    "brand_primary_color": "#abcdef",
                },
            ):
                response = app_module.servir_manifesto_site()

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.get_data(as_text=True))
        self.assertEqual(payload["name"], "Minha Operacao")
        self.assertEqual(payload["short_name"], "Minha Marca")
        self.assertEqual(payload["display"], "standalone")
        self.assertEqual(payload["scope"], "/")
        self.assertIn("camera", payload["permissions"])
        self.assertEqual(payload["icons"][0]["src"], "/static/icon-192.jpg")
        self.assertEqual(payload["icons"][1]["src"], "/static/icon-512.jpg")
        self.assertEqual(response.mimetype, "application/manifest+json")

    def test_service_worker_raiz_tem_escopo_do_app(self):
        response = self.client.get("/sw.js")
        conteudo = response.get_data()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("Service-Worker-Allowed"), "/")
        self.assertIn(b"wagen-pwa", conteudo)
        response.close()

    def test_api_pwa_status_exige_https_para_instalar(self):
        inseguro = self.client.get("/api/pwa/status", base_url="http://66.70.198.72:5000")
        seguro = self.client.get("/api/pwa/status", base_url="https://sistema.exemplo.com")

        self.assertFalse(inseguro.get_json()["ok"])
        self.assertIn("HTTPS", inseguro.get_json()["mensagem"])
        self.assertTrue(seguro.get_json()["ok"])

    def test_filtrar_registros_por_periodo_generico(self):
        referencia = app_module.date(2026, 5, 3)
        registros = [
            {"criado_em_dt": app_module.datetime(2026, 5, 3, 10, 0)},
            {"criado_em_dt": app_module.datetime(2026, 5, 1, 12, 0)},
            {"criado_em_dt": app_module.datetime(2026, 4, 1, 8, 0)},
        ]

        filtrados = app_module.filtrar_registros_por_periodo(
            registros,
            "7dias",
            referencia,
            "criado_em_dt",
        )

        self.assertEqual(len(filtrados), 2)

    def test_exportar_relatorios_csv_documentos(self):
        contexto = {
            "orcamentos_periodo_raw": [
                {
                    "numero_formatado": "0001",
                    "cliente_nome": "Cliente A",
                    "placa": "ABC1234",
                    "valor_exibicao": "120,00",
                    "status": "GERADO",
                    "criado_em_fmt": "03/05/2026 10:00",
                    "usuario": "admin",
                }
            ],
            "notas_periodo_raw": [
                {
                    "numero_nota": "NF-10",
                    "rps_formatado": "0010",
                    "cliente_nome": "Cliente B",
                    "placa": "XYZ9876",
                    "valor_exibicao": "80,00",
                    "status": "EMITIDA",
                    "criado_em_fmt": "03/05/2026 11:00",
                    "usuario": "admin",
                }
            ],
            "finalizados_periodo_raw": [],
        }

        with app_module.app.test_request_context("/relatorios/exportar.csv?tipo=documentos&periodo=mes", method="GET"):
            session["usuario"] = "admin"
            with patch.object(app_module, "carregar_contexto_relatorios", return_value=contexto):
                response = app_module.exportar_relatorios_csv()

        response.direct_passthrough = False
        corpo = response.get_data(as_text=True)
        self.assertEqual(response.status_code, 200)
        self.assertIn("attachment;", response.headers.get("Content-Disposition", ""))
        self.assertIn("Orcamento", corpo)
        self.assertIn("Nota fiscal", corpo)

    def test_carregar_contexto_relatorios_faz_fallback_em_erro_de_leitura(self):
        with app_module.app.test_request_context("/relatorios?periodo=mes", method="GET"):
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar_somente_leitura", side_effect=RuntimeError("falha banco")), \
                 patch.object(app_module, "banco_online_estritamente_obrigatorio", return_value=False), \
                 patch.object(app_module, "garantir_schema_sqlite_local_minima", side_effect=RuntimeError("falha local")), \
                 patch.object(app_module, "agora", return_value=app_module.datetime(2026, 5, 4, 10, 0)):
                contexto = app_module.carregar_contexto_relatorios("mes")

        self.assertEqual(contexto["quantidade_periodo"], 0)
        self.assertEqual(contexto["resumo_comercial"]["novos_total"], 0)
        self.assertEqual(contexto["ranking_faturamento"], [])

    def test_carregar_contexto_auditoria_faz_fallback_em_erro_de_leitura(self):
        with app_module.app.test_request_context("/auditoria?periodo=7dias", method="GET"):
            session["usuario"] = "admin"
            with patch.object(app_module, "conectar_somente_leitura", side_effect=RuntimeError("falha banco")), \
                 patch.object(app_module, "banco_online_estritamente_obrigatorio", return_value=False), \
                 patch.object(app_module, "garantir_schema_sqlite_local_minima", side_effect=RuntimeError("falha local")):
                contexto = app_module.carregar_contexto_auditoria({"periodo": "7dias"})

        self.assertEqual(contexto["registros"], [])
        self.assertEqual(contexto["usuarios"], [])
        self.assertEqual(contexto["resumo"]["total_eventos"], 0)

    def test_salvar_configuracao_banco_retorna_feedback_em_erro_inesperado(self):
        with app_module.app.test_request_context("/configuracoes/banco", method="POST", data={}):
            session["usuario"] = "admin"
            with patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "usuario_gerencia_banco_online", return_value=True), \
                 patch.object(app_module, "salvar_configuracao_banco_form", side_effect=RuntimeError("falha inesperada")):
                response = app_module.salvar_configuracao_banco()

            feedback = session.get("configuracoes_feedback") or {}

        self.assertEqual(response.status_code, 302)
        self.assertIn("/configuracoes", response.location)
        self.assertEqual(feedback.get("tipo"), "erro")
        self.assertIn("Nao foi possivel salvar a configuracao do banco online", feedback.get("mensagem", ""))

    def test_salvar_configuracao_backup_form_isola_por_empresa(self):
        conn = self._criar_banco_admin_memoria()
        wrapper = PersistentCompatConnection(conn)

        with patch.object(app_module, "conectar", return_value=wrapper), \
             patch.object(app_module, "conectar_somente_leitura", return_value=wrapper):
            with app_module.app.test_request_context("/configuracoes/backup", method="POST"):
                session["empresa_id"] = 2
                app_module.salvar_configuracao_backup_form(
                    {
                        "frequencia": "semanal",
                        "tipo_backup": "banco",
                        "retencao_arquivos": "9",
                    }
                )

            with app_module.app.test_request_context("/configuracoes/backup", method="POST"):
                session["empresa_id"] = 1
                app_module.salvar_configuracao_backup_form(
                    {
                        "frequencia": "diario",
                        "tipo_backup": "completo",
                        "retencao_arquivos": "15",
                    }
                )

            with app_module.app.test_request_context("/configuracoes", method="GET"):
                session["empresa_id"] = 2
                config_empresa_2 = app_module.obter_configuracao_backup()

            with app_module.app.test_request_context("/configuracoes", method="GET"):
                session["empresa_id"] = 1
                config_empresa_1 = app_module.obter_configuracao_backup()

        self.assertEqual(config_empresa_2["frequencia"], "semanal")
        self.assertEqual(config_empresa_2["retencao_arquivos"], 9)
        self.assertEqual(config_empresa_1["frequencia"], "diario")
        self.assertEqual(config_empresa_1["retencao_arquivos"], 15)

        conn.close()

    def test_salvar_status_manutencao_arquivos_isola_por_empresa(self):
        conn = self._criar_banco_admin_memoria()
        wrapper = PersistentCompatConnection(conn)

        with patch.object(app_module, "conectar", return_value=wrapper), \
             patch.object(app_module, "conectar_somente_leitura", return_value=wrapper):
            with app_module.app.test_request_context("/configuracoes", method="GET"):
                session["empresa_id"] = 2
                app_module.salvar_status_manutencao_arquivos({"ok": True}, "empresa-2")

            with app_module.app.test_request_context("/configuracoes", method="GET"):
                session["empresa_id"] = 1
                app_module.salvar_status_manutencao_arquivos({"ok": False}, "empresa-1")

            with app_module.app.test_request_context("/configuracoes", method="GET"):
                session["empresa_id"] = 2
                estado_empresa_2 = app_module.obter_status_manutencao_arquivos_db()

            with app_module.app.test_request_context("/configuracoes", method="GET"):
                session["empresa_id"] = 1
                estado_empresa_1 = app_module.obter_status_manutencao_arquivos_db()

        self.assertEqual(estado_empresa_2["ultima_mensagem"], "empresa-2")
        self.assertEqual(estado_empresa_1["ultima_mensagem"], "empresa-1")

        conn.close()

    def test_salvar_configuracao_integracao_fiscal_form_isola_por_empresa(self):
        conn = self._criar_banco_admin_memoria()
        wrapper = PersistentCompatConnection(conn)

        with patch.object(app_module, "conectar", return_value=wrapper), \
             patch.object(app_module, "conectar_somente_leitura", return_value=wrapper):
            with app_module.app.test_request_context("/nota_fiscal/integracao", method="POST"):
                session["empresa_id"] = 2
                app_module.salvar_configuracao_integracao_fiscal_form(
                    {
                        "tipo_integracao": "api",
                        "provedor_nome": "Provedor 2",
                        "ativo": "1",
                    }
                )

            with app_module.app.test_request_context("/nota_fiscal/integracao", method="POST"):
                session["empresa_id"] = 1
                app_module.salvar_configuracao_integracao_fiscal_form(
                    {
                        "tipo_integracao": "manual",
                        "provedor_nome": "Provedor 1",
                    }
                )

            with app_module.app.test_request_context("/nota_fiscal", method="GET"):
                session["empresa_id"] = 2
                integracao_empresa_2 = app_module.obter_configuracao_integracao_fiscal()

            with app_module.app.test_request_context("/nota_fiscal", method="GET"):
                session["empresa_id"] = 1
                integracao_empresa_1 = app_module.obter_configuracao_integracao_fiscal()

        self.assertEqual(integracao_empresa_2["tipo_integracao"], "api")
        self.assertEqual(integracao_empresa_2["provedor_nome"], "Provedor 2")
        self.assertEqual(integracao_empresa_1["tipo_integracao"], "manual")
        self.assertEqual(integracao_empresa_1["provedor_nome"], "Provedor 1")

        conn.close()

    def test_pagina_nota_fiscal_renderiza_com_dependencias_mockadas(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("CREATE TABLE tipos_servico (nome TEXT, valor REAL)")
        c.execute("INSERT INTO tipos_servico (nome, valor) VALUES (?, ?)", ("Lavagem", 40.0))
        conn.commit()
        wrapper = PersistentCompatConnection(conn)

        with app_module.app.test_request_context("/nota_fiscal", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "preparar_rotinas_interface_logada"), \
                 patch.object(app_module, "obter_configuracao_empresa", return_value=app_module.empresa_snapshot_padrao()), \
                 patch.object(app_module, "obter_configuracao_integracao_fiscal", return_value=app_module.integracao_fiscal_padrao()), \
                 patch.object(app_module, "conectar", return_value=wrapper), \
                 patch.object(app_module, "recurso_liberado_por_plano", return_value=True), \
                 patch.object(app_module, "avaliar_prontidao_integracao_fiscal", return_value={"pronta": False}), \
                 patch.object(app_module, "montar_payload_exemplo_integracao", return_value={}), \
                 patch.object(app_module, "listar_notas_fiscais_recentes", return_value=[]), \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                response = app_module.pagina_nota_fiscal()

        self.assertEqual(response, "ok")
        render_mock.assert_called_once()
        conn.close()

    def test_preparar_sincronizacoes_nao_dispara_workers_pesados(self):
        with app_module.app.test_request_context("/status_sync", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "INIT_DB_EXECUTADO", True), \
                 patch.object(app_module, "iniciar_worker_backup_banco") as backup_mock, \
                 patch.object(app_module, "iniciar_worker_manutencao_arquivos") as manutencao_mock, \
                 patch.object(app_module, "iniciar_worker_sincronizacao") as sync_mock, \
                 patch.object(app_module, "iniciar_worker_sincronizacao_bancos") as sync_bancos_mock, \
                 patch.object(app_module, "iniciar_bootstrap_schema_online") as schema_mock:
                app_module.preparar_sincronizacoes()

        backup_mock.assert_not_called()
        manutencao_mock.assert_not_called()
        sync_mock.assert_not_called()
        sync_bancos_mock.assert_not_called()
        schema_mock.assert_not_called()

    def test_aplicar_fluxo_etapa_atendimento_em_edicao_reabre_em_lavagem(self):
        conn = self._criar_banco_servico_fluxo_memoria()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO servicos (
                empresa_id, status, etapa_atual, etapa_atual_iniciada_em,
                lavagem_iniciada_em, finalizacao_iniciada_em, lavagem_segundos, finalizacao_segundos
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (1, "FINALIZADO", "FINALIZACAO", None, "2026-05-03T09:00:00-03:00", "2026-05-03T09:30:00-03:00", 1800, 900),
        )
        servico = {
            "id": 1,
            "empresa_id": 1,
            "status": "FINALIZADO",
            "etapa_atual": "FINALIZACAO",
            "etapa_atual_iniciada_em": None,
            "lavagem_iniciada_em": "2026-05-03T09:00:00-03:00",
            "finalizacao_iniciada_em": "2026-05-03T09:30:00-03:00",
            "lavagem_segundos": 1800,
            "finalizacao_segundos": 900,
        }

        atualizado = app_module.aplicar_fluxo_etapa_atendimento_em_edicao(
            c,
            servico,
            "EM ANDAMENTO",
            "LAVAGEM",
            instante=app_module.interpretar_datahora_sistema("2026-05-03T10:00:00-03:00"),
        )
        conn.commit()

        c.execute("SELECT etapa_atual, etapa_atual_iniciada_em FROM servicos WHERE id=1")
        row = c.fetchone()
        self.assertEqual(atualizado["etapa_atual"], "LAVAGEM")
        self.assertEqual(row["etapa_atual"], "LAVAGEM")
        self.assertTrue(row["etapa_atual_iniciada_em"])
        conn.close()

    def test_painel_agrupar_servicos_por_etapa(self):
        servicos = [
            {
                "id": 1,
                "placa": "AAA1234",
                "modelo": "Onix",
                "cor": "Preto",
                "tipo_nome": "Completa",
                "valor": 40.0,
                "valor_adicional": 0.0,
                "entrada": "2026-05-03T09:00:00-03:00",
                "cliente_nome": "Cliente 1",
                "cliente_telefone": "",
                "origem": "",
                "guarita": "",
                "observacoes": "",
                "pneu": "",
                "cera": "Nao",
                "hidro_lataria": "Nao",
                "hidro_vidros": "Nao",
                "status": "EM ANDAMENTO",
                "prioridade": 1,
                "etapa_atual": "LAVAGEM",
                "etapa_atual_iniciada_em": "2026-05-03T09:00:00-03:00",
                "lavagem_iniciada_em": "2026-05-03T09:00:00-03:00",
                "finalizacao_iniciada_em": None,
                "lavagem_segundos": 0,
                "finalizacao_segundos": 0,
                "criado_por_nome": "Operador 1",
                "criado_por_usuario": "operador1",
                "operacional_por_nome": None,
                "operacional_por_usuario": None,
                "entrega_prevista": None,
            },
            {
                "id": 2,
                "placa": "BBB1234",
                "modelo": "HB20",
                "cor": "Branco",
                "tipo_nome": "Interna",
                "valor": 35.0,
                "valor_adicional": 0.0,
                "entrada": "2026-05-03T09:10:00-03:00",
                "cliente_nome": "Cliente 2",
                "cliente_telefone": "",
                "origem": "",
                "guarita": "",
                "observacoes": "",
                "pneu": "",
                "cera": "Nao",
                "hidro_lataria": "Nao",
                "hidro_vidros": "Nao",
                "status": "EM ANDAMENTO",
                "prioridade": 2,
                "etapa_atual": "FINALIZACAO",
                "etapa_atual_iniciada_em": "2026-05-03T09:40:00-03:00",
                "lavagem_iniciada_em": "2026-05-03T09:10:00-03:00",
                "finalizacao_iniciada_em": "2026-05-03T09:40:00-03:00",
                "lavagem_segundos": 900,
                "finalizacao_segundos": 0,
                "criado_por_nome": "Operador 2",
                "criado_por_usuario": "operador2",
                "operacional_por_nome": None,
                "operacional_por_usuario": None,
                "entrega_prevista": "2026-05-03T11:00:00-03:00",
            },
        ]

        with app_module.app.test_request_context("/painel", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "preparar_rotinas_interface_logada"), \
                 patch.object(app_module, "obter_cache_consulta", return_value=None), \
                 patch.object(app_module, "executar_leitura_resiliente", return_value={
                     "servicos_db": servicos,
                     "produtos_pneu": [],
                     "fotos_por_servico": {},
                     "extras_por_servico": {},
                 }), \
                 patch.object(app_module, "salvar_cache_consulta"), \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                response = app_module.painel()

        self.assertEqual(response, "ok")
        kwargs = render_mock.call_args.kwargs
        self.assertEqual(len(kwargs["servicos_lavagem"]), 1)
        self.assertEqual(len(kwargs["servicos_finalizacao"]), 1)
        self.assertEqual(kwargs["resumo_fluxo"]["total"], 2)

    def test_api_painel_servico_detalhes_retorna_json_resiliente(self):
        detalhes = {
            "fotos": {
                "entrada": [
                    {
                        "url": "/fotos/1/arquivo",
                        "usuario_nome_exibicao": "Operador",
                    }
                ]
            },
            "cobrancas_extras": {
                "itens": [
                    {
                        "descricao": "Polimento",
                        "valor_exibicao": "50.00",
                        "criado_por_nome_exibicao": "Admin",
                    }
                ],
                "total": 50.0,
                "total_exibicao": "50.00",
            },
        }

        with self.client.session_transaction() as sess:
            sess["usuario"] = "admin"
            sess["empresa_id"] = 1

        with patch.object(app_module, "INIT_DB_EXECUTADO", True), \
             patch.object(app_module, "sincronizar_sessao_usuario"), \
             patch.object(app_module, "carregar_contexto_licenca_empresa_seguro", return_value={"bloqueada": False}), \
             patch.object(app_module, "executar_leitura_resiliente", return_value=detalhes):
            response = self.client.get("/api/painel/servico/10/detalhes")

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["servico_id"], 10)
        self.assertEqual(payload["fotos"]["entrada"][0]["url"], "/fotos/1/arquivo")
        self.assertEqual(payload["cobrancas_extras"]["itens"][0]["descricao"], "Polimento")

    def test_listar_fotos_servicos_prefere_rota_do_banco_quando_ha_blob(self):
        conn = self._criar_banco_fotos_memoria()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO fotos (
                empresa_id, servico_id, tipo, caminho, criado_em, usuario, usuario_nome,
                tamanho_bytes, largura, altura, arquivo_blob, mime_type, arquivo_nome
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                1,
                10,
                "entrada",
                "static/uploads/foto_teste.jpg",
                "2026-05-03T12:00:00-03:00",
                "operador",
                "Operador",
                128,
                100,
                100,
                b"abc",
                "image/jpeg",
                "foto_teste.jpg",
            ),
        )
        conn.commit()
        wrapper = PersistentCompatConnection(conn)

        with app_module.app.test_request_context("/historico", method="GET"):
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar", return_value=wrapper), \
                 patch.object(app_module, "foto_local_disponivel", return_value=True):
                fotos = app_module.listar_fotos_servicos([10])

        self.assertEqual(fotos[10]["entrada"][0]["url"], "/fotos/1/arquivo")
        self.assertEqual(fotos[10]["entrada"][0]["fonte_armazenamento"], "Banco de dados")
        conn.close()

    def test_excluir_foto_historico_remove_registro_no_escopo_do_servico(self):
        conn = self._criar_banco_fotos_memoria()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO fotos (
                empresa_id, servico_id, tipo, caminho, arquivo_blob, mime_type, arquivo_nome
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 22, "detalhe", "static/uploads/foto_remover.jpg", b"img", "image/jpeg", "foto_remover.jpg"),
        )
        conn.commit()
        wrapper = PersistentCompatConnection(conn)
        servico = {"id": 22, "placa": "ABC1234"}

        with app_module.app.test_request_context("/historico/servico/22/fotos/1/excluir", method="POST", data={"redirect_to": "/historico"}):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar", return_value=wrapper), \
                 patch.object(app_module, "buscar_servico_operacional", return_value=servico), \
                 patch.object(app_module, "registrar_auditoria"), \
                 patch.object(app_module, "definir_feedback_por_destino"), \
                 patch.object(app_module, "remover_foto_servico_local"):
                response = app_module.excluir_foto_historico(22, 1)

        self.assertEqual(response.status_code, 302)
        c.execute("SELECT COUNT(*) FROM fotos WHERE empresa_id=? AND servico_id=? AND id=?", (1, 22, 1))
        self.assertEqual(c.fetchone()[0], 0)
        conn.close()

    def test_validar_arquivo_backup_local_zip_com_manifesto(self):
        with tempfile.TemporaryDirectory(prefix="backup_test_") as pasta:
            caminho_zip = os.path.join(pasta, "sistema_completo_20260503_120000.zip")
            manifesto = {
                "tipo_backup": "completo",
                "gerado_em": "2026-05-03T12:00:00-03:00",
                "arquivos": [
                    app_module.BACKUP_ARQUIVO_POSTGRES_ATUAL,
                    "static/uploads/servicos/teste.jpg",
                ],
            }
            with zipfile.ZipFile(caminho_zip, "w", compression=zipfile.ZIP_DEFLATED) as arquivo_zip:
                arquivo_zip.writestr(
                    app_module.BACKUP_ARQUIVO_POSTGRES_ATUAL,
                    json.dumps({"backend": "postgres", "tabelas": {"usuarios": []}}),
                )
                arquivo_zip.writestr("static/uploads/servicos/teste.jpg", b"img")
                arquivo_zip.writestr(
                    app_module.BACKUP_ARQUIVO_MANIFESTO,
                    json.dumps(manifesto),
                )

            resultado = app_module.validar_arquivo_backup_local(caminho_zip, usar_cache=False)

        self.assertTrue(resultado["ok"])
        self.assertEqual(resultado["tipo_backup"], "completo")
        self.assertTrue(resultado["manifesto_encontrado"])

    def test_validar_arquivo_backup_local_sqlite(self):
        with tempfile.TemporaryDirectory(prefix="backup_sqlite_") as pasta:
            caminho_db = os.path.join(pasta, "database_v2_20260503_120000.db")
            conn = sqlite3.connect(caminho_db)
            c = conn.cursor()
            c.execute("CREATE TABLE usuarios (id INTEGER PRIMARY KEY, nome TEXT)")
            conn.commit()
            conn.close()

            resultado = app_module.validar_arquivo_backup_local(caminho_db, usar_cache=False)

        self.assertTrue(resultado["ok"])
        self.assertEqual(resultado["tipo_backup"], "banco")

    def test_salvar_cliente_veiculo_persiste_data_nascimento(self):
        conn = self._criar_banco_clientes_notificacoes_memoria()
        wrapper = PersistentCompatConnection(conn)

        with patch.object(app_module, "conectar", return_value=wrapper), \
             patch.object(app_module, "espelhar_cadastro_site_em_sincronizacoes_clientes", return_value={"sucesso": [], "falhas": [], "ignoradas": []}):
            resultado = app_module.salvar_cliente_veiculo(
                placa="ABC1234",
                nome="Maria",
                telefone="51999999999",
                data_nascimento="1990-05-05",
                modelo="Onix",
                cor="Preto",
            )

        self.assertEqual(resultado["acao"], "novo")
        self.assertEqual(resultado["cliente_acao"], "novo")
        c = conn.cursor()
        c.execute("SELECT data_nascimento FROM clientes WHERE empresa_id=? LIMIT 1", (1,))
        self.assertEqual(c.fetchone()["data_nascimento"], "1990-05-05")
        conn.close()

    def test_marcador_define_novo_atendimento_somente_para_cadastro_recente(self):
        with app_module.app.test_request_context("/"):
            session["usuario"] = "admin"
            app_module.registrar_cadastro_novo_para_atendimento("ABC1234")

            self.assertTrue(app_module.consumir_cadastro_novo_para_atendimento("abc1234"))
            self.assertFalse(app_module.consumir_cadastro_novo_para_atendimento("abc1234"))
            self.assertFalse(app_module.consumir_cadastro_novo_para_atendimento("XYZ9876"))

    def test_classificar_perfil_cliente_forca_retorno_quando_ja_existe_atendimento(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                veiculo_id INTEGER
            )
            """
        )
        c.execute("INSERT INTO servicos (empresa_id, veiculo_id) VALUES (1, 10)")

        with app_module.app.test_request_context("/"):
            session["usuario"] = "admin"
            app_module.registrar_cadastro_novo_para_atendimento("ABC1234")

            perfil, motivo, anteriores = app_module.classificar_perfil_cliente_atendimento(c, 1, 10, "ABC1234")

        self.assertEqual(perfil, "RETORNO")
        self.assertEqual(anteriores, 1)
        self.assertIn("atendimento anterior", motivo)
        conn.close()

    def test_servico_bloqueia_atendimento_duplicado_sem_consumir_marcador_novo(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            """
            CREATE TABLE veiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                placa TEXT,
                cliente_id INTEGER
            )
            """
        )
        c.execute(
            """
            CREATE TABLE servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                veiculo_id INTEGER,
                status TEXT
            )
            """
        )
        c.execute("INSERT INTO veiculos (empresa_id, placa, cliente_id) VALUES (1, 'ABC1234', 10)")
        veiculo_id = c.lastrowid
        c.execute("INSERT INTO servicos (empresa_id, veiculo_id, status) VALUES (1, ?, 'EM ANDAMENTO')", (veiculo_id,))
        wrapper = PersistentCompatConnection(conn)

        with app_module.app.test_request_context(
            "/servico",
            method="POST",
            data={"placa": "ABC1234", "tipo": "Lavagem"},
        ):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            app_module.registrar_cadastro_novo_para_atendimento("ABC1234")
            with patch.object(app_module, "conectar", return_value=wrapper), \
                 patch.object(app_module, "bloquear_criacao_atendimento_por_licenca", return_value=False):
                resposta = app_module.servico()

            self.assertEqual(resposta.status_code, 302)
            self.assertEqual(resposta.location, "/painel")
            self.assertIn("ja possui atendimento em andamento", session["painel_feedback"]["mensagem"])
            self.assertTrue(app_module.consumir_cadastro_novo_para_atendimento("ABC1234"))
        conn.close()

    def test_garantir_notificacoes_aniversario_cria_e_deduplica(self):
        conn = self._criar_banco_clientes_notificacoes_memoria()
        c = conn.cursor()
        c.execute(
            """
            INSERT INTO clientes (empresa_id, nome, telefone, data_nascimento)
            VALUES (?, ?, ?, ?)
            """,
            (1, "Maria", "51999999999", "1990-05-05"),
        )
        conn.commit()
        wrapper = PersistentCompatConnection(conn)

        with patch.object(app_module, "conectar", return_value=wrapper), \
             patch.object(app_module, "agora", return_value=app_module.datetime(2026, 5, 4, 10, 0, tzinfo=app_module.ZoneInfo("America/Sao_Paulo"))):
            inseridas_primeira = app_module.garantir_notificacoes_aniversario(empresa_id=1, force=True)
            inseridas_segunda = app_module.garantir_notificacoes_aniversario(empresa_id=1, force=True)

        self.assertEqual(inseridas_primeira, 1)
        self.assertEqual(inseridas_segunda, 0)
        c.execute("SELECT categoria, mensagem FROM notificacoes WHERE empresa_id=? ORDER BY id", (1,))
        registros = c.fetchall()
        self.assertEqual(len(registros), 1)
        self.assertEqual(registros[0]["categoria"], "aniversario_amanha")
        self.assertIn("Sugestao de mensagem", registros[0]["mensagem"])
        conn.close()

    def test_montar_resumo_fluxo_atendimento_conta_novos_e_retornos(self):
        resumo = app_module.montar_resumo_fluxo_atendimento(
            [
                {"perfil_cliente_atendimento": "NOVO", "etapa_atual": "LAVAGEM"},
                {"perfil_cliente_atendimento": "RETORNO", "etapa_atual": "FINALIZACAO"},
                {"perfil_cliente_atendimento": "", "etapa_atual": "LAVAGEM"},
            ]
        )

        self.assertEqual(resumo["total"], 3)
        self.assertEqual(resumo["novos"], 2)
        self.assertEqual(resumo["retornos"], 1)

    def test_carregar_contexto_clientes_respeita_empresa_da_sessao(self):
        with app_module.app.test_request_context("/clientes", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 2

            with patch.object(app_module, "obter_cache_consulta", return_value=None), \
                 patch.object(
                     app_module,
                     "executar_leitura_resiliente",
                     return_value={"clientes": [], "sincronizacoes_raw": []},
                 ):
                clientes, sincronizacoes = app_module.carregar_contexto_clientes(busca="ABC1234")

        self.assertEqual(clientes, [])
        self.assertEqual(sincronizacoes, [])

    def test_central_tecnica_filtra_e_limpa_erros_resolvidos(self):
        with tempfile.TemporaryDirectory(prefix="erros_producao_") as pasta:
            caminho = os.path.join(pasta, "erros.json")
            erros = [
                {"id": "aberto", "resolvido": False, "quando": "2026-05-07T10:00:00"},
                {"id": "resolvido", "resolvido": True, "quando": "2026-05-07T09:00:00"},
            ]
            with patch.object(app_module, "ERROS_PRODUCAO_ARQUIVO", caminho):
                app_module.salvar_erros_producao(erros)

                self.assertEqual([item["id"] for item in app_module.listar_erros_producao(filtro="abertos")], ["aberto"])
                self.assertEqual([item["id"] for item in app_module.listar_erros_producao(filtro="resolvidos")], ["resolvido"])
                self.assertEqual(app_module.contar_erros_producao_por_status()["todos"], 2)
                removidos = app_module.limpar_erros_producao_resolvidos()

                self.assertEqual(removidos, 1)
                self.assertEqual([item["id"] for item in app_module.carregar_erros_producao()], ["aberto"])

    def test_metricas_tempo_resposta_marcam_tendencia_e_alerta_2s(self):
        rota = "/painel"
        estado_original = dict(app_module.METRICAS_TEMPO_RESPOSTA[rota])
        try:
            app_module.METRICAS_TEMPO_RESPOSTA[rota] = {
                "rota": rota,
                "ultimo_ms": 400,
                "media_ms": 400,
                "max_ms": 400,
                "amostras": 1,
                "status": 200,
                "ultima_medicao": "",
                "classe": "rapido",
            }
            with app_module.app.test_request_context(rota):
                app_module.g.inicio_tempo_resposta = app_module.time.perf_counter() - 2.2
                response = app_module.app.response_class("ok", status=200)
                app_module.registrar_tempo_resposta(response)

            item = app_module.metricas_tempo_resposta_central_tecnica()
            painel = next(valor for valor in item if valor["rota"] == rota)
            self.assertEqual(painel["tendencia"], "piorou")
            self.assertTrue(painel["alerta_2s"])
            self.assertEqual(painel["classe"], "lento")
        finally:
            app_module.METRICAS_TEMPO_RESPOSTA[rota] = estado_original

    def test_banco_online_falha_registra_rota_atingida(self):
        with app_module.app.test_request_context("/clientes"):
            with patch.object(app_module, "modo_banco_preferido", return_value="postgres"), \
                 patch.object(app_module, "url_postgres_ajustada", return_value="postgresql://u:p@h:5432/db?sslmode=require"), \
                 patch.object(app_module, "banco_online_estritamente_obrigatorio", return_value=True), \
                 patch.object(app_module, "conectar_postgres_com_fallback", side_effect=TimeoutError("timeout teste")), \
                 patch.object(app_module, "registrar_ultimo_erro_producao") as registrar_mock:
                with self.assertRaises(app_module.BancoOnlineObrigatorioErro):
                    app_module.conectar()

        registrar_mock.assert_called()
        self.assertEqual(registrar_mock.call_args.kwargs.get("descricao"), "banco_online_conectar")

    def test_index_sem_placa_nao_abre_banco_para_listas_pesadas(self):
        with app_module.app.test_request_context("/", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "preparar_rotinas_interface_logada"), \
                 patch.object(app_module, "conectar") as conectar_mock, \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                resposta = app_module.index()

        self.assertEqual(resposta, "ok")
        conectar_mock.assert_not_called()
        kwargs = render_mock.call_args.kwargs
        self.assertEqual(kwargs["servicos_lista"], [])
        self.assertEqual(kwargs["produtos_pneu"], [])

    def test_exportar_relatorio_tecnico_json_requer_desenvolvedor(self):
        with app_module.app.test_request_context("/configuracoes/desenvolvedor/relatorio.json?erros=todos"):
            session["usuario"] = "dev"
            with patch.object(app_module, "usuario_desenvolvedor", return_value=True), \
                 patch.object(app_module, "montar_central_tecnica_desenvolvedor", return_value={"gerado_em": "agora"}) as central_mock:
                response = app_module.exportar_relatorio_tecnico_central()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.get_json()["ok"])
        central_mock.assert_called_once_with(filtro_erros="todos")

    def test_fluxos_criticos_tem_cobertura_minima_login_pwa_clientes(self):
        regras = {str(rule) for rule in app_module.app.url_map.iter_rules()}

        self.assertIn("/login", regras)
        self.assertIn("/", regras)
        self.assertIn("/clientes", regras)
        self.assertIn("/servico", regras)
        self.assertIn("/finalizar/<int:id>", regras)
        self.assertIn("/site.webmanifest", regras)
        self.assertIn("/sw.js", regras)
        self.assertIn("/api/pwa/status", regras)
        self.assertIn("/clientes/sincronizacao/<int:sync_id>/excluir", regras)

    def test_alerta_estabilidade_dispara_para_rota_lenta_e_piora_repetida(self):
        rota = "/financeiro"
        estado_original = dict(app_module.METRICAS_TEMPO_RESPOSTA[rota])
        try:
            app_module.METRICAS_TEMPO_RESPOSTA[rota] = {
                "rota": rota,
                "ultimo_ms": 400,
                "media_ms": 400,
                "max_ms": 400,
                "amostras": 2,
                "status": 200,
                "ultima_medicao": "",
                "classe": "rapido",
                "pioras_consecutivas": 2,
            }
            with app_module.app.test_request_context(rota):
                app_module.g.inicio_tempo_resposta = app_module.time.perf_counter() - 2.2
                response = app_module.app.response_class("ok", status=200)
                with patch.object(app_module, "enviar_alerta_estabilidade_assincrono") as alerta_mock:
                    app_module.registrar_tempo_resposta(response)

            chaves = [chamada.kwargs.get("chave") for chamada in alerta_mock.call_args_list]
            self.assertIn("rota_lenta:/financeiro", chaves)
            self.assertIn("rota_piorando:/financeiro", chaves)
            self.assertEqual(app_module.METRICAS_TEMPO_RESPOSTA[rota]["pioras_consecutivas"], 3)
        finally:
            app_module.METRICAS_TEMPO_RESPOSTA[rota] = estado_original

    def test_erro_global_agenda_alerta_resumido_telegram(self):
        with app_module.app.test_request_context("/painel"):
            session["usuario"] = "admin"
            with patch.object(app_module, "salvar_registro_erro_producao"), \
                 patch.object(app_module, "enviar_alerta_estabilidade_assincrono") as alerta_mock:
                app_module.registrar_ultimo_erro_producao(RuntimeError("falha teste"), descricao="erro_global")

        alerta_mock.assert_called_once()
        self.assertIn("erro500:/painel:RuntimeError", alerta_mock.call_args.kwargs.get("chave"))

    def test_clientes_sincronizacoes_sao_sob_demanda(self):
        with app_module.app.test_request_context("/clientes", method="GET"):
            session["usuario"] = "admin"
            with patch.object(app_module, "carregar_contexto_clientes", return_value=([], [])) as contexto_mock, \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                resposta = app_module.renderizar_pagina_clientes()

        self.assertEqual(resposta, "ok")
        self.assertFalse(contexto_mock.call_args.kwargs["detalhar_sincronizacoes"])
        self.assertFalse(render_mock.call_args.kwargs["detalhar_sincronizacoes"])

        with app_module.app.test_request_context("/clientes?detalhar_sincronizacoes=1", method="GET"):
            session["usuario"] = "admin"
            with patch.object(app_module, "carregar_contexto_clientes", return_value=([], [])) as contexto_mock, \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                resposta = app_module.renderizar_pagina_clientes()

        self.assertEqual(resposta, "ok")
        self.assertTrue(contexto_mock.call_args.kwargs["detalhar_sincronizacoes"])
        self.assertTrue(render_mock.call_args.kwargs["detalhar_sincronizacoes"])

    def test_financeiro_completo_e_sob_demanda(self):
        with app_module.app.test_request_context("/financeiro", method="GET"):
            session["usuario"] = "admin"
            with patch.object(app_module, "carregar_contexto_relatorios", return_value={}) as contexto_mock, \
                 patch.object(app_module, "render_template", return_value="ok"):
                resposta = app_module.financeiro()

        self.assertEqual(resposta, "ok")
        self.assertFalse(contexto_mock.call_args.kwargs["detalhado"])

        with app_module.app.test_request_context("/financeiro?detalhar=1", method="GET"):
            session["usuario"] = "admin"
            with patch.object(app_module, "carregar_contexto_relatorios", return_value={}) as contexto_mock, \
                 patch.object(app_module, "render_template", return_value="ok"):
                resposta = app_module.financeiro()

        self.assertEqual(resposta, "ok")
        self.assertTrue(contexto_mock.call_args.kwargs["detalhado"])

    def test_auto_suporte_sugere_pacote_codex_para_erro_500(self):
        sugestoes = app_module.montar_sugestoes_auto_suporte(
            {"itens": []},
            [],
            [],
            [],
            [{"id": "erro-1", "path": "/clientes"}],
        )

        erro = next(item for item in sugestoes if item["titulo"] == "Erro 500 aberto")
        self.assertEqual(erro["acao"], "gerar_pacote_codex")

    def test_auto_suporte_gera_pacote_codex_sem_enviar_telegram(self):
        pacote = {
            "gerado_em": "2026-05-07T10:00:00",
            "ultimo_erro": {"id": "erro-1", "path": "/clientes", "tipo": "RuntimeError"},
            "erros_abertos": [{"id": "erro-1"}],
            "texto_para_codex": "PACOTE",
        }
        with app_module.app.test_request_context("/api/auto-suporte/acao", method="POST"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "montar_pacote_codex_auto_suporte", return_value=pacote), \
                 patch.object(app_module, "enviar_alerta_telegram_auto_suporte") as telegram_mock, \
                 patch.object(app_module, "status_auto_suporte", return_value={"ok": True}), \
                 patch.object(app_module, "registrar_incidente_auto_suporte") as incidente_mock:
                resultado = app_module.executar_acao_auto_suporte("gerar_pacote_codex")

        self.assertTrue(resultado["ok"])
        self.assertEqual(resultado["detalhes"]["pacote_codex"]["texto_para_codex"], "PACOTE")
        telegram_mock.assert_not_called()
        incidente_detalhes = incidente_mock.call_args.kwargs["detalhes"]
        self.assertNotIn("pacote_codex", incidente_detalhes)
        self.assertEqual(incidente_detalhes["pacote_codex_resumo"]["ultimo_erro"], "erro-1")

    def test_api_pacote_codex_retorna_relatorio_para_usuario_autorizado(self):
        pacote = {"texto_para_codex": "PACOTE", "ultimo_erro": {}}
        with app_module.app.test_request_context("/api/auto-suporte/pacote-codex"):
            session["usuario"] = "admin"
            with patch.object(app_module, "usuario_pode_usar_auto_suporte", return_value=True), \
                 patch.object(app_module, "montar_pacote_codex_auto_suporte", return_value=pacote):
                response = app_module.api_auto_suporte_pacote_codex()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()["pacote_codex"]["texto_para_codex"], "PACOTE")

    def test_auto_suporte_planeja_somente_acoes_autonomas_seguras(self):
        status = {
            "sugestoes": [
                {"titulo": "Pagina lenta", "acao": "limpar_caches"},
                {"titulo": "Planilha com erro", "acao": "desativar_planilhas_com_erro"},
                {"titulo": "Erro aberto", "acao": "gerar_pacote_codex"},
            ],
            "diagnostico": {"itens": [{"titulo": "Banco", "acao": "testar_banco"}]},
        }

        planejadas = app_module.planejar_acoes_autonomas_auto_suporte(status, estado={})

        self.assertEqual([item["acao"] for item in planejadas], ["limpar_caches", "gerar_pacote_codex", "testar_banco"])

    def test_auto_suporte_autonomia_respeita_cooldown(self):
        status = {
            "sugestoes": [
                {"titulo": "Pagina lenta", "acao": "limpar_caches"},
                {"titulo": "Banco", "acao": "testar_banco"},
            ],
            "diagnostico": {"itens": []},
        }
        estado = {
            "autonomia": {
                "acoes": {
                    "limpar_caches": {"ultimo_ts": time.time()},
                }
            }
        }

        planejadas = app_module.planejar_acoes_autonomas_auto_suporte(status, estado=estado)

        self.assertEqual([item["acao"] for item in planejadas], ["testar_banco"])

    def test_auto_suporte_autonomia_lista_acoes_que_exigem_confirmacao(self):
        status = {
            "sugestoes": [
                {"titulo": "Planilha com erro", "acao": "desativar_planilhas_com_erro"},
                {"titulo": "Pagina lenta", "acao": "limpar_caches"},
            ],
            "diagnostico": {"itens": [{"titulo": "Telegram", "acao": "enviar_alerta_telegram"}]},
        }

        autonomia = app_module.montar_autonomia_auto_suporte(estado={}, status_payload=status)

        bloqueadas = {item["acao"]: item for item in autonomia["acoes_bloqueadas"]}
        self.assertIn("desativar_planilhas_com_erro", bloqueadas)
        self.assertIn("enviar_alerta_telegram", bloqueadas)
        self.assertNotIn("limpar_caches", bloqueadas)
        self.assertIn("confirmacao", bloqueadas["desativar_planilhas_com_erro"]["seguranca"])
        self.assertEqual(bloqueadas["desativar_planilhas_com_erro"]["confirmacao"], "PAUSAR PLANILHAS")

    def test_auto_suporte_modo_observador_simula_sem_executar(self):
        status_inicial = {
            "ok": False,
            "falhas": ["Pagina demorou"],
            "sugestoes": [{"titulo": "Pagina lenta", "acao": "limpar_caches"}],
            "diagnostico": {"titulo": "Pagina lenta", "itens": []},
            "tempo_resposta": [],
        }
        with tempfile.TemporaryDirectory(prefix="auto_suporte_observador_") as pasta:
            estado = os.path.join(pasta, "estado.json")
            historico = os.path.join(pasta, "historico.json")
            with app_module.app.test_request_context("/api/auto-suporte/autonomia", method="POST"):
                session["usuario"] = "admin"
                with patch.object(app_module, "AUTO_SUPORTE_ESTADO_ARQUIVO", estado), \
                     patch.object(app_module, "AUTO_SUPORTE_HISTORICO_ARQUIVO", historico), \
                     patch.object(app_module, "status_auto_suporte", side_effect=[status_inicial, {"ok": True, "sugestoes": [], "diagnostico": {"itens": []}}]), \
                     patch.object(app_module, "executar_acao_auto_suporte") as acao_mock:
                    resultado = app_module.executar_autonomia_auto_suporte(modo="observador")

        self.assertTrue(resultado["ok"])
        self.assertEqual(resultado["modo"], "observador")
        self.assertEqual(resultado["executadas"], [])
        self.assertTrue(resultado["simulacao"]["pretende_fazer"])
        acao_mock.assert_not_called()

    def test_api_auto_suporte_autonomia_salva_modo_e_simula(self):
        status_inicial = {
            "ok": False,
            "sugestoes": [{"titulo": "Pagina lenta", "acao": "limpar_caches"}],
            "diagnostico": {"titulo": "Pagina lenta", "itens": []},
            "tempo_resposta": [],
        }
        with tempfile.TemporaryDirectory(prefix="auto_suporte_modo_") as pasta:
            estado = os.path.join(pasta, "estado.json")
            historico = os.path.join(pasta, "historico.json")
            with app_module.app.test_request_context("/api/auto-suporte/autonomia", method="POST", json={"modo": "manual", "simular": True}):
                session["usuario"] = "admin"
                with patch.object(app_module, "AUTO_SUPORTE_ESTADO_ARQUIVO", estado), \
                     patch.object(app_module, "AUTO_SUPORTE_HISTORICO_ARQUIVO", historico), \
                     patch.object(app_module, "usuario_pode_usar_auto_suporte", return_value=True), \
                     patch.object(app_module, "usuario_auto_suporte_tecnico", return_value=True), \
                     patch.object(app_module, "status_auto_suporte", side_effect=[status_inicial, {"ok": True, "sugestoes": [], "diagnostico": {"itens": []}}]):
                    response = app_module.api_auto_suporte_autonomia()

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertEqual(payload["modo"], "manual")
        self.assertEqual(payload["executadas"], [])

    def test_auto_suporte_limpa_cache_especifico_da_rota_lenta(self):
        app_module.CLIENTES_CONTEXT_CACHE["testado_em"] = 999999.0
        app_module.CLIENTES_CONTEXT_CACHE["resultado"] = {"clientes": [1]}

        resultado = app_module.limpar_cache_rota_lenta_auto_suporte([
            {"rota": "/clientes", "classe": "lento", "ultimo_ms": 3000}
        ])

        self.assertEqual(resultado["rota"], "/clientes")
        self.assertIsNone(app_module.CLIENTES_CONTEXT_CACHE["resultado"])

    def test_api_auto_suporte_autonomia_executa_reparo_seguro(self):
        status_inicial = {
            "ok": False,
            "sugestoes": [{"titulo": "Pagina lenta", "acao": "limpar_caches"}],
            "diagnostico": {"itens": []},
        }
        with tempfile.TemporaryDirectory(prefix="auto_suporte_autonomia_") as pasta:
            estado = os.path.join(pasta, "estado.json")
            historico = os.path.join(pasta, "historico.json")
            with app_module.app.test_request_context("/api/auto-suporte/autonomia", method="POST"):
                session["usuario"] = "admin"
                with patch.object(app_module, "AUTO_SUPORTE_ESTADO_ARQUIVO", estado), \
                     patch.object(app_module, "AUTO_SUPORTE_HISTORICO_ARQUIVO", historico), \
                     patch.object(app_module, "usuario_pode_usar_auto_suporte", return_value=True), \
                     patch.object(app_module, "usuario_auto_suporte_tecnico", return_value=True), \
                     patch.object(app_module, "status_auto_suporte", side_effect=[status_inicial, {"ok": True, "sugestoes": [], "diagnostico": {"itens": []}}]), \
                     patch.object(app_module, "executar_acao_auto_suporte", return_value={"ok": True, "mensagem": "Caches limpos."}) as acao_mock:
                    response = app_module.api_auto_suporte_autonomia()

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["executadas"][0]["acao"], "limpar_caches")
        acao_mock.assert_called_once()

    def test_auto_suporte_fluxos_usa_agregacao_compativel_com_postgres(self):
        class CursorFake:
            backend = "postgres"

            def __init__(self):
                self.sql = ""
                self.params = None

            def execute(self, sql, params=None):
                self.sql = sql
                self.params = params

            def fetchall(self):
                return []

        class ConnFake:
            def __init__(self):
                self.cursor_fake = CursorFake()

            def cursor(self):
                return self.cursor_fake

            def close(self):
                return None

        conn = ConnFake()
        with app_module.app.test_request_context("/api/auto-suporte/status"):
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar", return_value=conn):
                resultado = app_module.detectar_fluxos_suspeitos_auto_suporte()

        self.assertEqual(resultado, [])
        self.assertIn("STRING_AGG(servicos.id::text, ',')", conn.cursor_fake.sql)
        self.assertNotIn("GROUP_CONCAT", conn.cursor_fake.sql)

    def test_auto_suporte_planilhas_erro_parametriza_like_para_postgres(self):
        class CursorFake:
            backend = "postgres"

            def __init__(self):
                self.sql = ""
                self.params = None

            def execute(self, sql, params=None):
                self.sql = sql
                self.params = params

            def fetchall(self):
                return []

        class ConnFake:
            def __init__(self):
                self.cursor_fake = CursorFake()

            def cursor(self):
                return self.cursor_fake

            def close(self):
                return None

        conn = ConnFake()
        with app_module.app.test_request_context("/api/auto-suporte/status"):
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar", return_value=conn):
                resultado = app_module.listar_planilhas_com_erro_auto_suporte(limite=3)

        self.assertEqual(resultado, [])
        self.assertNotIn("LIKE '%ERRO%'", conn.cursor_fake.sql)
        self.assertNotIn("LIKE '%FALHA%'", conn.cursor_fake.sql)
        self.assertEqual(conn.cursor_fake.params, (1, "%ERRO%", "%FALHA%", "%ERRO%", "%FALHA%", 3))

    def test_auto_suporte_resolve_erros_antigos_quando_checks_voltam_a_passar(self):
        with tempfile.TemporaryDirectory(prefix="erros_auto_suporte_") as pasta:
            caminho = os.path.join(pasta, "erros.json")
            erros = [
                {"id": "fluxo-antigo", "descricao": "auto_suporte_fluxos", "resolvido": False},
                {"id": "planilha-antiga", "descricao": "pacote_codex_planilhas", "resolvido": False},
                {"id": "erro-real", "descricao": "erro_global", "resolvido": False},
            ]
            with patch.object(app_module, "ERROS_PRODUCAO_ARQUIVO", caminho):
                app_module.salvar_erros_producao(erros)
                with app_module.app.test_request_context("/api/auto-suporte/status"):
                    session["usuario"] = "admin"
                    with patch.object(app_module, "montar_status_sistema_dono", return_value={"resumo": {"falhas": []}, "itens": []}), \
                         patch.object(app_module, "detectar_fluxos_suspeitos_auto_suporte", return_value=[]), \
                         patch.object(app_module, "listar_planilhas_com_erro_auto_suporte", return_value=[]), \
                         patch.object(app_module, "metricas_tempo_resposta_central_tecnica", return_value=[]):
                        status = app_module.status_auto_suporte()

                erros_atualizados = {item["id"]: item for item in app_module.carregar_erros_producao()}
                self.assertTrue(erros_atualizados["fluxo-antigo"]["resolvido"])
                self.assertTrue(erros_atualizados["planilha-antiga"]["resolvido"])
                self.assertEqual(erros_atualizados["fluxo-antigo"]["resolvido_por"], "auto_suporte")
                self.assertFalse(erros_atualizados["erro-real"]["resolvido"])
                self.assertEqual([item["id"] for item in status["erros_abertos"]], ["erro-real"])

    def test_pacote_codex_atualiza_erros_abertos_depois_dos_checks(self):
        with tempfile.TemporaryDirectory(prefix="erros_pacote_codex_") as pasta:
            caminho = os.path.join(pasta, "erros.json")
            erros = [
                {"id": "pacote-fluxo", "descricao": "pacote_codex_fluxos", "resolvido": False},
                {"id": "pacote-planilha", "descricao": "auto_suporte_planilhas_erro", "resolvido": False},
            ]
            with patch.object(app_module, "ERROS_PRODUCAO_ARQUIVO", caminho):
                app_module.salvar_erros_producao(erros)
                with app_module.app.test_request_context("/api/auto-suporte/pacote-codex"):
                    session["usuario"] = "admin"
                    with patch.object(app_module, "montar_status_sistema_dono", return_value={"resumo": {"falhas": []}, "itens": []}), \
                         patch.object(app_module, "obter_status_banco_online", return_value={"conectado": True}), \
                         patch.object(app_module, "obter_status_backup_banco", return_value={"ok": True}), \
                         patch.object(app_module, "metricas_tempo_resposta_central_tecnica", return_value=[]), \
                         patch.object(app_module, "detectar_inconsistencias_negocio_auto_suporte", return_value=[]), \
                         patch.object(app_module, "detectar_fluxos_suspeitos_auto_suporte", return_value=[]), \
                         patch.object(app_module, "listar_planilhas_com_erro_auto_suporte", return_value=[]):
                        pacote = app_module.montar_pacote_codex_auto_suporte()

                self.assertEqual(pacote["erros_abertos"], [])
                self.assertTrue(all(item["resolvido"] for item in app_module.carregar_erros_producao()))

    def test_auto_suporte_classifica_erro_aberto_como_critico(self):
        diagnostico = app_module.montar_diagnostico_auto_suporte(
            {"itens": [{"nome": "Banco online", "ok": True}, {"nome": "Backup", "ok": True}]},
            [],
            [],
            [],
            [{"id": "erro-500"}],
        )

        self.assertEqual(diagnostico["nivel"], "critico")
        self.assertTrue(diagnostico["auto_abrir"])
        self.assertIn("Erro 500", diagnostico["titulo"])

    def test_auto_suporte_status_registra_historico_e_diagnostico(self):
        with tempfile.TemporaryDirectory(prefix="auto_suporte_status_") as pasta:
            historico = os.path.join(pasta, "historico.json")
            estado = os.path.join(pasta, "estado.json")
            erros = os.path.join(pasta, "erros.json")
            with patch.object(app_module, "AUTO_SUPORTE_HISTORICO_ARQUIVO", historico), \
                 patch.object(app_module, "AUTO_SUPORTE_ESTADO_ARQUIVO", estado), \
                 patch.object(app_module, "ERROS_PRODUCAO_ARQUIVO", erros), \
                 patch.object(app_module, "enviar_alerta_estabilidade_assincrono", return_value=True), \
                 patch.object(app_module, "montar_status_sistema_dono", return_value={"resumo": {"falhas": []}, "itens": [{"nome": "Banco online", "ok": False, "detalhe": "offline"}]}), \
                 patch.object(app_module, "obter_configuracao_empresa", return_value={"auto_teste_telegram_bot_token": "x", "auto_teste_telegram_chat_id": "1"}), \
                 patch.object(app_module, "detectar_fluxos_suspeitos_auto_suporte", return_value=[]), \
                 patch.object(app_module, "listar_planilhas_com_erro_auto_suporte", return_value=[]), \
                 patch.object(app_module, "detectar_inconsistencias_negocio_auto_suporte", return_value=[]), \
                 patch.object(app_module, "metricas_tempo_resposta_central_tecnica", return_value=[]):
                with app_module.app.test_request_context("/api/auto-suporte/status"):
                    session["usuario"] = "admin"
                    status = app_module.status_auto_suporte()

            self.assertEqual(status["diagnostico"]["nivel"], "critico")
            self.assertTrue(status["auto_abrir"])
            self.assertTrue(status["historico"])
            self.assertEqual(status["historico"][0]["evento"], "diagnostico")
            self.assertEqual(status["perfil"], "administrador")
            self.assertEqual(status["modo_interface"], "simples")
            self.assertTrue(status["acoes_simples"])
            self.assertEqual(status["plano_acao"]["prioridade"], "alta")
            self.assertIn("Banco online", status["plano_acao"]["titulo"])
            acoes = {item["id"]: item for item in status["acoes"]}
            self.assertIn("limpar_cache_rota_lenta", acoes)
            self.assertEqual(acoes["limpar_todos_erros"]["confirmacao"], "LIMPAR TODOS OS ERROS")
            self.assertTrue(acoes["limpar_todos_erros"]["confirmacao_obrigatoria"])

    def test_auto_suporte_plano_acao_bloqueia_acao_tecnica_para_admin(self):
        status = {
            "diagnostico": {"nivel": "alerta", "titulo": "Planilha com erro", "frase": "Revisar sync."},
            "sugestoes": [{"titulo": "Planilha com erro", "acao": "desativar_planilhas_com_erro"}],
            "autonomia": {
                "acoes_bloqueadas": [
                    {"acao": "desativar_planilhas_com_erro", "label": "Pausar planilhas"}
                ],
                "simulacao": {"pretende_fazer": []},
            },
            "falhas": ["Planilha com erro"],
        }
        with app_module.app.test_request_context("/api/auto-suporte/status"):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            plano = app_module.montar_plano_acao_auto_suporte(status)

        self.assertFalse(plano["executavel"])
        self.assertFalse(plano["permitido"])
        self.assertIn("desenvolvedor", plano["bloqueio"])

    def test_auto_suporte_acao_limpa_erros_resolvidos(self):
        with tempfile.TemporaryDirectory(prefix="auto_suporte_limpa_erros_") as pasta:
            caminho = os.path.join(pasta, "erros.json")
            erros = [
                {"id": "aberto", "resolvido": False},
                {"id": "resolvido", "resolvido": True},
            ]
            with patch.object(app_module, "ERROS_PRODUCAO_ARQUIVO", caminho), \
                 patch.object(app_module, "registrar_incidente_auto_suporte"), \
                 patch.object(app_module, "status_auto_suporte", return_value={"ok": True, "falhas": []}):
                app_module.salvar_erros_producao(erros)
                resultado = app_module.executar_acao_auto_suporte("limpar_erros_resolvidos", confirmacao="LIMPAR ERROS RESOLVIDOS")
                erros_restantes = app_module.carregar_erros_producao()

            self.assertTrue(resultado["ok"])
            self.assertEqual(resultado["detalhes"]["erros_removidos"], 1)
            self.assertEqual([item["id"] for item in erros_restantes], ["aberto"])

    def test_auto_suporte_acao_limpa_todos_os_erros(self):
        with tempfile.TemporaryDirectory(prefix="auto_suporte_limpa_todos_") as pasta:
            caminho_erros = os.path.join(pasta, "erros.json")
            caminho_historico = os.path.join(pasta, "historico.json")
            erros = [
                {"id": "aberto", "resolvido": False},
                {"id": "resolvido", "resolvido": True},
            ]
            with app_module.app.test_request_context("/auto-suporte/acao"):
                session["usuario"] = "dev"
                session["usuario_perfil"] = "desenvolvedor"
                with patch.object(app_module, "ERROS_PRODUCAO_ARQUIVO", caminho_erros), \
                     patch.object(app_module, "AUTO_SUPORTE_HISTORICO_ARQUIVO", caminho_historico), \
                     patch.object(app_module, "registrar_incidente_auto_suporte"), \
                     patch.object(app_module, "status_auto_suporte", return_value={"ok": True, "falhas": []}):
                    app_module.salvar_erros_producao(erros)
                    resultado = app_module.executar_acao_auto_suporte("limpar_todos_erros", confirmacao="LIMPAR TODOS OS ERROS")
                    erros_restantes = app_module.carregar_erros_producao()
                    historico = app_module.listar_historico_auto_suporte(limite=5)

            self.assertTrue(resultado["ok"])
            self.assertEqual(resultado["detalhes"]["erros_removidos"], 2)
            self.assertEqual(erros_restantes, [])
            self.assertEqual(historico[0]["evento"], "limpeza_erros")

    def test_auto_suporte_detecta_inconsistencias_de_negocio(self):
        class CursorFake:
            def __init__(self):
                self.calls = 0

            def execute(self, sql, params=None):
                self.calls += 1

            def fetchone(self):
                return [1 if self.calls in {1, 3, 6} else 0]

        class ConnFake:
            def __init__(self):
                self.cursor_fake = CursorFake()

            def cursor(self):
                return self.cursor_fake

            def close(self):
                return None

        with app_module.app.test_request_context("/auto-suporte"):
            session["empresa_id"] = 1
            with patch.object(app_module, "conectar", return_value=ConnFake()):
                inconsistencias = app_module.detectar_inconsistencias_negocio_auto_suporte()

        ids = {item["id"] for item in inconsistencias}
        self.assertIn("servico_sem_veiculo", ids)
        self.assertIn("novo_com_historico", ids)
        self.assertIn("planilha_sincronizando_ha_muito_tempo", ids)
        self.assertNotIn("retorno_sem_historico", ids)
        novo = next(item for item in inconsistencias if item["id"] == "novo_com_historico")
        self.assertEqual(novo["acao"], "corrigir_classificacao_clientes")

    def test_auto_suporte_corrige_novo_com_historico_para_retorno(self):
        class CursorFake:
            rowcount = 1

            def __init__(self):
                self.sql = ""
                self.params = None

            def execute(self, sql, params=None):
                self.sql = sql
                self.params = params

        class ConnFake:
            def __init__(self):
                self.cursor_fake = CursorFake()
                self.committed = False

            def cursor(self):
                return self.cursor_fake

            def commit(self):
                self.committed = True

            def close(self):
                return None

        conn = ConnFake()
        with app_module.app.test_request_context("/auto-suporte"):
            session["empresa_id"] = 7
            with patch.object(app_module, "conectar", return_value=conn), \
                 patch.object(app_module, "limpar_caches_operacionais_leves") as caches_mock, \
                 patch.object(app_module, "limpar_cache_clientes") as clientes_mock:
                resultado = app_module.corrigir_classificacao_clientes_auto_suporte()

        self.assertEqual(resultado["novos_corrigidos_para_retorno"], 1)
        self.assertIn("SET perfil_cliente_atendimento='RETORNO'", conn.cursor_fake.sql)
        self.assertIn("UPPER(COALESCE(perfil_cliente_atendimento, ''))='NOVO'", conn.cursor_fake.sql)
        self.assertEqual(conn.cursor_fake.params, (7,))
        self.assertTrue(conn.committed)
        caches_mock.assert_called_once()
        clientes_mock.assert_called_once()

    def test_retornos_atualizar_mostra_feedback_quando_banco_falha_ao_ler(self):
        with app_module.app.test_request_context(
            "/retornos/atualizar",
            method="POST",
            data={"placa": "ABC1234", "acao": "sem_interesse", "retorno_url": "/retornos"},
        ):
            session["usuario"] = "admin"
            with patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "carregar_estados_retornos", side_effect=RuntimeError("banco offline")), \
                 patch.object(app_module, "registrar_ultimo_erro_producao") as erro_mock:
                response = app_module.atualizar_retorno_cliente()

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.location, "/retornos")
            self.assertEqual(session["retornos_feedback"]["tipo"], "erro")
            self.assertIn("Nao foi possivel atualizar", session["retornos_feedback"]["mensagem"])
            erro_mock.assert_called_once()

    def test_retornos_atualizar_nao_quebra_quando_auditoria_falha(self):
        with app_module.app.test_request_context(
            "/retornos/atualizar",
            method="POST",
            data={"placa": "ABC1234", "acao": "sem_interesse", "retorno_url": "/retornos"},
        ):
            session["usuario"] = "admin"
            with patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "carregar_estados_retornos", return_value={"ABC1234": {"status": "pendente"}}), \
                 patch.object(app_module, "upsert_retorno_cliente") as upsert_mock, \
                 patch.object(app_module, "registrar_auditoria", side_effect=RuntimeError("auditoria offline")), \
                 patch.object(app_module, "registrar_ultimo_erro_producao") as erro_mock:
                response = app_module.atualizar_retorno_cliente()

            self.assertEqual(response.status_code, 302)
            self.assertEqual(response.location, "/retornos")
            self.assertEqual(session["retornos_feedback"]["tipo"], "sucesso")
            self.assertIn("sem interesse", session["retornos_feedback"]["mensagem"])
            upsert_mock.assert_called_once()
            erro_mock.assert_called_once()

    def test_upsert_retorno_cliente_nao_depende_de_on_conflict(self):
        class CursorFake:
            def __init__(self):
                self.sqls = []
                self.params = []
                self.rowcount = 1

            def execute(self, sql, params=None):
                self.sqls.append(sql)
                self.params.append(params)

        class ConnFake:
            def __init__(self):
                self.cursor_fake = CursorFake()
                self.committed = False
                self.closed = False

            def cursor(self):
                return self.cursor_fake

            def commit(self):
                self.committed = True

            def close(self):
                self.closed = True

        conn = ConnFake()
        with app_module.app.test_request_context("/retornos/atualizar"):
            session["empresa_id"] = 4
            with patch.object(app_module, "conectar", return_value=conn), \
                 patch.object(app_module, "agora_iso", return_value="2026-05-08T10:00:00"):
                app_module.upsert_retorno_cliente(
                    "abc1234",
                    "sem_interesse",
                    observacao="pausar",
                    usuario={"usuario": "luiz", "nome": "Luiz"},
                )

        sql_total = "\n".join(conn.cursor_fake.sqls)
        self.assertIn("UPDATE retornos_clientes", sql_total)
        self.assertNotIn("ON CONFLICT", sql_total)
        self.assertEqual(len(conn.cursor_fake.sqls), 1)
        self.assertEqual(conn.cursor_fake.params[0][-2:], (4, "ABC1234"))
        self.assertTrue(conn.committed)
        self.assertTrue(conn.closed)

    def test_upsert_retorno_cliente_insere_quando_update_nao_afeta_linha(self):
        class CursorFake:
            def __init__(self):
                self.sqls = []
                self.params = []
                self.rowcount = 0

            def execute(self, sql, params=None):
                self.sqls.append(sql)
                self.params.append(params)
                self.rowcount = 0 if len(self.sqls) == 1 else 1

        class ConnFake:
            def __init__(self):
                self.cursor_fake = CursorFake()

            def cursor(self):
                return self.cursor_fake

            def commit(self):
                return None

            def close(self):
                return None

        conn = ConnFake()
        with app_module.app.test_request_context("/retornos/atualizar"):
            session["empresa_id"] = 5
            with patch.object(app_module, "conectar", return_value=conn), \
                 patch.object(app_module, "agora_iso", return_value="2026-05-08T10:00:00"):
                app_module.upsert_retorno_cliente("XYZ9876", "contatado")

        self.assertEqual(len(conn.cursor_fake.sqls), 2)
        self.assertIn("UPDATE retornos_clientes", conn.cursor_fake.sqls[0])
        self.assertIn("INSERT INTO retornos_clientes", conn.cursor_fake.sqls[1])
        self.assertEqual(conn.cursor_fake.params[1][0:2], (5, "XYZ9876"))

    def test_pagina_auto_suporte_renderiza_painel_proprio(self):
        status_auto = {
            "ok": True,
            "gerado_em": "2026-05-08T10:00:00",
            "diagnostico": {
                "nivel": "info",
                "label": "Informativo",
                "titulo": "Tudo operacional",
                "frase": "Sem incidentes.",
                "itens": [],
            },
            "erros_abertos": [],
            "inconsistencias_negocio": [],
            "sugestoes": [],
        }
        status_sistema = {
            "gerado_em": "2026-05-08T10:00:00",
            "resumo": {"ok": True, "falhas": []},
            "itens": [
                {"nome": "Banco online", "ok": True, "valor": "Ativo", "detalhe": "OK"},
                {"nome": "Backup", "ok": True, "valor": "Hoje", "detalhe": "OK"},
                {"nome": "PWA instalado", "ok": True, "valor": "Pronto", "detalhe": "OK"},
                {"nome": "Licenca", "ok": True, "valor": "Business", "detalhe": "OK"},
                {"nome": "Bot Telegram", "ok": True, "valor": "Ativo", "detalhe": "OK"},
            ],
            "ultimo_erro": {},
        }
        with app_module.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["usuario"] = "admin"
                sess["usuario_perfil"] = "admin"
                sess["empresa_id"] = 1
                sess["senha_alteracao_obrigatoria"] = False
            with patch.object(app_module, "usuario_pode_usar_auto_suporte", return_value=True), \
                 patch.object(app_module, "usuario_desenvolvedor", return_value=True), \
                 patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "status_auto_suporte", return_value=status_auto), \
                 patch.object(app_module, "montar_status_sistema_dono", return_value=status_sistema), \
                 patch.object(app_module, "listar_historico_auto_suporte", return_value=[]):
                response = client.get("/auto-suporte")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("AutoSuporte IA", html)
        self.assertIn("IA guiada", html)
        self.assertIn("O que voce autoriza a IA fazer agora", html)
        self.assertIn("Acoes sensiveis", html)
        self.assertIn("Registrar incidente", html)
        self.assertIn("LIMPAR TODOS OS ERROS", html)
        self.assertIn("CORRIGIR CLASSIFICACAO", html)

    def test_pagina_auto_suporte_admin_renderiza_fluxo_simples(self):
        status_auto = {
            "ok": True,
            "gerado_em": "2026-05-08T10:00:00",
            "diagnostico": {
                "nivel": "info",
                "label": "Informativo",
                "titulo": "Tudo operacional",
                "frase": "Sem incidentes.",
                "itens": [],
            },
            "falhas": [],
            "erros_abertos": [],
            "inconsistencias_negocio": [],
            "sugestoes": [],
            "tempo_resposta": [],
        }
        status_sistema = {
            "gerado_em": "2026-05-08T10:00:00",
            "resumo": {"ok": True, "falhas": []},
            "itens": [],
            "ultimo_erro": {},
        }
        with app_module.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["usuario"] = "admin"
                sess["usuario_perfil"] = "admin"
                sess["empresa_id"] = 1
                sess["senha_alteracao_obrigatoria"] = False
            with patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "status_auto_suporte", return_value=status_auto), \
                 patch.object(app_module, "montar_status_sistema_dono", return_value=status_sistema), \
                 patch.object(app_module, "listar_historico_auto_suporte", return_value=[]):
                response = client.get("/auto-suporte")

        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn("AutoSuporte IA", html)
        self.assertIn("O que voce autoriza a IA fazer agora", html)
        self.assertIn("Melhorar velocidade", html)
        self.assertIn("Melhorar carregamento", html)
        self.assertNotIn("Acoes sensiveis", html)
        self.assertNotIn("Limpar todos os erros", html)

    def test_auto_suporte_funcionario_nao_acessa_nem_ve_widget(self):
        with app_module.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["usuario"] = "operador"
                sess["usuario_perfil"] = "funcionario"
                sess["usuario_id"] = 2
                sess["empresa_id"] = 1
                sess["senha_alteracao_obrigatoria"] = False
            with patch.object(app_module, "sincronizar_sessao_usuario"):
                status_response = client.get("/api/auto-suporte/status")
                pagina_response = client.get("/auto-suporte")
                home_response = client.get("/")

        self.assertEqual(status_response.status_code, 403)
        self.assertEqual(pagina_response.status_code, 302)
        self.assertNotIn("auto_suporte.js", home_response.get_data(as_text=True))

    def test_fluxos_criticos_smoke_login_pwa_auto_suporte(self):
        regras = {str(rule) for rule in app_module.app.url_map.iter_rules()}
        self.assertIn("/login", regras)
        self.assertIn("/", regras)
        self.assertIn("/clientes", regras)
        self.assertIn("/auto-suporte", regras)
        self.assertIn("/site.webmanifest", regras)
        self.assertIn("/sw.js", regras)
        self.assertIn("/api/pwa/status", regras)

        with app_module.app.test_client() as client:
            self.assertEqual(client.get("/login").status_code, 200)
            self.assertEqual(client.get("/site.webmanifest").status_code, 200)
            self.assertEqual(client.get("/sw.js").status_code, 200)
            self.assertEqual(client.get("/api/pwa/status").status_code, 200)
            self.assertEqual(client.get("/clientes").status_code, 302)


if __name__ == "__main__":
    unittest.main()

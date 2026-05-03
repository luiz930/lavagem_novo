import sqlite3
import unittest
from unittest.mock import patch

from flask import session

import app as app_module


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

    def test_login_missing_fields_returns_error_message(self):
        with patch.object(app_module, "csrf_protection_ativa", return_value=False), \
             patch.object(app_module, "INIT_DB_EXECUTADO", True):
            response = self.client.post("/login", data={"usuario": "", "senha": ""})

        self.assertEqual(response.status_code, 200)
        self.assertIn(b"Informe usuario e senha.", response.data)

    def test_configuracoes_requires_login(self):
        with patch.object(app_module, "INIT_DB_EXECUTADO", True):
            response = self.client.get("/configuracoes")

        self.assertEqual(response.status_code, 302)
        self.assertIn("/login", response.headers.get("Location", ""))

    def test_configuracoes_site_get_renders_for_logged_admin(self):
        with app_module.app.test_request_context("/configuracoes/site", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 2
            session["senha_alteracao_obrigatoria"] = False
            with patch.object(app_module, "preparar_rotinas_interface_logada"), \
                 patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                 patch.object(app_module, "obter_configuracao_empresa", return_value=app_module.empresa_snapshot_padrao()), \
                 patch.object(app_module, "render_template", return_value="ok") as render_mock:
                response = app_module.configuracoes_site()

        self.assertEqual(response, "ok")
        render_mock.assert_called_once()

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


if __name__ == "__main__":
    unittest.main()

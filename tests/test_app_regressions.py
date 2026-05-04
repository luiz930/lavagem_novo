import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import zipfile

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


if __name__ == "__main__":
    unittest.main()

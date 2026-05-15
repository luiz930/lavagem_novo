import json
import os
import re
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

    def test_api_mobile_sync_exige_token_configurado(self):
        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": ""}, clear=False):
            resposta = self.client.post("/api/mobile/sync", json={"changes": []})

        self.assertEqual(resposta.status_code, 401)
        self.assertFalse(resposta.get_json()["ok"])

    def test_api_mobile_hud_retorna_payload_do_site_com_token(self):
        payload_hud = {
            "total": 12,
            "andamento": 4,
            "banco_online_resumo": "Online",
            "sync_bancos_resumo": "Atualizado",
            "versao": "Versao: 1.0.0",
        }

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "obter_payload_hud", return_value=payload_hud), \
             patch.object(app_module, "obter_versao_sistema", return_value="Versao: 1.0.0") as versao_mock:
            resposta = self.client.get(
                "/api/mobile/hud",
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        dados = resposta.get_json()
        self.assertTrue(dados["ok"])
        self.assertEqual(dados["hud"]["total"], 12)
        self.assertEqual(dados["versao_sistema"], "Versao: 1.0.0")
        versao_mock.assert_any_call(permitir_sem_sessao=True)

    def test_api_mobile_site_state_retorna_hud_clima_e_modulos(self):
        payload = {
            "hud": {"total": 5, "versao": "Versao: 1.0.0"},
            "clima": {"clima": "Tempo limpo", "temp": 24, "icone": "sol", "sugestao": "Lavagem completa"},
            "modulos": {"clima": {"rows": [{"title": "Tempo limpo", "detail": "Lavagem completa", "badge": "24"}]}},
            "versao_sistema": "Versao: 1.0.0",
            "server_time": "2026-05-14T12:00:00",
            "refresh_interval_seconds": 10,
        }

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "montar_payload_mobile_site_state", return_value=payload):
            resposta = self.client.get(
                "/api/mobile/site-state",
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        dados = resposta.get_json()
        self.assertTrue(dados["ok"])
        self.assertEqual(dados["clima"]["clima"], "Tempo limpo")
        self.assertIn("clima", dados["modulos"])
        self.assertEqual(dados["refresh_interval_seconds"], 10)

    def test_api_mobile_configuracao_salva_versao_no_site(self):
        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "salvar_campos_configuracao_empresa", return_value={"versao_sistema": "1.0.2"}) as salvar_mock, \
             patch.object(app_module, "obter_versao_sistema", return_value="Versao: 1.0.2") as versao_mock, \
             patch.object(app_module, "registrar_auditoria"):
            resposta = self.client.post(
                "/api/mobile/configuracao",
                json={"versao_sistema": "1.0.2"},
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        dados = resposta.get_json()
        self.assertTrue(dados["ok"])
        self.assertEqual(dados["versao_sistema"], "Versao: 1.0.2")
        salvar_mock.assert_called_once()
        self.assertEqual(salvar_mock.call_args.args[0]["versao_sistema"], "1.0.2")
        versao_mock.assert_any_call(permitir_sem_sessao=True)

    def test_sync_bancos_resolve_automatico_apenas_quando_mais_recente_e_seguro(self):
        origem = {"id": 1, "nome": "Cliente novo", "atualizado_em": "2026-05-14T12:00:00"}
        destino = {"id": 1, "nome": "Cliente antigo", "atualizado_em": "2026-05-14T11:00:00"}

        self.assertFalse(app_module.detectar_conflito_registro_sync("clientes", origem, destino))
        self.assertTrue(app_module.decidir_atualizar_registro_sync(origem, destino, tabela="clientes"))

    def test_sync_bancos_mantem_conflito_em_financeiro_ou_finalizado(self):
        financeiro_origem = {"id": 1, "valor": 120, "atualizado_em": "2026-05-14T12:00:00"}
        financeiro_destino = {"id": 1, "valor": 100, "atualizado_em": "2026-05-14T11:00:00"}
        finalizado_origem = {"id": 2, "status": "FINALIZADO", "observacoes": "Site", "atualizado_em": "2026-05-14T12:00:00"}
        finalizado_destino = {"id": 2, "status": "EM ANDAMENTO", "observacoes": "Local", "atualizado_em": "2026-05-14T11:00:00"}

        self.assertTrue(app_module.detectar_conflito_registro_sync("servico_cobrancas_extras", financeiro_origem, financeiro_destino))
        self.assertTrue(app_module.detectar_conflito_registro_sync("servicos", finalizado_origem, finalizado_destino))

    def test_sync_bancos_fotos_nao_viram_conflito_de_sobrescrita(self):
        origem = {"id": 1, "caminho": "/foto-online.jpg", "atualizado_em": "2026-05-14T12:00:00"}
        destino = {"id": 1, "caminho": "/foto-local.jpg", "atualizado_em": "2026-05-14T12:00:00"}

        self.assertFalse(app_module.detectar_conflito_registro_sync("fotos", origem, destino))

    def test_api_mobile_sync_registra_eventos_aceitos(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        compat = PersistentCompatConnection(conn)

        payload = {
            "changes": [
                {
                    "id": 7,
                    "entity": "fotos",
                    "entity_uuid": "foto-local-1",
                    "action": "upsert",
                    "payload": {"uri_local": "file:///foto.jpg", "usuario": "admin"},
                }
            ]
        }

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json=payload,
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(resposta.get_json()["accepted_ids"], [7])
        row = conn.execute("SELECT * FROM mobile_sync_eventos").fetchone()
        self.assertEqual(row["entity"], "fotos")
        self.assertEqual(row["entity_uuid"], "foto-local-1")
        self.assertIn('"usuario": "admin"', row["payload_json"])
        conn.close()

    def test_api_mobile_sync_aplica_servico_offline(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                veiculo_id INTEGER,
                status TEXT,
                observacoes TEXT,
                etapa_atual TEXT,
                entrada TEXT,
                entrega TEXT,
                criado_por_usuario TEXT,
                criado_por_nome TEXT,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            )
            """
        )
        compat = PersistentCompatConnection(conn)

        payload = {
            "changes": [
                {
                    "id": 8,
                    "entity": "servicos",
                    "entity_uuid": "servico-local-1",
                    "action": "upsert",
                    "payload": {
                        "status": "ABERTO",
                        "observacoes": "Lavagem cadastrada no app",
                        "etapa_atual": "LAVAGEM",
                        "entrada": "2026-05-14T10:00:00",
                    },
                }
            ]
        }

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json=payload,
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(resposta.get_json()["accepted_ids"], [8])
        servico = conn.execute("SELECT * FROM servicos WHERE mobile_uuid='servico-local-1'").fetchone()
        self.assertIsNotNone(servico)
        self.assertEqual(servico["status"], "ABERTO")
        self.assertEqual(servico["observacoes"], "Lavagem cadastrada no app")
        self.assertEqual(servico["etapa_atual"], "LAVAGEM")
        conn.close()

    def test_api_mobile_sync_retorna_clientes_e_veiculos_do_site(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT,
                telefone TEXT,
                placa_principal TEXT,
                data_nascimento TEXT,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            );
            CREATE TABLE veiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                placa TEXT,
                modelo TEXT,
                cor TEXT,
                cliente_id INTEGER,
                status_atendimento TEXT,
                atendimento_ativo INTEGER,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            );
            INSERT INTO clientes (nome, telefone, placa_principal) VALUES ('Cliente Site', '1199999', 'ABC1234');
            INSERT INTO veiculos (placa, modelo, cor, cliente_id, status_atendimento, atendimento_ativo)
            VALUES ('ABC1234', 'Gol', 'Prata', 1, 'SEM_ATENDIMENTO', 0);
            """
        )
        compat = PersistentCompatConnection(conn)

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json={"changes": []},
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        changes = resposta.get_json()["changes"]
        self.assertEqual(len(changes), 2)
        self.assertEqual(changes[0]["entity"], "clientes")
        self.assertEqual(changes[0]["payload"]["nome"], "Cliente Site")
        self.assertEqual(changes[1]["entity"], "veiculos")
        self.assertEqual(changes[1]["payload"]["placa"], "ABC1234")
        conn.close()

    def test_api_mobile_sync_retorna_tipos_servico_do_site(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE tipos_servico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT,
                valor REAL
            )
            """
        )
        conn.execute("INSERT INTO tipos_servico (nome, valor) VALUES ('Lavagem Completa', 80)")
        compat = PersistentCompatConnection(conn)

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json={"changes": []},
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        changes = resposta.get_json()["changes"]
        self.assertEqual(changes[0]["entity"], "tipos_servico")
        self.assertEqual(changes[0]["payload"]["nome"], "Lavagem Completa")
        self.assertEqual(changes[0]["payload"]["valor"], 80.0)
        conn.close()

    def test_api_mobile_sync_retorna_servicos_e_fotos_do_site(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE veiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mobile_uuid TEXT
            );
            CREATE TABLE tipos_servico (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT,
                valor REAL
            );
            CREATE TABLE servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                veiculo_id INTEGER,
                tipo_id INTEGER,
                valor REAL,
                valor_adicional REAL,
                status TEXT,
                observacoes TEXT,
                etapa_atual TEXT,
                entrada TEXT,
                entrega_prevista TEXT,
                entrega TEXT,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            );
            CREATE TABLE fotos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                servico_id INTEGER,
                tipo TEXT,
                caminho TEXT,
                usuario TEXT,
                usuario_nome TEXT,
                tamanho_bytes INTEGER,
                largura INTEGER,
                altura INTEGER,
                mime_type TEXT,
                criado_em TEXT,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            );
            INSERT INTO veiculos (mobile_uuid) VALUES ('veiculo-site-1');
            INSERT INTO tipos_servico (nome, valor) VALUES ('Lavagem Completa', 80);
            INSERT INTO servicos (
                veiculo_id, tipo_id, valor, valor_adicional, status, observacoes,
                etapa_atual, entrada, entrega_prevista, mobile_uuid
            )
            VALUES (1, 1, 90, 10, 'EM ANDAMENTO', 'Site', 'LAVAGEM', '2026-05-14T10:00:00', '2026-05-14T12:00:00', 'servico-site-1');
            INSERT INTO fotos (servico_id, tipo, usuario, usuario_nome, mime_type, criado_em, mobile_uuid)
            VALUES (1, 'entrada', 'admin', 'Admin', 'image/jpeg', '2026-05-14T10:01:00', 'foto-site-1');
            """
        )
        compat = PersistentCompatConnection(conn)

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json={"changes": []},
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        changes = resposta.get_json()["changes"]
        servico = next(item for item in changes if item["entity"] == "servicos")
        foto = next(item for item in changes if item["entity"] == "fotos")
        self.assertEqual(servico["payload"]["uuid"], "servico-site-1")
        self.assertEqual(servico["payload"]["veiculo_uuid"], "veiculo-site-1")
        self.assertEqual(servico["payload"]["fotos_entrada"], 1)
        self.assertEqual(foto["payload"]["servico_uuid"], "servico-site-1")
        self.assertEqual(foto["payload"]["tipo"], "entrada")
        conn.close()

    def test_api_mobile_sync_retorna_catalogos_operacionais_do_site(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE produtos_pneu (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT
            );
            CREATE TABLE checklist_itens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT,
                ativo INTEGER,
                ordem INTEGER,
                criado_em TEXT
            );
            CREATE TABLE adicionais (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT
            );
            CREATE TABLE servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mobile_uuid TEXT
            );
            CREATE TABLE servico_cobrancas_extras (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                servico_id INTEGER,
                descricao TEXT,
                valor REAL,
                criado_em TEXT,
                criado_por_usuario TEXT,
                criado_por_nome TEXT
            );
            INSERT INTO produtos_pneu (nome) VALUES ('Pretinho');
            INSERT INTO checklist_itens (nome, ativo, ordem, criado_em) VALUES ('Conferir vidros', 1, 2, '2026-05-14T10:00:00');
            INSERT INTO adicionais (nome) VALUES ('Higienizacao');
            INSERT INTO servicos (mobile_uuid) VALUES ('servico-site-1');
            INSERT INTO servico_cobrancas_extras (servico_id, descricao, valor, criado_em)
            VALUES (1, 'Produto extra', 15, '2026-05-14T10:10:00');
            """
        )
        compat = PersistentCompatConnection(conn)

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json={"changes": []},
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        changes = resposta.get_json()["changes"]
        entidades = {item["entity"]: item for item in changes}
        self.assertEqual(entidades["produtos_pneu"]["payload"]["nome"], "Pretinho")
        self.assertEqual(entidades["checklist_itens"]["payload"]["nome"], "Conferir vidros")
        self.assertEqual(entidades["adicionais"]["payload"]["nome"], "Higienizacao")
        self.assertEqual(entidades["servico_cobrancas_extras"]["payload"]["servico_uuid"], "servico-site-1")
        conn.close()

    def test_api_mobile_sync_aplica_veiculo_offline(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nome TEXT,
                telefone TEXT,
                placa_principal TEXT,
                data_nascimento TEXT,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            );
            CREATE TABLE veiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                placa TEXT,
                modelo TEXT,
                cor TEXT,
                cliente_id INTEGER,
                status_atendimento TEXT,
                atendimento_ativo INTEGER,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            );
            INSERT INTO clientes (nome, mobile_uuid) VALUES ('Cliente App', 'cliente-app-1');
            """
        )
        compat = PersistentCompatConnection(conn)
        payload = {
            "changes": [
                {
                    "id": 9,
                    "entity": "veiculos",
                    "entity_uuid": "veiculo-app-1",
                    "action": "upsert",
                    "payload": {
                        "cliente_uuid": "cliente-app-1",
                        "placa": "APP1234",
                        "modelo": "Onix",
                        "cor": "Preto",
                    },
                }
            ]
        }

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json=payload,
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        veiculo = conn.execute("SELECT * FROM veiculos WHERE mobile_uuid='veiculo-app-1'").fetchone()
        self.assertIsNotNone(veiculo)
        self.assertEqual(veiculo["placa"], "APP1234")
        self.assertEqual(veiculo["modelo"], "Onix")
        self.assertEqual(veiculo["cliente_id"], 1)
        conn.close()

    def test_api_mobile_sync_aplica_foto_offline_vinculada_ao_servico(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mobile_uuid TEXT
            );
            CREATE TABLE fotos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER,
                servico_id INTEGER,
                tipo TEXT,
                caminho TEXT,
                usuario TEXT,
                usuario_nome TEXT,
                tamanho_bytes INTEGER,
                largura INTEGER,
                altura INTEGER,
                arquivo_blob BLOB,
                mime_type TEXT,
                arquivo_nome TEXT,
                mobile_uuid TEXT,
                mobile_updated_at TEXT
            );
            INSERT INTO servicos (mobile_uuid) VALUES ('servico-app-1');
            """
        )
        compat = PersistentCompatConnection(conn)

        with patch.dict(os.environ, {"MOBILE_SYNC_TOKEN": "segredo"}, clear=False), \
             patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/sync",
                json={
                    "changes": [
                        {
                            "id": 10,
                            "entity": "fotos",
                            "entity_uuid": "foto-app-1",
                            "action": "upsert",
                            "payload": {
                                "servico_uuid": "servico-app-1",
                                "tipo": "saida",
                                "uri_local": "file:///foto.jpg",
                                "arquivo_base64": "Zm90bw==",
                                "mime_type": "image/jpeg",
                                "usuario": "admin",
                                "usuario_nome": "Admin",
                            },
                        }
                    ]
                },
                headers={"Authorization": "Bearer segredo"},
            )

        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(resposta.get_json()["accepted_ids"], [10])
        foto = conn.execute("SELECT * FROM fotos WHERE mobile_uuid='foto-app-1'").fetchone()
        servico = conn.execute("SELECT * FROM servicos WHERE mobile_uuid='servico-app-1'").fetchone()
        self.assertIsNotNone(foto)
        self.assertEqual(foto["servico_id"], servico["id"])
        self.assertEqual(foto["tipo"], "saida")
        self.assertEqual(foto["arquivo_blob"], b"foto")
        conn.close()

    def test_api_mobile_login_gera_token_e_usuario_para_app(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario TEXT UNIQUE,
                senha TEXT,
                nome TEXT,
                perfil TEXT,
                ativo INTEGER DEFAULT 1,
                criado_em TEXT,
                tentativas_login INTEGER DEFAULT 0,
                bloqueado_ate TEXT,
                ultimo_login_em TEXT,
                senha_alteracao_obrigatoria INTEGER DEFAULT 0,
                senha_atualizada_em TEXT,
                foto_perfil TEXT,
                hud_config_json TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO usuarios (usuario, senha, nome, perfil, ativo) VALUES (?, ?, ?, ?, 1)",
            ("admin", app_module.senha_hash_bcrypt("Senha@123"), "Administrador", "admin"),
        )
        conn.commit()
        compat = PersistentCompatConnection(conn)

        with patch.object(app_module, "conectar", return_value=compat):
            resposta = self.client.post(
                "/api/mobile/login",
                json={"usuario": "admin", "senha": "Senha@123"},
            )

        self.assertEqual(resposta.status_code, 200)
        dados = resposta.get_json()
        self.assertTrue(dados["ok"])
        self.assertTrue(dados["token"])
        self.assertEqual(dados["usuario"]["usuario"], "admin")
        token_row = conn.execute("SELECT * FROM mobile_tokens").fetchone()
        self.assertEqual(token_row["usuario"], "admin")
        self.assertNotEqual(token_row["token_hash"], dados["token"])
        conn.close()

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

    def test_normalizar_redirect_interno_bloqueia_destinos_externos(self):
        self.assertEqual(
            app_module.normalizar_redirect_interno("https://example.com/phish", "/historico"),
            "/historico",
        )
        self.assertEqual(
            app_module.normalizar_redirect_interno("//example.com/phish", "/historico"),
            "/historico",
        )
        self.assertEqual(
            app_module.normalizar_redirect_interno("/historico?filtro=abertos", "/historico"),
            "/historico?filtro=abertos",
        )

    def test_atualizar_status_servico_legado_nao_redireciona_para_dominio_externo(self):
        with app_module.app.test_request_context(
            "/servico/1/status",
            method="POST",
            data={"status": "INVALIDO", "redirect_to": "https://example.com/phish"},
        ):
            session["usuario"] = "admin"
            resposta = app_module.atualizar_status_servico_legado(1)

        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(resposta.headers["Location"], "/historico")

    def test_chave_secreta_nao_usa_fallback_estatico_previsivel(self):
        self.assertEqual(app_module.FLASK_SECRET_KEY_FALLBACK, "")
        self.assertNotEqual(app_module.app.secret_key, "wagen-estetica-local-secret")

    def test_checklist_producao_exige_chave_secreta_configurada(self):
        with patch.object(app_module, "FLASK_SECRET_KEY_RAW", ""):
            itens = app_module.montar_checklist_producao()

        item = next(entrada for entrada in itens if entrada["nome"] == "Chave secreta configurada")
        self.assertFalse(item["ok"])
        self.assertIn("FLASK_SECRET_KEY", item["detalhe"])

    def test_restaurar_backup_drive_remove_temporario_quando_sync_esta_ocupado(self):
        item_backup = {
            "nome": "database_v2_20260511_120000.zip",
            "caminho": "drive://arquivo-123",
            "tipo_backup": "banco",
        }
        caminhos_baixados = []

        def baixar_fake(_file_id, destino):
            with open(destino, "wb") as arquivo:
                arquivo.write(b"backup")
            caminhos_baixados.append(destino)

        class LockOcupadoFake:
            def acquire(self, blocking=False):
                return False

            def release(self):
                return None

        with patch.object(app_module, "listar_arquivos_backup_banco", return_value=[item_backup]), \
             patch.object(app_module, "baixar_arquivo_google_drive", side_effect=baixar_fake), \
             patch.object(app_module, "validar_arquivo_backup_local", return_value={"ok": True}), \
             patch.object(app_module, "sync_lock", LockOcupadoFake()):
            sucesso, mensagem = app_module.restaurar_backup_banco(item_backup["nome"])

        self.assertFalse(sucesso)
        self.assertIn("sincronizacao em andamento", mensagem)
        self.assertEqual(len(caminhos_baixados), 1)
        self.assertFalse(os.path.exists(caminhos_baixados[0]))

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

    def criar_banco_usuario_senha(self, senha="SenhaAtual1!"):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE usuarios (
                id INTEGER PRIMARY KEY,
                empresa_id INTEGER,
                usuario TEXT,
                senha TEXT,
                nome TEXT,
                perfil TEXT,
                ativo INTEGER,
                criado_em TEXT,
                tentativas_login INTEGER,
                bloqueado_ate TEXT,
                ultimo_login_em TEXT,
                senha_alteracao_obrigatoria INTEGER,
                senha_atualizada_em TEXT,
                foto_perfil TEXT,
                hud_config_json TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO usuarios (
                id, empresa_id, usuario, senha, nome, perfil, ativo, criado_em,
                tentativas_login, bloqueado_ate, senha_alteracao_obrigatoria,
                foto_perfil, hud_config_json
            )
            VALUES (1, 1, 'admin', ?, 'Admin', 'admin', 1, '2026-05-12T09:00:00', 2, '2026-05-12T10:00:00', 1, '', '')
            """,
            (app_module.senha_hash_bcrypt(senha),),
        )
        conn.commit()
        return conn, PersistentCompatConnection(conn)

    def autenticar_cliente_para_senha(self, usuario_id=1, usuario="admin", senha_pendente=True):
        with self.client.session_transaction() as sess:
            sess["usuario"] = usuario
            if usuario_id is not None:
                sess["usuario_id"] = usuario_id
            sess["usuario_nome"] = "Admin"
            sess["usuario_perfil"] = "admin"
            sess["empresa_id"] = 1
            sess["senha_alteracao_obrigatoria"] = senha_pendente
            return app_module.issue_csrf_token(sess)

    def extrair_cookie_resposta(self, response, nome):
        prefixo = f"{nome}="
        for cabecalho in response.headers.getlist("Set-Cookie"):
            if cabecalho.startswith(prefixo):
                return cabecalho.split(";", 1)[0].split("=", 1)[1]
        return ""

    def test_login_exibe_opcoes_de_lembrar_dados_e_manter_conectado(self):
        with open("templates/login.html", encoding="utf-8") as arquivo:
            conteudo = arquivo.read()

        self.assertIn('name="lembrar_dados_login"', conteudo)
        self.assertIn('name="manter_conectado"', conteudo)
        self.assertIn("Lembrar meus dados de login", conteudo)
        self.assertIn("Manter-me conectado", conteudo)

    def test_login_manter_conectado_controla_sessao_permanente(self):
        conn, compat = self.criar_banco_usuario_senha(senha="SenhaAtual1!")

        with app_module.app.test_client() as client, \
             patch.object(app_module, "csrf_protection_ativa", return_value=False), \
             patch.object(app_module, "INIT_DB_EXECUTADO", True), \
             patch.object(app_module, "conectar", return_value=compat):
            response = client.post(
                "/login",
                data={"usuario": "admin", "senha": "SenhaAtual1!"},
            )
            self.assertEqual(response.status_code, 302)
            with client.session_transaction() as sess:
                self.assertFalse(sess.permanent)

        conn.close()

        conn, compat = self.criar_banco_usuario_senha(senha="SenhaAtual1!")
        with app_module.app.test_client() as client, \
             patch.object(app_module, "csrf_protection_ativa", return_value=False), \
             patch.object(app_module, "INIT_DB_EXECUTADO", True), \
             patch.object(app_module, "conectar", return_value=compat):
            response = client.post(
                "/login",
                data={
                    "usuario": "admin",
                    "senha": "SenhaAtual1!",
                    "manter_conectado": "1",
                },
            )
            self.assertEqual(response.status_code, 302)
            with client.session_transaction() as sess:
                self.assertTrue(sess.permanent)

        conn.close()

    def test_login_manter_conectado_grava_cookie_persistente_local(self):
        conn, compat = self.criar_banco_usuario_senha(senha="SenhaAtual1!")
        conn.execute("UPDATE usuarios SET senha_alteracao_obrigatoria=0 WHERE id=1")
        conn.commit()

        with tempfile.TemporaryDirectory() as tmpdir:
            banco_local = os.path.join(tmpdir, "local.db")
            with app_module.app.test_client() as client, \
                 patch.object(app_module, "csrf_protection_ativa", return_value=False), \
                 patch.object(app_module, "INIT_DB_EXECUTADO", True), \
                 patch.object(app_module, "DATABASE_FILE", banco_local), \
                 patch.object(app_module, "conectar", return_value=compat):
                response = client.post(
                    "/login",
                    data={
                        "usuario": "admin",
                        "senha": "SenhaAtual1!",
                        "manter_conectado": "1",
                    },
                )

            token = self.extrair_cookie_resposta(response, app_module.LOGIN_PERSISTENTE_COOKIE)
            self.assertTrue(token)

            local_conn = sqlite3.connect(banco_local)
            local_conn.row_factory = sqlite3.Row
            row = local_conn.execute(
                "SELECT usuario, token_hash, revogado_em FROM login_persistente_tokens"
            ).fetchone()
            local_conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row["usuario"], "admin")
        self.assertEqual(row["token_hash"], app_module.hash_token_login_persistente(token))
        self.assertIsNone(row["revogado_em"])
        conn.close()

    def test_login_persistente_restaura_usuario_apos_sessao_flask_sumir(self):
        conn, compat = self.criar_banco_usuario_senha(senha="SenhaAtual1!")
        conn.execute("UPDATE usuarios SET senha_alteracao_obrigatoria=0 WHERE id=1")
        conn.commit()

        with tempfile.TemporaryDirectory() as tmpdir:
            banco_local = os.path.join(tmpdir, "local.db")
            with app_module.app.test_client() as client, \
                 patch.object(app_module, "csrf_protection_ativa", return_value=False), \
                 patch.object(app_module, "INIT_DB_EXECUTADO", True), \
                 patch.object(app_module, "DATABASE_FILE", banco_local), \
                 patch.object(app_module, "conectar", return_value=compat):
                response = client.post(
                    "/login",
                    data={
                        "usuario": "admin",
                        "senha": "SenhaAtual1!",
                        "manter_conectado": "1",
                    },
                )
                token = self.extrair_cookie_resposta(response, app_module.LOGIN_PERSISTENTE_COOKIE)

            self.assertTrue(token)

            with app_module.app.test_client() as restored, \
                 patch.object(app_module, "INIT_DB_EXECUTADO", True), \
                 patch.object(app_module, "DATABASE_FILE", banco_local), \
                 patch.object(app_module, "conectar", side_effect=RuntimeError("banco online reiniciando")), \
                 patch.object(app_module, "gerar_sync_token_leve", return_value="sync-ok"), \
                 patch.object(app_module, "obter_contexto_licenca_empresa_cached", return_value={"bloqueada": False}):
                restored.set_cookie(app_module.LOGIN_PERSISTENTE_COOKIE, token)
                response = restored.get("/api/sync-token")

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json()["sync_token"], "sync-ok")
                with restored.session_transaction() as sess:
                    self.assertEqual(sess["usuario"], "admin")
                    self.assertTrue(sess.permanent)

        conn.close()

    def test_troca_de_senha_preserva_manter_conectado(self):
        conn, compat = self.criar_banco_usuario_senha()
        with self.client.session_transaction() as sess:
            sess.permanent = True
        token = self.autenticar_cliente_para_senha()

        with patch.object(app_module, "conectar", return_value=compat), \
             patch.object(app_module, "sincronizar_sessao_usuario"), \
             patch.object(app_module, "registrar_auditoria_assincrona"):
            response = self.client.post(
                "/configuracoes/senha",
                data={
                    "_csrf_token": token,
                    "senha_atual": "SenhaAtual1!",
                    "nova_senha": "NovaSenha1!",
                    "confirmar_senha": "NovaSenha1!",
                },
            )

        self.assertEqual(response.status_code, 302)
        with self.client.session_transaction() as sess:
            self.assertTrue(sess.permanent)
        conn.close()

    def test_atualizar_minha_senha_nao_da_500_se_auditoria_falhar(self):
        conn, compat = self.criar_banco_usuario_senha()
        token = self.autenticar_cliente_para_senha()

        with patch.object(app_module, "conectar", return_value=compat), \
             patch.object(app_module, "sincronizar_sessao_usuario"), \
             patch.object(app_module, "registrar_auditoria_assincrona", side_effect=RuntimeError("auditoria indisponivel")):
            response = self.client.post(
                "/configuracoes/senha",
                data={
                    "_csrf_token": token,
                    "senha_atual": "SenhaAtual1!",
                    "nova_senha": "NovaSenha1!",
                    "confirmar_senha": "NovaSenha1!",
                },
            )

        self.assertEqual(response.status_code, 302)
        row = conn.execute("SELECT senha, senha_alteracao_obrigatoria, tentativas_login, bloqueado_ate FROM usuarios WHERE id=1").fetchone()
        self.assertTrue(app_module.verificar_senha_usuario("NovaSenha1!", row["senha"]))
        self.assertEqual(row["senha_alteracao_obrigatoria"], 0)
        self.assertEqual(row["tentativas_login"], 0)
        self.assertIsNone(row["bloqueado_ate"])
        conn.close()

    def test_atualizar_minha_senha_incorreta_retorna_feedback_sem_500(self):
        conn, compat = self.criar_banco_usuario_senha()
        token = self.autenticar_cliente_para_senha()

        with patch.object(app_module, "conectar", return_value=compat), \
             patch.object(app_module, "sincronizar_sessao_usuario"):
            response = self.client.post(
                "/configuracoes/senha",
                data={
                    "_csrf_token": token,
                    "senha_atual": "Errada1!",
                    "nova_senha": "NovaSenha1!",
                    "confirmar_senha": "NovaSenha1!",
                },
            )

        self.assertEqual(response.status_code, 302)
        row = conn.execute("SELECT senha, senha_alteracao_obrigatoria FROM usuarios WHERE id=1").fetchone()
        self.assertTrue(app_module.verificar_senha_usuario("SenhaAtual1!", row["senha"]))
        self.assertEqual(row["senha_alteracao_obrigatoria"], 1)
        conn.close()

    def test_atualizar_minha_senha_busca_usuario_pelo_login_quando_sessao_sem_id(self):
        conn, compat = self.criar_banco_usuario_senha()
        token = self.autenticar_cliente_para_senha(usuario_id=None)

        with patch.object(app_module, "conectar", return_value=compat), \
             patch.object(app_module, "sincronizar_sessao_usuario"), \
             patch.object(app_module, "registrar_auditoria_assincrona"):
            response = self.client.post(
                "/configuracoes/senha",
                data={
                    "_csrf_token": token,
                    "senha_atual": "SenhaAtual1!",
                    "nova_senha": "OutraSenha1!",
                    "confirmar_senha": "OutraSenha1!",
                },
            )

        self.assertEqual(response.status_code, 302)
        row = conn.execute("SELECT senha FROM usuarios WHERE id=1").fetchone()
        self.assertTrue(app_module.verificar_senha_usuario("OutraSenha1!", row["senha"]))
        conn.close()

    def test_login_com_sessao_existente_nao_da_500_se_sync_falhar(self):
        with self.client.session_transaction() as sess:
            sess["usuario"] = "admin"
            sess["senha_alteracao_obrigatoria"] = False

        with patch.object(app_module, "sincronizar_sessao_usuario", side_effect=RuntimeError("sync indisponivel")):
            response = self.client.get("/login")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/")

    def test_criar_usuario_nao_da_500_se_auditoria_falhar(self):
        conn, compat = self.criar_banco_usuario_senha()
        token = self.autenticar_cliente_para_senha(senha_pendente=False)

        with patch.object(app_module, "conectar", return_value=compat), \
             patch.object(app_module, "sincronizar_sessao_usuario"), \
             patch.object(app_module, "bloquear_criacao_usuario_por_licenca", return_value=False), \
             patch.object(app_module, "registrar_auditoria_assincrona", side_effect=RuntimeError("auditoria indisponivel")):
            response = self.client.post(
                "/configuracoes/usuarios",
                data={
                    "_csrf_token": token,
                    "nome": "Operador",
                    "usuario": "operador",
                    "senha": "AcessoNovo1!",
                    "perfil": "funcionario",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/configuracoes/usuarios?detalhar_usuarios=1")
        row = conn.execute("SELECT senha, senha_alteracao_obrigatoria, ativo FROM usuarios WHERE usuario='operador'").fetchone()
        self.assertIsNotNone(row)
        self.assertTrue(app_module.verificar_senha_usuario("AcessoNovo1!", row["senha"]))
        self.assertEqual(row["senha_alteracao_obrigatoria"], 1)
        self.assertEqual(row["ativo"], 1)
        conn.close()

    def test_redefinir_senha_usuario_nao_da_500_se_auditoria_falhar(self):
        conn, compat = self.criar_banco_usuario_senha()
        conn.execute(
            """
            INSERT INTO usuarios (
                id, empresa_id, usuario, senha, nome, perfil, ativo, criado_em,
                tentativas_login, bloqueado_ate, senha_alteracao_obrigatoria,
                foto_perfil, hud_config_json
            )
            VALUES (2, 1, 'operador', ?, 'Operador', 'funcionario', 1, '2026-05-12T09:00:00', 3, '2026-05-12T10:00:00', 0, '', '')
            """,
            (app_module.senha_hash_bcrypt("SenhaAntiga1!"),),
        )
        conn.commit()
        token = self.autenticar_cliente_para_senha(senha_pendente=False)

        with patch.object(app_module, "conectar", return_value=compat), \
             patch.object(app_module, "sincronizar_sessao_usuario"), \
             patch.object(app_module, "registrar_auditoria_assincrona", side_effect=RuntimeError("auditoria indisponivel")):
            response = self.client.post(
                "/configuracoes/usuarios/2/senha",
                data={
                    "_csrf_token": token,
                    "nova_senha": "SenhaNova2!",
                },
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "/configuracoes/usuarios?detalhar_usuarios=1")
        row = conn.execute("SELECT senha, senha_alteracao_obrigatoria, tentativas_login, bloqueado_ate FROM usuarios WHERE id=2").fetchone()
        self.assertTrue(app_module.verificar_senha_usuario("SenhaNova2!", row["senha"]))
        self.assertEqual(row["senha_alteracao_obrigatoria"], 1)
        self.assertEqual(row["tentativas_login"], 0)
        self.assertIsNone(row["bloqueado_ate"])
        conn.close()

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
            "templates/painel.html",
            "templates/components/service_card.html",
        ]

        for caminho in caminhos:
            with self.subTest(caminho=caminho):
                with open(caminho, encoding="utf-8") as arquivo:
                    conteudo = arquivo.read()
                self.assertNotIn("capture=", conteudo)

    def test_campos_de_senha_tem_botao_mostrar_senha(self):
        for caminho in ["templates/login.html", "templates/configuracoes.html", "templates/nota_fiscal.html"]:
            with self.subTest(caminho=caminho):
                with open(caminho, encoding="utf-8") as arquivo:
                    conteudo = arquivo.read()

                inputs = re.findall(r"<input\b(?=[^>]*type=\"password\")[^>]*>", conteudo, flags=re.S)
                targets = set(re.findall(r'data-password-target="([^"]+)"', conteudo))

                self.assertGreater(len(inputs), 0)
                for input_html in inputs:
                    id_match = re.search(r'id="([^"]+)"', input_html)
                    self.assertIsNotNone(id_match, input_html)
                    self.assertIn(id_match.group(1), targets)

    def test_senhas_salvas_de_usuarios_nao_sao_exibidas_no_template(self):
        with open("templates/configuracoes.html", encoding="utf-8") as arquivo:
            conteudo = arquivo.read()

        self.assertIn("hash seguro", conteudo)
        self.assertNotIn("{{ item.senha", conteudo)

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

    def test_index_hud_banco_inicia_com_status_de_carregamento(self):
        with open(os.path.join(app_module.app.root_path, "templates", "index.html"), encoding="utf-8") as arquivo:
            conteudo = arquivo.read()

        self.assertIn("banco_online_carregando: true", conteudo)
        self.assertIn('banco_online_resumo: "Conectando ao banco..."', conteudo)
        self.assertIn('banco_online_mensagem: "Carregando status de conexao. Aguarde."', conteudo)
        self.assertIn(".hud-line--status-loading", conteudo)
        self.assertIn("statusLoading: statusBanco.carregando", conteudo)
        self.assertNotIn('banco_online_resumo: "Banco online indisponivel"', conteudo)
        self.assertIn('sync_bancos_resumo: "Sync offline/online aguardando"', conteudo)
        self.assertIn("function montarStatusSyncBancosHud()", conteudo)

    def test_configuracoes_banco_exibe_status_sync_offline_online(self):
        with open(os.path.join(app_module.app.root_path, "templates", "configuracoes.html"), encoding="utf-8") as arquivo:
            conteudo = arquivo.read()

        self.assertIn("Banco offline + online", conteudo)
        self.assertIn("Fila local pendente", conteudo)
        self.assertIn("Conflitos abertos", conteudo)
        self.assertIn("IA de conflitos", conteudo)
        self.assertIn("alternar_ia_conflitos", conteudo)
        self.assertIn("Ativar IA de conflitos", conteudo)
        self.assertIn("Desativar IA de conflitos", conteudo)

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

    def test_sync_acao_banco_reprocessar_nao_retorna_500_quando_banco_falha(self):
        with app_module.app.test_request_context(
            "/configuracoes/banco/sync-acao",
            method="POST",
            data={"acao": "reprocessar_seguro"},
        ):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            with patch.object(app_module, "sincronizar_sessao_usuario", side_effect=RuntimeError("banco reiniciando")), \
                 patch.object(app_module, "iniciar_reprocessamento_sync_bancos_background", side_effect=RuntimeError("sync falhou")):
                response = app_module.executar_acao_sync_bancos_configuracoes()

            feedback = session.get("configuracoes_feedback") or {}

        self.assertEqual(response.status_code, 302)
        self.assertIn("/configuracoes/banco", response.location)
        self.assertEqual(feedback.get("tipo"), "erro")
        self.assertIn("Nao foi possivel executar a acao", feedback.get("mensagem", ""))

    def test_sync_acao_banco_reprocessar_inicia_background_sem_bloquear(self):
        with app_module.app.test_request_context(
            "/configuracoes/banco/sync-acao",
            method="POST",
            data={"acao": "reprocessar_seguro"},
        ):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            with patch.object(app_module, "sincronizar_sessao_usuario_seguro"), \
                 patch.object(app_module, "usuario_gerencia_banco_online", return_value=True), \
                 patch.object(
                     app_module,
                     "iniciar_reprocessamento_sync_bancos_background",
                     return_value={
                         "iniciado": True,
                         "mensagem": "Reprocessamento seguro iniciado em segundo plano.",
                     },
                 ) as iniciar_mock, \
                 patch.object(app_module, "sincronizar_bancos_incremental") as sync_mock:
                response = app_module.executar_acao_sync_bancos_configuracoes()

            feedback = session.get("configuracoes_feedback") or {}

        self.assertEqual(response.status_code, 302)
        self.assertIn("/configuracoes/banco", response.location)
        self.assertEqual(feedback.get("tipo"), "sucesso")
        self.assertIn("segundo plano", feedback.get("mensagem", ""))
        iniciar_mock.assert_called_once()
        sync_mock.assert_not_called()

    def test_api_sync_banco_reprocessar_inicia_background_sem_bloquear(self):
        with app_module.app.test_request_context(
            "/api/sync-bancos/acao",
            method="POST",
            json={"acao": "reprocessar_seguro"},
        ):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            with patch.object(app_module, "sincronizar_sessao_usuario_seguro"), \
                 patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                 patch.object(
                     app_module,
                     "iniciar_reprocessamento_sync_bancos_background",
                     return_value={
                         "iniciado": True,
                         "mensagem": "Reprocessamento seguro iniciado em segundo plano.",
                     },
                 ) as iniciar_mock, \
                 patch.object(app_module, "contar_conflitos_sync_bancos_abertos", return_value=2), \
                 patch.object(app_module, "sincronizar_bancos_incremental") as sync_mock:
                response, status_code = app_module.api_sync_bancos_acao()

        payload = response.get_json()
        self.assertEqual(status_code, 202)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["conflitos"], 2)
        self.assertIn("segundo plano", payload["mensagem"])
        iniciar_mock.assert_called_once()
        sync_mock.assert_not_called()

    def test_sync_acao_banco_alterna_ia_conflitos_com_feedback(self):
        with app_module.app.test_request_context(
            "/configuracoes/banco/sync-acao",
            method="POST",
            data={"acao": "alternar_ia_conflitos"},
        ):
            session["usuario"] = "admin"
            session["usuario_perfil"] = "admin"
            with patch.object(app_module, "sincronizar_sessao_usuario_seguro"), \
                 patch.object(app_module, "usuario_gerencia_banco_online", return_value=True), \
                 patch.object(
                     app_module,
                     "alternar_resolucao_ia_sync_bancos",
                     return_value={
                         "ativa": True,
                         "resultado_ia": {"mensagem": "IA resolveu 1 de 1 conflito(s) analisado(s)."},
                         "conflitos": 0,
                     },
                 ), \
                 patch.object(app_module, "registrar_auditoria"):
                response = app_module.executar_acao_sync_bancos_configuracoes()

            feedback = session.get("configuracoes_feedback") or {}

        self.assertEqual(response.status_code, 302)
        self.assertIn("/configuracoes/banco", response.location)
        self.assertEqual(feedback.get("tipo"), "sucesso")
        self.assertIn("IA de conflitos ativada", feedback.get("mensagem", ""))

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

    def _criar_banco_sync_veiculos_memoria(self, registros):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE veiculos (
                id INTEGER PRIMARY KEY,
                empresa_id INTEGER DEFAULT 1,
                placa TEXT,
                modelo TEXT,
                cor TEXT,
                atualizado_em TEXT
            )
            """
        )
        for registro in registros:
            conn.execute(
                """
                INSERT INTO veiculos (id, empresa_id, placa, modelo, cor, atualizado_em)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    registro["id"],
                    registro.get("empresa_id", 1),
                    registro["placa"],
                    registro.get("modelo", ""),
                    registro.get("cor", ""),
                    registro.get("atualizado_em", ""),
                ),
            )
        conn.commit()
        return conn, PersistentCompatConnection(conn)

    def test_sync_bancos_incremental_versao_mais_nova_vence_sem_apagar(self):
        origem_conn, origem = self._criar_banco_sync_veiculos_memoria([
            {"id": 1, "placa": "ABC1234", "modelo": "Onix Novo", "cor": "Preto", "atualizado_em": "2026-05-13T10:00:00-03:00"},
        ])
        destino_conn, destino = self._criar_banco_sync_veiculos_memoria([
            {"id": 1, "placa": "ABC1234", "modelo": "Onix Antigo", "cor": "Preto", "atualizado_em": "2026-05-13T09:00:00-03:00"},
            {"id": 2, "placa": "XYZ9876", "modelo": "Gol", "cor": "Branco", "atualizado_em": "2026-05-13T09:30:00-03:00"},
        ])
        conflitos = []

        resultado = app_module.sincronizar_tabela_incremental(origem, destino, "veiculos", conflitos=conflitos)

        row_atualizado = destino_conn.execute("SELECT modelo FROM veiculos WHERE id=1").fetchone()
        row_preservado = destino_conn.execute("SELECT modelo FROM veiculos WHERE id=2").fetchone()
        self.assertEqual(resultado["atualizados"], 1)
        self.assertEqual(row_atualizado["modelo"], "Onix Novo")
        self.assertEqual(row_preservado["modelo"], "Gol")
        self.assertEqual(conflitos, [])
        origem_conn.close()
        destino_conn.close()

    def test_sync_bancos_empate_divergente_registra_conflito_sem_sobrescrever(self):
        origem_conn, origem = self._criar_banco_sync_veiculos_memoria([
            {"id": 1, "placa": "ABC1234", "modelo": "Onix", "cor": "Preto", "atualizado_em": "2026-05-13T10:00:00-03:00"},
        ])
        destino_conn, destino = self._criar_banco_sync_veiculos_memoria([
            {"id": 1, "placa": "ABC1234", "modelo": "Gol", "cor": "Branco", "atualizado_em": "2026-05-13T10:00:00-03:00"},
        ])
        conflitos = []

        resultado = app_module.sincronizar_tabela_incremental(origem, destino, "veiculos", conflitos=conflitos)

        row = destino_conn.execute("SELECT modelo, cor FROM veiculos WHERE id=1").fetchone()
        self.assertEqual(resultado["atualizados"], 0)
        self.assertEqual(row["modelo"], "Gol")
        self.assertEqual(row["cor"], "Branco")
        self.assertEqual(len(conflitos), 1)
        self.assertEqual(conflitos[0]["tabela"], "veiculos")
        self.assertIn("placa=ABC1234", conflitos[0]["chave"])
        origem_conn.close()
        destino_conn.close()

    def test_resolucao_ia_sync_bancos_completa_registro_sem_sobrescrever_preenchido(self):
        local_conn, local = self._criar_banco_sync_veiculos_memoria([
            {"id": 1, "placa": "ABC1234", "modelo": "", "cor": "", "atualizado_em": "2026-05-13T10:00:00-03:00"},
        ])
        online_conn, online = self._criar_banco_sync_veiculos_memoria([
            {"id": 1, "placa": "ABC1234", "modelo": "Onix", "cor": "Preto", "atualizado_em": "2026-05-13T10:00:00-03:00"},
        ])
        registro_local = dict(local_conn.execute("SELECT * FROM veiculos WHERE id=1").fetchone())
        registro_online = dict(online_conn.execute("SELECT * FROM veiculos WHERE id=1").fetchone())
        conflito = app_module.montar_conflito_registro_sync("veiculos", registro_online, registro_local)

        with patch.object(app_module, "conectar_banco_local_forcado", return_value=local):
            app_module.registrar_conflitos_sync_bancos([conflito])

        resultado = app_module.resolver_conflitos_sync_bancos_por_ia(
            local_conn=local,
            online_conn=online,
        )

        row = local_conn.execute("SELECT modelo, cor FROM veiculos WHERE id=1").fetchone()
        conflito_aberto = local_conn.execute(
            "SELECT COUNT(*) FROM sync_bancos_conflitos WHERE resolvido=0"
        ).fetchone()[0]
        resolucao = local_conn.execute(
            "SELECT acao, direcao, status FROM sync_bancos_resolucoes ORDER BY id DESC LIMIT 1"
        ).fetchone()

        self.assertEqual(resultado["resolvidos"], 1)
        self.assertEqual(row["modelo"], "Onix")
        self.assertEqual(row["cor"], "Preto")
        self.assertEqual(conflito_aberto, 0)
        self.assertEqual(resolucao["acao"], "resolucao_ia_automatica")
        self.assertEqual(resolucao["direcao"], "online_para_local_ia")
        self.assertEqual(resolucao["status"], "aplicado")
        local_conn.close()
        online_conn.close()

    def test_resolucao_ia_sync_bancos_mantem_conflito_protegido(self):
        decisao = app_module.decidir_resolucao_ia_conflito_sync(
            "veiculos",
            {
                "id": 1,
                "placa": "ABC1234",
                "modelo": "Onix",
                "status": "FINALIZADO",
                "atualizado_em": "2026-05-13T10:00:00-03:00",
            },
            {
                "id": 1,
                "placa": "ABC1234",
                "modelo": "Gol",
                "status": "EM ANDAMENTO",
                "atualizado_em": "2026-05-13T10:00:00-03:00",
            },
        )

        self.assertFalse(decisao["aplicar"])
        self.assertEqual(decisao["motivo"], "regra_protecao_negocio")

    def test_alternar_resolucao_ia_sync_bancos_persiste_estado(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        wrapper = PersistentCompatConnection(conn)

        with patch.object(app_module, "conectar_banco_local_forcado", return_value=wrapper), \
             patch.object(
                 app_module,
                 "resolver_conflitos_sync_bancos_por_ia",
                 return_value={"mensagem": "IA sem conflitos.", "resolvidos": 0, "analisados": 0},
             ):
            resultado_ativar = app_module.alternar_resolucao_ia_sync_bancos()
            status_ativo = app_module.obter_status_sync_bancos(conn=wrapper)
            resultado_desativar = app_module.alternar_resolucao_ia_sync_bancos(executar_agora=False)
            status_inativo = app_module.obter_status_sync_bancos(conn=wrapper)

        self.assertTrue(resultado_ativar["ativa"])
        self.assertEqual(status_ativo["ia_resolucao_automatica"], 1)
        self.assertFalse(resultado_desativar["ativa"])
        self.assertEqual(status_inativo["ia_resolucao_automatica"], 0)
        conn.close()

    def test_status_sync_bancos_local_cria_tabelas_tecnicas(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        wrapper = PersistentCompatConnection(conn)

        app_module.garantir_tabelas_sync_bancos_local(wrapper)
        status = app_module.obter_status_sync_bancos(conn=wrapper)

        tabelas = {
            row["name"]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        self.assertIn("sync_bancos_status", tabelas)
        self.assertIn("sync_bancos_conflitos", tabelas)
        self.assertIn("sync_bancos_fila", tabelas)
        self.assertEqual(status["status"], "aguardando")
        self.assertEqual(status["ia_resolucao_automatica"], 0)
        conn.close()

    def test_mensagem_publica_cadastro_veiculo_nao_exibe_erro_planilha(self):
        resultado = {
            "placa": "ABC1234",
            "espelho_planilha": {
                "sucesso": [{"nome": "Clientes"}],
                "falhas": [{"nome": "Google Planilhas", "erro": "Erro 403: permissao negada"}],
            },
        }

        mensagem = app_module.montar_mensagem_publica_cadastro_veiculo(resultado)

        self.assertEqual(mensagem, "Cadastro da placa ABC1234 salvo com sucesso.")
        self.assertNotIn("403", mensagem)
        self.assertNotIn("Aviso na planilha", mensagem)
        self.assertNotIn("Espelhado", mensagem)

    def test_cadastrar_veiculo_mantem_feedback_sucesso_sem_erro_planilha(self):
        resultado = {
            "acao": "novo",
            "placa": "ABC1234",
            "espelho_planilha": {
                "falhas": [{"nome": "Google Planilhas", "erro": "Erro 403: permissao negada"}],
            },
        }

        with app_module.app.test_request_context(
            "/cadastrar",
            method="POST",
            data={"placa": "ABC1234", "nome": "Maria", "telefone": "", "modelo": "Onix", "cor": "Preto"},
        ):
            session["usuario"] = "admin"
            with patch.object(app_module, "salvar_cliente_veiculo", return_value=resultado), \
                 patch.object(app_module, "registrar_cadastro_novo_para_atendimento"), \
                 patch.object(app_module, "log_info") as log_mock:
                response = app_module.cadastrar()

            feedback = session.get("index_feedback")

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.location, "/?placa=ABC1234")
        self.assertEqual(feedback, {"tipo": "sucesso", "mensagem": "Cadastro da placa ABC1234 salvo com sucesso."})
        self.assertTrue(log_mock.called)

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

    def test_metricas_sql_resumem_por_pagina_e_gargalo(self):
        with app_module.SQL_METRICAS_LOCK:
            app_module.SQL_METRICAS_CONSULTAS.clear()

        try:
            app_module.registrar_metrica_consulta_sql("/financeiro", "banco", 120, origem="banco")
            app_module.registrar_metrica_consulta_sql("/financeiro", "render", 40, origem="template", cache_hit=True)

            metricas = app_module.obter_metricas_consultas_sql(limite=10)
            financeiro = next(item for item in metricas["por_pagina"] if item["pagina"] == "/financeiro")

            self.assertEqual(financeiro["amostras"], 2)
            self.assertEqual(financeiro["max_ms"], 120)
            self.assertEqual(financeiro["gargalo"], "banco")
            self.assertEqual(financeiro["cache_hits"], 1)
        finally:
            with app_module.SQL_METRICAS_LOCK:
                app_module.SQL_METRICAS_CONSULTAS.clear()

    def test_resumo_retornos_hud_somente_cache_nao_recalcula(self):
        app_module.RETORNOS_HUD_CACHE["testado_em"] = 0.0
        app_module.RETORNOS_HUD_CACHE["usuario"] = ""
        app_module.RETORNOS_HUD_CACHE["resultado"] = None

        with patch.object(app_module, "montar_itens_retornos_comerciais") as montar_mock:
            resumo = app_module.obter_resumo_retornos_hud("admin", app_module.time.time(), somente_cache=True)

        self.assertEqual(resumo["acao_agora"], 0)
        montar_mock.assert_not_called()

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

    def test_financeiro_reusa_cache_por_empresa_periodo_e_detalhe(self):
        app_module.RELATORIOS_CONTEXT_CACHE["testado_em"] = 0.0
        app_module.RELATORIOS_CONTEXT_CACHE["chave"] = ""
        app_module.RELATORIOS_CONTEXT_CACHE["resultado"] = None
        app_module.RELATORIOS_CONTEXT_CACHE["entradas"] = {}

        with app_module.app.test_request_context("/financeiro?periodo=mes", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "executar_leitura_resiliente", return_value={"servicos_raw": [], "orcamentos_raw": [], "notas_raw": []}) as leitura_mock, \
                 patch.object(app_module, "agora", return_value=app_module.datetime(2026, 5, 4, 10, 0)):
                primeiro = app_module.carregar_contexto_relatorios("mes")
                segundo = app_module.carregar_contexto_relatorios("mes")

        self.assertEqual(primeiro["quantidade_periodo"], 0)
        self.assertEqual(segundo["quantidade_periodo"], 0)
        leitura_mock.assert_called_once()
        self.assertEqual(app_module.RELATORIOS_CONTEXT_CACHE["chave"], "1|mes|det:0")
        self.assertIn("1|mes|det:0", app_module.RELATORIOS_CONTEXT_CACHE["entradas"])

    def test_financeiro_mantem_cache_separado_por_detalhe(self):
        app_module.RELATORIOS_CONTEXT_CACHE["testado_em"] = 0.0
        app_module.RELATORIOS_CONTEXT_CACHE["chave"] = ""
        app_module.RELATORIOS_CONTEXT_CACHE["resultado"] = None
        app_module.RELATORIOS_CONTEXT_CACHE["entradas"] = {}

        with app_module.app.test_request_context("/financeiro?periodo=mes", method="GET"):
            session["usuario"] = "admin"
            session["empresa_id"] = 1
            with patch.object(app_module, "executar_leitura_resiliente", return_value={"servicos_raw": [], "orcamentos_raw": [], "notas_raw": []}) as leitura_mock, \
                 patch.object(app_module, "agora", return_value=app_module.datetime(2026, 5, 4, 10, 0)):
                app_module.carregar_contexto_relatorios("mes", detalhado=False)
                app_module.carregar_contexto_relatorios("mes", detalhado=True)
                app_module.carregar_contexto_relatorios("mes", detalhado=False)

        self.assertEqual(leitura_mock.call_count, 2)
        self.assertIn("1|mes|det:0", app_module.RELATORIOS_CONTEXT_CACHE["entradas"])
        self.assertIn("1|mes|det:1", app_module.RELATORIOS_CONTEXT_CACHE["entradas"])

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
            sw_response = client.get("/sw.js")
            try:
                self.assertEqual(sw_response.status_code, 200)
            finally:
                sw_response.close()
            self.assertEqual(client.get("/api/pwa/status").status_code, 200)
            self.assertEqual(client.get("/clientes").status_code, 302)

    def test_telas_principais_renderizam_com_sessao_autenticada(self):
        leitura_painel = {
            "servicos_db": [],
            "produtos_pneu": [],
            "resumo_fotos_por_servico": {},
            "resumo_extras_por_servico": {},
        }
        status_auto = {
            "ok": True,
            "diagnostico": {"nivel": "info", "label": "OK", "titulo": "OK", "frase": "", "itens": []},
            "falhas": [],
            "erros_abertos": [],
            "inconsistencias_negocio": [],
            "sugestoes": [],
            "tempo_resposta": [],
            "acoes": {},
            "acoes_simples": [],
        }
        status_sistema = {
            "gerado_em": "agora",
            "resumo": {"ok": True, "falhas": []},
            "itens": [],
            "ultimo_erro": {},
        }

        with app_module.app.test_client() as client:
            with client.session_transaction() as sess:
                sess["usuario"] = "admin"
                sess["usuario_perfil"] = "desenvolvedor"
                sess["usuario_id"] = 1
                sess["empresa_id"] = 1
                sess["senha_alteracao_obrigatoria"] = False

            with patch.object(app_module, "sincronizar_sessao_usuario"), \
                 patch.object(app_module, "obter_contexto_licenca_empresa_cached", return_value={"bloqueada": False}), \
                 patch.object(app_module, "usuario_desenvolvedor", return_value=True), \
                 patch.object(app_module, "usuario_gerencia_configuracao_sistema", return_value=True), \
                 patch.object(app_module, "preparar_rotinas_interface_logada"), \
                 patch.object(app_module, "carregar_contexto_clientes", return_value=([], [])), \
                 patch.object(app_module, "carregar_contexto_relatorios", return_value={}), \
                 patch.object(app_module, "executar_leitura_resiliente", return_value=leitura_painel), \
                 patch.object(app_module, "montar_status_sistema_dono", return_value=status_sistema), \
                 patch.object(app_module, "status_auto_suporte", return_value=status_auto), \
                 patch.object(app_module, "listar_historico_auto_suporte", return_value=[]), \
                 patch.object(app_module, "render_template", return_value="ok"):
                for rota in ["/", "/painel", "/clientes", "/financeiro", "/configuracoes", "/auto-suporte"]:
                    with self.subTest(rota=rota):
                        response = client.get(rota)
                        try:
                            self.assertEqual(response.status_code, 200)
                        finally:
                            response.close()


if __name__ == "__main__":
    unittest.main()

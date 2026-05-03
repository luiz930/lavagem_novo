import sqlite3
import unittest

from core.product_foundation import (
    apply_admin_settings_scope,
    apply_empresa_indexes,
    apply_extended_empresa_scope,
    build_brand_context,
    run_product_foundation_migrations,
)


def add_column_if_needed(cursor, tabela, definicao_coluna):
    nome_coluna = definicao_coluna.split()[0]
    cursor.execute(f"PRAGMA table_info({tabela})")
    existentes = {row[1] for row in cursor.fetchall()}
    if nome_coluna in existentes:
        return
    cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN {definicao_coluna}")


class ProductFoundationMigrationsTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        c = self.conn.cursor()
        c.execute("CREATE TABLE usuarios (id INTEGER PRIMARY KEY AUTOINCREMENT, usuario TEXT)")
        c.execute("CREATE TABLE clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT)")
        c.execute("CREATE TABLE veiculos (id INTEGER PRIMARY KEY AUTOINCREMENT, placa TEXT)")
        c.execute("CREATE TABLE servicos (id INTEGER PRIMARY KEY AUTOINCREMENT, valor REAL)")
        c.execute("CREATE TABLE fotos (id INTEGER PRIMARY KEY AUTOINCREMENT, caminho TEXT)")
        c.execute("CREATE TABLE retornos_clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, placa TEXT)")
        c.execute("CREATE TABLE sincronizacoes_clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT)")
        c.execute("CREATE TABLE orcamentos (id INTEGER PRIMARY KEY AUTOINCREMENT, numero INTEGER)")
        c.execute("CREATE TABLE notas_fiscais (id INTEGER PRIMARY KEY AUTOINCREMENT, numero_nota TEXT)")
        c.execute("CREATE TABLE configuracao_empresa (id INTEGER PRIMARY KEY, nome_fantasia TEXT)")
        c.execute("CREATE TABLE configuracao_backup (id INTEGER PRIMARY KEY CHECK (id = 1), frequencia TEXT)")
        c.execute("CREATE TABLE manutencao_arquivos (id INTEGER PRIMARY KEY CHECK (id = 1), ultima_mensagem TEXT)")
        c.execute("CREATE TABLE integracao_fiscal (id INTEGER PRIMARY KEY CHECK (id = 1), tipo_integracao TEXT)")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_run_product_foundation_migrations_creates_product_tables(self):
        run_product_foundation_migrations(
            self.conn,
            add_column_if_needed,
            lambda: "2026-05-01T00:00:00",
            print_func=lambda *_args, **_kwargs: None,
        )
        c = self.conn.cursor()
        c.execute("SELECT nome FROM schema_migrations")
        migrations = {row[0] for row in c.fetchall()}
        self.assertIn("foundation_enterprises", migrations)
        self.assertIn("foundation_branding_storage", migrations)
        self.assertIn("foundation_admin_settings_scope", migrations)

        c.execute("SELECT id, slug FROM empresas WHERE id=1")
        empresa = c.fetchone()
        self.assertEqual(empresa["id"], 1)
        self.assertEqual(empresa["slug"], "wagen-estetica")

        c.execute("PRAGMA table_info(configuracao_empresa)")
        colunas = {row[1] for row in c.fetchall()}
        self.assertIn("marca_nome", colunas)
        self.assertIn("licenca_plano", colunas)
        self.assertIn("site_titulo", colunas)
        self.assertIn("site_rodape_texto", colunas)
        self.assertIn("marca_logo_blob", colunas)
        self.assertIn("marca_cor_fundo", colunas)

    def test_build_brand_context_prefers_config_values(self):
        contexto = build_brand_context(
            {
                "marca_nome": "Minha Marca",
                "marca_subtitulo": "Minha Operacao",
                "site_titulo": "Meu Sistema",
                "site_rodape_texto": "Rodape customizado",
                "marca_logo_blob": b"abc",
                "marca_cor_fundo": "#010203",
                "marca_cor_superficie": "#111111",
                "marca_cor_texto": "#eeeeee",
                "licenca_plano": "pro",
                "licenca_status": "ativa",
            },
            {"nome_fantasia": "Empresa Padrao"},
        )
        self.assertEqual(contexto["brand_name"], "Minha Marca")
        self.assertEqual(contexto["brand_subtitle"], "Minha Operacao")
        self.assertEqual(contexto["site_title"], "Meu Sistema")
        self.assertEqual(contexto["site_footer_text"], "Rodape customizado")
        self.assertEqual(contexto["brand_logo_url"], "/branding/logo")
        self.assertEqual(contexto["brand_background_color"], "#010203")
        self.assertEqual(contexto["brand_surface_color"], "#111111")
        self.assertEqual(contexto["brand_text_color"], "#eeeeee")
        self.assertEqual(contexto["licenca_plano"], "pro")
        self.assertEqual(contexto["licenca_status"], "ativa")

    def test_migration_replaces_global_unique_placa_with_empresa_scope(self):
        self.conn.close()
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        c = self.conn.cursor()
        c.execute("CREATE TABLE usuarios (id INTEGER PRIMARY KEY AUTOINCREMENT, usuario TEXT)")
        c.execute("CREATE TABLE clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, nome TEXT)")
        c.execute("CREATE TABLE veiculos (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER DEFAULT 1, placa TEXT UNIQUE NOT NULL)")
        c.execute("CREATE TABLE servicos (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER DEFAULT 1, valor REAL)")
        c.execute("CREATE TABLE fotos (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER DEFAULT 1, caminho TEXT)")
        c.execute("CREATE TABLE retornos_clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER DEFAULT 1, placa TEXT NOT NULL UNIQUE)")
        c.execute("CREATE TABLE sincronizacoes_clientes (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER DEFAULT 1, nome TEXT)")
        c.execute("CREATE TABLE orcamentos (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER DEFAULT 1, numero INTEGER)")
        c.execute("CREATE TABLE notas_fiscais (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER DEFAULT 1, numero_nota TEXT)")
        c.execute("CREATE TABLE configuracao_empresa (id INTEGER PRIMARY KEY, empresa_id INTEGER DEFAULT 1, nome_fantasia TEXT)")
        self.conn.commit()

        run_product_foundation_migrations(
            self.conn,
            add_column_if_needed,
            lambda: "2026-05-01T00:00:00",
            print_func=lambda *_args, **_kwargs: None,
        )

        c = self.conn.cursor()
        c.execute("INSERT INTO veiculos (empresa_id, placa) VALUES (?, ?)", (1, "AAA1234"))
        c.execute("INSERT INTO veiculos (empresa_id, placa) VALUES (?, ?)", (2, "AAA1234"))
        c.execute("INSERT INTO retornos_clientes (empresa_id, placa) VALUES (?, ?)", (1, "BBB1234"))
        c.execute("INSERT INTO retornos_clientes (empresa_id, placa) VALUES (?, ?)", (2, "BBB1234"))
        self.conn.commit()

        c.execute("PRAGMA index_list(veiculos)")
        indices_veiculos = c.fetchall()
        unico_global_veiculos = []
        for indice in indices_veiculos:
            nome = indice[1]
            c.execute(f"PRAGMA index_info({nome})")
            colunas = [row[2] for row in c.fetchall()]
            if indice[2] and colunas == ["placa"]:
                unico_global_veiculos.append(nome)
        self.assertEqual(unico_global_veiculos, [])

    def test_migration_converts_admin_singleton_tables_to_empresa_scope(self):
        c = self.conn.cursor()
        c.execute("INSERT INTO configuracao_backup (id, frequencia) VALUES (1, 'diario')")
        c.execute("INSERT INTO manutencao_arquivos (id, ultima_mensagem) VALUES (1, 'ok')")
        c.execute("INSERT INTO integracao_fiscal (id, tipo_integracao) VALUES (1, 'manual')")
        self.conn.commit()

        run_product_foundation_migrations(
            self.conn,
            add_column_if_needed,
            lambda: "2026-05-01T00:00:00",
            print_func=lambda *_args, **_kwargs: None,
        )

        c.execute("PRAGMA table_info(configuracao_backup)")
        colunas_backup = {row[1] for row in c.fetchall()}
        self.assertIn("empresa_id", colunas_backup)

        c.execute("PRAGMA table_info(manutencao_arquivos)")
        colunas_manutencao = {row[1] for row in c.fetchall()}
        self.assertIn("empresa_id", colunas_manutencao)

        c.execute("PRAGMA table_info(integracao_fiscal)")
        colunas_fiscal = {row[1] for row in c.fetchall()}
        self.assertIn("empresa_id", colunas_fiscal)

        c.execute("SELECT empresa_id, frequencia FROM configuracao_backup WHERE id=1")
        self.assertEqual(tuple(c.fetchone()), (1, "diario"))

        c.execute("INSERT INTO configuracao_backup (empresa_id, frequencia) VALUES (?, ?)", (2, "semanal"))
        c.execute("INSERT INTO manutencao_arquivos (empresa_id, ultima_mensagem) VALUES (?, ?)", (2, "empresa-2"))
        c.execute("INSERT INTO integracao_fiscal (empresa_id, tipo_integracao) VALUES (?, ?)", (2, "api"))
        self.conn.commit()

        c.execute("SELECT COUNT(*) FROM configuracao_backup WHERE empresa_id IN (1, 2)")
        self.assertEqual(c.fetchone()[0], 2)

    def test_postgres_helpers_do_not_use_sqlite_metadata(self):
        class FakeCursor:
            backend = "postgres"

            def __init__(self):
                self.commands = []

            def execute(self, sql, params=None):
                texto = str(sql)
                self.commands.append((texto, tuple(params or ())))
                if "sqlite_master" in texto or "PRAGMA " in texto.upper():
                    raise AssertionError("SQLite metadata SQL should not run on Postgres")
                self._last_sql = texto
                self._last_params = tuple(params or ())

            def fetchone(self):
                sql = getattr(self, "_last_sql", "")
                params = getattr(self, "_last_params", ())
                if "information_schema.tables" in sql:
                    return (1,) if params and params[0] == "historico_lavagens_sync" else None
                return None

            def fetchall(self):
                sql = getattr(self, "_last_sql", "")
                params = getattr(self, "_last_params", ())
                if "information_schema.columns" in sql:
                    tabela = params[0] if params else ""
                    colunas_por_tabela = {
                        "clientes": [("empresa_id",), ("telefone",)],
                        "veiculos": [("empresa_id",), ("placa",)],
                        "servicos": [("empresa_id",), ("status",), ("id",), ("veiculo_id",)],
                        "fotos": [("empresa_id",), ("servico_id",)],
                        "retornos_clientes": [("empresa_id",), ("placa",)],
                        "historico_lavagens_sync": [("empresa_id",), ("placa",)],
                        "sincronizacoes_clientes": [("empresa_id",), ("ativo",), ("proximo_sync_em",)],
                        "configuracao_backup": [("empresa_id",), ("frequencia",)],
                        "manutencao_arquivos": [("empresa_id",), ("ultima_mensagem",)],
                        "integracao_fiscal": [("empresa_id",), ("tipo_integracao",)],
                    }
                    return colunas_por_tabela.get(tabela, [])
                return []

        cursor = FakeCursor()

        def add_column_fake(_cursor, _tabela, _definicao):
            return None

        apply_extended_empresa_scope(cursor, add_column_fake)
        apply_admin_settings_scope(cursor, add_column_fake)
        apply_empresa_indexes(cursor)

        comandos = "\n".join(sql for sql, _ in cursor.commands)
        self.assertNotIn("sqlite_master", comandos)
        self.assertNotIn("PRAGMA", comandos.upper())
        self.assertIn("information_schema.tables", comandos)
        self.assertIn("information_schema.columns", comandos)


if __name__ == "__main__":
    unittest.main()

import sqlite3
import unittest

from domains.empresas import (
    garantir_licenca,
    montar_contexto_licenca,
    obter_uso_licenca,
    salvar_empresa,
    salvar_licenca,
)


class EmpresasDomainTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        c = self.conn.cursor()
        c.execute(
            """
            CREATE TABLE empresas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slug TEXT UNIQUE,
                razao_social TEXT,
                nome_fantasia TEXT,
                documento TEXT,
                email TEXT,
                telefone TEXT,
                ativa INTEGER DEFAULT 1,
                storage_provider TEXT,
                dominio_personalizado TEXT,
                plano_codigo TEXT,
                licenca_status TEXT,
                criado_em TEXT,
                atualizado_em TEXT
            )
            """
        )
        c.execute(
            """
            CREATE TABLE licencas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER,
                codigo_plano TEXT,
                status TEXT,
                limite_usuarios INTEGER,
                limite_atendimentos_mes INTEGER,
                limite_unidades INTEGER,
                limite_storage_mb INTEGER,
                validade_em TEXT,
                recursos_json TEXT,
                criado_em TEXT,
                atualizado_em TEXT
            )
            """
        )
        c.execute("CREATE TABLE usuarios (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER, ativo INTEGER)")
        c.execute("CREATE TABLE servicos (id INTEGER PRIMARY KEY AUTOINCREMENT, empresa_id INTEGER, entrada TEXT)")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def test_salvar_empresa_licenca_e_calcular_limites(self):
        c = self.conn.cursor()
        empresa_id = salvar_empresa(
            c,
            {
                "slug": "acme",
                "nome_fantasia": "Acme",
                "plano_codigo": "starter",
                "licenca_status": "trial",
                "ativa": 1,
            },
            "2026-05-05T10:00:00",
        )
        salvar_licenca(
            c,
            empresa_id,
            {
                "codigo_plano": "starter",
                "status": "trial",
                "limite_usuarios": 1,
                "limite_atendimentos_mes": 2,
            },
            "2026-05-05T10:00:00",
        )
        c.execute("INSERT INTO usuarios (empresa_id, ativo) VALUES (?, 1)", (empresa_id,))
        c.execute("INSERT INTO servicos (empresa_id, entrada) VALUES (?, ?)", (empresa_id, "2026-05-05T10:00:00"))
        c.execute("INSERT INTO servicos (empresa_id, entrada) VALUES (?, ?)", (empresa_id, "2026-05-06T10:00:00"))

        licenca = garantir_licenca(c, empresa_id, "2026-05-05T10:00:00")
        uso = obter_uso_licenca(c, empresa_id, "2026-05")
        contexto = montar_contexto_licenca(licenca, uso)

        self.assertEqual(contexto["usuarios_ativos"], 1)
        self.assertEqual(contexto["atendimentos_mes"], 2)
        self.assertTrue(contexto["excedeu_atendimentos"])


if __name__ == "__main__":
    unittest.main()

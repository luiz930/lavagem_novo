import sqlite3
import unittest

from domains.clientes import consultar_registros_clientes


class ClientesDomainTests(unittest.TestCase):
    def test_consultar_registros_clientes_aplica_limite_inicial(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute("CREATE TABLE clientes (id INTEGER PRIMARY KEY, empresa_id INTEGER, nome TEXT, telefone TEXT, placa_principal TEXT, data_nascimento TEXT)")
        c.execute("CREATE TABLE veiculos (id INTEGER PRIMARY KEY, empresa_id INTEGER, placa TEXT, modelo TEXT, cor TEXT, cliente_id INTEGER)")
        for idx in range(1, 6):
            c.execute("INSERT INTO clientes (id, empresa_id, nome) VALUES (?, 1, ?)", (idx, f"Cliente {idx}"))
            c.execute("INSERT INTO veiculos (id, empresa_id, placa, cliente_id) VALUES (?, 1, ?, ?)", (idx, f"AAA{idx}", idx))
        conn.commit()

        try:
            registros = consultar_registros_clientes(c, 1, limite=2)
        finally:
            conn.close()

        self.assertEqual(len(registros), 2)
        self.assertEqual([item["placa"] for item in registros], ["AAA5", "AAA4"])


if __name__ == "__main__":
    unittest.main()

import unittest

from domains.financeiro import (
    montar_chave_cache_relatorios,
    montar_periodo_descricao,
    montar_resumo_operacional_financeiro,
)


class FinanceiroDomainTests(unittest.TestCase):
    def test_montar_chave_cache_relatorios_independe_do_usuario(self):
        self.assertEqual(montar_chave_cache_relatorios(2, "mes", False), "2|mes|det:0")
        self.assertEqual(montar_chave_cache_relatorios(2, "mes", True), "2|mes|det:1")

    def test_montar_periodo_descricao(self):
        self.assertIn("mes atual", montar_periodo_descricao("mes"))

    def test_montar_resumo_operacional_financeiro(self):
        resumo = montar_resumo_operacional_financeiro(
            [60, 120],
            [30],
            [90],
            1,
            2,
            1,
            {
                "total": 3,
                "lavagem": 1,
                "finalizacao": 1,
                "novos": 2,
                "retornos": 1,
                "atrasados": 0,
                "proxima_entrega_exibicao": "10:00",
            },
            lambda segundos: f"{int(segundos)}s",
            lambda parte, total: f"{parte}/{total}",
        )

        self.assertEqual(resumo["lavagem_media_exibicao"], "90s")
        self.assertEqual(resumo["taxa_no_prazo"], "1/2")
        self.assertEqual(resumo["abertos_total"], 3)


if __name__ == "__main__":
    unittest.main()

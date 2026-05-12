import unittest
from datetime import date, datetime

from domains.financeiro import (
    dias_do_periodo_financeiro,
    filtrar_registros_por_periodo,
    filtrar_servicos_por_periodo,
    montar_chave_cache_relatorios,
    montar_periodo_descricao,
    montar_ranking_equipe_relatorios,
    montar_resumo_operacional_financeiro,
    normalizar_periodo_financeiro,
)


class FinanceiroDomainTests(unittest.TestCase):
    def test_montar_chave_cache_relatorios_independe_do_usuario(self):
        self.assertEqual(montar_chave_cache_relatorios(2, "mes", False), "2|mes|det:0")
        self.assertEqual(montar_chave_cache_relatorios(2, "mes", True), "2|mes|det:1")

    def test_montar_periodo_descricao(self):
        self.assertIn("mes atual", montar_periodo_descricao("mes"))

    def test_normaliza_e_filtra_periodos(self):
        servicos = [
            {"entrega_dt": datetime(2026, 5, 12, 9, 0)},
            {"entrega_dt": datetime(2026, 5, 8, 9, 0)},
            {"entrega_dt": datetime(2026, 4, 30, 9, 0)},
        ]
        registros = [
            {"criado_em_dt": datetime(2026, 5, 12, 9, 0)},
            {"criado_em_dt": datetime(2026, 4, 30, 9, 0)},
        ]

        self.assertEqual(normalizar_periodo_financeiro("invalido"), "mes")
        self.assertEqual(dias_do_periodo_financeiro("7dias", date(2026, 5, 12)), 7)
        self.assertEqual(len(filtrar_servicos_por_periodo(servicos, "mes", date(2026, 5, 12))), 2)
        self.assertEqual(len(filtrar_registros_por_periodo(registros, "hoje", date(2026, 5, 12), "criado_em_dt")), 1)

    def test_montar_ranking_equipe_relatorios(self):
        ranking = montar_ranking_equipe_relatorios(
            [
                {"finalizado_por_nome": "Ana", "valor_num": 50, "lavagem_segundos": 20, "finalizacao_segundos": 10},
                {"finalizado_por_nome": "Ana", "valor_num": 30, "lavagem_segundos": 10, "finalizacao_segundos": 10},
                {"finalizado_por_nome": "Beto", "valor_num": 90, "lavagem_segundos": 5, "finalizacao_segundos": 5},
            ],
            lambda valor: str(valor or "").strip(),
            float,
            lambda valor: f"R$ {valor:.2f}",
            lambda segundos: f"{int(segundos)}s",
        )

        self.assertEqual(ranking[0]["nome"], "Ana")
        self.assertEqual(ranking[0]["quantidade"], 2)
        self.assertEqual(ranking[0]["tempo_medio_exibicao"], "25s")

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

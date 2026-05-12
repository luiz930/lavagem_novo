import unittest

from domains.documentos_fiscais import (
    calcular_totais_nota_fiscal,
    calcular_totais_orcamento,
    montar_prefill_nota_por_orcamento,
)


class DocumentosFiscaisDomainTests(unittest.TestCase):
    def test_calcula_totais_orcamento(self):
        totais = calcular_totais_orcamento(
            [{"valor_total": 100}, {"valor_total": 50.25}],
            desconto=10,
        )

        self.assertEqual(totais["subtotal"], 150.25)
        self.assertEqual(totais["desconto"], 10)
        self.assertEqual(totais["total"], 140.25)

    def test_calcula_totais_nota_fiscal(self):
        totais = calcular_totais_nota_fiscal(
            [{"valor_total": 200}],
            desconto=50,
            aliquota_iss=5,
        )

        self.assertEqual(totais["valor_servicos"], 200)
        self.assertEqual(totais["base_calculo"], 150)
        self.assertEqual(totais["valor_iss"], 7.5)
        self.assertEqual(totais["valor_total"], 150)

    def test_monta_prefill_nota_por_orcamento(self):
        prefill = montar_prefill_nota_por_orcamento(
            {
                "id": 7,
                "cliente_nome": "Cliente",
                "desconto": 3,
                "itens": [{"descricao": "Lavagem"}, {"descricao": "Cera"}],
            },
            {"codigo_servico_padrao": "14.01", "aliquota_padrao": 2},
        )

        self.assertEqual(prefill["origem_orcamento_id"], 7)
        self.assertEqual(prefill["codigo_servico"], "14.01")
        self.assertEqual(prefill["aliquota_iss"], 2)
        self.assertEqual(prefill["discriminacao"], "Lavagem; Cera")


if __name__ == "__main__":
    unittest.main()

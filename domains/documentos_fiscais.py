def calcular_totais_orcamento(itens, desconto=0):
    subtotal = round(sum(float(item.get("valor_total") or 0) for item in itens or []), 2)
    desconto = max(0.0, float(desconto or 0))
    total = max(subtotal - desconto, 0.0)
    return {"subtotal": subtotal, "desconto": desconto, "total": total}


def calcular_totais_nota_fiscal(itens, desconto=0, aliquota_iss=0):
    valor_servicos = round(sum(float(item.get("valor_total") or 0) for item in itens or []), 2)
    desconto = max(0.0, float(desconto or 0))
    base_calculo = max(valor_servicos - desconto, 0.0)
    aliquota_iss = max(0.0, float(aliquota_iss or 0))
    valor_iss = round((base_calculo * aliquota_iss) / 100, 2)
    return {
        "valor_servicos": valor_servicos,
        "desconto": desconto,
        "base_calculo": base_calculo,
        "aliquota_iss": aliquota_iss,
        "valor_iss": valor_iss,
        "valor_total": base_calculo,
    }


def montar_prefill_nota_por_orcamento(orcamento, empresa):
    if not orcamento:
        return None

    empresa = empresa or {}
    discriminacao = "; ".join(item["descricao"] for item in orcamento.get("itens", []))
    return {
        "origem_orcamento_id": orcamento["id"],
        "cliente_nome": orcamento.get("cliente_nome", ""),
        "cliente_documento": orcamento.get("cliente_documento", ""),
        "email": orcamento.get("email", ""),
        "telefone": orcamento.get("telefone", ""),
        "placa": orcamento.get("placa", ""),
        "modelo": orcamento.get("modelo", ""),
        "codigo_servico": empresa.get("codigo_servico_padrao", ""),
        "aliquota_iss": empresa.get("aliquota_padrao", 0),
        "discriminacao": discriminacao,
        "desconto": orcamento.get("desconto", 0),
        "itens": orcamento.get("itens", []),
    }

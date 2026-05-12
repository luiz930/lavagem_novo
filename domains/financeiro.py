from datetime import timedelta


PERIODOS_VALIDOS_FINANCEIRO = {"hoje", "7dias", "30dias", "mes"}


def montar_chave_cache_relatorios(empresa_id, periodo, detalhado=False):
    return f"{int(empresa_id or 1)}|{str(periodo or '').strip()}|det:{int(bool(detalhado))}"


def normalizar_periodo_financeiro(valor):
    periodo = str(valor or "mes").strip().lower()
    return periodo if periodo in PERIODOS_VALIDOS_FINANCEIRO else "mes"


def dias_do_periodo_financeiro(periodo, data_referencia):
    if periodo == "hoje":
        return 1
    if periodo == "7dias":
        return 7
    if periodo == "30dias":
        return 30
    return max(1, data_referencia.day)


def filtrar_servicos_por_periodo(servicos, periodo, data_referencia):
    if periodo == "hoje":
        return [
            item for item in servicos
            if item["entrega_dt"].date() == data_referencia
        ]

    if periodo == "7dias":
        inicio = data_referencia - timedelta(days=6)
        return [
            item for item in servicos
            if inicio <= item["entrega_dt"].date() <= data_referencia
        ]

    if periodo == "30dias":
        inicio = data_referencia - timedelta(days=29)
        return [
            item for item in servicos
            if inicio <= item["entrega_dt"].date() <= data_referencia
        ]

    return [
        item for item in servicos
        if (
            item["entrega_dt"].year == data_referencia.year and
            item["entrega_dt"].month == data_referencia.month
        )
    ]


def filtrar_registros_por_periodo(registros, periodo, data_referencia, campo_data):
    resultado = []
    for item in registros or []:
        datahora = item.get(campo_data)
        if not datahora:
            continue
        data_item = datahora.date()
        if periodo == "hoje" and data_item == data_referencia:
            resultado.append(item)
        elif periodo == "7dias" and data_referencia - timedelta(days=6) <= data_item <= data_referencia:
            resultado.append(item)
        elif periodo == "30dias" and data_referencia - timedelta(days=29) <= data_item <= data_referencia:
            resultado.append(item)
        elif periodo == "mes" and data_item.year == data_referencia.year and data_item.month == data_referencia.month:
            resultado.append(item)
    return resultado


def montar_periodo_descricao(periodo_atual):
    descricoes = {
        "hoje": "Resultados fechados hoje com leitura financeira, operacional e comercial.",
        "7dias": "Panorama consolidado dos ultimos 7 dias com documentos e produtividade.",
        "30dias": "Panorama consolidado dos ultimos 30 dias com servicos, equipe e documentos.",
        "mes": "Fechamentos acumulados no mes atual com visao completa da operacao.",
    }
    return descricoes[periodo_atual]


def montar_ranking_equipe_relatorios(
    servicos,
    normalizar_texto,
    converter_valor,
    formatar_valor,
    formatar_duracao,
):
    ranking = {}
    for item in servicos or []:
        responsavel = (
            normalizar_texto(item.get("finalizado_por_nome"))
            or normalizar_texto(item.get("operacional_por_nome"))
            or normalizar_texto(item.get("criado_por_nome"))
            or normalizar_texto(item.get("usuario"))
            or "Equipe"
        )
        resumo = ranking.setdefault(
            responsavel,
            {"nome": responsavel, "quantidade": 0, "valor_total": 0.0, "tempo_total_segundos": 0},
        )
        resumo["quantidade"] += 1
        resumo["valor_total"] += converter_valor(item.get("valor_num"))
        resumo["tempo_total_segundos"] += max(
            0,
            int(item.get("lavagem_segundos") or 0) + int(item.get("finalizacao_segundos") or 0),
        )

    itens = sorted(
        ranking.values(),
        key=lambda row: (row["quantidade"], row["valor_total"]),
        reverse=True,
    )
    referencia = itens[0]["quantidade"] if itens else 0
    for item in itens:
        item["valor_exibicao"] = formatar_valor(item["valor_total"])
        item["tempo_medio_exibicao"] = formatar_duracao(
            item["tempo_total_segundos"] / item["quantidade"] if item["quantidade"] else 0
        )
        item["percentual"] = round((item["quantidade"] / referencia) * 100) if referencia else 0
    return itens[:5]


def montar_resumo_operacional_financeiro(
    tempos_lavagem,
    tempos_finalizacao,
    tempos_ciclo,
    entregas_no_prazo,
    entregas_com_previsao,
    entregas_fora_prazo,
    resumo_fluxo_aberto,
    formatar_duracao,
    formatar_taxa,
):
    return {
        "lavagem_media_exibicao": formatar_duracao(
            sum(tempos_lavagem) / len(tempos_lavagem) if tempos_lavagem else 0
        ),
        "finalizacao_media_exibicao": formatar_duracao(
            sum(tempos_finalizacao) / len(tempos_finalizacao) if tempos_finalizacao else 0
        ),
        "ciclo_medio_exibicao": formatar_duracao(
            sum(tempos_ciclo) / len(tempos_ciclo) if tempos_ciclo else 0
        ),
        "entregas_no_prazo": entregas_no_prazo,
        "entregas_com_previsao": entregas_com_previsao,
        "entregas_fora_prazo": entregas_fora_prazo,
        "taxa_no_prazo": formatar_taxa(entregas_no_prazo, entregas_com_previsao),
        "abertos_total": resumo_fluxo_aberto["total"],
        "abertos_lavagem": resumo_fluxo_aberto["lavagem"],
        "abertos_finalizacao": resumo_fluxo_aberto["finalizacao"],
        "abertos_novos": resumo_fluxo_aberto["novos"],
        "abertos_retornos": resumo_fluxo_aberto["retornos"],
        "abertos_atrasados": resumo_fluxo_aberto["atrasados"],
        "proxima_entrega_exibicao": resumo_fluxo_aberto["proxima_entrega_exibicao"],
    }

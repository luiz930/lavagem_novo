def montar_chave_cache_relatorios(empresa_id, periodo, detalhado=False):
    return f"{int(empresa_id or 1)}|{str(periodo or '').strip()}|det:{int(bool(detalhado))}"


def montar_periodo_descricao(periodo_atual):
    descricoes = {
        "hoje": "Resultados fechados hoje com leitura financeira, operacional e comercial.",
        "7dias": "Panorama consolidado dos ultimos 7 dias com documentos e produtividade.",
        "30dias": "Panorama consolidado dos ultimos 30 dias com servicos, equipe e documentos.",
        "mes": "Fechamentos acumulados no mes atual com visao completa da operacao.",
    }
    return descricoes[periodo_atual]


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

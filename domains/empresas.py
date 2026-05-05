from __future__ import annotations

import json
from datetime import date, datetime

from .tenant import normalize_empresa_id, row_to_dict, rows_to_dicts


PLANOS_LICENCA = {
    "starter": {
        "label": "Starter",
        "limite_usuarios": 3,
        "limite_atendimentos_mes": 120,
        "limite_unidades": 1,
        "limite_storage_mb": 512,
        "recursos": {
            "financeiro": True,
            "notas": False,
            "backup_online": False,
            "multiempresa": False,
            "whitelabel": False,
        },
    },
    "pro": {
        "label": "Pro",
        "limite_usuarios": 10,
        "limite_atendimentos_mes": 800,
        "limite_unidades": 1,
        "limite_storage_mb": 2048,
        "recursos": {
            "financeiro": True,
            "notas": True,
            "backup_online": True,
            "multiempresa": False,
            "whitelabel": True,
        },
    },
    "business": {
        "label": "Business",
        "limite_usuarios": 50,
        "limite_atendimentos_mes": 5000,
        "limite_unidades": 10,
        "limite_storage_mb": 10240,
        "recursos": {
            "financeiro": True,
            "notas": True,
            "backup_online": True,
            "multiempresa": True,
            "whitelabel": True,
        },
    },
}

STATUS_LICENCA = {
    "trial": "Trial",
    "ativa": "Ativa",
    "vencida": "Vencida",
    "bloqueada": "Bloqueada",
}


def normalizar_plano(valor):
    plano = str(valor or "starter").strip().lower()
    return plano if plano in PLANOS_LICENCA else "starter"


def normalizar_status_licenca(valor):
    status = str(valor or "trial").strip().lower()
    return status if status in STATUS_LICENCA else "trial"


def plano_padrao(codigo):
    return dict(PLANOS_LICENCA[normalizar_plano(codigo)])


def _parse_data(valor):
    texto = str(valor or "").strip()
    if not texto:
        return None
    try:
        return datetime.fromisoformat(texto.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return date.fromisoformat(texto[:10])
        except Exception:
            return None


def listar_empresas(cursor):
    cursor.execute(
        """
        SELECT
            empresas.*,
            licencas.codigo_plano AS licenca_codigo_plano,
            licencas.status AS licenca_status_atual,
            licencas.limite_usuarios,
            licencas.limite_atendimentos_mes,
            licencas.limite_unidades,
            licencas.limite_storage_mb,
            licencas.validade_em,
            licencas.recursos_json
        FROM empresas
        LEFT JOIN licencas ON licencas.empresa_id = empresas.id
        ORDER BY empresas.ativa DESC, LOWER(COALESCE(empresas.nome_fantasia, empresas.razao_social, empresas.slug, '')) ASC, empresas.id ASC
        """
    )
    return rows_to_dicts(cursor)


def obter_empresa(cursor, empresa_id):
    empresa_id = normalize_empresa_id(empresa_id)
    cursor.execute("SELECT * FROM empresas WHERE id=?", (empresa_id,))
    return row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])


def obter_licenca(cursor, empresa_id):
    empresa_id = normalize_empresa_id(empresa_id)
    cursor.execute("SELECT * FROM licencas WHERE empresa_id=? ORDER BY id LIMIT 1", (empresa_id,))
    return row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])


def garantir_licenca(cursor, empresa_id, agora_iso):
    empresa_id = normalize_empresa_id(empresa_id)
    licenca = obter_licenca(cursor, empresa_id)
    if licenca:
        return licenca

    plano = plano_padrao("starter")
    cursor.execute(
        """
        INSERT INTO licencas (
            empresa_id, codigo_plano, status, limite_usuarios,
            limite_atendimentos_mes, limite_unidades, limite_storage_mb,
            recursos_json, criado_em, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            empresa_id,
            "starter",
            "trial",
            plano["limite_usuarios"],
            plano["limite_atendimentos_mes"],
            plano["limite_unidades"],
            plano["limite_storage_mb"],
            json.dumps(plano["recursos"], ensure_ascii=False),
            agora_iso,
            agora_iso,
        ),
    )
    return obter_licenca(cursor, empresa_id)


def salvar_empresa(cursor, dados, agora_iso, empresa_id=None):
    payload = dict(dados or {})
    plano = normalizar_plano(payload.get("plano_codigo"))
    status = normalizar_status_licenca(payload.get("licenca_status"))
    empresa_id = normalize_empresa_id(empresa_id) if empresa_id else None

    if empresa_id:
        cursor.execute(
            """
            UPDATE empresas
            SET slug=?, razao_social=?, nome_fantasia=?, documento=?, email=?, telefone=?,
                ativa=?, storage_provider=?, dominio_personalizado=?, plano_codigo=?,
                licenca_status=?, atualizado_em=?
            WHERE id=?
            """,
            (
                payload.get("slug"),
                payload.get("razao_social"),
                payload.get("nome_fantasia"),
                payload.get("documento"),
                payload.get("email"),
                payload.get("telefone"),
                int(payload.get("ativa", 1)),
                payload.get("storage_provider") or "database",
                payload.get("dominio_personalizado"),
                plano,
                status,
                agora_iso,
                empresa_id,
            ),
        )
        return empresa_id

    cursor.execute(
        """
        INSERT INTO empresas (
            slug, razao_social, nome_fantasia, documento, email, telefone,
            ativa, storage_provider, dominio_personalizado, plano_codigo,
            licenca_status, criado_em, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("slug"),
            payload.get("razao_social"),
            payload.get("nome_fantasia"),
            payload.get("documento"),
            payload.get("email"),
            payload.get("telefone"),
            int(payload.get("ativa", 1)),
            payload.get("storage_provider") or "database",
            payload.get("dominio_personalizado"),
            plano,
            status,
            agora_iso,
            agora_iso,
        ),
    )
    return int(getattr(cursor, "lastrowid", None) or 0)


def salvar_licenca(cursor, empresa_id, dados, agora_iso):
    empresa_id = normalize_empresa_id(empresa_id)
    payload = dict(dados or {})
    plano_codigo = normalizar_plano(payload.get("codigo_plano"))
    status = normalizar_status_licenca(payload.get("status"))
    plano = plano_padrao(plano_codigo)
    recursos = payload.get("recursos")
    if not isinstance(recursos, dict):
        recursos = plano["recursos"]

    existente = obter_licenca(cursor, empresa_id)
    valores = (
        plano_codigo,
        status,
        int(payload.get("limite_usuarios") or plano["limite_usuarios"]),
        int(payload.get("limite_atendimentos_mes") or plano["limite_atendimentos_mes"]),
        int(payload.get("limite_unidades") or plano["limite_unidades"]),
        int(payload.get("limite_storage_mb") or plano["limite_storage_mb"]),
        payload.get("validade_em") or None,
        json.dumps(recursos, ensure_ascii=False),
        agora_iso,
    )

    if existente:
        cursor.execute(
            """
            UPDATE licencas
            SET codigo_plano=?, status=?, limite_usuarios=?, limite_atendimentos_mes=?,
                limite_unidades=?, limite_storage_mb=?, validade_em=?, recursos_json=?,
                atualizado_em=?
            WHERE empresa_id=?
            """,
            valores + (empresa_id,),
        )
    else:
        cursor.execute(
            """
            INSERT INTO licencas (
                codigo_plano, status, limite_usuarios, limite_atendimentos_mes,
                limite_unidades, limite_storage_mb, validade_em, recursos_json,
                atualizado_em, empresa_id, criado_em
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            valores + (empresa_id, agora_iso),
        )

    cursor.execute(
        "UPDATE empresas SET plano_codigo=?, licenca_status=?, atualizado_em=? WHERE id=?",
        (plano_codigo, status, agora_iso, empresa_id),
    )


def obter_uso_licenca(cursor, empresa_id, mes_prefixo):
    empresa_id = normalize_empresa_id(empresa_id)
    cursor.execute("SELECT COUNT(*) AS total FROM usuarios WHERE empresa_id=? AND COALESCE(ativo, 1)=1", (empresa_id,))
    usuarios = row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])
    cursor.execute(
        """
        SELECT COUNT(*) AS total
        FROM servicos
        WHERE empresa_id=?
          AND substr(COALESCE(entrada, ''), 1, 7)=?
        """,
        (empresa_id, mes_prefixo),
    )
    atendimentos = row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])
    return {
        "usuarios_ativos": int((usuarios or {}).get("total") or 0),
        "atendimentos_mes": int((atendimentos or {}).get("total") or 0),
    }


def montar_contexto_licenca(licenca, uso=None, hoje=None):
    licenca = dict(licenca or {})
    uso = dict(uso or {})
    hoje = hoje or date.today()
    plano = normalizar_plano(licenca.get("codigo_plano"))
    status = normalizar_status_licenca(licenca.get("status"))
    validade = _parse_data(licenca.get("validade_em"))
    dias_restantes = None
    if validade:
        dias_restantes = (validade - hoje).days
        if dias_restantes < 0 and status in {"trial", "ativa"}:
            status = "vencida"

    limite_usuarios = int(licenca.get("limite_usuarios") or plano_padrao(plano)["limite_usuarios"])
    limite_atendimentos = int(licenca.get("limite_atendimentos_mes") or plano_padrao(plano)["limite_atendimentos_mes"])
    usuarios_ativos = int(uso.get("usuarios_ativos") or 0)
    atendimentos_mes = int(uso.get("atendimentos_mes") or 0)
    bloqueada = status in {"vencida", "bloqueada"} or usuarios_ativos > limite_usuarios or atendimentos_mes >= limite_atendimentos

    return {
        "codigo_plano": plano,
        "plano_label": PLANOS_LICENCA[plano]["label"],
        "status": status,
        "status_label": STATUS_LICENCA[status],
        "validade_em": licenca.get("validade_em") or "",
        "dias_restantes": dias_restantes,
        "limite_usuarios": limite_usuarios,
        "limite_atendimentos_mes": limite_atendimentos,
        "usuarios_ativos": usuarios_ativos,
        "atendimentos_mes": atendimentos_mes,
        "bloqueada": bloqueada,
        "aviso": validade is not None and 0 <= dias_restantes <= 7,
        "excedeu_usuarios": usuarios_ativos > limite_usuarios,
        "excedeu_atendimentos": atendimentos_mes >= limite_atendimentos,
    }

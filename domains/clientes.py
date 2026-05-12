from __future__ import annotations

from .tenant import normalize_empresa_id, row_to_dict, rows_to_dicts


def consultar_registros_clientes(cursor, empresa_id, busca="", limite=None):
    empresa_id = normalize_empresa_id(empresa_id)
    params = [empresa_id, empresa_id]
    sql = """
        SELECT
            veiculos.id AS veiculo_id,
            veiculos.placa,
            veiculos.modelo,
            veiculos.cor,
            clientes.id AS cliente_id,
            clientes.nome,
            clientes.telefone,
            clientes.placa_principal,
            clientes.data_nascimento
        FROM veiculos
        LEFT JOIN clientes
            ON clientes.id = veiculos.cliente_id
           AND clientes.empresa_id = ?
        WHERE veiculos.empresa_id = ?
    """

    if busca:
        termo = f"%{str(busca).strip()}%"
        sql += """
            AND (
                veiculos.placa LIKE ?
                OR veiculos.modelo LIKE ?
                OR clientes.nome LIKE ?
                OR clientes.telefone LIKE ?
            )
        """
        params.extend([termo, termo, termo, termo])

    sql += " ORDER BY veiculos.id DESC"
    if limite:
        sql += " LIMIT ?"
        params.append(max(1, int(limite)))
    cursor.execute(sql, tuple(params))
    return rows_to_dicts(cursor)


def salvar_cliente_veiculo_cursor(
    cursor,
    empresa_id,
    placa_nova,
    nome="",
    telefone="",
    data_nascimento=None,
    modelo="",
    cor="",
    placa_referencia="",
    sincronizar_placa_principal_fn=None,
):
    empresa_id = normalize_empresa_id(empresa_id)
    placa_nova = str(placa_nova or "").strip().upper()
    placa_referencia = str(placa_referencia or placa_nova).strip().upper() or placa_nova

    if not placa_nova:
        raise ValueError("Informe a placa do veiculo.")

    cursor.execute(
        """
        SELECT id, placa, modelo, cor, cliente_id
        FROM veiculos
        WHERE empresa_id=? AND placa=?
        """,
        (empresa_id, placa_referencia),
    )
    veiculo_existente = row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])

    if placa_referencia != placa_nova:
        cursor.execute(
            "SELECT id FROM veiculos WHERE empresa_id=? AND placa=?",
            (empresa_id, placa_nova),
        )
        conflito = row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])
        if conflito and (not veiculo_existente or conflito["id"] != veiculo_existente["id"]):
            raise ValueError("Ja existe um veiculo cadastrado com essa placa.")

    cliente_existente = {}
    cliente_id = None

    if veiculo_existente and veiculo_existente.get("cliente_id"):
        cursor.execute(
            """
            SELECT id, nome, telefone, data_nascimento
            FROM clientes
            WHERE empresa_id=? AND id=?
            """,
            (empresa_id, veiculo_existente["cliente_id"]),
        )
        cliente_existente = row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])
    elif telefone:
        cursor.execute(
            """
            SELECT id, nome, telefone, data_nascimento
            FROM clientes
            WHERE empresa_id=? AND telefone=?
            """,
            (empresa_id, telefone),
        )
        cliente_existente = row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])

    cliente_acao = "nenhum"

    if cliente_existente:
        cliente_id = cliente_existente["id"]
        cursor.execute(
            """
            UPDATE clientes
            SET nome=?, telefone=?, data_nascimento=?
            WHERE empresa_id=? AND id=?
            """,
            (nome or "Sem nome", telefone, data_nascimento, empresa_id, cliente_id),
        )
        cliente_acao = "atualizado"
    elif nome or telefone:
        cursor.execute(
            """
            INSERT INTO clientes (empresa_id, nome, telefone, data_nascimento)
            VALUES (?, ?, ?, ?)
            """,
            (empresa_id, nome or "Sem nome", telefone, data_nascimento),
        )
        cliente_id = cursor.lastrowid
        cliente_acao = "novo"

    if veiculo_existente:
        cursor.execute(
            """
            UPDATE veiculos
            SET placa=?, modelo=?, cor=?, cliente_id=?
            WHERE empresa_id=? AND id=?
            """,
            (placa_nova, modelo, cor, cliente_id, empresa_id, veiculo_existente["id"]),
        )
        acao = "atualizado"
    else:
        cursor.execute(
            """
            INSERT INTO veiculos (empresa_id, placa, modelo, cor, cliente_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (empresa_id, placa_nova, modelo, cor, cliente_id),
        )
        acao = "novo"

    if cliente_id and callable(sincronizar_placa_principal_fn):
        sincronizar_placa_principal_fn(cursor, cliente_id, placa_nova)

    return {
        "placa": placa_nova,
        "acao": acao,
        "cliente_acao": cliente_acao,
        "cliente_id": cliente_id,
    }


def consultar_sincronizacoes_clientes(cursor, empresa_id):
    empresa_id = normalize_empresa_id(empresa_id)
    cursor.execute(
        """
        SELECT *
        FROM sincronizacoes_clientes
        WHERE empresa_id=?
          AND COALESCE(excluido_em, '')=''
        ORDER BY id DESC
        """,
        (empresa_id,),
    )
    return rows_to_dicts(cursor)


def consultar_sincronizacao_cliente(cursor, sync_id, empresa_id=None):
    params = [int(sync_id)]
    sql = "SELECT * FROM sincronizacoes_clientes WHERE id=? AND COALESCE(excluido_em, '')=''"

    if empresa_id is not None:
        sql += " AND empresa_id=?"
        params.append(normalize_empresa_id(empresa_id))

    cursor.execute(sql, tuple(params))
    return row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])


def consultar_ultima_sincronizacao_cliente(cursor, empresa_id):
    empresa_id = normalize_empresa_id(empresa_id)
    cursor.execute(
        """
        SELECT id, ultima_mensagem, ultimo_status
        FROM sincronizacoes_clientes
        WHERE empresa_id=?
          AND COALESCE(excluido_em, '')=''
        ORDER BY id DESC
        LIMIT 1
        """,
        (empresa_id,),
    )
    return row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])


def consultar_historico_sync_por_placa(cursor, empresa_id, placa):
    empresa_id = normalize_empresa_id(empresa_id)
    cursor.execute(
        """
        SELECT *
        FROM historico_lavagens_sync
        WHERE empresa_id=? AND placa=?
        ORDER BY
            CASE WHEN data_lavagem IS NULL OR data_lavagem='' THEN 1 ELSE 0 END,
            data_lavagem DESC,
            id DESC
        LIMIT 1
        """,
        (empresa_id, str(placa or "").strip().upper()),
    )
    return row_to_dict(cursor.fetchone(), columns=[item[0] for item in cursor.description or []])


def consultar_ultimas_lavagens_sync(cursor, empresa_id):
    empresa_id = normalize_empresa_id(empresa_id)
    cursor.execute(
        """
        SELECT
            placa,
            cliente,
            carro,
            cor,
            servico,
            data_lavagem,
            data_original,
            id
        FROM historico_lavagens_sync
        WHERE empresa_id=?
          AND placa IS NOT NULL
          AND TRIM(placa) <> ''
        ORDER BY
            UPPER(placa) ASC,
            CASE
                WHEN data_lavagem IS NULL OR data_lavagem = '' THEN 1
                ELSE 0
            END,
            data_lavagem DESC,
            id DESC
        """,
        (empresa_id,),
    )
    return rows_to_dicts(cursor)


def consultar_contatos_clientes_por_placas(cursor, empresa_id, placas):
    empresa_id = normalize_empresa_id(empresa_id)
    placas_normalizadas = [str(item or "").strip().upper() for item in placas if str(item or "").strip()]
    if not placas_normalizadas:
        return []
    placeholders = ",".join(["?"] * len(placas_normalizadas))
    cursor.execute(
        f"""
        SELECT
            UPPER(veiculos.placa) AS placa,
            clientes.nome AS cliente_nome,
            clientes.telefone,
            veiculos.modelo AS carro,
            veiculos.cor AS cor
        FROM veiculos
        LEFT JOIN clientes
            ON veiculos.cliente_id = clientes.id
           AND clientes.empresa_id = ?
        WHERE veiculos.empresa_id = ?
          AND UPPER(veiculos.placa) IN ({placeholders})
        """,
        (empresa_id, empresa_id, *placas_normalizadas),
    )
    return rows_to_dicts(cursor)


def consultar_estados_retorno(cursor, empresa_id, placas=None):
    empresa_id = normalize_empresa_id(empresa_id)
    if placas:
        placas_normalizadas = [str(item or "").strip().upper() for item in placas if str(item or "").strip()]
        if not placas_normalizadas:
            return []
        placeholders = ",".join(["?"] * len(placas_normalizadas))
        cursor.execute(
            f"""
            SELECT *
            FROM retornos_clientes
            WHERE empresa_id=?
              AND placa IN ({placeholders})
            """,
            (empresa_id, *placas_normalizadas),
        )
    else:
        cursor.execute(
            "SELECT * FROM retornos_clientes WHERE empresa_id=?",
            (empresa_id,),
        )
    return rows_to_dicts(cursor)

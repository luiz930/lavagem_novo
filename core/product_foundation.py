from __future__ import annotations

import json
import re


FOUNDATION_MIGRATIONS = (
    "foundation_enterprises",
    "foundation_licencas",
    "foundation_telemetria",
    "foundation_empresa_scope",
    "foundation_branding_storage",
    "foundation_site_customization",
    "foundation_empresa_scope_extended",
    "foundation_admin_settings_scope",
    "foundation_empresa_indexes",
    "foundation_legacy_plate_uniqueness",
)


def ensure_schema_migrations(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT UNIQUE NOT NULL,
            aplicado_em TEXT NOT NULL
        )
        """
    )


def applied_migration_names(cursor):
    cursor.execute("SELECT nome FROM schema_migrations")
    return {str(row[0] if not hasattr(row, "keys") else row["nome"]) for row in cursor.fetchall()}


def mark_migration_applied(cursor, nome, aplicado_em):
    cursor.execute(
        "INSERT INTO schema_migrations (nome, aplicado_em) VALUES (?, ?)",
        (nome, aplicado_em),
    )


def create_enterprises_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS empresas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            slug TEXT UNIQUE,
            razao_social TEXT,
            nome_fantasia TEXT,
            documento TEXT,
            email TEXT,
            telefone TEXT,
            ativa INTEGER DEFAULT 1,
            onboarding_concluido INTEGER DEFAULT 0,
            storage_provider TEXT DEFAULT 'database',
            storage_bucket TEXT,
            storage_public_base_url TEXT,
            dominio_personalizado TEXT,
            plano_codigo TEXT DEFAULT 'starter',
            licenca_status TEXT DEFAULT 'trial',
            cor_primaria TEXT DEFAULT '#facc15',
            cor_secundaria TEXT DEFAULT '#111827',
            logo_url TEXT,
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def seed_default_enterprise(cursor, now_iso):
    cursor.execute("SELECT id FROM empresas WHERE id=1")
    existe = cursor.fetchone()
    if existe:
        return
    cursor.execute(
        """
        INSERT INTO empresas (
            id, slug, razao_social, nome_fantasia, ativa,
            onboarding_concluido, storage_provider, plano_codigo,
            licenca_status, criado_em, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "wagen-estetica",
            "Wagen Estetica Automotiva",
            "Wagen Estetica",
            1,
            0,
            "database",
            "starter",
            "trial",
            now_iso,
            now_iso,
        ),
    )


def create_licencas_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS licencas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            empresa_id INTEGER NOT NULL,
            codigo_plano TEXT DEFAULT 'starter',
            status TEXT DEFAULT 'trial',
            limite_usuarios INTEGER DEFAULT 5,
            limite_unidades INTEGER DEFAULT 1,
            limite_storage_mb INTEGER DEFAULT 512,
            validade_em TEXT,
            recursos_json TEXT,
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(empresa_id) REFERENCES empresas(id)
        )
        """
    )


def seed_default_license(cursor, now_iso):
    cursor.execute("SELECT id FROM licencas WHERE empresa_id=1")
    existe = cursor.fetchone()
    if existe:
        return
    cursor.execute(
        """
        INSERT INTO licencas (
            empresa_id, codigo_plano, status, limite_usuarios,
            limite_unidades, limite_storage_mb, recursos_json,
            criado_em, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            "starter",
            "trial",
            5,
            1,
            512,
            json.dumps(
                {
                    "multiempresa": True,
                    "telemetria": True,
                    "whitelabel": True,
                    "backup_online": True,
                },
                ensure_ascii=False,
            ),
            now_iso,
            now_iso,
        ),
    )


def create_telemetry_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS telemetria_eventos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            empresa_id INTEGER DEFAULT 1,
            usuario_id INTEGER,
            usuario TEXT,
            categoria TEXT NOT NULL,
            evento TEXT NOT NULL,
            severidade TEXT DEFAULT 'info',
            payload_json TEXT,
            ip TEXT,
            user_agent TEXT,
            criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(empresa_id) REFERENCES empresas(id)
        )
        """
    )


def apply_empresa_scope(cursor, add_column):
    add_column(cursor, "usuarios", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "clientes", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "veiculos", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "servicos", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "fotos", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "retornos_clientes", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "sincronizacoes_clientes", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "orcamentos", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "notas_fiscais", "empresa_id INTEGER DEFAULT 1")
    add_column(cursor, "configuracao_empresa", "empresa_id INTEGER DEFAULT 1")

    for tabela in (
        "usuarios",
        "clientes",
        "veiculos",
        "servicos",
        "fotos",
        "retornos_clientes",
        "sincronizacoes_clientes",
        "orcamentos",
        "notas_fiscais",
        "configuracao_empresa",
    ):
        cursor.execute(f"UPDATE {tabela} SET empresa_id=1 WHERE empresa_id IS NULL")


def apply_extended_empresa_scope(cursor, add_column):
    if not _table_exists(cursor, "historico_lavagens_sync"):
        return
    add_column(cursor, "historico_lavagens_sync", "empresa_id INTEGER DEFAULT 1")
    cursor.execute(
        "UPDATE historico_lavagens_sync SET empresa_id=1 WHERE empresa_id IS NULL"
    )


def _singleton_admin_tables():
    return (
        "configuracao_backup",
        "manutencao_arquivos",
        "integracao_fiscal",
    )


def apply_branding_and_storage(cursor, add_column):
    add_column(cursor, "configuracao_empresa", "atualizado_em TEXT")
    add_column(cursor, "configuracao_empresa", "marca_nome TEXT")
    add_column(cursor, "configuracao_empresa", "marca_subtitulo TEXT")
    add_column(cursor, "configuracao_empresa", "marca_logo_url TEXT")
    add_column(cursor, "configuracao_empresa", "marca_cor_primaria TEXT")
    add_column(cursor, "configuracao_empresa", "marca_cor_secundaria TEXT")
    add_column(cursor, "configuracao_empresa", "whitelabel_ativo INTEGER DEFAULT 0")
    add_column(cursor, "configuracao_empresa", "storage_provider TEXT DEFAULT 'database'")
    add_column(cursor, "configuracao_empresa", "storage_bucket TEXT")
    add_column(cursor, "configuracao_empresa", "storage_public_base_url TEXT")
    add_column(cursor, "configuracao_empresa", "telemetria_ativo INTEGER DEFAULT 1")
    add_column(cursor, "configuracao_empresa", "licenca_plano TEXT DEFAULT 'starter'")
    add_column(cursor, "configuracao_empresa", "licenca_status TEXT DEFAULT 'trial'")
    add_column(cursor, "configuracao_empresa", "onboarding_concluido INTEGER DEFAULT 0")
    add_column(cursor, "configuracao_empresa", "suporte_email TEXT")
    add_column(cursor, "configuracao_empresa", "suporte_whatsapp TEXT")

    cursor.execute("SELECT id FROM configuracao_empresa WHERE id=1")
    existe = cursor.fetchone()
    if not existe:
        cursor.execute(
            """
            INSERT INTO configuracao_empresa (
                id, empresa_id, marca_nome, marca_subtitulo, marca_cor_primaria,
                marca_cor_secundaria, storage_provider, telemetria_ativo,
                licenca_plano, licenca_status, onboarding_concluido, atualizado_em
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            (
                1,
                1,
                "Wagen Estetica Automotiva",
                "Gestao Estetica",
                "#facc15",
                "#111827",
                "database",
                1,
                "starter",
                "trial",
                0,
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE configuracao_empresa
            SET empresa_id=COALESCE(empresa_id, 1),
                marca_nome=COALESCE(NULLIF(marca_nome, ''), COALESCE(nome_fantasia, 'Wagen Estetica Automotiva')),
                marca_subtitulo=COALESCE(NULLIF(marca_subtitulo, ''), 'Gestao Estetica'),
                marca_cor_primaria=COALESCE(NULLIF(marca_cor_primaria, ''), '#facc15'),
                marca_cor_secundaria=COALESCE(NULLIF(marca_cor_secundaria, ''), '#111827'),
                storage_provider=COALESCE(NULLIF(storage_provider, ''), 'database'),
                telemetria_ativo=COALESCE(telemetria_ativo, 1),
                licenca_plano=COALESCE(NULLIF(licenca_plano, ''), 'starter'),
                licenca_status=COALESCE(NULLIF(licenca_status, ''), 'trial'),
                onboarding_concluido=COALESCE(onboarding_concluido, 0)
            WHERE id=1
            """
        )


def apply_site_customization(cursor, add_column):
    add_column(cursor, "configuracao_empresa", "site_titulo TEXT")
    add_column(cursor, "configuracao_empresa", "site_rodape_texto TEXT")
    add_column(cursor, "configuracao_empresa", "marca_cor_fundo TEXT")
    add_column(cursor, "configuracao_empresa", "marca_cor_superficie TEXT")
    add_column(cursor, "configuracao_empresa", "marca_cor_texto TEXT")
    add_column(cursor, "configuracao_empresa", "marca_logo_blob BLOB")
    add_column(cursor, "configuracao_empresa", "marca_logo_mime_type TEXT")
    add_column(cursor, "configuracao_empresa", "marca_logo_arquivo_nome TEXT")

    cursor.execute(
        """
        UPDATE configuracao_empresa
        SET
            site_titulo = COALESCE(NULLIF(site_titulo, ''), 'Gestao Estetica'),
            site_rodape_texto = COALESCE(
                NULLIF(site_rodape_texto, ''),
                'Desenvolvido por Luiz Henrique | Qualquer Erro Contate o Desenvolvedor | Wagen Estetica Automotiva | Direitos Reservados.'
            ),
            marca_cor_fundo = COALESCE(NULLIF(marca_cor_fundo, ''), '#0b0b0b'),
            marca_cor_superficie = COALESCE(NULLIF(marca_cor_superficie, ''), '#111827'),
            marca_cor_texto = COALESCE(NULLIF(marca_cor_texto, ''), '#f9fafb')
        WHERE id = 1
        """
        )


def _sqlite_table_create_sql(cursor, tabela):
    cursor.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (tabela,),
    )
    row = cursor.fetchone()
    if not row:
        return ""
    if hasattr(row, "keys"):
        return str(row["sql"] or "")
    return str(row[0] or "")


def _has_singleton_id_check_sql(sql):
    return bool(re.search(r"CHECK\s*\(\s*\"?id\"?\s*=\s*1\s*\)", str(sql or ""), re.IGNORECASE))


def rebuild_sqlite_table_with_empresa_scope(cursor, tabela):
    if not _sqlite_table_exists(cursor, tabela):
        return

    sql_origem = _sqlite_table_create_sql(cursor, tabela)
    cursor.execute(f"PRAGMA table_info({_quote_ident(tabela)})")
    colunas = cursor.fetchall()
    if not colunas:
        return

    nomes_colunas = [coluna[1] for coluna in colunas]
    precisa_empresa_id = "empresa_id" not in nomes_colunas
    precisa_rebuild = precisa_empresa_id or _has_singleton_id_check_sql(sql_origem)
    if not precisa_rebuild:
        cursor.execute(
            f"UPDATE {_quote_ident(tabela)} SET empresa_id=1 WHERE empresa_id IS NULL"
        )
        cursor.execute(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_ident(f'idx_{tabela}_empresa_unique')} "
            f"ON {_quote_ident(tabela)}(empresa_id)"
        )
        return

    total_pk_colunas = sum(1 for coluna in colunas if int(coluna[5] or 0))
    foreign_keys = _sqlite_foreign_key_sqls(cursor, tabela)
    index_sqls = _sqlite_index_sqls(cursor, tabela)

    definicoes_colunas = []
    insert_colunas = []
    select_colunas = []
    empresa_inserida = False

    for coluna in colunas:
        nome_coluna = coluna[1]
        definicoes_colunas.append(_sqlite_column_sql(coluna, total_pk_colunas))
        insert_colunas.append(_quote_ident(nome_coluna))
        select_colunas.append(_quote_ident(nome_coluna))

        if nome_coluna == "id" and precisa_empresa_id:
            definicoes_colunas.append('"empresa_id" INTEGER NOT NULL DEFAULT 1')
            insert_colunas.append('"empresa_id"')
            select_colunas.append("1 AS empresa_id")
            empresa_inserida = True

    if precisa_empresa_id and not empresa_inserida:
        definicoes_colunas.append('"empresa_id" INTEGER NOT NULL DEFAULT 1')
        insert_colunas.append('"empresa_id"')
        select_colunas.append("1 AS empresa_id")

    tabela_legado = f"{tabela}__legacy_empresa_scope"
    definicoes = definicoes_colunas + foreign_keys
    sql_create = f"CREATE TABLE {_quote_ident(tabela)} ({', '.join(definicoes)})"
    insert_sql = ", ".join(insert_colunas)
    select_sql = ", ".join(select_colunas)

    cursor.execute("PRAGMA foreign_keys=OFF")
    cursor.execute(
        f"ALTER TABLE {_quote_ident(tabela)} RENAME TO {_quote_ident(tabela_legado)}"
    )
    cursor.execute(sql_create)
    cursor.execute(
        f"""
        INSERT INTO {_quote_ident(tabela)} ({insert_sql})
        SELECT {select_sql}
        FROM {_quote_ident(tabela_legado)}
        """
    )
    cursor.execute(f"DROP TABLE {_quote_ident(tabela_legado)}")

    for _, sql_indice in index_sqls:
        if not sql_indice:
            continue
        cursor.execute(sql_indice)

    cursor.execute(
        f"UPDATE {_quote_ident(tabela)} SET empresa_id=1 WHERE empresa_id IS NULL"
    )
    cursor.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_ident(f'idx_{tabela}_empresa_unique')} "
        f"ON {_quote_ident(tabela)}(empresa_id)"
    )
    cursor.execute("PRAGMA foreign_keys=ON")


def drop_postgres_singleton_id_checks(cursor, tabela):
    cursor.execute(
        """
        SELECT con.conname, pg_get_constraintdef(con.oid) AS definicao
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'public'
          AND rel.relname = ?
          AND con.contype = 'c'
        """,
        (tabela,),
    )
    for row in cursor.fetchall():
        nome = row[0] if not hasattr(row, "keys") else row["conname"]
        definicao = row[1] if not hasattr(row, "keys") else row["definicao"]
        if not re.search(r"\bid\s*=\s*1\b", str(definicao or ""), re.IGNORECASE):
            continue
        cursor.execute(
            f"ALTER TABLE {_quote_ident(tabela)} DROP CONSTRAINT IF EXISTS {_quote_ident(nome)}"
        )


def apply_admin_settings_scope(cursor, _add_column):
    backend = _backend_name(cursor)
    for tabela in _singleton_admin_tables():
        if not _table_exists(cursor, tabela):
            continue
        if backend == "postgres":
            cursor.execute(
                f"ALTER TABLE {_quote_ident(tabela)} ADD COLUMN IF NOT EXISTS empresa_id INTEGER DEFAULT 1"
            )
            cursor.execute(
                f"UPDATE {_quote_ident(tabela)} SET empresa_id=1 WHERE empresa_id IS NULL"
            )
            drop_postgres_singleton_id_checks(cursor, tabela)
            cursor.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {_quote_ident(f'idx_{tabela}_empresa_unique')} "
                f"ON {_quote_ident(tabela)}(empresa_id)"
            )
            continue

        rebuild_sqlite_table_with_empresa_scope(cursor, tabela)


def apply_empresa_indexes(cursor):
    def colunas_da_tabela(tabela):
        return _table_columns(cursor, tabela)

    def criar_indice_se_possivel(sql, tabela, *colunas):
        existentes = colunas_da_tabela(tabela)
        if all(coluna in existentes for coluna in colunas):
            cursor.execute(sql)

    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_clientes_empresa_telefone ON clientes(empresa_id, telefone)",
        "clientes",
        "empresa_id",
        "telefone",
    )
    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_veiculos_empresa_placa ON veiculos(empresa_id, placa)",
        "veiculos",
        "empresa_id",
        "placa",
    )
    criar_indice_se_possivel(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_veiculos_empresa_placa_unique ON veiculos(empresa_id, placa)",
        "veiculos",
        "empresa_id",
        "placa",
    )
    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_servicos_empresa_status_id ON servicos(empresa_id, status, id)",
        "servicos",
        "empresa_id",
        "status",
        "id",
    )
    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_servicos_empresa_veiculo ON servicos(empresa_id, veiculo_id)",
        "servicos",
        "empresa_id",
        "veiculo_id",
    )
    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_fotos_empresa_servico ON fotos(empresa_id, servico_id)",
        "fotos",
        "empresa_id",
        "servico_id",
    )
    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_retornos_empresa_placa ON retornos_clientes(empresa_id, placa)",
        "retornos_clientes",
        "empresa_id",
        "placa",
    )
    criar_indice_se_possivel(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_retornos_empresa_placa_unique ON retornos_clientes(empresa_id, placa)",
        "retornos_clientes",
        "empresa_id",
        "placa",
    )
    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_historico_sync_empresa_placa ON historico_lavagens_sync(empresa_id, placa)",
        "historico_lavagens_sync",
        "empresa_id",
        "placa",
    )
    criar_indice_se_possivel(
        "CREATE INDEX IF NOT EXISTS idx_sync_clientes_empresa_ativo_proximo ON sincronizacoes_clientes(empresa_id, ativo, proximo_sync_em)",
        "sincronizacoes_clientes",
        "empresa_id",
        "ativo",
        "proximo_sync_em",
    )


def _quote_ident(nome):
    return '"' + str(nome).replace('"', '""') + '"'


def _backend_name(cursor):
    return getattr(cursor, "backend", "") or "sqlite"


def _table_exists(cursor, tabela):
    backend = _backend_name(cursor)
    if backend == "postgres":
        cursor.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name = ?
            LIMIT 1
            """,
            (tabela,),
        )
        return cursor.fetchone() is not None
    cursor.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (tabela,),
    )
    return cursor.fetchone() is not None


def _table_columns(cursor, tabela):
    backend = _backend_name(cursor)
    if backend == "postgres":
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = ?
            ORDER BY ordinal_position
            """,
            (tabela,),
        )
        colunas = []
        for row in cursor.fetchall():
            if hasattr(row, "keys"):
                colunas.append(str(row["column_name"]))
            else:
                colunas.append(str(row[0]))
        return set(colunas)

    cursor.execute(f"PRAGMA table_info({_quote_ident(tabela)})")
    colunas = []
    for row in cursor.fetchall():
        if hasattr(row, "keys"):
            colunas.append(str(row["name"]))
        else:
            colunas.append(str(row[1]))
    return set(colunas)


def _sqlite_table_exists(cursor, tabela):
    return _table_exists(cursor, tabela)


def _sqlite_column_sql(coluna, total_pk_colunas):
    nome = coluna[1]
    tipo = (coluna[2] or "TEXT").strip()
    notnull = bool(coluna[3])
    default = coluna[4]
    pk_posicao = int(coluna[5] or 0)
    parts = [_quote_ident(nome)]

    if tipo:
        parts.append(tipo)

    if pk_posicao and total_pk_colunas == 1:
        parts.append("PRIMARY KEY")
    elif pk_posicao:
        parts.append("NOT NULL")
    elif notnull:
        parts.append("NOT NULL")

    if default is not None:
        parts.append(f"DEFAULT {default}")

    return " ".join(parts)


def _sqlite_foreign_key_sqls(cursor, tabela):
    cursor.execute(f"PRAGMA foreign_key_list({_quote_ident(tabela)})")
    rows = cursor.fetchall()
    agrupados = {}
    for row in rows:
        agrupados.setdefault(row[0], []).append(row)

    sqls = []
    for grupo in agrupados.values():
        grupo = sorted(grupo, key=lambda item: item[1])
        origem = ", ".join(_quote_ident(item[3]) for item in grupo)
        destino_tabela = _quote_ident(grupo[0][2])
        destino = ", ".join(_quote_ident(item[4]) for item in grupo)
        sql = f"FOREIGN KEY ({origem}) REFERENCES {destino_tabela} ({destino})"
        on_update = (grupo[0][5] or "").strip().upper()
        on_delete = (grupo[0][6] or "").strip().upper()
        if on_update and on_update != "NO ACTION":
            sql += f" ON UPDATE {on_update}"
        if on_delete and on_delete != "NO ACTION":
            sql += f" ON DELETE {on_delete}"
        sqls.append(sql)
    return sqls


def _sqlite_index_sqls(cursor, tabela):
    cursor.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type='index' AND tbl_name=? AND sql IS NOT NULL
        """,
        (tabela,),
    )
    return [(row[0], row[1]) for row in cursor.fetchall()]


def _is_global_unique_placa_sql(sql):
    texto = str(sql or "")
    if "UNIQUE" not in texto.upper():
        return False
    if "empresa_id" in texto.lower():
        return False
    return bool(re.search(r"\(\s*\"?placa\"?\s*\)", texto, re.IGNORECASE))


def rebuild_sqlite_table_without_global_unique_placa(cursor, tabela):
    if not _sqlite_table_exists(cursor, tabela):
        return

    cursor.execute(f"PRAGMA table_info({_quote_ident(tabela)})")
    colunas = cursor.fetchall()
    if not colunas:
        return

    nomes_colunas = [coluna[1] for coluna in colunas]
    total_pk_colunas = sum(1 for coluna in colunas if int(coluna[5] or 0))
    foreign_keys = _sqlite_foreign_key_sqls(cursor, tabela)
    index_sqls = _sqlite_index_sqls(cursor, tabela)

    tabela_legado = f"{tabela}__legacy_plate_scope"
    definicoes_colunas = [
        _sqlite_column_sql(coluna, total_pk_colunas)
        for coluna in colunas
    ]
    definicoes = definicoes_colunas + foreign_keys
    sql_create = f"CREATE TABLE {_quote_ident(tabela)} ({', '.join(definicoes)})"
    colunas_sql = ", ".join(_quote_ident(nome) for nome in nomes_colunas)

    cursor.execute("PRAGMA foreign_keys=OFF")
    cursor.execute(f"ALTER TABLE {_quote_ident(tabela)} RENAME TO {_quote_ident(tabela_legado)}")
    cursor.execute(sql_create)
    cursor.execute(
        f"""
        INSERT INTO {_quote_ident(tabela)} ({colunas_sql})
        SELECT {colunas_sql}
        FROM {_quote_ident(tabela_legado)}
        """
    )
    cursor.execute(f"DROP TABLE {_quote_ident(tabela_legado)}")

    for _, sql_indice in index_sqls:
        if _is_global_unique_placa_sql(sql_indice):
            continue
        cursor.execute(sql_indice)

    cursor.execute("PRAGMA foreign_keys=ON")


def drop_postgres_global_unique_placa(cursor, tabela):
    cursor.execute(
        """
        SELECT con.conname
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE nsp.nspname = 'public'
          AND rel.relname = ?
          AND con.contype = 'u'
          AND array_length(con.conkey, 1) = 1
          AND EXISTS (
              SELECT 1
              FROM unnest(con.conkey) AS key_col(attnum)
              JOIN pg_attribute attr
                ON attr.attrelid = rel.oid
               AND attr.attnum = key_col.attnum
              WHERE attr.attname = 'placa'
          )
        """,
        (tabela,),
    )
    for row in cursor.fetchall():
        nome = row[0] if not hasattr(row, "keys") else row["conname"]
        cursor.execute(
            f"ALTER TABLE {_quote_ident(tabela)} DROP CONSTRAINT IF EXISTS {_quote_ident(nome)}"
        )

    cursor.execute(
        """
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND tablename = ?
        """,
        (tabela,),
    )
    for row in cursor.fetchall():
        nome = row[0] if not hasattr(row, "keys") else row["indexname"]
        definicao = row[1] if not hasattr(row, "keys") else row["indexdef"]
        if not _is_global_unique_placa_sql(definicao):
            continue
        cursor.execute(f"DROP INDEX IF EXISTS {_quote_ident(nome)}")


def apply_legacy_plate_uniqueness_migration(cursor):
    backend = _backend_name(cursor)
    tabelas = ("veiculos", "retornos_clientes")

    if backend == "postgres":
        for tabela in tabelas:
            drop_postgres_global_unique_placa(cursor, tabela)
        return

    for tabela in tabelas:
        rebuild_sqlite_table_without_global_unique_placa(cursor, tabela)


def run_product_foundation_migrations(conn, add_column, now_iso, print_func=print):
    cursor = conn.cursor()
    ensure_schema_migrations(cursor)
    applied = applied_migration_names(cursor)

    if "foundation_enterprises" not in applied:
        create_enterprises_table(cursor)
        seed_default_enterprise(cursor, now_iso())
        mark_migration_applied(cursor, "foundation_enterprises", now_iso())

    if "foundation_licencas" not in applied:
        create_licencas_table(cursor)
        seed_default_license(cursor, now_iso())
        mark_migration_applied(cursor, "foundation_licencas", now_iso())

    if "foundation_telemetria" not in applied:
        create_telemetry_table(cursor)
        mark_migration_applied(cursor, "foundation_telemetria", now_iso())

    if "foundation_empresa_scope" not in applied:
        apply_empresa_scope(cursor, add_column)
        mark_migration_applied(cursor, "foundation_empresa_scope", now_iso())

    if "foundation_branding_storage" not in applied:
        apply_branding_and_storage(cursor, add_column)
        mark_migration_applied(cursor, "foundation_branding_storage", now_iso())

    if "foundation_site_customization" not in applied:
        apply_site_customization(cursor, add_column)
        mark_migration_applied(cursor, "foundation_site_customization", now_iso())

    if "foundation_empresa_scope_extended" not in applied:
        apply_extended_empresa_scope(cursor, add_column)
        mark_migration_applied(cursor, "foundation_empresa_scope_extended", now_iso())

    if "foundation_admin_settings_scope" not in applied:
        apply_admin_settings_scope(cursor, add_column)
        mark_migration_applied(cursor, "foundation_admin_settings_scope", now_iso())

    if "foundation_empresa_indexes" not in applied:
        apply_empresa_indexes(cursor)
        mark_migration_applied(cursor, "foundation_empresa_indexes", now_iso())

    if "foundation_legacy_plate_uniqueness" not in applied:
        apply_legacy_plate_uniqueness_migration(cursor)
        apply_empresa_indexes(cursor)
        mark_migration_applied(cursor, "foundation_legacy_plate_uniqueness", now_iso())

    conn.commit()
    print_func("FOUNDATION: migrations de produto aplicadas/verificadas.")


def build_brand_context(config_row=None, empresa_row=None):
    config = dict(config_row or {})
    empresa = dict(empresa_row or {})

    brand_name = (
        config.get("marca_nome")
        or empresa.get("nome_fantasia")
        or config.get("nome_fantasia")
        or "Wagen Estetica Automotiva"
    )
    brand_subtitle = (
        config.get("marca_subtitulo")
        or "Gestao Estetica"
    )
    logo_blob = bool(config.get("marca_logo_blob") or config.get("marca_logo_tem_blob"))
    site_title = (
        config.get("site_titulo")
        or config.get("marca_subtitulo")
        or "Gestao Estetica"
    )
    site_footer_text = (
        config.get("site_rodape_texto")
        or f"Desenvolvido por Luiz Henrique | Qualquer Erro Contate o Desenvolvedor | {brand_name} | Direitos Reservados."
    )
    brand_primary_color = config.get("marca_cor_primaria") or empresa.get("cor_primaria") or "#facc15"
    brand_secondary_color = config.get("marca_cor_secundaria") or empresa.get("cor_secundaria") or "#111827"
    brand_background_color = config.get("marca_cor_fundo") or "#0b0b0b"
    brand_surface_color = config.get("marca_cor_superficie") or brand_secondary_color or "#111827"
    brand_text_color = config.get("marca_cor_texto") or "#f9fafb"

    return {
        "brand_name": brand_name,
        "brand_subtitle": brand_subtitle,
        "brand_logo_url": "/branding/logo" if logo_blob else (config.get("marca_logo_url") or empresa.get("logo_url") or "/static/logo.jpg"),
        "brand_primary_color": brand_primary_color,
        "brand_secondary_color": brand_secondary_color,
        "brand_background_color": brand_background_color,
        "brand_surface_color": brand_surface_color,
        "brand_text_color": brand_text_color,
        "site_title": site_title,
        "site_footer_text": site_footer_text,
        "whitelabel_ativo": bool(int(config.get("whitelabel_ativo") or 0)),
        "storage_provider": config.get("storage_provider") or empresa.get("storage_provider") or "database",
        "licenca_plano": config.get("licenca_plano") or empresa.get("plano_codigo") or "starter",
        "licenca_status": config.get("licenca_status") or empresa.get("licenca_status") or "trial",
        "onboarding_concluido": bool(int(config.get("onboarding_concluido") or empresa.get("onboarding_concluido") or 0)),
    }

import { getDatabase } from "../database/db";
import { normalizeServerUrl } from "../config";
import { MobileConfigResult, MobileHudPayload, MobileHudResult, MobileSiteState, QueueRow, SyncConfig, SyncResult } from "./types";

const SYNC_BATCH_SIZE = 50;

type ServerChange = {
  entity?: string;
  entity_uuid?: string;
  action?: string;
  payload?: Record<string, unknown>;
};

export async function pendingSyncCount() {
  const db = await getDatabase();
  const row = await db.getFirstAsync<{ total: number }>(
    "SELECT COUNT(*) as total FROM sync_queue WHERE synced_at IS NULL"
  );
  return Number(row?.total || 0);
}

export async function runSync(config: SyncConfig): Promise<SyncResult> {
  const endpointUrl = normalizeServerUrl(config.endpointUrl);

  const db = await getDatabase();
  const queue = await db.getAllAsync<QueueRow>(
    `
    SELECT id, entity, entity_uuid, action, payload_json
    FROM sync_queue
    WHERE synced_at IS NULL
    ORDER BY id
    LIMIT ?
    `,
    SYNC_BATCH_SIZE
  );

  const payload = {
    changes: queue.map((item) => ({
      id: item.id,
      entity: item.entity,
      entity_uuid: item.entity_uuid,
      action: item.action,
      payload: JSON.parse(item.payload_json)
    }))
  };

  try {
    const response = await fetch(`${endpointUrl}/api/mobile/sync`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(config.token ? { Authorization: `Bearer ${config.token}` } : {})
      },
      body: JSON.stringify(payload)
    });

    if (response.status === 404) {
      throw new Error("O site ainda nao tem a API mobile publicada.");
    }

    if (!response.ok) {
      throw new Error(`Servidor retornou HTTP ${response.status}`);
    }

    const data = await response.json();
    const acceptedIds: number[] = Array.isArray(data.accepted_ids) ? data.accepted_ids : [];
    const serverChanges: ServerChange[] = Array.isArray(data.changes) ? data.changes : [];

    for (const id of acceptedIds) {
      await db.runAsync("UPDATE sync_queue SET synced_at = CURRENT_TIMESTAMP WHERE id = ?", id);
    }

    for (const change of serverChanges) {
      await applyServerChange(change);
    }

    return {
      sent: acceptedIds.length,
      pulled: serverChanges.length
    };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    for (const item of queue) {
      await db.runAsync(
        "UPDATE sync_queue SET attempts = attempts + 1, last_error = ? WHERE id = ?",
        message,
        item.id
      );
    }
    return { sent: 0, pulled: 0, error: message };
  }
}

function authHeaders(config: SyncConfig) {
  return {
    ...(config.token ? { Authorization: `Bearer ${config.token}` } : {})
  };
}

function normalizeVersionText(value: unknown) {
  return String(value || "").replace(/^Vers[aã]o:\s*/i, "").trim();
}

export async function fetchMobileHud(config: SyncConfig): Promise<MobileHudResult> {
  const endpointUrl = normalizeServerUrl(config.endpointUrl);
  try {
    let response = await fetch(`${endpointUrl}/api/mobile/site-state`, {
      headers: authHeaders(config)
    });
    if (response.status === 404) {
      response = await fetch(`${endpointUrl}/api/mobile/hud`, {
        headers: authHeaders(config)
      });
    }
    if (!response.ok) {
      throw new Error(`Servidor retornou HTTP ${response.status}`);
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error(String(data.erro || "Falha ao carregar HUD do site."));
    }
    const hud: MobileHudPayload = data.hud || {};
    const site: MobileSiteState = {
      clima: data.clima,
      hud,
      modulos: data.modulos,
      refresh_interval_seconds: data.refresh_interval_seconds,
      server_time: data.server_time,
      versao_sistema: data.versao_sistema
    };
    return {
      clima: data.clima,
      hud,
      site,
      version: normalizeVersionText(data.versao_sistema || hud.versao)
    };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

export async function fetchMobileConfig(config: SyncConfig): Promise<MobileConfigResult> {
  const endpointUrl = normalizeServerUrl(config.endpointUrl);
  try {
    const response = await fetch(`${endpointUrl}/api/mobile/configuracao`, {
      headers: authHeaders(config)
    });
    if (!response.ok) {
      throw new Error(`Servidor retornou HTTP ${response.status}`);
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error(String(data.erro || "Falha ao carregar configuracao do site."));
    }
    return { version: normalizeVersionText(data.versao_sistema || data.app_version) };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

export async function updateMobileVersion(config: SyncConfig, version: string): Promise<MobileConfigResult> {
  const endpointUrl = normalizeServerUrl(config.endpointUrl);
  try {
    const response = await fetch(`${endpointUrl}/api/mobile/configuracao`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...authHeaders(config)
      },
      body: JSON.stringify({ versao_sistema: version })
    });
    if (!response.ok) {
      throw new Error(`Servidor retornou HTTP ${response.status}`);
    }
    const data = await response.json();
    if (!data.ok) {
      throw new Error(String(data.erro || "Falha ao salvar versao no site."));
    }
    return { version: normalizeVersionText(data.versao_sistema || data.app_version) };
  } catch (error) {
    return { error: error instanceof Error ? error.message : String(error) };
  }
}

async function applyServerChange(change: ServerChange) {
  const entity = String(change.entity || "");
  const action = String(change.action || "upsert");
  const payload = change.payload || {};
  const uuid = String(payload.uuid || change.entity_uuid || "");
  if (!uuid || action === "delete") {
    return;
  }

  const db = await getDatabase();
  if (entity === "clientes") {
    await db.runAsync(
      `
      INSERT INTO clientes (uuid, nome, telefone, placa_principal, data_nascimento, updated_at, deleted_at)
      VALUES (?, ?, ?, ?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        nome=excluded.nome,
        telefone=excluded.telefone,
        placa_principal=excluded.placa_principal,
        data_nascimento=excluded.data_nascimento,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.nome || "Cliente"),
      String(payload.telefone || ""),
      String(payload.placa_principal || "").toUpperCase(),
      String(payload.data_nascimento || ""),
      String(payload.updated_at || new Date().toISOString())
    );
  }

  if (entity === "veiculos") {
    await db.runAsync(
      `
      INSERT INTO veiculos (
        uuid, cliente_uuid, placa, modelo, cor, status_atendimento,
        atendimento_ativo, updated_at, deleted_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        cliente_uuid=excluded.cliente_uuid,
        placa=excluded.placa,
        modelo=excluded.modelo,
        cor=excluded.cor,
        status_atendimento=excluded.status_atendimento,
        atendimento_ativo=excluded.atendimento_ativo,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.cliente_uuid || ""),
      String(payload.placa || "").toUpperCase(),
      String(payload.modelo || ""),
      String(payload.cor || ""),
      String(payload.status_atendimento || "SEM_ATENDIMENTO"),
      Number(payload.atendimento_ativo || 0),
      String(payload.updated_at || new Date().toISOString())
    );
  }

  if (entity === "tipos_servico") {
    await db.runAsync(
      `
      INSERT INTO tipos_servico (uuid, nome, valor, updated_at, deleted_at)
      VALUES (?, ?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        nome=excluded.nome,
        valor=excluded.valor,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.nome || "Servico"),
      Number(payload.valor || 0),
      String(payload.updated_at || new Date().toISOString())
    );
  }

  if (entity === "produtos_pneu") {
    await db.runAsync(
      `
      INSERT INTO produtos_pneu (uuid, nome, updated_at, deleted_at)
      VALUES (?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        nome=excluded.nome,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.nome || "Produto"),
      String(payload.updated_at || new Date().toISOString())
    );
  }

  if (entity === "checklist_itens") {
    await db.runAsync(
      `
      INSERT INTO checklist_itens (uuid, nome, ativo, ordem, updated_at, deleted_at)
      VALUES (?, ?, ?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        nome=excluded.nome,
        ativo=excluded.ativo,
        ordem=excluded.ordem,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.nome || "Item"),
      Number(payload.ativo ?? 1),
      Number(payload.ordem || 0),
      String(payload.updated_at || new Date().toISOString())
    );
  }

  if (entity === "adicionais") {
    await db.runAsync(
      `
      INSERT INTO adicionais (uuid, nome, updated_at, deleted_at)
      VALUES (?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        nome=excluded.nome,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.nome || "Adicional"),
      String(payload.updated_at || new Date().toISOString())
    );
  }

  if (entity === "servico_cobrancas_extras") {
    await db.runAsync(
      `
      INSERT INTO servico_cobrancas_extras (
        uuid, servico_uuid, descricao, valor, criado_em, criado_por_usuario,
        criado_por_nome, updated_at, deleted_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        servico_uuid=excluded.servico_uuid,
        descricao=excluded.descricao,
        valor=excluded.valor,
        criado_em=excluded.criado_em,
        criado_por_usuario=excluded.criado_por_usuario,
        criado_por_nome=excluded.criado_por_nome,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.servico_uuid || ""),
      String(payload.descricao || ""),
      Number(payload.valor || 0),
      String(payload.criado_em || payload.updated_at || ""),
      String(payload.criado_por_usuario || ""),
      String(payload.criado_por_nome || ""),
      String(payload.updated_at || payload.criado_em || new Date().toISOString())
    );
  }

  if (entity === "servicos") {
    await db.runAsync(
      `
      INSERT INTO servicos (
        uuid, veiculo_uuid, tipo_nome, valor, valor_adicional, status, observacoes,
        etapa_atual, entrada, entrega_prevista, entrega,
        fotos_entrada, fotos_detalhe, fotos_saida, updated_at, deleted_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        veiculo_uuid=excluded.veiculo_uuid,
        tipo_nome=excluded.tipo_nome,
        valor=excluded.valor,
        valor_adicional=excluded.valor_adicional,
        status=excluded.status,
        observacoes=excluded.observacoes,
        etapa_atual=excluded.etapa_atual,
        entrada=excluded.entrada,
        entrega_prevista=excluded.entrega_prevista,
        entrega=excluded.entrega,
        fotos_entrada=excluded.fotos_entrada,
        fotos_detalhe=excluded.fotos_detalhe,
        fotos_saida=excluded.fotos_saida,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      String(payload.veiculo_uuid || ""),
      String(payload.tipo_nome || "Servico"),
      Number(payload.valor || 0),
      Number(payload.valor_adicional || 0),
      String(payload.status || "ABERTO"),
      String(payload.observacoes || ""),
      String(payload.etapa_atual || "LAVAGEM"),
      String(payload.entrada || ""),
      String(payload.entrega_prevista || ""),
      String(payload.entrega || ""),
      Number(payload.fotos_entrada || 0),
      Number(payload.fotos_detalhe || 0),
      Number(payload.fotos_saida || 0),
      String(payload.updated_at || new Date().toISOString())
    );
  }

  if (entity === "fotos") {
    const servicoUuid = String(payload.servico_uuid || "");
    const tipo = ["entrada", "detalhe", "saida"].includes(String(payload.tipo)) ? String(payload.tipo) : "entrada";
    await db.runAsync(
      `
      INSERT INTO fotos (
        uuid, servico_uuid, tipo, uri_local, mime_type, usuario, usuario_nome,
        tamanho_bytes, largura, altura, created_at, updated_at, deleted_at
      )
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
      ON CONFLICT(uuid) DO UPDATE SET
        servico_uuid=excluded.servico_uuid,
        tipo=excluded.tipo,
        uri_local=excluded.uri_local,
        mime_type=excluded.mime_type,
        usuario=excluded.usuario,
        usuario_nome=excluded.usuario_nome,
        tamanho_bytes=excluded.tamanho_bytes,
        largura=excluded.largura,
        altura=excluded.altura,
        updated_at=excluded.updated_at,
        deleted_at=NULL
      `,
      uuid,
      servicoUuid,
      tipo,
      String(payload.uri_local || ""),
      String(payload.mime_type || "image/jpeg"),
      String(payload.usuario || ""),
      String(payload.usuario_nome || ""),
      Number(payload.tamanho_bytes || 0),
      Number(payload.largura || 0),
      Number(payload.altura || 0),
      String(payload.created_at || payload.updated_at || new Date().toISOString()),
      String(payload.updated_at || payload.created_at || new Date().toISOString())
    );
    if (servicoUuid) {
      await db.runAsync(
        `
        UPDATE servicos
        SET fotos_entrada=(SELECT COUNT(*) FROM fotos WHERE servico_uuid=? AND tipo='entrada' AND deleted_at IS NULL),
            fotos_detalhe=(SELECT COUNT(*) FROM fotos WHERE servico_uuid=? AND tipo='detalhe' AND deleted_at IS NULL),
            fotos_saida=(SELECT COUNT(*) FROM fotos WHERE servico_uuid=? AND tipo='saida' AND deleted_at IS NULL)
        WHERE uuid=?
        `,
        servicoUuid,
        servicoUuid,
        servicoUuid,
        servicoUuid
      );
    }
  }
}

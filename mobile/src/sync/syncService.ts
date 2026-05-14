import { getDatabase } from "../database/db";
import { normalizeServerUrl } from "../config";
import { QueueRow, SyncConfig, SyncResult } from "./types";

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
}

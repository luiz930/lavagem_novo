import { enqueueSync, getDatabase, newUuid } from "../database/db";

export type ClienteLocal = {
  uuid: string;
  nome: string;
  telefone?: string;
  placa_principal?: string;
  updated_at?: string;
};

export type ServicoLocal = {
  uuid: string;
  status?: string;
  observacoes?: string;
  etapa_atual?: string;
  entrada?: string;
  updated_at?: string;
};

export type BuscaPlacaResultado = {
  placa: string;
  modelo?: string;
  cor?: string;
  status_atendimento?: string;
  atendimento_ativo?: number;
  cliente_nome?: string;
  cliente_telefone?: string;
};

export async function listarClientes() {
  const db = await getDatabase();
  return db.getAllAsync<ClienteLocal>(
    `
    SELECT uuid, nome, telefone, placa_principal, updated_at
    FROM clientes
    WHERE deleted_at IS NULL
    ORDER BY updated_at DESC, nome ASC
    LIMIT 100
    `
  );
}

export async function buscarPorPlaca(placa: string) {
  const termo = placa.trim().toUpperCase();
  if (!termo) {
    return [];
  }
  const db = await getDatabase();
  return db.getAllAsync<BuscaPlacaResultado>(
    `
    SELECT
      v.placa,
      v.modelo,
      v.cor,
      v.status_atendimento,
      v.atendimento_ativo,
      c.nome AS cliente_nome,
      c.telefone AS cliente_telefone
    FROM veiculos v
    LEFT JOIN clientes c ON c.uuid = v.cliente_uuid
    WHERE v.deleted_at IS NULL
      AND UPPER(v.placa) LIKE ?
    ORDER BY v.updated_at DESC, v.placa ASC
    LIMIT 20
    `,
    `%${termo}%`
  );
}

export async function salvarCliente(dados: { nome: string; telefone?: string; placa_principal?: string }) {
  const db = await getDatabase();
  const uuid = newUuid();
  const payload = {
    uuid,
    nome: dados.nome.trim(),
    telefone: (dados.telefone || "").trim(),
    placa_principal: (dados.placa_principal || "").trim().toUpperCase(),
    updated_at: new Date().toISOString()
  };
  await db.runAsync(
    `
    INSERT INTO clientes (uuid, nome, telefone, placa_principal, updated_at)
    VALUES (?, ?, ?, ?, ?)
    `,
    payload.uuid,
    payload.nome,
    payload.telefone,
    payload.placa_principal,
    payload.updated_at
  );
  await enqueueSync("clientes", uuid, "upsert", payload);
}

export async function listarServicos() {
  const db = await getDatabase();
  return db.getAllAsync<ServicoLocal>(
    `
    SELECT uuid, status, observacoes, etapa_atual, entrada, updated_at
    FROM servicos
    WHERE deleted_at IS NULL
    ORDER BY updated_at DESC, entrada DESC
    LIMIT 100
    `
  );
}

export async function salvarServico(dados: { observacoes?: string; status?: string }) {
  const db = await getDatabase();
  const uuid = newUuid();
  const payload = {
    uuid,
    status: dados.status || "ABERTO",
    observacoes: (dados.observacoes || "").trim(),
    etapa_atual: "LAVAGEM",
    entrada: new Date().toISOString(),
    updated_at: new Date().toISOString()
  };
  await db.runAsync(
    `
    INSERT INTO servicos (uuid, status, observacoes, etapa_atual, entrada, updated_at)
    VALUES (?, ?, ?, ?, ?, ?)
    `,
    payload.uuid,
    payload.status,
    payload.observacoes,
    payload.etapa_atual,
    payload.entrada,
    payload.updated_at
  );
  await enqueueSync("servicos", uuid, "upsert", payload);
}

export async function resumoLocal() {
  const db = await getDatabase();
  const clientes = await db.getFirstAsync<{ total: number }>("SELECT COUNT(*) as total FROM clientes WHERE deleted_at IS NULL");
  const servicos = await db.getFirstAsync<{ total: number }>("SELECT COUNT(*) as total FROM servicos WHERE deleted_at IS NULL");
  const fotos = await db.getFirstAsync<{ total: number }>("SELECT COUNT(*) as total FROM fotos WHERE deleted_at IS NULL");
  const pendencias = await db.getFirstAsync<{ total: number }>("SELECT COUNT(*) as total FROM sync_queue WHERE synced_at IS NULL");
  return {
    clientes: Number(clientes?.total || 0),
    servicos: Number(servicos?.total || 0),
    fotos: Number(fotos?.total || 0),
    pendencias: Number(pendencias?.total || 0)
  };
}

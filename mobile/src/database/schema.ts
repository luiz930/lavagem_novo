export const schemaSql = `
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS usuarios (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  usuario TEXT UNIQUE NOT NULL,
  senha TEXT NOT NULL,
  nome TEXT,
  perfil TEXT,
  ativo INTEGER DEFAULT 1,
  criado_em TEXT,
  tentativas_login INTEGER DEFAULT 0,
  bloqueado_ate TEXT,
  ultimo_login_em TEXT,
  senha_alteracao_obrigatoria INTEGER DEFAULT 0,
  senha_atualizada_em TEXT,
  foto_perfil TEXT,
  hud_config_json TEXT,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS clientes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  nome TEXT NOT NULL,
  telefone TEXT,
  placa_principal TEXT,
  data_nascimento TEXT,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS veiculos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  cliente_uuid TEXT,
  placa TEXT NOT NULL,
  modelo TEXT,
  cor TEXT,
  status_atendimento TEXT DEFAULT 'SEM_ATENDIMENTO',
  atendimento_ativo INTEGER DEFAULT 0,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS servicos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  veiculo_uuid TEXT,
  tipo_nome TEXT,
  valor REAL,
  valor_adicional REAL DEFAULT 0,
  entrada TEXT,
  entrega_prevista TEXT,
  entrega TEXT,
  status TEXT,
  observacoes TEXT,
  etapa_atual TEXT DEFAULT 'LAVAGEM',
  criado_por_usuario TEXT,
  criado_por_nome TEXT,
  fotos_entrada INTEGER DEFAULT 0,
  fotos_detalhe INTEGER DEFAULT 0,
  fotos_saida INTEGER DEFAULT 0,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS tipos_servico (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  nome TEXT NOT NULL,
  valor REAL DEFAULT 0,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS produtos_pneu (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  nome TEXT NOT NULL,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS checklist_itens (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  nome TEXT NOT NULL,
  ativo INTEGER DEFAULT 1,
  ordem INTEGER DEFAULT 0,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS adicionais (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  nome TEXT NOT NULL,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS servico_cobrancas_extras (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  servico_uuid TEXT,
  descricao TEXT NOT NULL,
  valor REAL DEFAULT 0,
  criado_em TEXT,
  criado_por_usuario TEXT,
  criado_por_nome TEXT,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS fotos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  uuid TEXT UNIQUE NOT NULL,
  servico_uuid TEXT,
  tipo TEXT,
  uri_local TEXT NOT NULL,
  mime_type TEXT DEFAULT 'image/jpeg',
  usuario TEXT,
  usuario_nome TEXT,
  tamanho_bytes INTEGER,
  largura INTEGER,
  altura INTEGER,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  uploaded_at TEXT,
  upload_attempts INTEGER DEFAULT 0,
  upload_last_error TEXT,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS sync_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entity TEXT NOT NULL,
  entity_uuid TEXT NOT NULL,
  action TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  synced_at TEXT,
  attempts INTEGER DEFAULT 0,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS sync_state (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_queue_pending ON sync_queue(synced_at, id);
CREATE INDEX IF NOT EXISTS idx_sync_queue_entity_pending ON sync_queue(entity, synced_at);
CREATE INDEX IF NOT EXISTS idx_veiculos_placa ON veiculos(placa);
CREATE INDEX IF NOT EXISTS idx_veiculos_updated ON veiculos(updated_at);
CREATE INDEX IF NOT EXISTS idx_veiculos_deleted ON veiculos(deleted_at);
CREATE INDEX IF NOT EXISTS idx_servicos_updated ON servicos(updated_at);
CREATE INDEX IF NOT EXISTS idx_servicos_deleted ON servicos(deleted_at);
CREATE INDEX IF NOT EXISTS idx_servicos_veiculo ON servicos(veiculo_uuid, deleted_at);
CREATE INDEX IF NOT EXISTS idx_fotos_servico ON fotos(servico_uuid, deleted_at);
CREATE INDEX IF NOT EXISTS idx_fotos_upload_pending ON fotos(uploaded_at, deleted_at);
CREATE INDEX IF NOT EXISTS idx_clientes_updated ON clientes(updated_at);
CREATE INDEX IF NOT EXISTS idx_clientes_deleted ON clientes(deleted_at);
`;

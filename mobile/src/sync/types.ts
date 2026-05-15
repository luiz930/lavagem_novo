export type SyncConfig = {
  endpointUrl: string;
  token?: string;
};

export type SyncResult = {
  sent: number;
  pulled: number;
  photosUploaded?: number;
  durationMs?: number;
  serverCursor?: string;
  nextRetrySeconds?: number;
  error?: string;
};

export type SyncDiagnostics = {
  pending: number;
  pendingPhotos: number;
  lastSuccessAt: string;
  lastErrorAt: string;
  lastError: string;
  lastDurationMs: number;
  lastSent: number;
  lastPulled: number;
  lastPhotosUploaded: number;
  lastPullAt: string;
};

export type MobileHudPayload = Record<string, unknown> & {
  andamento?: number;
  atrasados?: number;
  banco_online_mensagem?: string;
  banco_online_resumo?: string;
  clientes_mes?: number;
  entregas_hoje?: number;
  faturamento_mes?: number;
  servicos_ativos?: number;
  sync_bancos_mensagem?: string;
  sync_bancos_pendentes?: number;
  sync_bancos_resumo?: string;
  ticket?: number;
  total?: number;
  usuario_nome?: string;
  versao?: string;
};

export type MobileWeatherPayload = {
  clima?: string;
  temp?: string | number;
  icone?: string;
  sugestao?: string;
};

export type MobileModuleCounter = {
  label: string;
  value: string | number;
  icon?: string;
};

export type MobileModuleRow = {
  title?: string;
  detail?: string;
  badge?: string;
  tabela?: string;
  chave?: string;
  acao?: string;
  direcao?: string;
  criado_em?: string;
};

export type MobileModulePayload = {
  counters?: MobileModuleCounter[];
  rows?: MobileModuleRow[];
};

export type MobileSiteState = {
  clima?: MobileWeatherPayload;
  hud?: MobileHudPayload;
  modulos?: Record<string, MobileModulePayload>;
  refresh_interval_seconds?: number;
  server_time?: string;
  versao_sistema?: string;
};

export type MobileConfigResult = {
  version?: string;
  error?: string;
};

export type MobileHudResult = MobileConfigResult & {
  clima?: MobileWeatherPayload;
  hud?: MobileHudPayload;
  site?: MobileSiteState;
};

export type QueueRow = {
  id: number;
  entity: string;
  entity_uuid: string;
  action: string;
  payload_json: string;
};

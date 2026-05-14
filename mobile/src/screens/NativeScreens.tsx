import { ReactNode, useEffect, useMemo, useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { Ionicons } from "@expo/vector-icons";

import { BuscaPlacaResultado, buscarPorPlaca, ClienteLocal, listarClientes, listarServicos, resumoLocal, salvarCliente, salvarServico, ServicoLocal } from "../data/localRepository";
import { colors, spacing } from "../theme";
import { AppScreenKey } from "./AppShell";

type Props = {
  screen: AppScreenKey;
  onOpenCamera: () => void;
  onRefreshPending: () => void;
  sync: {
    pending: number;
    message: string;
    endpointUrl: string;
    onSyncNow: () => void;
  };
};

const titles: Record<AppScreenKey, string> = {
  inicio: "Inicio",
  painel: "Painel",
  clientes: "Clientes",
  historico: "Historico",
  retornos: "Retornos",
  servicos: "Servicos",
  checklist: "Itens Checklist",
  pneus: "Pneus",
  financeiro: "Relatorios",
  orcamentos: "Orcamentos",
  notaFiscal: "Nota fiscal",
  clima: "Clima",
  auditoria: "Auditoria",
  changelog: "Changelog",
  empresas: "Empresas",
  diagnostico: "Diagnostico",
  status: "Status do sistema",
  autoSuporte: "AutoSuporte",
  configSite: "Configuracoes do site",
  configuracoes: "Configuracoes"
};

export function screenTitle(screen: AppScreenKey) {
  return titles[screen];
}

export function NativeScreenContent({ screen, onOpenCamera, onRefreshPending, sync }: Props) {
  if (screen === "inicio") {
    return <InicioPainel onOpenCamera={onOpenCamera} />;
  }
  if (screen === "painel") {
    return <PainelScreen onOpenCamera={onOpenCamera} />;
  }
  if (screen === "clientes") {
    return <ClientesScreen onSaved={onRefreshPending} />;
  }
  if (screen === "servicos") {
    return <ServicosScreen onSaved={onRefreshPending} onOpenCamera={onOpenCamera} />;
  }
  return <ModuleScreen screen={screen} sync={sync} />;
}

function InicioPainel({ onOpenCamera }: { onOpenCamera: () => void }) {
  const [resumo, setResumo] = useState({ clientes: 0, servicos: 0, fotos: 0, pendencias: 0 });
  const [placa, setPlaca] = useState("");
  const [resultados, setResultados] = useState<BuscaPlacaResultado[]>([]);

  useEffect(() => {
    resumoLocal().then(setResumo);
  }, []);

  async function pesquisarPlaca(texto = placa) {
    setResultados(await buscarPorPlaca(texto));
  }

  return (
    <>
      <View style={styles.searchHero}>
        <Text style={styles.pill}>Busca rapida</Text>
        <Text style={styles.heroTitle}>Buscar por placa</Text>
        <Text style={styles.muted}>Consulta os clientes e veiculos sincronizados do site no banco local do app.</Text>
        <View style={styles.searchRow}>
          <TextInput
            autoCapitalize="characters"
            onChangeText={(value) => {
              setPlaca(value);
              if (value.trim().length >= 2) {
                pesquisarPlaca(value);
              } else {
                setResultados([]);
              }
            }}
            placeholder="Digite a placa"
            placeholderTextColor={colors.muted}
            style={[styles.input, styles.searchInput]}
            value={placa}
          />
          <Pressable onPress={() => pesquisarPlaca()} style={styles.searchButton}>
            <Ionicons color="#111827" name="search" size={22} />
          </Pressable>
        </View>
        {resultados.length === 0 ? (
          <Text style={styles.muted}>Sincronize em Configuracoes para baixar os dados do site.</Text>
        ) : (
          <View style={styles.resultList}>
            {resultados.map((item) => (
              <View key={`${item.placa}-${item.cliente_nome}`} style={styles.resultItem}>
                <View style={styles.itemRow}>
                  <Text style={styles.itemTitle}>{item.placa}</Text>
                  <Text style={styles.badge}>{item.atendimento_ativo ? "Em atendimento" : "Cliente"}</Text>
                </View>
                <Text style={styles.muted}>{item.cliente_nome || "Cliente sem nome"} | {item.cliente_telefone || "Sem telefone"}</Text>
                <Text style={styles.muted}>{item.modelo || "Modelo nao informado"} {item.cor ? `/${item.cor}` : ""}</Text>
              </View>
            ))}
          </View>
        )}
      </View>
      <View style={styles.grid}>
        <Metric label="Clientes" value={resumo.clientes} icon="person" />
        <Metric label="Servicos" value={resumo.servicos} icon="water" />
        <Metric label="Fotos" value={resumo.fotos} icon="camera" />
        <Metric label="Pendencias" value={resumo.pendencias} icon="sync" />
      </View>
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Operacao</Text>
        <Text style={styles.muted}>Atalhos principais do mesmo fluxo operacional do site.</Text>
        <Pressable onPress={onOpenCamera} style={styles.primaryButton}>
          <Ionicons color="#111827" name="camera" size={22} />
          <Text style={styles.primaryButtonText}>Registrar foto</Text>
        </Pressable>
      </View>
    </>
  );
}

function PainelScreen({ onOpenCamera }: { onOpenCamera: () => void }) {
  return (
    <>
      <View style={styles.toolbarCard}>
        <View>
          <Text style={styles.pill}>Painel operacional</Text>
          <Text style={styles.cardTitle}>Controle dos servicos em andamento</Text>
          <Text style={styles.muted}>Fluxo separado por lavagem, finalizacao, prioridade e fotos.</Text>
        </View>
        <Pressable onPress={onOpenCamera} style={styles.cameraFab}>
          <Ionicons color="#111827" name="camera" size={22} />
        </Pressable>
      </View>
      <View style={styles.grid}>
        <Metric label="Em atendimento" value={0} icon="car" />
        <Metric label="Lavagem" value={0} icon="water" />
        <Metric label="Finalizacao" value={0} icon="checkmark-done" />
        <Metric label="Atrasados" value={0} icon="alert-circle" />
      </View>
      {["Etapa de Lavagem", "Etapa de Finalizacao"].map((title) => (
        <View key={title} style={styles.stageCard}>
          <View style={styles.sectionHeader}>
            <View>
              <Text style={styles.pill}>{title}</Text>
              <Text style={styles.muted}>Nenhum atendimento sincronizado nesta etapa.</Text>
            </View>
            <Ionicons color={colors.primary} name="timer" size={24} />
          </View>
          <View style={styles.servicePreview}>
            <Text style={styles.itemTitle}>Fila vazia</Text>
            <Text style={styles.muted}>Ao abrir servicos no app, eles aparecem aqui com cliente, veiculo, etapa e fotos.</Text>
          </View>
        </View>
      ))}
    </>
  );
}

function ClientesScreen({ onSaved }: { onSaved: () => void }) {
  const [clientes, setClientes] = useState<ClienteLocal[]>([]);
  const [nome, setNome] = useState("");
  const [telefone, setTelefone] = useState("");
  const [placa, setPlaca] = useState("");

  async function refresh() {
    setClientes(await listarClientes());
  }

  useEffect(() => {
    refresh();
  }, []);

  async function submit() {
    if (!nome.trim()) {
      return;
    }
    await salvarCliente({ nome, telefone, placa_principal: placa });
    setNome("");
    setTelefone("");
    setPlaca("");
    await refresh();
    onSaved();
  }

  return (
    <>
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Cadastrar cliente</Text>
        <TextInput value={nome} onChangeText={setNome} placeholder="Nome" placeholderTextColor={colors.muted} style={styles.input} />
        <TextInput value={telefone} onChangeText={setTelefone} placeholder="Telefone" placeholderTextColor={colors.muted} style={styles.input} keyboardType="phone-pad" />
        <TextInput value={placa} onChangeText={setPlaca} placeholder="Placa principal" placeholderTextColor={colors.muted} style={styles.input} autoCapitalize="characters" />
        <Pressable onPress={submit} style={styles.primaryButton}>
          <Text style={styles.primaryButtonText}>Salvar cliente</Text>
        </Pressable>
      </View>
      <ListCard title="Clientes cadastrados" empty="Nenhum cliente offline ainda.">
        {clientes.map((item) => (
          <View key={item.uuid} style={styles.listItem}>
            <Text style={styles.itemTitle}>{item.nome}</Text>
            <Text style={styles.muted}>{item.placa_principal || "-"} | {item.telefone || "-"}</Text>
          </View>
        ))}
      </ListCard>
    </>
  );
}

function ServicosScreen({ onSaved, onOpenCamera }: { onSaved: () => void; onOpenCamera: () => void }) {
  const [servicos, setServicos] = useState<ServicoLocal[]>([]);
  const [observacoes, setObservacoes] = useState("");

  async function refresh() {
    setServicos(await listarServicos());
  }

  useEffect(() => {
    refresh();
  }, []);

  async function submit() {
    await salvarServico({ observacoes });
    setObservacoes("");
    await refresh();
    onSaved();
  }

  return (
    <>
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Novo servico</Text>
        <TextInput value={observacoes} onChangeText={setObservacoes} placeholder="Observacoes" placeholderTextColor={colors.muted} style={styles.input} />
        <View style={styles.row}>
          <Pressable onPress={submit} style={styles.primaryButton}>
            <Text style={styles.primaryButtonText}>Abrir servico</Text>
          </Pressable>
          <Pressable onPress={onOpenCamera} style={styles.secondaryButton}>
            <Ionicons color={colors.text} name="camera" size={20} />
          </Pressable>
        </View>
      </View>
      <ListCard title="Servicos locais" empty="Nenhum servico offline ainda.">
        {servicos.map((item) => (
          <View key={item.uuid} style={styles.listItem}>
            <Text style={styles.itemTitle}>{item.status || "ABERTO"} | {item.etapa_atual || "LAVAGEM"}</Text>
            <Text style={styles.muted}>{item.observacoes || "Sem observacoes"}</Text>
          </View>
        ))}
      </ListCard>
    </>
  );
}

function ModuleScreen({ screen, sync }: { screen: AppScreenKey; sync: Props["sync"] }) {
  const config = useMemo(() => moduleConfig(screen), [screen]);
  return (
    <>
      <View style={styles.pageHeaderCard}>
        <View>
          <Text style={styles.pill}>{config.kicker}</Text>
          <Text style={styles.cardTitle}>{screenTitle(screen)}</Text>
          <Text style={styles.muted}>{config.subtitle}</Text>
        </View>
        <Ionicons color={colors.primary} name={config.icon} size={28} />
      </View>

      {config.variant === "calendar" && <CalendarStrip />}
      {config.variant === "finance" && <FinanceSummary />}
      {config.variant === "diagnostic" && <DiagnosticSummary sync={sync} />}

      <View style={styles.card}>
        <View style={styles.filterGrid}>
          {config.filters.map((item) => (
            <View key={item} style={styles.fakeInput}>
              <Text style={styles.fakeInputText}>{item}</Text>
            </View>
          ))}
        </View>
        <View style={styles.rowWrap}>
          {config.actions.map((item, index) => (
            <Pressable key={item} style={index === 0 ? styles.primaryButtonCompact : styles.secondaryButtonWide}>
              <Text style={index === 0 ? styles.primaryButtonText : styles.secondaryButtonText}>{item}</Text>
            </Pressable>
          ))}
        </View>
      </View>

      {config.sections.map((section) => (
        <View key={section.title} style={config.variant === "timeline" ? styles.timelineCard : styles.card}>
          <View style={styles.sectionHeader}>
            <View>
              <Text style={styles.cardTitle}>{section.title}</Text>
              <Text style={styles.muted}>{section.subtitle}</Text>
            </View>
            <Ionicons color={colors.primary} name={section.icon} size={22} />
          </View>
          {section.rows.map((item) => (
            <View key={item.title} style={config.variant === "table" ? styles.tableRow : styles.listItem}>
              <View style={styles.itemRow}>
                <Text style={styles.itemTitle}>{item.title}</Text>
                <Text style={styles.badge}>{item.badge}</Text>
              </View>
              <Text style={styles.muted}>{item.detail}</Text>
            </View>
          ))}
        </View>
      ))}

      {(screen === "configuracoes" || screen === "status") && <SyncPanel sync={sync} />}
    </>
  );
}

type SectionConfig = {
  title: string;
  subtitle: string;
  icon: keyof typeof Ionicons.glyphMap;
  rows: { title: string; detail: string; badge: string }[];
};

type ModuleConfig = {
  kicker: string;
  subtitle: string;
  icon: keyof typeof Ionicons.glyphMap;
  variant?: "cards" | "timeline" | "calendar" | "table" | "finance" | "diagnostic";
  filters: string[];
  actions: string[];
  sections: SectionConfig[];
};

function moduleConfig(screen: AppScreenKey): ModuleConfig {
  const defaults: Record<AppScreenKey, ModuleConfig> = {
    inicio: baseModule("Inicio"),
    painel: baseModule("Painel"),
    clientes: baseModule("Clientes"),
    servicos: baseModule("Servicos"),
    historico: {
      kicker: "Historico",
      subtitle: "Busca e linha do tempo dos atendimentos",
      icon: "time",
      variant: "timeline",
      filters: ["Buscar placa ou cliente", "Periodo", "Status"],
      actions: ["Filtrar", "Exportar"],
      sections: [
        section("Atendimentos", "Linha do tempo operacional", "time", [
          row("Ultimos servicos", "Cliente, veiculo, etapa e fotos vinculadas.", "Hoje"),
          row("Finalizados", "Registros com entrega e responsavel.", "OK")
        ]),
        section("Galeria", "Fotos por atendimento", "images", [
          row("Antes e depois", "Imagens salvas no app e sincronizadas com o site.", "Fotos"),
          row("Vistoria", "Evidencias anexadas ao servico.", "Check")
        ])
      ]
    },
    retornos: {
      kicker: "Agenda",
      subtitle: "Retornos por data, cliente e responsavel",
      icon: "calendar",
      variant: "calendar",
      filters: ["Data", "Cliente", "Responsavel"],
      actions: ["Agendar", "Confirmar"],
      sections: [
        section("Agenda", "Retornos por dia", "calendar", [
          row("Pendentes", "Clientes aguardando contato.", "Aberto"),
          row("Confirmados", "Retornos com horario e responsavel.", "OK")
        ])
      ]
    },
    checklist: {
      kicker: "Finalizacao",
      subtitle: "Itens de conferencia usados no atendimento",
      icon: "checkbox",
      variant: "table",
      filters: ["Item", "Categoria", "Ativo"],
      actions: ["Novo item", "Ordenar"],
      sections: [
        section("Checklist", "Itens de finalizacao", "checkbox", [
          row("Lavagem", "Marcacoes usadas durante o servico.", "Ativo"),
          row("Finalizacao", "Conferencia antes da entrega.", "Ativo")
        ])
      ]
    },
    pneus: {
      kicker: "Produtos",
      subtitle: "Pneus e aplicacoes vinculadas aos servicos",
      icon: "construct",
      variant: "table",
      filters: ["Produto", "Estoque", "Aplicacao"],
      actions: ["Cadastrar", "Baixa"],
      sections: [
        section("Pneus", "Controle usado nos servicos", "construct", [
          row("Produtos", "Cadastro e disponibilidade.", "Estoque"),
          row("Aplicacoes", "Historico por atendimento.", "Uso")
        ])
      ]
    },
    financeiro: {
      kicker: "Relatorios",
      subtitle: "Resumo financeiro e filtros do periodo",
      icon: "cash",
      variant: "finance",
      filters: ["Periodo", "Tipo", "Forma"],
      actions: ["Atualizar", "Relatorio"],
      sections: [
        section("Resumo", "Entradas, extras e totais", "cash", [
          row("Receitas", "Servicos e cobrancas adicionais.", "R$"),
          row("Fechamento", "Visao consolidada do periodo.", "Total")
        ])
      ]
    },
    orcamentos: {
      kicker: "Comercial",
      subtitle: "Orcamentos, itens, totais e envio",
      icon: "document-text",
      variant: "cards",
      filters: ["Cliente", "Validade", "Status"],
      actions: ["Novo", "Enviar"],
      sections: [
        section("Orcamentos", "Propostas e itens", "document-text", [
          row("Rascunhos", "Itens aguardando fechamento.", "Aberto"),
          row("Aprovados", "Orcamentos convertidos em servico.", "OK")
        ])
      ]
    },
    notaFiscal: {
      kicker: "Fiscal",
      subtitle: "Emissao, consulta e status das notas",
      icon: "receipt",
      variant: "table",
      filters: ["Periodo", "Cliente", "Status"],
      actions: ["Emitir", "Consultar"],
      sections: [
        section("Notas", "Emissao e retorno fiscal", "receipt", [
          row("Fila de emissao", "Notas aguardando processamento.", "Fila"),
          row("Autorizadas", "Documentos emitidos.", "OK")
        ])
      ]
    },
    clima: {
      kicker: "Operacao",
      subtitle: "Clima atual e previsao para planejar atendimentos",
      icon: "cloud",
      variant: "cards",
      filters: ["Cidade", "Periodo", "Alerta"],
      actions: ["Atualizar", "Ver previsao"],
      sections: [
        section("Clima", "Condicao operacional", "cloud", [
          row("Agora", "Condicao atual usada no planejamento.", "Atual"),
          row("Previsao", "Janela dos proximos atendimentos.", "7 dias")
        ])
      ]
    },
    auditoria: {
      kicker: "Seguranca",
      subtitle: "Usuarios, acoes e detalhes de auditoria",
      icon: "search",
      variant: "timeline",
      filters: ["Usuario", "Acao", "Periodo"],
      actions: ["Buscar", "Detalhes"],
      sections: [
        section("Auditoria", "Eventos do sistema", "search", [
          row("Acessos", "Entradas e operacoes sensiveis.", "Log"),
          row("Alteracoes", "Mudancas feitas em registros.", "Dados")
        ])
      ]
    },
    changelog: {
      kicker: "Versoes",
      subtitle: "Historico de mudancas publicadas",
      icon: "list",
      variant: "timeline",
      filters: ["Versao", "Tipo", "Data"],
      actions: ["Atualizar", "Detalhar"],
      sections: [
        section("Versoes", "Mudancas publicadas", "list", [
          row("App mobile", "Atualizacoes do aplicativo.", "0.1"),
          row("Site", "Historico do sistema web.", "1.0")
        ])
      ]
    },
    empresas: {
      kicker: "Licenca",
      subtitle: "Empresa ativa, plano, limites e status",
      icon: "business",
      variant: "cards",
      filters: ["Empresa", "Plano", "Status"],
      actions: ["Salvar", "Licenca"],
      sections: [
        section("Empresas", "Dados e licenca", "business", [
          row("Empresa ativa", "Identidade usada no sistema.", "Ativa"),
          row("Limites", "Plano, usuarios e recursos.", "Plano")
        ])
      ]
    },
    diagnostico: {
      kicker: "Diagnostico",
      subtitle: "Checklist tecnico do sistema",
      icon: "medkit",
      variant: "diagnostic",
      filters: ["Componente", "Resultado", "Data"],
      actions: ["Verificar", "Corrigir"],
      sections: [
        section("Diagnostico", "Checklist tecnico", "medkit", [
          row("Banco", "Conexao, schema e integridade.", "Check"),
          row("Backup", "Estado das rotinas de copia.", "Check")
        ])
      ]
    },
    status: {
      kicker: "Status",
      subtitle: "Saude do app, site e fila de sincronizacao",
      icon: "pulse",
      variant: "diagnostic",
      filters: ["Servico", "Severidade", "Periodo"],
      actions: ["Atualizar", "Logs"],
      sections: [
        section("Status", "Saude do sistema", "pulse", [
          row("API do site", "Resposta do backend conectado.", "Online"),
          row("Fila mobile", "Pendencias locais para envio.", "Sync")
        ])
      ]
    },
    autoSuporte: {
      kicker: "Suporte",
      subtitle: "Acoes tecnicas e resultados protegidos",
      icon: "hardware-chip",
      variant: "diagnostic",
      filters: ["Acao", "Permissao", "Resultado"],
      actions: ["Executar", "Confirmar"],
      sections: [
        section("AutoSuporte", "Acoes tecnicas", "hardware-chip", [
          row("Validacoes", "Checks de ambiente e banco.", "Admin"),
          row("Manutencao", "Acoes protegidas por permissao.", "Seguro")
        ])
      ]
    },
    configSite: {
      kicker: "Aparencia",
      subtitle: "Identidade, tema e menu do sistema",
      icon: "color-palette",
      variant: "cards",
      filters: ["Marca", "Menu", "Tema"],
      actions: ["Salvar", "Preview"],
      sections: [
        section("Identidade", "Marca e aparencia", "color-palette", [
          row("Cores", "Tema escuro com destaque dourado.", "Tema"),
          row("Menu", "Ordem e visibilidade dos modulos.", "Site")
        ])
      ]
    },
    configuracoes: {
      kicker: "Sistema",
      subtitle: "Conta, banco local, backup e sincronizacao",
      icon: "settings",
      variant: "diagnostic",
      filters: ["Conta", "Banco", "Backup"],
      actions: ["Salvar", "Sincronizar"],
      sections: [
        section("Configuracoes", "Preferencias e integracoes", "settings", [
          row("Meu acesso", "Dados do usuario logado.", "Conta"),
          row("Banco", "App local conectado ao site.", "Sync")
        ])
      ]
    }
  };
  return defaults[screen];
}

function baseModule(name: string): ModuleConfig {
  return {
    kicker: name,
    subtitle: "Resumo do modulo",
    icon: "apps",
    variant: "cards",
    filters: ["Busca", "Periodo", "Status"],
    actions: ["Atualizar", "Novo"],
    sections: [section(name, "Resumo do modulo", "apps", [row("Registros", "Dados locais e sincronizados.", "Local")])]
  };
}

function CalendarStrip() {
  return (
    <View style={styles.calendarStrip}>
      {["Hoje", "Amanha", "Semana"].map((label, index) => (
        <View key={label} style={[styles.calendarDay, index === 0 && styles.calendarDayActive]}>
          <Text style={[styles.calendarDayText, index === 0 && styles.calendarDayTextActive]}>{label}</Text>
          <Text style={[styles.muted, index === 0 && styles.calendarDayTextActive]}>0 retorno(s)</Text>
        </View>
      ))}
    </View>
  );
}

function FinanceSummary() {
  return (
    <View style={styles.financeBand}>
      <Metric label="Receitas" value={0} icon="trending-up" />
      <Metric label="Extras" value={0} icon="add-circle" />
      <Metric label="Fechamento" value={0} icon="calculator" />
    </View>
  );
}

function DiagnosticSummary({ sync }: { sync: Props["sync"] }) {
  return (
    <View style={styles.diagnosticGrid}>
      <View style={styles.diagnosticItem}>
        <Ionicons color={colors.success} name="checkmark-circle" size={22} />
        <Text style={styles.itemTitle}>App nativo</Text>
        <Text style={styles.muted}>React Native com banco local.</Text>
      </View>
      <View style={styles.diagnosticItem}>
        <Ionicons color={sync.pending > 0 ? colors.primary : colors.success} name="sync" size={22} />
        <Text style={styles.itemTitle}>{sync.pending} pendencia(s)</Text>
        <Text style={styles.muted}>{sync.message}</Text>
      </View>
    </View>
  );
}

function SyncPanel({ sync }: { sync: Props["sync"] }) {
  return (
    <View style={styles.card}>
      <View style={styles.sectionHeader}>
        <View>
          <Text style={styles.cardTitle}>Sincronizacao</Text>
          <Text style={styles.muted}>{sync.endpointUrl}</Text>
        </View>
        <Pressable onPress={sync.onSyncNow} style={styles.cameraFab}>
          <Ionicons color="#111827" name="sync" size={22} />
        </Pressable>
      </View>
      <Text style={styles.muted}>{sync.message}</Text>
    </View>
  );
}

function section(title: string, subtitle: string, icon: keyof typeof Ionicons.glyphMap, rows: SectionConfig["rows"]): SectionConfig {
  return { title, subtitle, icon, rows };
}

function row(title: string, detail: string, badge: string) {
  return { title, detail, badge };
}

function Metric({ label, value, icon }: { label: string; value: number; icon: keyof typeof Ionicons.glyphMap }) {
  return (
    <View style={styles.metric}>
      <Ionicons color={colors.primary} name={icon} size={20} />
      <Text style={styles.metricValue}>{value}</Text>
      <Text style={styles.muted}>{label}</Text>
    </View>
  );
}

function ListCard({ title, empty, children }: { title: string; empty: string; children: ReactNode }) {
  const list = Array.isArray(children) ? children.filter(Boolean) : children;
  const isEmpty = Array.isArray(list) ? list.length === 0 : !list;
  return (
    <View style={styles.card}>
      <Text style={styles.cardTitle}>{title}</Text>
      {isEmpty ? <Text style={styles.muted}>{empty}</Text> : list}
    </View>
  );
}

const styles = StyleSheet.create({
  card: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.lg,
    gap: spacing.md
  },
  searchHero: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.lg,
    gap: spacing.md
  },
  heroTitle: {
    color: colors.text,
    fontSize: 30,
    fontWeight: "900"
  },
  searchRow: {
    flexDirection: "row",
    gap: spacing.sm
  },
  searchInput: {
    flex: 1
  },
  searchButton: {
    width: 54,
    minHeight: 50,
    borderRadius: 14,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center"
  },
  resultList: {
    gap: spacing.sm
  },
  resultItem: {
    borderRadius: 14,
    backgroundColor: colors.surfaceSoft,
    padding: spacing.md,
    gap: spacing.xs
  },
  pageHeaderCard: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: "rgba(250, 204, 21, 0.06)",
    padding: spacing.lg,
    gap: spacing.md,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center"
  },
  toolbarCard: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.lg,
    gap: spacing.md,
    flexDirection: "row",
    justifyContent: "space-between",
    alignItems: "center"
  },
  stageCard: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.lg,
    gap: spacing.md
  },
  servicePreview: {
    borderRadius: 18,
    borderWidth: 1,
    borderColor: "rgba(250, 204, 21, 0.12)",
    backgroundColor: colors.surfaceSoft,
    padding: spacing.md
  },
  grid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.md
  },
  metric: {
    flexBasis: "47%",
    flexGrow: 1,
    borderRadius: 18,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surfaceSoft,
    padding: spacing.md,
    gap: spacing.xs
  },
  metricValue: {
    color: colors.text,
    fontSize: 24,
    fontWeight: "900"
  },
  cardTitle: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "900"
  },
  pill: {
    alignSelf: "flex-start",
    color: "#111827",
    backgroundColor: colors.primary,
    borderRadius: 999,
    paddingHorizontal: spacing.sm,
    paddingVertical: 4,
    overflow: "hidden",
    fontSize: 11,
    fontWeight: "900",
    textTransform: "uppercase"
  },
  cameraFab: {
    width: 50,
    height: 50,
    borderRadius: 16,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center"
  },
  input: {
    minHeight: 50,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(250, 204, 21, 0.18)",
    backgroundColor: colors.surfaceSoft,
    color: colors.text,
    paddingHorizontal: spacing.md
  },
  primaryButton: {
    minHeight: 52,
    borderRadius: 14,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center",
    flexDirection: "row",
    gap: spacing.sm,
    paddingHorizontal: spacing.md,
    flex: 1
  },
  primaryButtonText: {
    color: "#111827",
    fontWeight: "900"
  },
  secondaryButton: {
    width: 56,
    minHeight: 52,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: colors.border,
    alignItems: "center",
    justifyContent: "center"
  },
  primaryButtonCompact: {
    minHeight: 48,
    borderRadius: 14,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: spacing.md,
    minWidth: 120
  },
  secondaryButtonWide: {
    minHeight: 48,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: colors.border,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: spacing.md,
    minWidth: 120
  },
  secondaryButtonText: {
    color: colors.text,
    fontWeight: "900"
  },
  row: {
    flexDirection: "row",
    gap: spacing.md
  },
  rowWrap: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.md
  },
  calendarStrip: {
    flexDirection: "row",
    gap: spacing.sm
  },
  calendarDay: {
    flex: 1,
    borderRadius: 16,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.md
  },
  calendarDayActive: {
    backgroundColor: colors.primary
  },
  calendarDayText: {
    color: colors.text,
    fontWeight: "900"
  },
  calendarDayTextActive: {
    color: "#111827"
  },
  financeBand: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.md
  },
  diagnosticGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.md
  },
  diagnosticItem: {
    flexBasis: "47%",
    flexGrow: 1,
    borderRadius: 18,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.md,
    gap: spacing.xs
  },
  filterGrid: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.sm
  },
  fakeInput: {
    minHeight: 48,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(250, 204, 21, 0.18)",
    backgroundColor: colors.surfaceSoft,
    justifyContent: "center",
    paddingHorizontal: spacing.md,
    flexGrow: 1,
    flexBasis: "30%"
  },
  fakeInputText: {
    color: colors.muted,
    fontWeight: "700"
  },
  listItem: {
    borderRadius: 14,
    backgroundColor: colors.surfaceSoft,
    padding: spacing.md,
    gap: spacing.xs
  },
  timelineCard: {
    borderRadius: 22,
    borderLeftWidth: 4,
    borderLeftColor: colors.primary,
    borderTopWidth: 1,
    borderRightWidth: 1,
    borderBottomWidth: 1,
    borderTopColor: colors.border,
    borderRightColor: colors.border,
    borderBottomColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.lg,
    gap: spacing.md
  },
  tableRow: {
    borderBottomWidth: 1,
    borderBottomColor: "rgba(250, 204, 21, 0.12)",
    paddingVertical: spacing.md,
    gap: spacing.xs
  },
  itemTitle: {
    color: colors.text,
    fontWeight: "800"
  },
  itemRow: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.md
  },
  badge: {
    color: "#111827",
    backgroundColor: colors.primary,
    borderRadius: 999,
    paddingHorizontal: spacing.sm,
    paddingVertical: 3,
    overflow: "hidden",
    fontSize: 12,
    fontWeight: "900"
  },
  muted: {
    color: colors.muted,
    lineHeight: 20
  },
  sectionHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.md
  }
});

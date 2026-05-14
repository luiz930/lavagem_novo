import { ReactNode, useEffect, useMemo, useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { Ionicons } from "@expo/vector-icons";

import { ClienteLocal, listarClientes, listarServicos, resumoLocal, salvarCliente, salvarServico, ServicoLocal } from "../data/localRepository";
import { colors, spacing } from "../theme";
import { AppScreenKey } from "./AppShell";

type Props = {
  screen: AppScreenKey;
  onOpenCamera: () => void;
  onRefreshPending: () => void;
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

export function NativeScreenContent({ screen, onOpenCamera, onRefreshPending }: Props) {
  if (screen === "inicio" || screen === "painel") {
    return <InicioPainel onOpenCamera={onOpenCamera} />;
  }
  if (screen === "clientes") {
    return <ClientesScreen onSaved={onRefreshPending} />;
  }
  if (screen === "servicos") {
    return <ServicosScreen onSaved={onRefreshPending} onOpenCamera={onOpenCamera} />;
  }
  return <ModuleScreen screen={screen} />;
}

function InicioPainel({ onOpenCamera }: { onOpenCamera: () => void }) {
  const [resumo, setResumo] = useState({ clientes: 0, servicos: 0, fotos: 0, pendencias: 0 });

  useEffect(() => {
    resumoLocal().then(setResumo);
  }, []);

  return (
    <>
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

function ModuleScreen({ screen }: { screen: AppScreenKey }) {
  const config = useMemo(() => moduleConfig(screen), [screen]);
  return (
    <>
      <View style={styles.card}>
        <Text style={styles.cardTitle}>{screenTitle(screen)}</Text>
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
        <View key={section.title} style={styles.card}>
          <View style={styles.sectionHeader}>
            <View>
              <Text style={styles.cardTitle}>{section.title}</Text>
              <Text style={styles.muted}>{section.subtitle}</Text>
            </View>
            <Ionicons color={colors.primary} name={section.icon} size={22} />
          </View>
          {section.rows.map((item) => (
            <View key={item.title} style={styles.listItem}>
              <View style={styles.itemRow}>
                <Text style={styles.itemTitle}>{item.title}</Text>
                <Text style={styles.badge}>{item.badge}</Text>
              </View>
              <Text style={styles.muted}>{item.detail}</Text>
            </View>
          ))}
        </View>
      ))}
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
    filters: ["Busca", "Periodo", "Status"],
    actions: ["Atualizar", "Novo"],
    sections: [section(name, "Resumo do modulo", "apps", [row("Registros", "Dados locais e sincronizados.", "Local")])]
  };
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

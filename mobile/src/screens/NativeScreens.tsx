import { ReactNode, useEffect, useMemo, useState } from "react";
import { Pressable, StyleSheet, Text, TextInput, View } from "react-native";
import { Ionicons } from "@expo/vector-icons";

import {
  atualizarClienteVeiculo,
  BuscaPlacaResultado,
  buscarPorPlaca,
  ChecklistItemLocal,
  ClienteLocal,
  listarClientes,
  listarChecklistItens,
  listarProdutosPneu,
  listarServicos,
  listarTiposServico,
  ProdutoPneuLocal,
  resumoLocal,
  salvarCliente,
  salvarClienteVeiculo,
  salvarServico,
  ServicoLocal,
  TipoServicoLocal
} from "../data/localRepository";
import { MobileHudPayload, MobileModuleCounter, MobileModuleRow, MobileSiteState } from "../sync/types";
import { colors, spacing } from "../theme";
import { AppScreenKey } from "./AppShell";
import { CameraTarget } from "./CameraScreen";

type Props = {
  screen: AppScreenKey;
  onOpenCamera: (target: CameraTarget) => void;
  onRefreshPending: () => void;
  sync: {
    pending: number;
    message: string;
    endpointUrl: string;
    hud: MobileHudPayload | null;
    siteState: MobileSiteState | null;
    updatedAt: string;
    version: string;
    onRefreshHud: () => Promise<void>;
    onSyncNow: () => Promise<void>;
    onUpdateVersion: (version: string) => Promise<string>;
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
    return <InicioPainel onOpenCamera={onOpenCamera} onSaved={onRefreshPending} sync={sync} />;
  }
  if (screen === "painel") {
    return <PainelScreen onOpenCamera={onOpenCamera} sync={sync} />;
  }
  if (screen === "clientes") {
    return <ClientesScreen onSaved={onRefreshPending} sync={sync} />;
  }
  if (screen === "servicos") {
    return <ServicosScreen onSaved={onRefreshPending} onOpenCamera={onOpenCamera} sync={sync} />;
  }
  if (screen === "pneus") {
    return <PneusScreen sync={sync} />;
  }
  if (screen === "checklist") {
    return <ChecklistScreen sync={sync} />;
  }
  if (screen === "configuracoes") {
    return <ConfiguracoesScreen sync={sync} />;
  }
  return <ModuleScreen screen={screen} sync={sync} />;
}

function InicioPainel({ onOpenCamera, onSaved, sync }: { onOpenCamera: (target: CameraTarget) => void; onSaved: () => void; sync: Props["sync"] }) {
  const [resumo, setResumo] = useState({ clientes: 0, servicos: 0, fotos: 0, pendencias: 0 });
  const [placa, setPlaca] = useState("");
  const [resultados, setResultados] = useState<BuscaPlacaResultado[]>([]);
  const [buscou, setBuscou] = useState(false);
  const [selecionado, setSelecionado] = useState<BuscaPlacaResultado | null>(null);
  const [editandoCliente, setEditandoCliente] = useState(false);
  const [novoAtendimento, setNovoAtendimento] = useState(false);
  const [tiposServico, setTiposServico] = useState<TipoServicoLocal[]>([]);
  const [formCliente, setFormCliente] = useState({ nome: "", telefone: "", modelo: "", cor: "" });
  const [tipoServico, setTipoServico] = useState("");
  const [valorAdicional, setValorAdicional] = useState("");
  const [entregaPrevista, setEntregaPrevista] = useState("");
  const [observacoes, setObservacoes] = useState("");

  useEffect(() => {
    resumoLocal().then(setResumo);
    listarTiposServico().then((items) => {
      setTiposServico(items);
      setTipoServico(items[0]?.nome || "");
    });
  }, []);

  async function pesquisarPlaca(texto = placa) {
    const normalizada = texto.trim().toUpperCase();
    setPlaca(normalizada);
    setBuscou(Boolean(normalizada));
    const encontrados = await buscarPorPlaca(normalizada);
    setResultados(encontrados);
    const primeiro = encontrados[0] || null;
    setSelecionado(primeiro);
    setEditandoCliente(false);
    setNovoAtendimento(false);
    if (primeiro) {
      setFormCliente({
        nome: primeiro.cliente_nome || "",
        telefone: primeiro.cliente_telefone || "",
        modelo: primeiro.modelo || "",
        cor: primeiro.cor || ""
      });
    } else {
      setFormCliente({ nome: "", telefone: "", modelo: "", cor: "" });
    }
  }

  async function cadastrarNovoCliente() {
    if (!placa.trim()) {
      return;
    }
    const salvo = await salvarClienteVeiculo({
      placa,
      nome: formCliente.nome,
      telefone: formCliente.telefone,
      modelo: formCliente.modelo,
      cor: formCliente.cor
    });
    await onSaved();
    await resumoLocal().then(setResumo);
    await pesquisarPlaca(salvo.placa);
  }

  async function salvarEdicaoCliente() {
    if (!selecionado) {
      return;
    }
    await atualizarClienteVeiculo({
      veiculo_uuid: selecionado.veiculo_uuid,
      cliente_uuid: selecionado.cliente_uuid,
      placa: selecionado.placa,
      nome: formCliente.nome,
      telefone: formCliente.telefone,
      modelo: formCliente.modelo,
      cor: formCliente.cor
    });
    await onSaved();
    await pesquisarPlaca(selecionado.placa);
  }

  async function iniciarAtendimento() {
    if (!selecionado) {
      return;
    }
    const tipo = tiposServico.find((item) => item.nome === tipoServico);
    const servicoUuid = await salvarServico({
      veiculo_uuid: selecionado.veiculo_uuid,
      tipo_nome: tipoServico || tipo?.nome || "Servico",
      valor: tipo?.valor || 0,
      valor_adicional: Number(String(valorAdicional || "0").replace(",", ".")) || 0,
      entrega_prevista: entregaPrevista,
      observacoes,
      status: "EM ANDAMENTO"
    });
    setNovoAtendimento(false);
    setObservacoes("");
    setValorAdicional("");
    setEntregaPrevista("");
    await onSaved();
    await resumoLocal().then(setResumo);
    await pesquisarPlaca(selecionado.placa);
    onOpenCamera({
      servico_uuid: servicoUuid,
      tipo: "entrada",
      titulo: "Fotos de entrada"
    });
  }

  async function abrirCameraAtendimento(tipo: "entrada" | "detalhe" | "saida") {
    if (!selecionado?.servico_uuid) {
      return;
    }
    onOpenCamera({
      servico_uuid: selecionado.servico_uuid,
      tipo,
      titulo: tipo === "entrada" ? "Fotos de entrada" : tipo === "detalhe" ? "Fotos de detalhe" : "Fotos de finalizacao",
      finalizarAoSalvar: tipo === "saida"
    });
  }

  return (
    <>
      <HudSiteCard sync={sync} />

      <View style={styles.searchHero}>
        <Text style={styles.pill}>Pagina principal</Text>
        <Text style={styles.heroTitle}>Digite a placa para comecar</Text>
        <Text style={styles.muted}>Mesmo fluxo da home do site: buscar placa, cadastrar cliente ou iniciar atendimento.</Text>
        <View style={styles.searchRow}>
          <TextInput
            autoCapitalize="characters"
            onChangeText={(value) => {
              setPlaca(value.toUpperCase());
            }}
            placeholder="Digite a placa"
            placeholderTextColor={colors.muted}
            style={[styles.input, styles.searchInput]}
            value={placa}
          />
          <Pressable onPress={() => pesquisarPlaca()} style={styles.searchButton}>
            <Ionicons color={colors.primaryText} name="search" size={22} />
          </Pressable>
        </View>
        <View style={styles.rowWrap}>
          <Pressable onPress={sync.onSyncNow} style={styles.secondaryButtonWide}>
            <Text style={styles.secondaryButtonText}>Sincronizar dados</Text>
          </Pressable>
          <Text style={styles.muted}>{sync.message}</Text>
        </View>
      </View>

      {!buscou && (
        <View style={styles.centerStateCard}>
          <Text style={styles.cardTitle}>Digite uma placa para comecar</Text>
          <Text style={styles.muted}>O app procura primeiro no banco local sincronizado com o site.</Text>
        </View>
      )}

      {buscou && selecionado && (
        <>
          <View style={styles.card}>
            <View style={styles.sectionHeader}>
              <View>
                <Text style={styles.cardTitle}>Cliente</Text>
                <Text style={styles.muted}>{selecionado.placa}</Text>
              </View>
              <Text style={styles.badge}>{selecionado.atendimento_ativo ? "Em andamento" : "Encontrado"}</Text>
            </View>
            <Text style={styles.infoLine}>Nome: {selecionado.cliente_nome || "Nao informado"}</Text>
            <Text style={styles.infoLine}>Telefone: {selecionado.cliente_telefone || "Nao informado"}</Text>
            <Text style={styles.infoLine}>Carro: {selecionado.modelo || "Nao informado"} {selecionado.cor ? `- ${selecionado.cor}` : ""}</Text>
            <Pressable onPress={() => setEditandoCliente((value) => !value)} style={styles.secondaryButtonWide}>
              <Text style={styles.secondaryButtonText}>{editandoCliente ? "Fechar edicao" : "Editar cliente"}</Text>
            </Pressable>
            {editandoCliente && (
              <View style={styles.stackForm}>
                <TextInput value={formCliente.nome} onChangeText={(nome) => setFormCliente((old) => ({ ...old, nome }))} placeholder="Nome" placeholderTextColor={colors.muted} style={styles.input} />
                <TextInput value={formCliente.telefone} onChangeText={(telefone) => setFormCliente((old) => ({ ...old, telefone }))} placeholder="Telefone" placeholderTextColor={colors.muted} style={styles.input} />
                <TextInput value={formCliente.modelo} onChangeText={(modelo) => setFormCliente((old) => ({ ...old, modelo }))} placeholder="Modelo" placeholderTextColor={colors.muted} style={styles.input} />
                <TextInput value={formCliente.cor} onChangeText={(cor) => setFormCliente((old) => ({ ...old, cor }))} placeholder="Cor" placeholderTextColor={colors.muted} style={styles.input} />
                <Pressable onPress={salvarEdicaoCliente} style={styles.primaryButton}>
                  <Text style={styles.primaryButtonText}>Salvar alteracoes</Text>
                </Pressable>
              </View>
            )}
          </View>

          <View style={styles.card}>
            <Text style={styles.cardTitle}>Atendimento</Text>
            {selecionado.servico_uuid && (
              <View style={styles.photoFlow}>
                <Text style={styles.muted}>Fluxo de fotos do atendimento aberto</Text>
                <View style={styles.photoFlowRow}>
                  <Pressable onPress={() => abrirCameraAtendimento("entrada")} style={styles.photoFlowButton}>
                    <Ionicons color={colors.text} name="camera" size={18} />
                    <Text style={styles.photoFlowTitle}>Entrada</Text>
                    <Text style={styles.photoFlowCount}>{Number(selecionado.fotos_entrada || 0)}</Text>
                  </Pressable>
                  <Pressable onPress={() => abrirCameraAtendimento("detalhe")} style={styles.photoFlowButton}>
                    <Ionicons color={colors.text} name="images" size={18} />
                    <Text style={styles.photoFlowTitle}>Detalhe</Text>
                    <Text style={styles.photoFlowCount}>{Number(selecionado.fotos_detalhe || 0)}</Text>
                  </Pressable>
                  <Pressable onPress={() => abrirCameraAtendimento("saida")} style={styles.photoFlowButton}>
                    <Ionicons color={colors.text} name="checkmark-done" size={18} />
                    <Text style={styles.photoFlowTitle}>Finalizacao</Text>
                    <Text style={styles.photoFlowCount}>{Number(selecionado.fotos_saida || 0)}</Text>
                  </Pressable>
                </View>
              </View>
            )}
            <Pressable onPress={() => setNovoAtendimento((value) => !value)} style={styles.primaryButton}>
              <Text style={styles.primaryButtonText}>{novoAtendimento ? "Fechar atendimento" : "+ Novo Atendimento"}</Text>
            </Pressable>
            {novoAtendimento && (
              <View style={styles.stackForm}>
                <Text style={styles.muted}>Ao iniciar, o app abre a camera para registrar as fotos de entrada.</Text>
                <TextInput value={tipoServico} onChangeText={setTipoServico} placeholder="Tipo de servico" placeholderTextColor={colors.muted} style={styles.input} />
                {tiposServico.length > 0 && (
                  <View style={styles.rowWrap}>
                    {tiposServico.slice(0, 6).map((item) => (
                      <Pressable key={item.uuid} onPress={() => setTipoServico(item.nome)} style={tipoServico === item.nome ? styles.primaryButtonCompact : styles.secondaryButtonWide}>
                        <Text style={tipoServico === item.nome ? styles.primaryButtonText : styles.secondaryButtonText}>{item.nome}</Text>
                      </Pressable>
                    ))}
                  </View>
                )}
                <TextInput value={valorAdicional} onChangeText={setValorAdicional} keyboardType="decimal-pad" placeholder="Valor adicional manual" placeholderTextColor={colors.muted} style={styles.input} />
                <TextInput value={entregaPrevista} onChangeText={setEntregaPrevista} placeholder="Horario de entrega combinado (HH:MM)" placeholderTextColor={colors.muted} style={styles.input} />
                <TextInput value={observacoes} onChangeText={setObservacoes} placeholder="Observacoes" placeholderTextColor={colors.muted} style={styles.input} />
                <Text style={styles.muted}>Quando informar o horario combinado, o painel usa esse horario para destacar a entrega.</Text>
                <Pressable onPress={iniciarAtendimento} style={styles.primaryButton}>
                  <Text style={styles.primaryButtonText}>Iniciar Atendimento</Text>
                </Pressable>
              </View>
            )}
          </View>
        </>
      )}

      {buscou && !selecionado && (
        <View style={styles.card}>
          <Text style={styles.cardTitle}>Novo Cliente</Text>
          <TextInput value={placa} onChangeText={setPlaca} placeholder="Placa" placeholderTextColor={colors.muted} style={styles.input} autoCapitalize="characters" />
          <TextInput value={formCliente.nome} onChangeText={(nome) => setFormCliente((old) => ({ ...old, nome }))} placeholder="Nome" placeholderTextColor={colors.muted} style={styles.input} />
          <TextInput value={formCliente.telefone} onChangeText={(telefone) => setFormCliente((old) => ({ ...old, telefone }))} placeholder="Telefone" placeholderTextColor={colors.muted} style={styles.input} />
          <TextInput value={formCliente.modelo} onChangeText={(modelo) => setFormCliente((old) => ({ ...old, modelo }))} placeholder="Modelo" placeholderTextColor={colors.muted} style={styles.input} />
          <TextInput value={formCliente.cor} onChangeText={(cor) => setFormCliente((old) => ({ ...old, cor }))} placeholder="Cor" placeholderTextColor={colors.muted} style={styles.input} />
          <Pressable onPress={cadastrarNovoCliente} style={styles.primaryButton}>
            <Text style={styles.primaryButtonText}>Cadastrar</Text>
          </Pressable>
        </View>
      )}

      {resultados.length > 1 && (
        <View style={styles.resultList}>
          {resultados.map((item) => (
            <Pressable key={`${item.veiculo_uuid}-${item.placa}`} onPress={() => setSelecionado(item)} style={styles.resultItem}>
              <View style={styles.itemRow}>
                <Text style={styles.itemTitle}>{item.placa}</Text>
                <Text style={styles.badge}>{item.atendimento_ativo ? "Em atendimento" : "Cliente"}</Text>
              </View>
              <Text style={styles.muted}>{item.cliente_nome || "Cliente sem nome"} | {item.modelo || "Modelo nao informado"}</Text>
            </Pressable>
          ))}
        </View>
      )}

      <View style={styles.grid}>
        <Metric label="Clientes" value={resumo.clientes} icon="person" />
        <Metric label="Servicos" value={resumo.servicos} icon="water" />
        <Metric label="Fotos" value={resumo.fotos} icon="camera" />
        <Metric label="Pendencias" value={resumo.pendencias} icon="sync" />
      </View>
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Operacao</Text>
        <Text style={styles.muted}>As fotos agora ficam dentro do atendimento, separadas em entrada, detalhe e finalizacao como no site.</Text>
      </View>
    </>
  );
}

function HudSiteCard({ sync }: { sync: Props["sync"] }) {
  const hud = sync.hud || {};
  const versao = sync.version || String(hud.versao || "").replace(/^Vers[aã]o:\s*/i, "");
  const clima = sync.siteState?.clima || {};
  const totalHoje = hudNumber(hud, ["total", "servicos_ativos", "servicos_hoje"]);
  const andamento = hudNumber(hud, ["andamento", "servicos_ativos"]);
  const atrasados = hudNumber(hud, ["atrasados", "entregas_atrasadas"]);
  const ticket = hudNumber(hud, ["ticket", "ticket_medio"]);
  const statusBanco = String(hud.banco_online_resumo || hud.sync_bancos_resumo || "HUD do site");
  const mensagemBanco = String(hud.banco_online_mensagem || hud.sync_bancos_mensagem || sync.message);

  return (
    <View style={styles.hudCard}>
      <View style={styles.sectionHeader}>
        <View>
          <Text style={styles.pill}>HUD do site</Text>
          <Text style={styles.heroTitleSmall}>Operacao em tempo real</Text>
          <Text style={styles.muted}>{statusBanco}</Text>
        </View>
        <Pressable onPress={sync.onRefreshHud} style={styles.cameraFab}>
          <Ionicons color={colors.primaryText} name="refresh" size={21} />
        </Pressable>
      </View>
      <View style={styles.grid}>
        <HudMetric label="Total" value={totalHoje} icon="speedometer" />
        <HudMetric label="Andamento" value={andamento} icon="car-sport" />
        <HudMetric label="Atrasados" value={atrasados} icon="alert-circle" />
        <HudMetric label="Ticket" value={ticket ? `R$ ${ticket.toFixed(2)}` : "R$ 0,00"} icon="cash" />
      </View>
      <View style={styles.hudStatusRow}>
        <View style={styles.hudStatusItem}>
          <Text style={styles.muted}>Clima</Text>
          <Text style={styles.itemTitle}>{`${clima.icone || ""} ${clima.clima || "Carregando clima"}`.trim()}</Text>
          <Text style={styles.muted}>{[clima.temp ? `${clima.temp}°C` : "", clima.sugestao || ""].filter(Boolean).join(" | ")}</Text>
        </View>
        <View style={styles.hudStatusItem}>
          <Text style={styles.muted}>Banco</Text>
          <Text style={styles.itemTitle}>{mensagemBanco}</Text>
        </View>
        <View style={styles.hudStatusItem}>
          <Text style={styles.muted}>Versao</Text>
          <Text style={styles.itemTitle}>{versao || "Sincronizando"}</Text>
        </View>
      </View>
    </View>
  );
}

function hudNumber(hud: MobileHudPayload, keys: string[]) {
  for (const key of keys) {
    const value = Number(hud[key]);
    if (Number.isFinite(value)) {
      return value;
    }
  }
  return 0;
}

function PainelScreen({ onOpenCamera, sync }: { onOpenCamera: (target: CameraTarget) => void; sync: Props["sync"] }) {
  const [servicos, setServicos] = useState<ServicoLocal[]>([]);

  useEffect(() => {
    listarServicos().then(setServicos);
  }, [sync.updatedAt]);

  const lavagem = servicos.filter((item) => String(item.status || "").toUpperCase() !== "FINALIZADO" && String(item.etapa_atual || "LAVAGEM").toUpperCase() !== "FINALIZACAO");
  const finalizacao = servicos.filter((item) => String(item.status || "").toUpperCase() !== "FINALIZADO" && String(item.etapa_atual || "").toUpperCase() === "FINALIZACAO");
  const finalizados = servicos.filter((item) => String(item.status || "").toUpperCase() === "FINALIZADO");

  function renderServico(item: ServicoLocal) {
    return (
      <View key={item.uuid} style={styles.servicePreview}>
        <View style={styles.itemRow}>
          <Text style={styles.itemTitle}>{item.tipo_nome || "Atendimento"}</Text>
          <Text style={styles.badge}>{item.status || "ABERTO"}</Text>
        </View>
        <Text style={styles.muted}>{item.observacoes || "Sem observacoes"}</Text>
        <Text style={styles.muted}>Fotos: entrada {Number(item.fotos_entrada || 0)} | detalhe {Number(item.fotos_detalhe || 0)} | finalizacao {Number(item.fotos_saida || 0)}</Text>
        <View style={styles.photoFlowRow}>
          <Pressable onPress={() => onOpenCamera({ servico_uuid: item.uuid, tipo: "entrada", titulo: "Fotos de entrada" })} style={styles.photoActionButton}>
            <Text style={styles.secondaryButtonText}>Entrada</Text>
          </Pressable>
          <Pressable onPress={() => onOpenCamera({ servico_uuid: item.uuid, tipo: "detalhe", titulo: "Fotos de detalhe" })} style={styles.photoActionButton}>
            <Text style={styles.secondaryButtonText}>Detalhe</Text>
          </Pressable>
          <Pressable onPress={() => onOpenCamera({ servico_uuid: item.uuid, tipo: "saida", titulo: "Fotos de finalizacao", finalizarAoSalvar: true })} style={styles.photoActionButton}>
            <Text style={styles.secondaryButtonText}>Finalizar</Text>
          </Pressable>
        </View>
      </View>
    );
  }

  return (
    <>
      <View style={styles.toolbarCard}>
        <View>
          <Text style={styles.pill}>Painel operacional</Text>
          <Text style={styles.cardTitle}>Controle dos servicos em andamento</Text>
          <Text style={styles.muted}>Fluxo separado por lavagem, finalizacao, prioridade e fotos.</Text>
        </View>
      </View>
      <SiteModulePanel module={sync.siteState?.modulos?.painel} screen="painel" updatedAt={sync.updatedAt} />
      <View style={styles.grid}>
        <Metric label="Em atendimento" value={lavagem.length + finalizacao.length} icon="car" />
        <Metric label="Lavagem" value={lavagem.length} icon="water" />
        <Metric label="Finalizacao" value={finalizacao.length} icon="checkmark-done" />
        <Metric label="Finalizados" value={finalizados.length} icon="flag" />
      </View>
      {[
        { title: "Etapa de Lavagem", items: lavagem },
        { title: "Etapa de Finalizacao", items: finalizacao },
        { title: "Finalizados", items: finalizados }
      ].map((group) => (
        <View key={group.title} style={styles.stageCard}>
          <View style={styles.sectionHeader}>
            <View>
              <Text style={styles.pill}>{group.title}</Text>
              <Text style={styles.muted}>{group.items.length} atendimento(s)</Text>
            </View>
            <Ionicons color={colors.primary} name="timer" size={24} />
          </View>
          {group.items.length ? group.items.map(renderServico) : (
            <View style={styles.servicePreview}>
              <Text style={styles.itemTitle}>Fila vazia</Text>
              <Text style={styles.muted}>Sincronize para carregar os atendimentos do site.</Text>
            </View>
          )}
        </View>
      ))}
    </>
  );
}

function ClientesScreen({ onSaved, sync }: { onSaved: () => void; sync: Props["sync"] }) {
  const [clientes, setClientes] = useState<ClienteLocal[]>([]);
  const [nome, setNome] = useState("");
  const [telefone, setTelefone] = useState("");
  const [placa, setPlaca] = useState("");

  async function refresh() {
    setClientes(await listarClientes());
  }

  useEffect(() => {
    refresh();
  }, [sync.updatedAt]);

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
      <SiteModulePanel module={sync.siteState?.modulos?.clientes} screen="clientes" updatedAt={sync.updatedAt} />
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

function ServicosScreen({ onSaved, onOpenCamera, sync }: { onSaved: () => void; onOpenCamera: (target: CameraTarget) => void; sync: Props["sync"] }) {
  const [servicos, setServicos] = useState<ServicoLocal[]>([]);
  const [tipos, setTipos] = useState<TipoServicoLocal[]>([]);
  const [observacoes, setObservacoes] = useState("");

  async function refresh() {
    setServicos(await listarServicos());
    setTipos(await listarTiposServico());
  }

  useEffect(() => {
    refresh();
  }, [sync.updatedAt]);

  async function submit() {
    const servicoUuid = await salvarServico({ observacoes });
    setObservacoes("");
    await refresh();
    onSaved();
    onOpenCamera({
      servico_uuid: servicoUuid,
      tipo: "entrada",
      titulo: "Fotos de entrada"
    });
  }

  return (
    <>
      <SiteModulePanel module={sync.siteState?.modulos?.servicos} screen="servicos" updatedAt={sync.updatedAt} />
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Novo servico</Text>
        <TextInput value={observacoes} onChangeText={setObservacoes} placeholder="Observacoes" placeholderTextColor={colors.muted} style={styles.input} />
        <View style={styles.row}>
          <Pressable onPress={submit} style={styles.primaryButton}>
            <Text style={styles.primaryButtonText}>Abrir servico</Text>
          </Pressable>
        </View>
      </View>
      <ListCard title="Servicos locais" empty="Nenhum servico offline ainda.">
        {tipos.map((item) => (
          <View key={`tipo-${item.uuid}`} style={styles.listItem}>
            <View style={styles.itemRow}>
              <Text style={styles.itemTitle}>{item.nome}</Text>
              <Text style={styles.badge}>R$ {Number(item.valor || 0).toFixed(2)}</Text>
            </View>
            <Text style={styles.muted}>Catalogo sincronizado do site</Text>
          </View>
        ))}
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

function PneusScreen({ sync }: { sync: Props["sync"] }) {
  const [produtos, setProdutos] = useState<ProdutoPneuLocal[]>([]);

  useEffect(() => {
    listarProdutosPneu().then(setProdutos);
  }, [sync.updatedAt]);

  return (
    <>
      <View style={styles.pageHeaderCard}>
        <View>
          <Text style={styles.pill}>Pneus</Text>
          <Text style={styles.cardTitle}>Produtos de pneu</Text>
          <Text style={styles.muted}>Lista sincronizada da tela Pneus do site.</Text>
        </View>
        <Ionicons color={colors.primary} name="construct" size={28} />
      </View>
      <SiteModulePanel module={sync.siteState?.modulos?.pneus} screen="pneus" updatedAt={sync.updatedAt} />
      <View style={styles.card}>
        <Pressable onPress={sync.onSyncNow} style={styles.primaryButton}>
          <Ionicons color={colors.primaryText} name="sync" size={22} />
          <Text style={styles.primaryButtonText}>Sincronizar pneus</Text>
        </Pressable>
      </View>
      <ListCard title="Produtos cadastrados no site" empty="Nenhum produto de pneu sincronizado ainda.">
        {produtos.map((item) => (
          <View key={item.uuid} style={styles.listItem}>
            <Text style={styles.itemTitle}>{item.nome}</Text>
          </View>
        ))}
      </ListCard>
    </>
  );
}

function ChecklistScreen({ sync }: { sync: Props["sync"] }) {
  const [itens, setItens] = useState<ChecklistItemLocal[]>([]);

  useEffect(() => {
    listarChecklistItens().then(setItens);
  }, [sync.updatedAt]);

  return (
    <>
      <View style={styles.pageHeaderCard}>
        <View>
          <Text style={styles.pill}>Checklist</Text>
          <Text style={styles.cardTitle}>Itens de finalizacao</Text>
          <Text style={styles.muted}>Itens obrigatorios sincronizados do site.</Text>
        </View>
        <Ionicons color={colors.primary} name="checkbox" size={28} />
      </View>
      <SiteModulePanel module={sync.siteState?.modulos?.checklist} screen="checklist" updatedAt={sync.updatedAt} />
      <View style={styles.card}>
        <Pressable onPress={sync.onSyncNow} style={styles.primaryButton}>
          <Ionicons color={colors.primaryText} name="sync" size={22} />
          <Text style={styles.primaryButtonText}>Sincronizar checklist</Text>
        </Pressable>
      </View>
      <ListCard title="Itens cadastrados no site" empty="Nenhum item de checklist sincronizado ainda.">
        {itens.map((item) => (
          <View key={item.uuid} style={styles.listItem}>
            <View style={styles.itemRow}>
              <Text style={styles.itemTitle}>{item.nome}</Text>
              <Text style={styles.badge}>{item.ativo ? "Ativo" : "Inativo"}</Text>
            </View>
          </View>
        ))}
      </ListCard>
    </>
  );
}

function ConfiguracoesScreen({ sync }: { sync: Props["sync"] }) {
  const [version, setVersion] = useState(sync.version || "");
  const [status, setStatus] = useState(sync.message);

  useEffect(() => {
    if (sync.version) {
      setVersion(sync.version);
    }
  }, [sync.version]);

  async function salvarVersao() {
    const texto = version.trim();
    if (!texto) {
      setStatus("Informe a versao antes de salvar.");
      return;
    }
    setStatus(await sync.onUpdateVersion(texto));
  }

  return (
    <>
      <View style={styles.pageHeaderCard}>
        <View>
          <Text style={styles.pill}>Configuracoes</Text>
          <Text style={styles.cardTitle}>Versao e sincronizacao do site</Text>
          <Text style={styles.muted}>A versao salva aqui e a mesma usada nas configuracoes do site.</Text>
        </View>
        <Ionicons color={colors.primary} name="settings" size={28} />
      </View>
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Versao do sistema</Text>
        <Text style={styles.muted}>Atual no site: {sync.version || "Sincronizando"}</Text>
        <TextInput
          value={version}
          onChangeText={setVersion}
          placeholder="Ex.: 1.0.1"
          placeholderTextColor={colors.muted}
          style={styles.input}
        />
        <Pressable onPress={salvarVersao} style={styles.primaryButton}>
          <Ionicons color={colors.primaryText} name="save" size={22} />
          <Text style={styles.primaryButtonText}>Salvar versao no site</Text>
        </Pressable>
      </View>
      <SyncPanel sync={sync} />
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Retorno</Text>
        <Text style={styles.muted}>{status}</Text>
      </View>
    </>
  );
}

function ModuleScreen({ screen, sync }: { screen: AppScreenKey; sync: Props["sync"] }) {
  const config = useMemo(() => moduleConfig(screen), [screen]);
  const siteModule = sync.siteState?.modulos?.[screen];
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

      <SiteModulePanel module={siteModule} screen={screen} updatedAt={sync.updatedAt} />

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

function iconName(name?: string): keyof typeof Ionicons.glyphMap {
  const fallback: keyof typeof Ionicons.glyphMap = "analytics";
  if (!name) {
    return fallback;
  }
  return Object.prototype.hasOwnProperty.call(Ionicons.glyphMap, name) ? name as keyof typeof Ionicons.glyphMap : fallback;
}

function formatarLinhaSite(row: MobileModuleRow) {
  const title = row.title || row.acao || row.tabela || "Registro do site";
  const detail = row.detail || [row.chave, row.direcao, row.criado_em].filter(Boolean).join(" | ") || "Atualizado pelo backend do site.";
  const badge = row.badge || row.tabela || "Site";
  return { title, detail, badge };
}

function SiteModulePanel({ module, screen, updatedAt }: { module?: { counters?: MobileModuleCounter[]; rows?: MobileModuleRow[] }; screen: AppScreenKey; updatedAt: string }) {
  const counters = module?.counters || [];
  const rows = module?.rows || [];

  if (!counters.length && !rows.length) {
    return (
      <View style={styles.card}>
        <Text style={styles.cardTitle}>Dados do site</Text>
        <Text style={styles.muted}>Sincronizando esta aba com o backend. Atualizacao automatica a cada 10 segundos.</Text>
      </View>
    );
  }

  return (
    <View style={styles.card}>
      <View style={styles.sectionHeader}>
        <View>
          <Text style={styles.cardTitle}>Dados do site</Text>
          <Text style={styles.muted}>{screenTitle(screen)} atualizado automaticamente{updatedAt ? ` em ${new Date(updatedAt).toLocaleTimeString("pt-BR")}` : ""}</Text>
        </View>
        <Ionicons color={colors.primary} name="cloud-done" size={24} />
      </View>
      {counters.length > 0 && (
        <View style={styles.grid}>
          {counters.slice(0, 4).map((item) => (
            <HudMetric key={`${item.label}-${item.value}`} label={item.label} value={item.value} icon={iconName(item.icon)} />
          ))}
        </View>
      )}
      {rows.slice(0, 8).map((row, index) => {
        const item = formatarLinhaSite(row);
        return (
          <View key={`${item.title}-${index}`} style={styles.listItem}>
            <View style={styles.itemRow}>
              <Text style={styles.itemTitle}>{item.title}</Text>
              <Text style={styles.badge}>{item.badge}</Text>
            </View>
            <Text style={styles.muted}>{item.detail}</Text>
          </View>
        );
      })}
    </View>
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
          <Ionicons color={colors.primaryText} name="sync" size={22} />
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

function HudMetric({ label, value, icon }: { label: string; value: number | string; icon: keyof typeof Ionicons.glyphMap }) {
  return (
    <View style={styles.metric}>
      <Ionicons color={colors.primary} name={icon} size={20} />
      <Text style={styles.metricValue}>{value}</Text>
      <Text style={styles.muted}>{label}</Text>
    </View>
  );
}

function ListCard({ title, empty, children }: { title: string; empty: string; children: ReactNode }) {
  const list = Array.isArray(children) ? children.flat().filter(Boolean) : children;
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
    backgroundColor: colors.panel,
    padding: spacing.lg,
    gap: spacing.md
  },
  hudCard: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.primary,
    backgroundColor: colors.panel,
    padding: spacing.lg,
    gap: spacing.md
  },
  searchHero: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.panel,
    padding: spacing.lg,
    gap: spacing.md
  },
  heroTitle: {
    color: colors.text,
    fontSize: 30,
    fontWeight: "900"
  },
  heroTitleSmall: {
    color: colors.text,
    fontSize: 22,
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
  centerStateCard: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.headerSoft,
    padding: spacing.lg,
    alignItems: "center",
    gap: spacing.sm
  },
  stackForm: {
    gap: spacing.sm
  },
  infoLine: {
    color: colors.text,
    lineHeight: 22
  },
  hudStatusRow: {
    flexDirection: "row",
    flexWrap: "wrap",
    gap: spacing.md
  },
  hudStatusItem: {
    flexBasis: "47%",
    flexGrow: 1,
    borderRadius: 16,
    backgroundColor: colors.surfaceSoft,
    padding: spacing.md,
    gap: spacing.xs
  },
  pageHeaderCard: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.headerSoft,
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
    backgroundColor: colors.panel,
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
    backgroundColor: colors.panel,
    padding: spacing.lg,
    gap: spacing.md
  },
  servicePreview: {
    borderRadius: 18,
    borderWidth: 1,
    borderColor: colors.borderSoft,
    backgroundColor: colors.surfaceSoft,
    padding: spacing.md
  },
  photoFlow: {
    borderRadius: 18,
    borderWidth: 1,
    borderColor: colors.borderSoft,
    backgroundColor: colors.surfaceSoft,
    padding: spacing.md,
    gap: spacing.sm
  },
  photoFlowRow: {
    flexDirection: "row",
    gap: spacing.sm
  },
  photoFlowButton: {
    flex: 1,
    minHeight: 92,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: colors.border,
    alignItems: "center",
    justifyContent: "center",
    gap: 4,
    padding: spacing.sm
  },
  photoFlowTitle: {
    color: colors.text,
    fontSize: 12,
    fontWeight: "900",
    textAlign: "center"
  },
  photoFlowCount: {
    color: colors.primary,
    fontSize: 18,
    fontWeight: "900"
  },
  photoActionButton: {
    flex: 1,
    minHeight: 44,
    borderRadius: 12,
    borderWidth: 1,
    borderColor: colors.border,
    alignItems: "center",
    justifyContent: "center",
    paddingHorizontal: spacing.sm
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
    color: colors.primaryText,
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
    borderColor: colors.borderInput,
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
    color: colors.primaryText,
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
    backgroundColor: colors.panel,
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
    color: colors.primaryText
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
    backgroundColor: colors.panel,
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
    borderColor: colors.borderInput,
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
    backgroundColor: colors.panel,
    padding: spacing.lg,
    gap: spacing.md
  },
  tableRow: {
    borderBottomWidth: 1,
    borderBottomColor: colors.borderSoft,
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
    color: colors.primaryText,
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

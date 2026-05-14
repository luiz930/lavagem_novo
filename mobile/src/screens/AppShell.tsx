import { ReactNode, useState } from "react";
import { Modal, Pressable, ScrollView, StyleSheet, Text, useWindowDimensions, View } from "react-native";
import { Ionicons } from "@expo/vector-icons";

import { colors, spacing } from "../theme";

export type AppScreenKey =
  | "inicio"
  | "painel"
  | "clientes"
  | "historico"
  | "retornos"
  | "servicos"
  | "checklist"
  | "pneus"
  | "financeiro"
  | "orcamentos"
  | "notaFiscal"
  | "clima"
  | "auditoria"
  | "changelog"
  | "empresas"
  | "diagnostico"
  | "status"
  | "autoSuporte"
  | "configSite"
  | "configuracoes";

export type MenuItem = {
  key: AppScreenKey;
  label: string;
  icon: keyof typeof Ionicons.glyphMap;
};

export const menuItems: MenuItem[] = [
  { key: "inicio", label: "Inicio", icon: "home" },
  { key: "painel", label: "Painel", icon: "bar-chart" },
  { key: "clientes", label: "Clientes", icon: "person" },
  { key: "historico", label: "Historico", icon: "time" },
  { key: "retornos", label: "Retornos", icon: "calendar" },
  { key: "servicos", label: "Servicos", icon: "water" },
  { key: "checklist", label: "Itens Checklist", icon: "checkbox" },
  { key: "pneus", label: "Pneus", icon: "construct" },
  { key: "financeiro", label: "Relatorios", icon: "cash" },
  { key: "orcamentos", label: "Orcamentos", icon: "document-text" },
  { key: "notaFiscal", label: "Nota fiscal", icon: "receipt" },
  { key: "clima", label: "Clima", icon: "cloud" },
  { key: "auditoria", label: "Auditoria", icon: "search" },
  { key: "changelog", label: "Changelog", icon: "list" },
  { key: "empresas", label: "Empresas", icon: "business" },
  { key: "diagnostico", label: "Diagnostico", icon: "medkit" },
  { key: "status", label: "Status", icon: "pulse" },
  { key: "autoSuporte", label: "AutoSuporte", icon: "hardware-chip" },
  { key: "configSite", label: "Site", icon: "color-palette" },
  { key: "configuracoes", label: "Configuracoes", icon: "settings" }
];

const menuGroups: { label: string; items: MenuItem[] }[] = [
  { label: "Principal", items: menuItems.filter((item) => ["inicio", "painel"].includes(item.key)) },
  { label: "Operacao", items: menuItems.filter((item) => ["clientes", "historico", "retornos", "servicos", "checklist", "pneus"].includes(item.key)) },
  { label: "Gestao", items: menuItems.filter((item) => ["financeiro", "orcamentos", "notaFiscal", "clima"].includes(item.key)) },
  { label: "Sistema", items: menuItems.filter((item) => ["auditoria", "changelog", "empresas", "diagnostico", "status", "autoSuporte", "configSite", "configuracoes"].includes(item.key)) }
];

type Props = {
  active: AppScreenKey;
  title: string;
  subtitle: string;
  onSelect: (screen: AppScreenKey) => void;
  onLogout: () => void;
  children: ReactNode;
};

export function AppShell({ active, title, subtitle, onSelect, onLogout, children }: Props) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const { width } = useWindowDimensions();
  const desktopSidebar = width >= 780;

  function selectScreen(screen: AppScreenKey) {
    onSelect(screen);
    setSidebarOpen(false);
  }

  return (
    <View style={styles.root}>
      <View style={styles.appFrame}>
        {desktopSidebar && <Sidebar active={active} onSelect={selectScreen} />}

        {!desktopSidebar && (
          <Modal visible={sidebarOpen} transparent animationType="slide" onRequestClose={() => setSidebarOpen(false)}>
            <View style={styles.modalLayer}>
              <Sidebar active={active} onSelect={selectScreen} overlay />
              <Pressable style={styles.drawerBackdrop} onPress={() => setSidebarOpen(false)} />
            </View>
          </Modal>
        )}

        <View style={styles.main}>
          <View style={styles.topbar}>
            {!desktopSidebar && (
              <Pressable onPress={() => setSidebarOpen(true)} style={styles.menuButton}>
                <Ionicons color="#111827" name="menu" size={22} />
              </Pressable>
            )}
            <View style={styles.headerText}>
              <Text style={styles.kicker}>WAGEN ESTETICA</Text>
              <Text style={styles.title}>{title}</Text>
              <Text style={styles.subtitle}>{subtitle}</Text>
            </View>
            <Pressable onPress={onLogout} style={styles.iconButton}>
              <Ionicons color={colors.text} name="log-out-outline" size={22} />
            </Pressable>
          </View>

          <ScrollView contentContainerStyle={styles.content}>{children}</ScrollView>
        </View>
      </View>
    </View>
  );
}

function Sidebar({ active, onSelect, overlay = false }: { active: AppScreenKey; onSelect: (screen: AppScreenKey) => void; overlay?: boolean }) {
  return (
    <View style={[styles.sidebar, overlay && styles.sidebarOverlay]}>
      <View style={styles.sidebarHeader}>
        <View style={styles.sidebarLogo}>
          <Text style={styles.sidebarLogoText}>W</Text>
        </View>
        <Text style={styles.sidebarTitle}>Wagen</Text>
        <Text style={styles.sidebarSubtitle}>Estetica automotiva</Text>
      </View>

      <ScrollView showsVerticalScrollIndicator={false} style={styles.sidebarScroller} contentContainerStyle={styles.sidebarNav}>
        {menuGroups.map((group) => (
          <View key={group.label} style={styles.sidebarGroup}>
            <Text style={styles.sidebarGroupLabel}>{group.label}</Text>
            {group.items.map((item) => (
              <Pressable
                key={item.key}
                onPress={() => onSelect(item.key)}
                style={[styles.sidebarItem, active === item.key && styles.sidebarItemActive]}
              >
                <Ionicons color={active === item.key ? "#111827" : colors.text} name={item.icon} size={18} />
                <Text style={[styles.sidebarItemText, active === item.key && styles.sidebarItemTextActive]}>{item.label}</Text>
              </Pressable>
            ))}
          </View>
        ))}
      </ScrollView>
    </View>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    backgroundColor: colors.bg
  },
  appFrame: {
    flex: 1,
    flexDirection: "row",
    backgroundColor: colors.bg
  },
  modalLayer: {
    flex: 1,
    flexDirection: "row"
  },
  sidebar: {
    width: 260,
    height: "100%",
    backgroundColor: colors.surface,
    borderRightWidth: 2,
    borderRightColor: colors.primary,
    paddingHorizontal: spacing.md,
    paddingTop: spacing.xl,
    paddingBottom: spacing.md,
    zIndex: 30,
    elevation: 30,
    flexShrink: 0
  },
  sidebarOverlay: {
    shadowColor: "#000",
    shadowOpacity: 0.45,
    shadowRadius: 18,
    elevation: 16
  },
  drawerBackdrop: {
    flex: 1,
    backgroundColor: "rgba(0, 0, 0, 0.62)",
    zIndex: 20,
    elevation: 20
  },
  sidebarHeader: {
    marginTop: spacing.lg,
    marginBottom: spacing.md,
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 18,
    backgroundColor: "rgba(250, 204, 21, 0.06)",
    alignItems: "center",
    padding: spacing.lg
  },
  sidebarLogo: {
    width: 88,
    height: 88,
    borderRadius: 44,
    borderWidth: 3,
    borderColor: "rgba(250, 204, 21, 0.45)",
    alignItems: "center",
    justifyContent: "center",
    marginBottom: spacing.md
  },
  sidebarLogoText: {
    color: colors.primary,
    fontSize: 46,
    fontWeight: "900"
  },
  sidebarTitle: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "900"
  },
  sidebarSubtitle: {
    color: colors.primary,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 1.4,
    textTransform: "uppercase",
    marginTop: spacing.xs
  },
  sidebarNav: {
    gap: spacing.md,
    paddingBottom: spacing.xl,
    flexGrow: 1
  },
  sidebarScroller: {
    flex: 1
  },
  sidebarGroup: {
    borderRadius: 18,
    borderWidth: 1,
    borderColor: "rgba(250, 204, 21, 0.14)",
    backgroundColor: "rgba(11, 11, 11, 0.34)",
    padding: spacing.sm,
    gap: spacing.sm
  },
  sidebarGroupLabel: {
    color: colors.primary,
    opacity: 0.84,
    fontSize: 10,
    fontWeight: "900",
    letterSpacing: 1.7,
    textTransform: "uppercase",
    paddingHorizontal: spacing.xs
  },
  sidebarItem: {
    minHeight: 48,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: "rgba(250, 204, 21, 0.18)",
    backgroundColor: colors.surfaceSoft,
    alignItems: "center",
    flexDirection: "row",
    gap: spacing.sm,
    paddingHorizontal: spacing.md
  },
  sidebarItemActive: {
    backgroundColor: colors.primary,
    borderColor: colors.primary
  },
  sidebarItemText: {
    color: colors.text,
    fontWeight: "800",
    flex: 1
  },
  sidebarItemTextActive: {
    color: "#111827"
  },
  main: {
    flex: 1
  },
  topbar: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.md,
    margin: spacing.lg,
    marginBottom: spacing.sm,
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 22,
    backgroundColor: colors.surface,
    padding: spacing.md
  },
  headerText: {
    flex: 1
  },
  kicker: {
    color: colors.primary,
    fontSize: 11,
    fontWeight: "800",
    letterSpacing: 1.4
  },
  title: {
    color: colors.text,
    fontSize: 26,
    fontWeight: "900"
  },
  subtitle: {
    color: colors.muted
  },
  iconButton: {
    width: 42,
    height: 42,
    borderRadius: 14,
    borderWidth: 1,
    borderColor: colors.border,
    alignItems: "center",
    justifyContent: "center"
  },
  menuButton: {
    width: 42,
    height: 42,
    borderRadius: 8,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center"
  },
  content: {
    padding: spacing.lg,
    paddingTop: 0,
    gap: spacing.lg
  }
});

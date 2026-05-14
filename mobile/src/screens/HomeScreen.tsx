import { useEffect, useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { Ionicons } from "@expo/vector-icons";

import { UserSession } from "../auth/authRepository";
import { DEFAULT_SERVER_URL, normalizeServerUrl } from "../config";
import { getSetting, setSetting } from "../database/db";
import { pendingSyncCount, runSync } from "../sync/syncService";
import { colors, spacing } from "../theme";
import { AppScreenKey, AppShell } from "./AppShell";
import { CameraScreen } from "./CameraScreen";
import { NativeScreenContent, screenTitle } from "./NativeScreens";

type Props = {
  session: UserSession;
  onLogout: () => void;
};

export function HomeScreen({ session, onLogout }: Props) {
  const [pending, setPending] = useState(0);
  const [syncMessage, setSyncMessage] = useState("Banco local ativo");
  const [cameraOpen, setCameraOpen] = useState(false);
  const [activeScreen, setActiveScreen] = useState<AppScreenKey>("inicio");
  const [endpointUrl, setEndpointUrl] = useState("");
  const [syncToken, setSyncToken] = useState("");

  async function refreshPending() {
    setPending(await pendingSyncCount());
  }

  useEffect(() => {
    refreshPending();
    getSetting("sync_endpoint_url").then((value) => setEndpointUrl(normalizeServerUrl(value || DEFAULT_SERVER_URL)));
    getSetting("sync_token").then(setSyncToken);
  }, []);

  async function syncNow() {
    const normalizedUrl = normalizeServerUrl(endpointUrl);
    await setSetting("sync_endpoint_url", normalizedUrl);
    const savedToken = syncToken.trim() || await getSetting("sync_token");
    const result = await runSync({ endpointUrl: normalizedUrl, token: savedToken });
    setSyncMessage(result.error || `Enviado: ${result.sent} | Recebido: ${result.pulled}`);
    await refreshPending();
  }

  if (cameraOpen) {
    return <CameraScreen session={session} onClose={() => setCameraOpen(false)} onSaved={refreshPending} />;
  }

  return (
    <AppShell
      active={activeScreen}
      title={screenTitle(activeScreen)}
      subtitle={`${session.nome} | ${session.perfil}`}
      onSelect={setActiveScreen}
      onLogout={onLogout}
    >
      <View style={styles.statusBand}>
        <View style={styles.statusItem}>
          <Text style={styles.statusLabel}>Banco</Text>
          <Text style={styles.statusValue}>SQLite local</Text>
        </View>
        <View style={styles.statusItem}>
          <Text style={styles.statusLabel}>Pendencias</Text>
          <Text style={styles.statusValue}>{pending}</Text>
        </View>
      </View>

      <View style={styles.card}>
        <View style={styles.cardHeader}>
          <View>
            <Text style={styles.cardTitle}>Sincronizacao</Text>
            <Text style={styles.muted}>{syncMessage}</Text>
          </View>
          <Pressable onPress={syncNow} style={styles.syncButton}>
            <Ionicons color="#111827" name="sync" size={20} />
          </Pressable>
        </View>
        <Text style={styles.serverText}>{endpointUrl || DEFAULT_SERVER_URL}</Text>
      </View>

      <NativeScreenContent
        screen={activeScreen}
        onOpenCamera={() => setCameraOpen(true)}
        onRefreshPending={refreshPending}
      />
    </AppShell>
  );
}

const styles = StyleSheet.create({
  statusBand: {
    flexDirection: "row",
    gap: spacing.md
  },
  statusItem: {
    flex: 1,
    borderRadius: 18,
    backgroundColor: colors.surfaceSoft,
    borderWidth: 1,
    borderColor: colors.border,
    padding: spacing.md
  },
  statusLabel: {
    color: colors.muted,
    marginBottom: 4
  },
  statusValue: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "900"
  },
  card: {
    borderRadius: 22,
    borderWidth: 1,
    borderColor: colors.border,
    backgroundColor: colors.surface,
    padding: spacing.lg,
    gap: spacing.md
  },
  cardTitle: {
    color: colors.text,
    fontSize: 20,
    fontWeight: "900"
  },
  cardHeader: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    gap: spacing.md
  },
  syncButton: {
    width: 48,
    height: 48,
    borderRadius: 14,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center"
  },
  muted: {
    color: colors.muted,
    lineHeight: 20
  },
  serverText: {
    color: colors.text,
    fontWeight: "800"
  }
});

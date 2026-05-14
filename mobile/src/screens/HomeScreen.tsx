import { useEffect, useState } from "react";

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
      <NativeScreenContent
        key={activeScreen}
        screen={activeScreen}
        onOpenCamera={() => setCameraOpen(true)}
        onRefreshPending={refreshPending}
        sync={{
          pending,
          message: syncMessage,
          endpointUrl: endpointUrl || DEFAULT_SERVER_URL,
          onSyncNow: syncNow
        }}
      />
    </AppShell>
  );
}

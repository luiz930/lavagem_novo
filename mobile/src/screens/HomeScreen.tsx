import { useEffect, useRef, useState } from "react";

import { UserSession } from "../auth/authRepository";
import { DEFAULT_SERVER_URL, normalizeServerUrl } from "../config";
import { getSetting, setSetting } from "../database/db";
import { fetchMobileConfig, fetchMobileHud, getSyncDiagnostics, runSync, updateMobileVersion } from "../sync/syncService";
import { MobileHudPayload, MobileSiteState, SyncDiagnostics } from "../sync/types";
import { AppScreenKey, AppShell } from "./AppShell";
import { CameraScreen, CameraTarget } from "./CameraScreen";
import { NativeScreenContent, screenTitle } from "./NativeScreens";

type Props = {
  session: UserSession;
  onLogout: () => void;
};

export function HomeScreen({ session, onLogout }: Props) {
  const syncInFlight = useRef(false);
  const hudInFlight = useRef(false);
  const syncDebounceTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const retryBackoffSeconds = useRef(0);
  const retryAfterTs = useRef(0);
  const [pending, setPending] = useState(0);
  const [pendingPhotos, setPendingPhotos] = useState(0);
  const [syncMessage, setSyncMessage] = useState("Banco local ativo");
  const [syncing, setSyncing] = useState(false);
  const [cameraTarget, setCameraTarget] = useState<CameraTarget | null>(null);
  const [activeScreen, setActiveScreen] = useState<AppScreenKey>("inicio");
  const [endpointUrl, setEndpointUrl] = useState("");
  const [syncToken, setSyncToken] = useState("");
  const [hud, setHud] = useState<MobileHudPayload | null>(null);
  const [siteState, setSiteState] = useState<MobileSiteState | null>(null);
  const [diagnostics, setDiagnostics] = useState<SyncDiagnostics | null>(null);
  const [appVersion, setAppVersion] = useState("");
  const [updatedAt, setUpdatedAt] = useState("");

  async function refreshSyncStatus() {
    const status = await getSyncDiagnostics();
    setDiagnostics(status);
    setPending(status.pending);
    setPendingPhotos(status.pendingPhotos);
    return status;
  }

  useEffect(() => {
    refreshSyncStatus();
    getSetting("sync_endpoint_url").then((value) => setEndpointUrl(normalizeServerUrl(value || DEFAULT_SERVER_URL)));
    getSetting("sync_token").then(async (value) => {
      const token = value || session.onlineToken || "";
      if (!value && token) {
        await setSetting("sync_token", token);
      }
      setSyncToken(token);
      getSetting("site_version").then(setAppVersion);
    });
    getSetting("site_state_cache").then((value) => {
      if (!value) {
        return;
      }
      try {
        const cached = JSON.parse(value);
        if (cached.hud) {
          setHud(cached.hud);
        }
        if (cached.siteState) {
          setSiteState(cached.siteState);
        }
        if (cached.version) {
          setAppVersion(cached.version);
        }
        if (cached.updatedAt) {
          setUpdatedAt(cached.updatedAt);
        }
      } catch {
        undefined;
      }
    });
  }, []);

  useEffect(() => {
    if (endpointUrl && (syncToken || session.onlineToken)) {
      syncNow();
    }
  }, [endpointUrl, syncToken]);

  useEffect(() => {
    if (!endpointUrl) {
      return;
    }
    const timer = setInterval(() => {
      refreshHud();
    }, 10000);
    return () => clearInterval(timer);
  }, [endpointUrl, syncToken]);

  useEffect(() => {
    if (!endpointUrl) {
      return;
    }
    const timer = setInterval(async () => {
      const status = await refreshSyncStatus();
      if (status.pending <= 0 && status.pendingPhotos <= 0) {
        return;
      }
      if (Date.now() < retryAfterTs.current) {
        return;
      }
      runFullSync(false);
    }, 30000);
    return () => clearInterval(timer);
  }, [endpointUrl, syncToken]);

  useEffect(() => () => {
    if (syncDebounceTimer.current) {
      clearTimeout(syncDebounceTimer.current);
    }
  }, []);

  async function syncNow() {
    await runFullSync(true);
  }

  async function runFullSync(manual: boolean) {
    if (syncInFlight.current) {
      return;
    }
    syncInFlight.current = true;
    setSyncing(true);
    const normalizedUrl = normalizeServerUrl(endpointUrl || DEFAULT_SERVER_URL);
    await setSetting("sync_endpoint_url", normalizedUrl);
    const savedToken = syncToken.trim() || await getSetting("sync_token");
    try {
      if (!savedToken) {
        setSyncMessage("Token mobile ausente. Entre online uma vez para liberar a sincronizacao.");
        return;
      }
      const result = await runSync({ endpointUrl: normalizedUrl, token: savedToken });
      if (result.error) {
        retryBackoffSeconds.current = Math.min(Math.max(retryBackoffSeconds.current * 2 || 20, 20), 120);
        retryAfterTs.current = Date.now() + retryBackoffSeconds.current * 1000;
        setSyncMessage(`${result.error}. Nova tentativa em ${retryBackoffSeconds.current}s.`);
      } else {
        retryBackoffSeconds.current = 0;
        retryAfterTs.current = 0;
        const partes = [`Enviado: ${result.sent}`, `Recebido: ${result.pulled}`];
        if (result.photosUploaded) {
          partes.push(`Fotos: ${result.photosUploaded}`);
        }
        if (result.durationMs) {
          partes.push(`${(result.durationMs / 1000).toFixed(1)}s`);
        }
        setSyncMessage(partes.join(" | "));
      }
      await refreshSyncStatus();
      await refreshHud(normalizedUrl, savedToken);
    } finally {
      syncInFlight.current = false;
      setSyncing(false);
    }
  }

  async function refreshHud(url = endpointUrl, token = syncToken) {
    if (hudInFlight.current) {
      return;
    }
    hudInFlight.current = true;
    const normalizedUrl = normalizeServerUrl(url || DEFAULT_SERVER_URL);
    const savedToken = token.trim() || await getSetting("sync_token");
    if (!savedToken) {
      hudInFlight.current = false;
      return;
    }

    try {
      const [hudResult, configResult] = await Promise.all([
        fetchMobileHud({ endpointUrl: normalizedUrl, token: savedToken }),
        fetchMobileConfig({ endpointUrl: normalizedUrl, token: savedToken })
      ]);

      if (hudResult.hud) {
        setHud(hudResult.hud);
      }
      if (hudResult.site) {
        setSiteState(hudResult.site);
      }

      const version = configResult.version || hudResult.version;
      if (version) {
        setAppVersion(version);
        await setSetting("site_version", version);
      }

      if (hudResult.error || configResult.error) {
        setSyncMessage(hudResult.error || configResult.error || "Falha ao carregar dados do site.");
      }
      const snapshotUpdatedAt = new Date().toISOString();
      setUpdatedAt(snapshotUpdatedAt);
      await setSetting("site_state_cache", JSON.stringify({
        hud: hudResult.hud || hud,
        siteState: hudResult.site || siteState,
        version: version || appVersion,
        updatedAt: snapshotUpdatedAt
      }));
    } finally {
      hudInFlight.current = false;
    }
  }

  async function saveVersionOnSite(version: string) {
    const normalizedUrl = normalizeServerUrl(endpointUrl || DEFAULT_SERVER_URL);
    const savedToken = syncToken.trim() || await getSetting("sync_token");
    const result = await updateMobileVersion({ endpointUrl: normalizedUrl, token: savedToken }, version);
    if (result.error) {
      setSyncMessage(result.error);
      return result.error;
    }
    const savedVersion = result.version || version;
    setAppVersion(savedVersion);
    await setSetting("site_version", savedVersion);
    setSyncMessage(`Versao ${savedVersion} salva no site`);
    await refreshHud(normalizedUrl, savedToken);
    return `Versao ${savedVersion} salva no site`;
  }

  async function handleLocalSaved() {
    await refreshSyncStatus();
    if (syncDebounceTimer.current) {
      clearTimeout(syncDebounceTimer.current);
    }
    syncDebounceTimer.current = setTimeout(() => {
      runFullSync(false);
    }, 1200);
  }

  if (cameraTarget) {
    return <CameraScreen session={session} target={cameraTarget} onClose={() => setCameraTarget(null)} onSaved={handleLocalSaved} />;
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
        screen={activeScreen}
        onOpenCamera={setCameraTarget}
        onRefreshPending={handleLocalSaved}
        sync={{
          pending,
          pendingPhotos,
          message: syncMessage,
          syncing,
          endpointUrl: endpointUrl || DEFAULT_SERVER_URL,
          hud,
          siteState,
          diagnostics,
          updatedAt,
          version: appVersion,
          onRefreshHud: refreshHud,
          onSyncNow: syncNow,
          onUpdateVersion: saveVersionOnSite
        }}
      />
    </AppShell>
  );
}

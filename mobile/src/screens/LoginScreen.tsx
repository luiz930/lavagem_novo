import { useState } from "react";
import {
  KeyboardAvoidingView,
  Platform,
  Pressable,
  StyleSheet,
  Text,
  TextInput,
  View
} from "react-native";

import { loginOffline, loginOnline, UserSession } from "../auth/authRepository";
import { DEFAULT_SERVER_URL } from "../config";
import { runSync } from "../sync/syncService";
import { colors, spacing } from "../theme";

type Props = {
  onLoggedIn: (session: UserSession) => void;
};

export function LoginScreen({ onLoggedIn }: Props) {
  const [usuario, setUsuario] = useState("");
  const [senha, setSenha] = useState("");
  const [mostrarSenha, setMostrarSenha] = useState(false);
  const [erro, setErro] = useState("");
  const [loading, setLoading] = useState(false);

  async function submit() {
    setErro("");
    setLoading(true);
    try {
      const session = await loginOnline(DEFAULT_SERVER_URL, usuario, senha);
      await runSync({ endpointUrl: DEFAULT_SERVER_URL, token: session.onlineToken || "" });
      onLoggedIn(session);
    } catch (error) {
      try {
        onLoggedIn(await loginOffline(usuario, senha));
      } catch {
        setErro(error instanceof Error ? error.message : "Nao foi possivel entrar.");
      }
    } finally {
      setLoading(false);
    }
  }

  return (
    <KeyboardAvoidingView
      behavior={Platform.OS === "ios" ? "padding" : undefined}
      style={styles.screen}
    >
      <View style={styles.header}>
        <View style={styles.logo}>
          <Text style={styles.logoText}>W</Text>
        </View>
        <View>
          <Text style={styles.kicker}>WAGEN ESTETICA</Text>
          <Text style={styles.title}>Login</Text>
          <Text style={styles.subtitle}>Acesso offline com sincronizacao posterior</Text>
        </View>
      </View>

      <View style={styles.card}>
        <TextInput
          autoCapitalize="none"
          autoComplete="username"
          onChangeText={setUsuario}
          placeholder="Usuario"
          placeholderTextColor={colors.muted}
          style={styles.input}
          value={usuario}
        />
        <View style={styles.passwordRow}>
          <TextInput
            autoCapitalize="none"
            autoComplete="password"
            onChangeText={setSenha}
            placeholder="Senha"
            placeholderTextColor={colors.muted}
            secureTextEntry={!mostrarSenha}
            style={[styles.input, styles.passwordInput]}
            value={senha}
          />
          <Pressable onPress={() => setMostrarSenha((value) => !value)} style={styles.secondaryButton}>
            <Text style={styles.secondaryButtonText}>{mostrarSenha ? "Ocultar" : "Mostrar"}</Text>
          </Pressable>
        </View>

        <Pressable disabled={loading} onPress={submit} style={styles.primaryButton}>
          <Text style={styles.primaryButtonText}>{loading ? "Entrando..." : "Entrar"}</Text>
        </Pressable>

        {erro ? <Text style={styles.error}>{erro}</Text> : null}
        <Text style={styles.version}>Conectado ao site Wagen</Text>
        <Text style={styles.version}>App 0.1.0 offline-first</Text>
      </View>
    </KeyboardAvoidingView>
  );
}

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    justifyContent: "center",
    padding: spacing.lg,
    backgroundColor: colors.bg
  },
  header: {
    flexDirection: "row",
    alignItems: "center",
    gap: spacing.md,
    marginBottom: spacing.lg
  },
  logo: {
    width: 72,
    height: 72,
    borderRadius: 36,
    borderWidth: 3,
    borderColor: "rgba(250, 204, 21, 0.45)",
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: colors.surface
  },
  logoText: {
    color: colors.primary,
    fontSize: 34,
    fontWeight: "900"
  },
  kicker: {
    color: colors.primary,
    fontSize: 12,
    fontWeight: "800",
    letterSpacing: 1.7
  },
  title: {
    color: colors.text,
    fontSize: 34,
    fontWeight: "900"
  },
  subtitle: {
    color: colors.muted,
    marginTop: 4
  },
  card: {
    borderWidth: 1,
    borderColor: colors.border,
    borderRadius: 22,
    backgroundColor: colors.surface,
    padding: spacing.lg,
    gap: spacing.md
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
  passwordRow: {
    flexDirection: "row",
    gap: spacing.sm
  },
  passwordInput: {
    flex: 1
  },
  primaryButton: {
    minHeight: 50,
    borderRadius: 14,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: colors.primary
  },
  primaryButtonText: {
    color: "#111827",
    fontWeight: "900"
  },
  secondaryButton: {
    minWidth: 88,
    borderRadius: 14,
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
    borderColor: colors.border
  },
  secondaryButtonText: {
    color: colors.text,
    fontWeight: "800"
  },
  error: {
    color: colors.danger,
    textAlign: "center"
  },
  version: {
    color: colors.muted,
    textAlign: "center"
  }
});

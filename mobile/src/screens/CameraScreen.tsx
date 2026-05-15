import { CameraView, useCameraPermissions } from "expo-camera";
import * as FileSystem from "expo-file-system";
import { useRef, useState } from "react";
import { Pressable, StyleSheet, Text, View } from "react-native";
import { Ionicons } from "@expo/vector-icons";

import { UserSession } from "../auth/authRepository";
import { finalizarServico, FotoTipo, salvarFotoAtendimento } from "../data/localRepository";
import { newUuid } from "../database/db";
import { colors, spacing } from "../theme";

export type CameraTarget = {
  servico_uuid: string;
  tipo: FotoTipo;
  titulo: string;
  finalizarAoSalvar?: boolean;
};

type Props = {
  session: UserSession;
  target: CameraTarget;
  onClose: () => void;
  onSaved: () => void;
};

export function CameraScreen({ session, target, onClose, onSaved }: Props) {
  const cameraRef = useRef<CameraView | null>(null);
  const [permission, requestPermission] = useCameraPermissions();
  const [saving, setSaving] = useState(false);
  const [savedCount, setSavedCount] = useState(0);
  const [finalizado, setFinalizado] = useState(false);

  async function takePhoto() {
    if (!cameraRef.current || saving) {
      return;
    }

    setSaving(true);
    try {
      const photo = await cameraRef.current.takePictureAsync({ quality: 0.78 });
      if (!photo?.uri) {
        throw new Error("Nao foi possivel capturar a foto.");
      }
      const uuid = newUuid();
      const targetDir = `${FileSystem.documentDirectory}fotos/`;
      await FileSystem.makeDirectoryAsync(targetDir, { intermediates: true });
      const targetUri = `${targetDir}${uuid}.jpg`;
      await FileSystem.copyAsync({ from: photo.uri, to: targetUri });
      const info = await FileSystem.getInfoAsync(targetUri);

      await salvarFotoAtendimento({
        servico_uuid: target.servico_uuid,
        tipo: target.tipo,
        uri_local: targetUri,
        mime_type: "image/jpeg",
        usuario: session.usuario,
        usuario_nome: session.nome,
        tamanho_bytes: info.exists ? Number(info.size || 0) : 0,
        largura: Number(photo.width || 0),
        altura: Number(photo.height || 0)
      });
      if (target.finalizarAoSalvar && !finalizado) {
        await finalizarServico(target.servico_uuid);
        setFinalizado(true);
      }
      setSavedCount((value) => value + 1);
      await onSaved();
    } finally {
      setSaving(false);
    }
  }

  if (!permission) {
    return <View style={styles.center} />;
  }

  if (!permission.granted) {
    return (
      <View style={styles.center}>
        <Text style={styles.title}>Camera bloqueada</Text>
        <Text style={styles.muted}>Autorize a camera para registrar fotos dos atendimentos.</Text>
        <Pressable onPress={requestPermission} style={styles.primaryButton}>
          <Text style={styles.primaryButtonText}>Autorizar camera</Text>
        </Pressable>
        <Pressable onPress={onClose} style={styles.secondaryButton}>
          <Text style={styles.secondaryButtonText}>Voltar</Text>
        </Pressable>
      </View>
    );
  }

  return (
    <View style={styles.root}>
      <CameraView ref={cameraRef} style={styles.camera} facing="back" />
      <View style={styles.header}>
        <Text style={styles.headerText}>{target.titulo}</Text>
        <Text style={styles.headerSubText}>{savedCount} foto(s) salva(s). Tire quantas precisar e toque em concluir.</Text>
      </View>
      <View style={styles.toolbar}>
        <Pressable onPress={onClose} style={styles.iconButton}>
          <Ionicons color={colors.text} name="close" size={26} />
        </Pressable>
        <Pressable onPress={takePhoto} style={styles.captureButton}>
          <Ionicons color={colors.primaryText} name="camera" size={30} />
        </Pressable>
        <Pressable onPress={onClose} style={styles.doneButton}>
          <Text style={styles.doneButtonText}>Concluir</Text>
        </Pressable>
      </View>
      {saving ? <Text style={styles.saving}>Salvando no banco local...</Text> : null}
    </View>
  );
}

const styles = StyleSheet.create({
  root: {
    flex: 1,
    backgroundColor: colors.bg
  },
  camera: {
    flex: 1
  },
  header: {
    position: "absolute",
    left: spacing.lg,
    right: spacing.lg,
    top: spacing.xl,
    backgroundColor: "rgba(17, 24, 39, 0.9)",
    borderRadius: 14,
    padding: spacing.md
  },
  headerText: {
    color: colors.text,
    fontWeight: "900",
    textAlign: "center"
  },
  headerSubText: {
    color: colors.muted,
    textAlign: "center",
    marginTop: 4,
    fontSize: 12
  },
  toolbar: {
    position: "absolute",
    left: spacing.lg,
    right: spacing.lg,
    bottom: spacing.xl,
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between"
  },
  iconButton: {
    width: 52,
    height: 52,
    borderRadius: 18,
    backgroundColor: "rgba(17, 24, 39, 0.9)",
    alignItems: "center",
    justifyContent: "center"
  },
  doneButton: {
    minWidth: 82,
    height: 52,
    borderRadius: 18,
    backgroundColor: "rgba(17, 24, 39, 0.9)",
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
    borderColor: colors.border
  },
  doneButtonText: {
    color: colors.text,
    fontWeight: "900"
  },
  captureButton: {
    width: 74,
    height: 74,
    borderRadius: 37,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center"
  },
  saving: {
    position: "absolute",
    left: spacing.lg,
    right: spacing.lg,
    bottom: 118,
    color: colors.text,
    textAlign: "center",
    backgroundColor: "rgba(17, 24, 39, 0.9)",
    padding: spacing.md,
    borderRadius: 14
  },
  center: {
    flex: 1,
    padding: spacing.lg,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: colors.bg,
    gap: spacing.md
  },
  title: {
    color: colors.text,
    fontSize: 26,
    fontWeight: "900"
  },
  muted: {
    color: colors.muted,
    textAlign: "center"
  },
  primaryButton: {
    minHeight: 50,
    borderRadius: 14,
    paddingHorizontal: spacing.lg,
    backgroundColor: colors.primary,
    alignItems: "center",
    justifyContent: "center"
  },
  primaryButtonText: {
    color: colors.primaryText,
    fontWeight: "900"
  },
  secondaryButton: {
    minHeight: 50,
    borderRadius: 14,
    paddingHorizontal: spacing.lg,
    borderWidth: 1,
    borderColor: colors.border,
    alignItems: "center",
    justifyContent: "center"
  },
  secondaryButtonText: {
    color: colors.text,
    fontWeight: "900"
  }
});

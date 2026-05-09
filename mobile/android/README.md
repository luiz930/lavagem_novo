# Wagen Banco Android

APK Android do sistema Wagen Estetica.

O app nao conecta diretamente no Supabase. Ele abre `/app-banco` no sistema Flask em WebView e usa as APIs autenticadas do servidor. Assim, a senha do banco continua protegida no backend.

## URL do servidor

A URL padrao fica em `app/build.gradle`:

```gradle
resValue "string", "app_base_url", "https://wagenestetica.duckdns.org"
```

Para buildar apontando para outro servidor:

```powershell
gradle :app:assembleDebug -PappBaseUrl=https://seu-dominio.com
```

## Build local

Requisitos:

- Android SDK instalado
- Java 17
- Gradle

Com os requisitos instalados:

```powershell
cd mobile/android
gradle :app:assembleDebug
```

O APK sai em:

```text
mobile/android/app/build/outputs/apk/debug/app-debug.apk
```

## Build no GitHub

O workflow `Build Android APK` gera o APK como artefato em cada execucao manual.

# Wagen Estetica Android Nativo

APK Android nativo do sistema Wagen Estetica.

Este app nao usa WebView e nao depende do Flask. Ele conversa diretamente com o Supabase usando:

- Supabase Auth para login.
- PostgREST para `clientes`, `veiculos`, `servicos` e `fotos`.
- Supabase Storage para upload de fotos no bucket `fotos`.

## Seguranca

Nao coloque `DATABASE_URL`, senha PostgreSQL, service role key ou senha do pooler dentro do APK. Um APK pode ser extraido por qualquer pessoa que o instalar.

O build usa somente:

- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`

As permissoes reais devem ser controladas no Supabase por RLS.

## Configurar build no GitHub

Cadastre os secrets em:

```text
Settings > Secrets and variables > Actions
```

Secrets esperados:

```text
SUPABASE_URL=https://SEU-PROJETO.supabase.co
SUPABASE_ANON_KEY=SUA_ANON_KEY
```

Depois rode o workflow `Build Android APK` ou faça push em `mobile/android/**`.

## Build local

Requisitos:

- Android SDK instalado
- Java 17
- Gradle

Com os requisitos instalados:

```powershell
cd mobile/android
gradle :app:assembleDebug -PsupabaseUrl=https://SEU-PROJETO.supabase.co -PsupabaseAnonKey=SUA_ANON_KEY
```

O APK sai em:

```text
mobile/android/app/build/outputs/apk/debug/app-debug.apk
```

## Telas nativas iniciais

- Login Supabase Auth.
- Painel com contagens.
- Cadastro e lista de clientes.
- Cadastro e lista de atendimentos.
- Captura de foto com camera nativa e upload para Supabase Storage.
- Tela de conexao/configuracao.

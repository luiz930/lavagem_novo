# Deploy

## Variaveis minimas

```env
DATABASE_BACKEND=postgres
STRICT_ONLINE_DATABASE=true
SUPABASE_DATABASE_URL=
FLASK_SECRET_KEY=
SESSION_COOKIE_SECURE=1
CSRF_PROTECTION=1
TELEMETRIA_ATIVA=1
```

## Google Drive

Opcao 1:

```env
GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON=
GOOGLE_DRIVE_FOLDER_ID=
```

Opcao 2:

```env
GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE=/caminho/google_drive_service_account.json
GOOGLE_DRIVE_FOLDER_ID=
```

## Deploy Linux basico

```bash
git pull
pip install -r requirements.txt
python -m py_compile app.py
python -m unittest discover -s tests -v
gunicorn app:app --bind 0.0.0.0:5000
```

## Observacoes

- o projeto espera banco online quando `STRICT_ONLINE_DATABASE=true`
- o JSON do Google Drive nao deve ir para o Git
- as migrations de fundacao rodam no boot via `init_db()`
- a tela `/diagnostico` valida banco, CSRF, cookie seguro, backup e Google Drive
- a tela `/empresas` controla empresa ativa, plano, status da licenca e limites

## Checklist de producao

- `FLASK_SECRET_KEY` preenchida com valor unico fora do Git
- `CSRF_PROTECTION=1`
- `SESSION_COOKIE_SECURE=1` em HTTPS
- `DATABASE_BACKEND=postgres`
- `STRICT_ONLINE_DATABASE=true`
- `SUPABASE_DATABASE_URL` preenchida com senha real fora do Git
- backup manual validado antes de migracoes ou importacoes grandes
- destino externo do backup configurado quando houver backup online contratado
- empresa, plano, status e validade revisados em `/empresas`

## Release 1.0.0

Para sair do beta, use a porta de saida em `RELEASE_1_0_0.md`. O bump para `1.0.0` deve acontecer somente depois da validacao no ambiente real, com backup novo e smoke test concluido.

# Instalacao

## Requisitos

- Python 3.11+
- PostgreSQL/Supabase opcional
- credencial do Google Drive opcional

## 1. Clonar e instalar

```bash
git clone <repo>
cd lavagem_novo
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

No Windows:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. Configurar ambiente

Copie o template:

```bash
cp .env.example .env
```

Preencha ao menos:

```env
DATABASE_BACKEND=postgres
STRICT_ONLINE_DATABASE=true
SUPABASE_DATABASE_URL=
FLASK_SECRET_KEY=
```

## 3. Subir o sistema

Para rodar localmente no Windows sem misturar com as configuracoes de producao do `.env`, use:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_local.ps1
```

Esse atalho forca `SESSION_COOKIE_SECURE=0` e `DATABASE_BACKEND=sqlite` somente no processo local. Isso evita erro de token CSRF ao abrir o login por `http://127.0.0.1:5000` ou pelo IP local mostrado no terminal.

Se quiser subir manualmente:

```powershell
$env:DATABASE_BACKEND="sqlite"
$env:STRICT_ONLINE_DATABASE="false"
$env:SESSION_COOKIE_SECURE="0"
$env:CSRF_PROTECTION="1"
python -m flask --app app run --host 0.0.0.0 --port 5000
```

Modo simples:

```bash
python app.py
```

## 4. Validar

```bash
python -m py_compile app.py
python -m unittest discover -s tests -v
```

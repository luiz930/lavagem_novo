# Rodar em casa no Windows com Cloudflare Tunnel + Supabase

## 1. Instalar dependencias do projeto

No PowerShell, dentro da pasta do projeto:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 2. Garantir que o banco online esta configurado

No arquivo `.env`, deixe o projeto apontando para o Supabase.

Exemplo minimo:

```env
DATABASE_BACKEND=postgres
STRICT_ONLINE_DATABASE=true
SUPABASE_DATABASE_URL=postgresql://usuario:senha@host:5432/postgres
```

## 3. Subir o site no Windows

```powershell
.venv\Scripts\Activate.ps1
python run_windows_home.py
```

Ou apenas:

```powershell
iniciar_em_casa.bat
```

O site vai responder em:

```text
http://127.0.0.1:5000
```

## 4. Instalar o Cloudflare Tunnel

Baixe o `cloudflared` para Windows e instale/extraia conforme a documentacao oficial.

## 5. Criar e autenticar o tunnel

No PowerShell:

```powershell
cloudflared tunnel login
cloudflared tunnel create lavagem-casa
cloudflared tunnel route dns lavagem-casa seu-subdominio.seudominio.com
```

## 6. Criar o arquivo de configuracao do tunnel

Crie o arquivo:

```text
%USERPROFILE%\.cloudflared\config.yml
```

Com este conteudo:

```yaml
tunnel: UUID_DO_TUNNEL
credentials-file: C:\Users\SEU_USUARIO\.cloudflared\UUID_DO_TUNNEL.json

ingress:
  - hostname: seu-subdominio.seudominio.com
    service: http://localhost:5000
  - service: http_status:404
```

## 7. Rodar o tunnel

Teste manual:

```powershell
cloudflared tunnel run lavagem-casa
```

Se funcionar, instale como servico no Windows:

```powershell
cloudflared service install
```

## 8. Resultado final

- `Supabase` guarda os dados
- `PC de casa` roda o Flask/Waitress
- `Cloudflare Tunnel` publica o acesso sem abrir porta no roteador

## 9. Observacoes praticas

- O PC precisa ficar ligado.
- Se a internet ou energia cair, o site cai.
- O arquivo `google_drive_service_account.json` nao deve ir para o GitHub.
- Para uso pela empresa, prefira um DNS fixo no Cloudflare e deixe o Windows iniciar o `cloudflared` automaticamente.

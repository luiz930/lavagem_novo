# Monitoramento no Telegram

O monitor testa o site em producao e envia um relatorio para o Telegram. Ele valida:

- HTTPS do dominio principal
- Tela de login
- Manifest do PWA
- Service worker
- Endpoint `/api/pwa/status`

## Seguranca do token

Nao grave o token do bot no repositorio. Configure sempre por variavel de ambiente, secret do GitHub ou arquivo protegido no servidor.

Se o token foi colado em conversa, print ou local publico, gere outro no BotFather.

## Descobrir o chat_id

1. Abra o Telegram e envie `/start` para `@wagenesteticabot`.
2. No servidor ou no computador local, rode:

```bash
curl "https://api.telegram.org/botSEU_TOKEN/getUpdates"
```

3. Procure no retorno o campo `chat.id`.
4. Use esse valor como `TELEGRAM_CHAT_ID`.

## Rodar manualmente

```bash
export SITE_MONITOR_URL="https://wagenestetica.duckdns.org"
export TELEGRAM_BOT_TOKEN="SEU_TOKEN"
export TELEGRAM_CHAT_ID="SEU_CHAT_ID"
python scripts/site_monitor.py
```

## GitHub Actions

O workflow `.github/workflows/testes_periodicos.yml` ja roda a cada 2 horas.

Configure estes secrets no GitHub:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Opcionalmente configure a variable:

- `SITE_MONITOR_URL=https://wagenestetica.duckdns.org`

## VPS com systemd timer

Crie o arquivo de ambiente fora do repositorio:

```bash
tee /etc/lavagem-monitor.env >/dev/null <<'EOF'
SITE_MONITOR_URL=https://wagenestetica.duckdns.org
SITE_MONITOR_TIMEOUT=15
TELEGRAM_BOT_TOKEN=SEU_TOKEN
TELEGRAM_CHAT_ID=SEU_CHAT_ID
EOF

chmod 600 /etc/lavagem-monitor.env
```

Crie o servico:

```bash
tee /etc/systemd/system/lavagem-monitor.service >/dev/null <<'EOF'
[Unit]
Description=Monitoramento Wagen Estetica

[Service]
Type=oneshot
WorkingDirectory=/root/lavagem_novo
EnvironmentFile=/etc/lavagem-monitor.env
ExecStart=/usr/bin/python3 /root/lavagem_novo/scripts/site_monitor.py
EOF
```

Crie o timer de 2 em 2 horas:

```bash
tee /etc/systemd/system/lavagem-monitor.timer >/dev/null <<'EOF'
[Unit]
Description=Executa monitoramento Wagen Estetica a cada 2 horas

[Timer]
OnBootSec=5min
OnUnitActiveSec=2h
Unit=lavagem-monitor.service

[Install]
WantedBy=timers.target
EOF
```

Ative:

```bash
systemctl daemon-reload
systemctl enable --now lavagem-monitor.timer
systemctl start lavagem-monitor.service
systemctl list-timers lavagem-monitor.timer --no-pager
```

Ver logs:

```bash
journalctl -u lavagem-monitor.service -n 80 --no-pager
```

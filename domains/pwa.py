def montar_manifesto_pwa(produto):
    produto = dict(produto or {})
    nome_app = produto.get("site_title") or produto.get("brand_name") or "Gestao Estetica"
    nome_curto = (produto.get("brand_name") or "Gestao")[:24]
    cor_fundo = produto.get("brand_background_color") or "#0b0b0b"
    cor_tema = produto.get("brand_primary_color") or "#facc15"
    icone_192 = "/static/icon-192.jpg"
    icone_512 = "/static/icon-512.jpg"

    return {
        "id": "/?source=pwa",
        "name": nome_app,
        "short_name": nome_curto,
        "description": "Aplicativo operacional para gestao de estetica automotiva, atendimentos, fotos, clientes, financeiro e licencas.",
        "start_url": "/?source=pwa",
        "scope": "/",
        "display": "standalone",
        "display_override": ["standalone", "minimal-ui", "browser"],
        "orientation": "portrait",
        "background_color": cor_fundo,
        "theme_color": cor_tema,
        "categories": ["business", "productivity", "utilities"],
        "lang": "pt-BR",
        "dir": "ltr",
        "prefer_related_applications": False,
        "capture_links": "existing-client-navigate",
        "launch_handler": {"client_mode": "navigate-existing"},
        "permissions": ["camera", "microphone"],
        "shortcuts": [
            {
                "name": "Painel operacional",
                "short_name": "Painel",
                "description": "Abrir atendimentos em andamento.",
                "url": "/painel?source=pwa_shortcut",
                "icons": [{"src": icone_192, "sizes": "192x192", "type": "image/jpeg", "purpose": "any maskable"}],
            },
            {
                "name": "Novo atendimento",
                "short_name": "Atender",
                "description": "Abrir a tela inicial para iniciar atendimento.",
                "url": "/?source=pwa_shortcut",
                "icons": [{"src": icone_192, "sizes": "192x192", "type": "image/jpeg", "purpose": "any maskable"}],
            },
        ],
        "icons": [
            {
                "src": icone_192,
                "sizes": "192x192",
                "type": "image/jpeg",
                "purpose": "any maskable",
            },
            {
                "src": icone_512,
                "sizes": "512x512",
                "type": "image/jpeg",
                "purpose": "any maskable",
            },
        ],
    }


def montar_status_pwa(request_is_secure=False, host=""):
    host = str(host or "")
    seguro = bool(request_is_secure or host.startswith(("localhost", "127.0.0.1")))
    return {
        "ok": seguro,
        "secure_context_required": True,
        "secure_request": bool(request_is_secure),
        "host": host,
        "manifest_url": "/site.webmanifest",
        "service_worker_url": "/sw.js",
        "service_worker_scope": "/",
        "mensagem": (
            "PWA pronto para instalacao."
            if seguro else
            "Para instalar como app no Chrome Android, acesse por HTTPS com certificado valido. Em HTTP o Chrome permite apenas criar atalho."
        ),
    }

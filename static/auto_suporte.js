(function iniciarAutoSuporte() {
    if (window.__autoSuporteIniciado) {
        return;
    }
    window.__autoSuporteIniciado = true;

    const estado = {
        status: null,
        aberto: localStorage.getItem("wagen_auto_suporte_aberto") === "1",
        logs: [],
    };

    function criarElemento(tag, classe, texto) {
        const el = document.createElement(tag);
        if (classe) el.className = classe;
        if (texto) el.textContent = texto;
        return el;
    }

    function obterCsrfToken() {
        const meta = document.querySelector('meta[name="csrf-token"]');
        if (meta && meta.getAttribute("content")) {
            return meta.getAttribute("content");
        }
        const input = document.querySelector('input[name="_csrf_token"]');
        return input ? input.value : "";
    }

    function textoFalhas(status) {
        const falhas = status && Array.isArray(status.falhas) ? status.falhas : [];
        if (!falhas.length) {
            return "[OK] nenhum incidente critico";
        }
        return falhas.map((falha) => `[WARN] ${falha}`).join("\n");
    }

    function renderizarSugestoes(status) {
        const sugestoes = Array.isArray(status.sugestoes) ? status.sugestoes : [];
        if (!sugestoes.length) {
            return null;
        }
        const bloco = criarElemento("div", "auto-support-suggestions");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", "Sugestoes seguras"));
        sugestoes.forEach((item) => {
            const card = criarElemento("div", "auto-support-suggestion");
            card.appendChild(criarElemento("p", "auto-support-log-title", item.titulo || "Revisao sugerida"));
            card.appendChild(criarElemento("p", "auto-support-log-message", item.mensagem || "Revisar este ponto."));
            if (item.acao) {
                const btn = criarElemento("button", "auto-support-mini-action", "Executar acao sugerida");
                btn.type = "button";
                btn.addEventListener("click", () => executarAcao(item.acao, item.titulo || "Acao sugerida"));
                card.appendChild(btn);
            }
            bloco.appendChild(card);
        });
        return bloco;
    }

    function adicionarLog(titulo, mensagem, ok) {
        estado.logs.unshift({
            titulo,
            mensagem,
            ok: ok !== false,
            quando: new Date().toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit", second: "2-digit" }),
        });
        estado.logs = estado.logs.slice(0, 6);
    }

    function abrirPainel() {
        estado.aberto = true;
        localStorage.setItem("wagen_auto_suporte_aberto", "1");
        renderizar();
    }

    function fecharPainel() {
        estado.aberto = false;
        localStorage.setItem("wagen_auto_suporte_aberto", "0");
        renderizar();
    }

    async function carregarStatus({ abrirSeFalha = false } = {}) {
        try {
            const resposta = await fetch("/api/auto-suporte/status", { cache: "no-store" });
            if (!resposta.ok) return;
            estado.status = await resposta.json();
            if (abrirSeFalha && estado.status && estado.status.ok === false) {
                abrirPainel();
            } else {
                renderizar();
            }
        } catch (erro) {
            adicionarLog("Falha no AutoSuporte", String(erro), false);
            renderizar();
        }
    }

    async function executarAcao(acao, label) {
        adicionarLog(label, "Executando acao segura...", true);
        abrirPainel();
        try {
            const resposta = await fetch("/api/auto-suporte/acao", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRF-Token": obterCsrfToken(),
                },
                body: JSON.stringify({ acao }),
            });
            const dados = await resposta.json();
            if (!resposta.ok || dados.erro) {
                throw new Error(dados.erro || "Falha ao executar acao.");
            }
            estado.status = dados.status || estado.status;
            adicionarLog(dados.titulo || label, dados.mensagem || "Acao concluida.", dados.ok !== false);
            abrirPainel();
        } catch (erro) {
            adicionarLog(label, String(erro), false);
            abrirPainel();
        }
    }

    function renderizar() {
        let raiz = document.querySelector("[data-auto-suporte-widget]");
        if (!raiz) {
            raiz = criarElemento("div", "auto-support-widget");
            raiz.setAttribute("data-auto-suporte-widget", "1");
            document.body.appendChild(raiz);
        }

        raiz.innerHTML = "";

        const status = estado.status || {};
        const bubble = criarElemento("button", "auto-support-bubble");
        bubble.type = "button";
        bubble.title = "AutoSuporte";
        bubble.setAttribute("aria-label", "Abrir AutoSuporte");
        bubble.setAttribute("data-alert", status.ok === false ? "true" : "false");
        bubble.innerHTML = `
            <span class="auto-support-bot" aria-hidden="true">
                <span class="auto-support-bot-antenna"></span>
                <span class="auto-support-bot-face">
                    <span class="auto-support-bot-eye"></span>
                    <span class="auto-support-bot-eye"></span>
                </span>
                <span class="auto-support-bot-mouth"></span>
            </span>
        `;
        bubble.addEventListener("click", () => {
            estado.aberto ? fecharPainel() : abrirPainel();
        });

        const panel = criarElemento("section", "auto-support-panel");
        if (estado.aberto) panel.classList.add("is-open");

        const header = criarElemento("div", "auto-support-header");
        const headerText = criarElemento("div");
        headerText.appendChild(criarElemento("p", "auto-support-kicker", status.ok === false ? "[WARN] AUTO-REPARO" : "[OK] AUTO-REPARO"));
        headerText.appendChild(criarElemento("h3", "auto-support-title", "AutoSuporte Seguro"));
        const close = criarElemento("button", "auto-support-close", "x");
        close.type = "button";
        close.addEventListener("click", fecharPainel);
        header.appendChild(headerText);
        header.appendChild(close);

        const body = criarElemento("div", "auto-support-body");
        const statusBox = criarElemento("div", "auto-support-status");
        statusBox.setAttribute("data-ok", status.ok === false ? "false" : "true");
        statusBox.appendChild(criarElemento("p", "auto-support-message", status.mensagem || "AutoSuporte pronto para acoes seguras."));
        statusBox.appendChild(criarElemento("pre", "auto-support-terminal", textoFalhas(status)));
        body.appendChild(statusBox);

        const sugestoes = renderizarSugestoes(status);
        if (sugestoes) {
            body.appendChild(sugestoes);
        }

        const acoes = [
            ["limpar_caches", "Limpar caches"],
            ["validar_ambiente", "Validar ambiente"],
            ["testar_banco", "Testar banco"],
            ["gerar_backup_suporte", "Backup suporte"],
            ["desativar_planilhas_com_erro", "Pausar planilhas"],
            ["marcar_fluxo_suspeito", "Fluxos suspeitos"],
            ["registrar_incidente", "Registrar incidente"],
            ["enviar_alerta_telegram", "Alerta Telegram"],
        ];
        const actions = criarElemento("div", "auto-support-actions");
        acoes.forEach(([acao, label]) => {
            const btn = criarElemento("button", "auto-support-action", label);
            btn.type = "button";
            btn.addEventListener("click", () => executarAcao(acao, label));
            actions.appendChild(btn);
        });
        body.appendChild(actions);

        const logs = criarElemento("div", "auto-support-log");
        estado.logs.forEach((item) => {
            const log = criarElemento("div", "auto-support-log-item");
            log.setAttribute("data-ok", item.ok ? "true" : "false");
            log.appendChild(criarElemento("p", "auto-support-log-title", `${item.quando} - ${item.titulo}`));
            log.appendChild(criarElemento("p", "auto-support-log-message", item.mensagem));
            logs.appendChild(log);
        });
        body.appendChild(logs);

        panel.appendChild(header);
        panel.appendChild(body);
        raiz.appendChild(panel);
        raiz.appendChild(bubble);
    }

    renderizar();
    carregarStatus({ abrirSeFalha: false });
    setInterval(() => carregarStatus({ abrirSeFalha: false }), 120000);
})();

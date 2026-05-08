(function iniciarAutoSuporte() {
    if (window.__autoSuporteIniciado) {
        return;
    }
    window.__autoSuporteIniciado = true;

    const estado = {
        status: null,
        aberto: localStorage.getItem("wagen_auto_suporte_aberto") === "1",
        logs: [],
        pacoteCodex: null,
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
        const diagnostico = status && status.diagnostico ? status.diagnostico : {};
        const falhas = status && Array.isArray(status.falhas) ? status.falhas : [];
        if (!falhas.length) {
            return `[${(diagnostico.label || "OK").toUpperCase()}] ${diagnostico.frase || "nenhum incidente critico"}`;
        }
        return falhas.map((falha) => `[${(diagnostico.label || "WARN").toUpperCase()}] ${falha}`).join("\n");
    }

    function hojeOcultado() {
        const hoje = new Date().toISOString().slice(0, 10);
        return localStorage.getItem("wagen_auto_suporte_oculto_hoje") === hoje;
    }

    function ocultarPorHoje() {
        localStorage.setItem("wagen_auto_suporte_oculto_hoje", new Date().toISOString().slice(0, 10));
        adicionarLog("Ocultado por hoje", "O painel nao vai abrir sozinho ate amanha.", true);
        fecharPainel();
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

    function renderizarDiagnostico(status) {
        const diagnostico = status && status.diagnostico ? status.diagnostico : {};
        const itens = Array.isArray(diagnostico.itens) ? diagnostico.itens : [];
        const bloco = criarElemento("div", "auto-support-diagnostics");
        bloco.setAttribute("data-level", diagnostico.nivel || "info");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", "Diagnostico inteligente"));
        bloco.appendChild(criarElemento("p", "auto-support-diagnostic-title", `${diagnostico.label || "Informativo"} - ${diagnostico.titulo || "AutoSuporte"}`));
        bloco.appendChild(criarElemento("p", "auto-support-log-message", diagnostico.frase || "Sem incidente critico no momento."));
        itens.slice(0, 4).forEach((item) => {
            const linha = criarElemento("div", "auto-support-diagnostic-item");
            linha.setAttribute("data-level", item.nivel || "info");
            linha.appendChild(criarElemento("span", "auto-support-diagnostic-pill", item.label || "Info"));
            linha.appendChild(criarElemento("span", "auto-support-diagnostic-text", item.titulo || item.mensagem || "Revisao"));
            bloco.appendChild(linha);
        });
        return bloco;
    }

    function renderizarHistorico(status) {
        const historico = Array.isArray(status.historico) ? status.historico : [];
        const bloco = criarElemento("div", "auto-support-history");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", "Historico do bot"));
        if (!historico.length) {
            bloco.appendChild(criarElemento("p", "auto-support-log-message", "Nenhuma acao registrada ainda."));
            return bloco;
        }
        historico.slice(0, 5).forEach((item) => {
            const linha = criarElemento("div", "auto-support-history-item");
            linha.setAttribute("data-level", item.severidade || "info");
            linha.appendChild(criarElemento("p", "auto-support-log-title", `${item.quando || "-"} - ${item.titulo || item.evento || "Evento"}`));
            linha.appendChild(criarElemento("p", "auto-support-log-message", item.mensagem || "-"));
            bloco.appendChild(linha);
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
            if (abrirSeFalha && estado.status && estado.status.ok === false && !hojeOcultado()) {
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
            if (dados.detalhes && dados.detalhes.pacote_codex) {
                estado.pacoteCodex = dados.detalhes.pacote_codex;
            }
            adicionarLog(dados.titulo || label, dados.mensagem || "Acao concluida.", dados.ok !== false);
            abrirPainel();
        } catch (erro) {
            adicionarLog(label, String(erro), false);
            abrirPainel();
        }
    }

    async function carregarPacoteCodex() {
        adicionarLog("Pacote Codex", "Gerando pacote tecnico...", true);
        abrirPainel();
        try {
            const resposta = await fetch("/api/auto-suporte/pacote-codex", {
                headers: { Accept: "application/json" },
                cache: "no-store",
                credentials: "same-origin",
            });
            const dados = await resposta.json();
            if (!resposta.ok || dados.erro) {
                throw new Error(dados.erro || "Nao foi possivel gerar o pacote.");
            }
            estado.pacoteCodex = dados.pacote_codex;
            adicionarLog("Pacote Codex", "Pacote pronto para copiar ou baixar.", true);
            abrirPainel();
        } catch (erro) {
            adicionarLog("Pacote Codex", String(erro), false);
            abrirPainel();
        }
    }

    async function copiarPacoteCodex() {
        const pacote = estado.pacoteCodex;
        if (!pacote) {
            await carregarPacoteCodex();
            return;
        }
        const texto = pacote.texto_para_codex || JSON.stringify(pacote, null, 2);
        try {
            await navigator.clipboard.writeText(texto);
            adicionarLog("Pacote copiado", "Relatorio copiado. Cole aqui no Codex para eu corrigir.", true);
        } catch (erro) {
            adicionarLog("Copiar pacote", "Nao foi possivel copiar automaticamente. Baixe o JSON.", false);
        }
        abrirPainel();
    }

    function baixarPacoteCodex() {
        const pacote = estado.pacoteCodex;
        if (!pacote) {
            carregarPacoteCodex();
            return;
        }
        const blob = new Blob([JSON.stringify(pacote, null, 2)], { type: "application/json;charset=utf-8" });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        const quando = String(pacote.gerado_em || new Date().toISOString()).replace(/[^0-9]/g, "").slice(0, 14);
        link.href = url;
        link.download = `pacote-codex-${quando || "autosporte"}.json`;
        document.body.appendChild(link);
        link.click();
        link.remove();
        URL.revokeObjectURL(url);
        adicionarLog("Pacote baixado", "Arquivo JSON gerado para enviar no Codex.", true);
        abrirPainel();
    }

    function renderizarPacoteCodex() {
        const bloco = criarElemento("div", "auto-support-package");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", "Pacote para Codex"));
        bloco.appendChild(criarElemento("p", "auto-support-log-message", estado.pacoteCodex ? "Relatorio tecnico pronto." : "Gere um pacote tecnico para me enviar aqui no Codex."));
        const acoes = criarElemento("div", "auto-support-package-actions");
        const gerar = criarElemento("button", "auto-support-mini-action", estado.pacoteCodex ? "Atualizar pacote" : "Gerar pacote");
        gerar.type = "button";
        gerar.addEventListener("click", carregarPacoteCodex);
        const copiar = criarElemento("button", "auto-support-mini-action", "Copiar para Codex");
        copiar.type = "button";
        copiar.disabled = !estado.pacoteCodex;
        copiar.addEventListener("click", copiarPacoteCodex);
        const baixar = criarElemento("button", "auto-support-mini-action", "Baixar JSON");
        baixar.type = "button";
        baixar.disabled = !estado.pacoteCodex;
        baixar.addEventListener("click", baixarPacoteCodex);
        acoes.appendChild(gerar);
        acoes.appendChild(copiar);
        acoes.appendChild(baixar);
        bloco.appendChild(acoes);
        if (estado.pacoteCodex && estado.pacoteCodex.ultimo_erro) {
            const erro = estado.pacoteCodex.ultimo_erro;
            bloco.appendChild(criarElemento("pre", "auto-support-terminal", `Erro: ${erro.tipo || "-"}\nRota: ${erro.path || erro.endpoint || "-"}\nMensagem: ${erro.mensagem || "-"}`));
        }
        return bloco;
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
        const diagnostico = status.diagnostico || {};
        const bubble = criarElemento("button", "auto-support-bubble");
        bubble.type = "button";
        bubble.title = "AutoSuporte";
        bubble.setAttribute("aria-label", "Abrir AutoSuporte");
        bubble.setAttribute("data-alert", status.ok === false ? "true" : "false");
        bubble.setAttribute("data-level", diagnostico.nivel || "info");
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
        headerText.appendChild(criarElemento("p", "auto-support-kicker", `[${(diagnostico.label || (status.ok === false ? "WARN" : "OK")).toUpperCase()}] AUTO-REPARO`));
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
        body.appendChild(renderizarDiagnostico(status));

        const sugestoes = renderizarSugestoes(status);
        if (sugestoes) {
            body.appendChild(sugestoes);
        }

        body.appendChild(renderizarPacoteCodex());

        const atalhos = criarElemento("div", "auto-support-quick-actions");
        const primeiraSugestao = Array.isArray(status.sugestoes) ? status.sugestoes.find((item) => item.acao) : null;
        const corrigir = criarElemento("button", "auto-support-primary-action", "Corrigir agora");
        corrigir.type = "button";
        corrigir.disabled = !primeiraSugestao;
        corrigir.addEventListener("click", () => primeiraSugestao && executarAcao(primeiraSugestao.acao, primeiraSugestao.titulo || "Corrigir agora"));
        const pacote = criarElemento("button", "auto-support-primary-action", "Pacote Codex");
        pacote.type = "button";
        pacote.addEventListener("click", carregarPacoteCodex);
        const central = criarElemento("a", "auto-support-primary-link", "Central");
        central.href = "/configuracoes?aba=desenvolvedor";
        const ocultar = criarElemento("button", "auto-support-primary-action", "Silenciar hoje");
        ocultar.type = "button";
        ocultar.addEventListener("click", ocultarPorHoje);
        atalhos.appendChild(corrigir);
        atalhos.appendChild(pacote);
        atalhos.appendChild(central);
        atalhos.appendChild(ocultar);
        body.appendChild(atalhos);

        const acoes = [
            ["limpar_caches", "Caches"],
            ["validar_ambiente", "Ambiente"],
            ["testar_banco", "Banco"],
            ["testar_backup", "Backup"],
            ["testar_telegram", "Telegram"],
            ["revalidar_pwa", "PWA"],
            ["gerar_backup_suporte", "Backup suporte"],
            ["desativar_planilhas_com_erro", "Pausar planilhas"],
            ["corrigir_classificacao_clientes", "Novo/retorno"],
            ["limpar_erros_resolvidos", "Erros resolvidos"],
            ["limpar_todos_erros", "Limpar todos"],
            ["gerar_pacote_codex", "Pacote Codex"],
            ["enviar_relatorio_telegram", "Relatorio"],
            ["marcar_fluxo_suspeito", "Fluxos"],
            ["registrar_incidente", "Incidente"],
            ["enviar_alerta_telegram", "Alerta"],
        ];
        const actions = criarElemento("div", "auto-support-actions");
        acoes.forEach(([acao, label]) => {
            const btn = criarElemento("button", "auto-support-action", label);
            btn.type = "button";
            btn.addEventListener("click", () => executarAcao(acao, label));
            actions.appendChild(btn);
        });
        body.appendChild(actions);
        body.appendChild(renderizarHistorico(status));

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

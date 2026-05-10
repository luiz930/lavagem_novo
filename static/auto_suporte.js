(function iniciarAutoSuporte() {
    if (window.location.pathname === "/auto-suporte") {
        return;
    }
    if (window.__autoSuporteIniciado) {
        return;
    }
    window.__autoSuporteIniciado = true;

    const estado = {
        status: null,
        aberto: localStorage.getItem("wagen_auto_suporte_aberto") === "1",
        logs: [],
        pacoteCodex: null,
        autonomiaRodando: false,
        dialogoEscolha: "",
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

    function renderizarPlanoAcao(status) {
        const plano = status && status.plano_acao ? status.plano_acao : {};
        const bloco = criarElemento("div", "auto-support-action-plan");
        bloco.setAttribute("data-priority", plano.prioridade || "normal");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", `Plano de acao - ${plano.prioridade || "normal"}`));
        bloco.appendChild(criarElemento("p", "auto-support-diagnostic-title", plano.titulo || "Proxima melhor acao"));
        bloco.appendChild(criarElemento("p", "auto-support-log-message", plano.mensagem || plano.resumo || "Nenhuma acao pendente nesta leitura."));

        const itens = Array.isArray(plano.itens) ? plano.itens : [];
        itens.slice(0, 4).forEach((item) => {
            bloco.appendChild(criarElemento("p", "auto-support-plan-item", item));
        });

        if (plano.acao && plano.executavel) {
            const btn = criarElemento("button", "auto-support-mini-action", plano.cta_label || "Executar");
            btn.type = "button";
            btn.addEventListener("click", () => executarAcao(plano.acao, plano.acao_label || plano.cta_label || "Plano de acao"));
            bloco.appendChild(btn);
        } else if (plano.acao === "gerar_pacote_codex") {
            const btn = criarElemento("button", "auto-support-mini-action", "Gerar pacote Codex");
            btn.type = "button";
            btn.addEventListener("click", carregarPacoteCodex);
            bloco.appendChild(btn);
        }
        return bloco;
    }

    function renderizarNarrativa(status) {
        const narrativa = status && status.narrativa ? status.narrativa : {};
        const linhas = Array.isArray(narrativa.linhas) ? narrativa.linhas : [];
        const bloco = criarElemento("div", "auto-support-ai-narrative");
        bloco.setAttribute("data-priority", narrativa.prioridade || "normal");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", "Leitura inteligente"));
        bloco.appendChild(criarElemento("p", "auto-support-diagnostic-title", narrativa.titulo || "AutoSuporte em tempo real"));
        if (linhas.length) {
            linhas.slice(0, 5).forEach((linha) => {
                bloco.appendChild(criarElemento("p", "auto-support-narrative-line", linha));
            });
        } else {
            bloco.appendChild(criarElemento("p", "auto-support-narrative-line", "Estou lendo o status do sistema e preparando o diagnostico."));
        }
        bloco.appendChild(criarElemento("p", "auto-support-log-message", `Status: ${narrativa.status || "monitorando"}`));
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

    function obterConfirmacaoAcao(acao) {
        const autonomia = estado.status && estado.status.autonomia ? estado.status.autonomia : {};
        const bloqueadas = Array.isArray(autonomia.acoes_bloqueadas) ? autonomia.acoes_bloqueadas : [];
        const item = bloqueadas.find((entrada) => entrada.acao === acao);
        if (item && item.confirmacao) {
            return item;
        }
        const acoes = estado.status && Array.isArray(estado.status.acoes) ? estado.status.acoes : [];
        const acaoCatalogo = acoes.find((entrada) => entrada.id === acao || entrada.acao === acao);
        return acaoCatalogo && acaoCatalogo.confirmacao ? acaoCatalogo : null;
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
            const modo = estado.status && estado.status.autonomia ? estado.status.autonomia.modo : "seguro";
            const tecnico = estado.status && estado.status.modo_interface === "tecnico";
            if (tecnico && estado.status && estado.status.ok === false && !["manual", "observador"].includes(modo)) {
                executarAutonomia();
            }
        } catch (erro) {
            adicionarLog("Falha no AutoSuporte", String(erro), false);
            renderizar();
        }
    }

    async function executarAutonomia(opcoes = {}) {
        if (estado.autonomiaRodando) {
            return;
        }
        estado.autonomiaRodando = true;
        const simular = Boolean(opcoes.simular);
        const modo = opcoes.modo || "";
        try {
            const resposta = await fetch("/api/auto-suporte/autonomia", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRF-Token": obterCsrfToken(),
                },
                body: JSON.stringify({ origem: "widget", simular, modo }),
            });
            const dados = await resposta.json();
            if (!resposta.ok || dados.erro) {
                throw new Error(dados.erro || "Falha ao executar autonomia.");
            }
            estado.status = dados.status || estado.status;
            const executadas = Array.isArray(dados.executadas) ? dados.executadas : [];
            if (executadas.length) {
                adicionarLog("Auto-reparo seguro", dados.mensagem || `${executadas.length} acao(oes) executada(s).`, dados.ok !== false);
                abrirPainel();
            } else if (simular) {
                adicionarLog("Simulacao", dados.mensagem || "Simulacao concluida sem executar reparos.", true);
                abrirPainel();
            } else {
                renderizar();
            }
        } catch (erro) {
            adicionarLog("Auto-reparo seguro", String(erro), false);
            renderizar();
        } finally {
            estado.autonomiaRodando = false;
        }
    }

    function atualizarModoAutonomia(modo) {
        executarAutonomia({ modo, simular: true });
    }

    async function executarAcao(acao, label) {
        const regraConfirmacao = obterConfirmacaoAcao(acao);
        let confirmacao = "";
        if (regraConfirmacao) {
            const texto = window.prompt(`${regraConfirmacao.label || label}\n${regraConfirmacao.seguranca || "Acao sensivel."}\nDigite exatamente: ${regraConfirmacao.confirmacao}`);
            confirmacao = texto || "";
            if (confirmacao !== regraConfirmacao.confirmacao) {
                adicionarLog(label, "Acao cancelada: confirmacao obrigatoria nao confere.", false);
                abrirPainel();
                return;
            }
        }
        adicionarLog(label, "Executando acao segura...", true);
        abrirPainel();
        try {
            const resposta = await fetch("/api/auto-suporte/acao", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "X-CSRF-Token": obterCsrfToken(),
                },
                body: JSON.stringify({ acao, confirmacao }),
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

    function renderizarAutonomia(status) {
        const autonomia = status && status.autonomia ? status.autonomia : {};
        const bloco = criarElemento("div", "auto-support-autonomy");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", `Autonomia - ${autonomia.modo_label || "Seguro"}`));
        bloco.appendChild(criarElemento("p", "auto-support-log-message", autonomia.modo_descricao || "Executa somente reparos seguros."));
        const modos = Array.isArray(autonomia.modos) ? autonomia.modos : [];
        if (modos.length) {
            const seletor = criarElemento("div", "auto-support-mode-actions");
            modos.forEach((item) => {
                const btn = criarElemento("button", "auto-support-mode-button", item.label || item.id);
                btn.type = "button";
                btn.disabled = estado.autonomiaRodando;
                btn.setAttribute("data-active", item.id === autonomia.modo ? "true" : "false");
                btn.addEventListener("click", () => atualizarModoAutonomia(item.id));
                seletor.appendChild(btn);
            });
            bloco.appendChild(seletor);
        }
        const ultimoResultado = Array.isArray(autonomia.ultimo_resultado) ? autonomia.ultimo_resultado : [];
        const bloqueadas = Array.isArray(autonomia.acoes_bloqueadas) ? autonomia.acoes_bloqueadas : [];
        const simulacao = autonomia.simulacao || {};
        const resumo = ultimoResultado.length
            ? `${ultimoResultado.length} acao(oes) no ultimo ciclo.`
            : "Acoes simples rodam sozinhas com limite e cooldown.";
        bloco.appendChild(criarElemento("p", "auto-support-log-message", resumo));
        const acoes = criarElemento("div", "auto-support-package-actions");
        const rodar = criarElemento("button", "auto-support-mini-action", estado.autonomiaRodando ? "Rodando..." : "Rodar agora");
        rodar.type = "button";
        rodar.disabled = estado.autonomiaRodando;
        rodar.addEventListener("click", executarAutonomia);
        const simular = criarElemento("button", "auto-support-mini-action", "Simular");
        simular.type = "button";
        simular.disabled = estado.autonomiaRodando;
        simular.addEventListener("click", () => executarAutonomia({ simular: true }));
        acoes.appendChild(rodar);
        acoes.appendChild(simular);
        bloco.appendChild(acoes);
        if (Array.isArray(simulacao.pretende_fazer) && simulacao.pretende_fazer.length) {
            simulacao.pretende_fazer.slice(0, 3).forEach((item) => {
                const linha = criarElemento("div", "auto-support-history-item");
                linha.setAttribute("data-level", "info");
                linha.appendChild(criarElemento("p", "auto-support-log-title", `Simula: ${item.label || item.acao}`));
                linha.appendChild(criarElemento("p", "auto-support-log-message", `${item.risco || "baixo"} risco; ${item.reversivel ? "reversivel" : "confirmar antes"}.`));
                bloco.appendChild(linha);
            });
        }
        ultimoResultado.slice(0, 3).forEach((item) => {
            const linha = criarElemento("div", "auto-support-history-item");
            linha.setAttribute("data-level", item.ok === false ? "alerta" : "info");
            linha.appendChild(criarElemento("p", "auto-support-log-title", item.label || item.acao || "Auto-reparo"));
            linha.appendChild(criarElemento("p", "auto-support-log-message", item.mensagem || "-"));
            bloco.appendChild(linha);
        });
        bloqueadas.slice(0, 3).forEach((item) => {
            const linha = criarElemento("div", "auto-support-history-item");
            linha.setAttribute("data-level", "warning");
            linha.appendChild(criarElemento("p", "auto-support-log-title", `${item.label || item.acao} precisa de confirmacao`));
            linha.appendChild(criarElemento("p", "auto-support-log-message", item.seguranca || item.motivo || "Acao mantida manual por seguranca."));
            bloco.appendChild(linha);
        });
        const log = Array.isArray(autonomia.log) ? autonomia.log : [];
        if (log.length) {
            bloco.appendChild(criarElemento("p", "auto-support-section-title", "Log de autonomia"));
            log.slice(0, 4).forEach((item) => {
                const linha = criarElemento("div", "auto-support-history-item");
                linha.setAttribute("data-level", item.severidade || "info");
                linha.appendChild(criarElemento("p", "auto-support-log-title", `${item.quando || "-"} - ${item.categoria || "log"}`));
                linha.appendChild(criarElemento("p", "auto-support-log-message", item.mensagem || item.titulo || "-"));
                bloco.appendChild(linha);
            });
        }
        return bloco;
    }

    function renderizarAcoesSimples(status) {
        const acoesSimples = Array.isArray(status.acoes_simples) ? status.acoes_simples : [];
        const bloco = criarElemento("div", "auto-support-simple-actions");
        bloco.appendChild(criarElemento("p", "auto-support-section-title", "Acoes diretas"));
        acoesSimples.forEach((item) => {
            const btn = criarElemento("button", "auto-support-simple-action");
            btn.type = "button";
            btn.appendChild(criarElemento("span", "auto-support-simple-title", item.label || "Executar"));
            btn.appendChild(criarElemento("small", "auto-support-simple-text", item.descricao || "Acao segura do AutoSuporte."));
            btn.addEventListener("click", () => executarAcao(item.acao, item.label || "Acao direta"));
            bloco.appendChild(btn);
        });
        return bloco;
    }

    function renderizarPainelSimples(status, body) {
        const statusBox = criarElemento("div", "auto-support-status");
        statusBox.setAttribute("data-ok", status.ok === false ? "false" : "true");
        statusBox.appendChild(criarElemento("p", "auto-support-message", status.mensagem || "AutoSuporte pronto para acoes seguras."));
        statusBox.appendChild(criarElemento("p", "auto-support-log-message", textoFalhas(status).replace(/\n/g, " ")));
        body.appendChild(statusBox);
        body.appendChild(renderizarNarrativa(status));
        body.appendChild(renderizarDiagnostico(status));
        body.appendChild(renderizarPlanoAcao(status));
        body.appendChild(renderizarAcoesSimples(status));

        const atalhos = criarElemento("div", "auto-support-quick-actions auto-support-quick-actions--simple");
        const pacote = criarElemento("button", "auto-support-primary-action", "Pacote Codex");
        pacote.type = "button";
        pacote.addEventListener("click", carregarPacoteCodex);
        const central = criarElemento("a", "auto-support-primary-link", "Abrir painel");
        central.href = "/auto-suporte";
        const ocultar = criarElemento("button", "auto-support-primary-action", "Silenciar hoje");
        ocultar.type = "button";
        ocultar.addEventListener("click", ocultarPorHoje);
        atalhos.appendChild(pacote);
        atalhos.appendChild(central);
        atalhos.appendChild(ocultar);
        body.appendChild(atalhos);
        body.appendChild(renderizarHistorico(status));
    }

    function obterMetaAcao(status, acao) {
        const catalogo = status && Array.isArray(status.acoes) ? status.acoes : [];
        const meta = catalogo.find((item) => item.id === acao || item.acao === acao);
        if (meta) return meta;
        const simples = status && Array.isArray(status.acoes_simples) ? status.acoes_simples : [];
        const itemSimples = simples.find((item) => item.acao === acao || item.id === acao);
        return itemSimples ? { id: acao, label: itemSimples.label, descricao: itemSimples.descricao } : { id: acao, label: acao };
    }

    function criarBotaoAcaoGuiada(status, acao, label) {
        const meta = obterMetaAcao(status, acao);
        const texto = label || meta.label || acao;
        const btn = criarElemento("button", meta.confirmacao ? "auto-support-guided-action auto-support-guided-action--danger" : "auto-support-guided-action");
        btn.type = "button";
        btn.appendChild(criarElemento("span", "", texto));
        const detalhe = meta.descricao || meta.seguranca || (meta.confirmacao ? `Exige: ${meta.confirmacao}` : "Acao segura do AutoSuporte.");
        btn.appendChild(criarElemento("small", "", detalhe));
        btn.addEventListener("click", () => executarAcao(acao, texto));
        return btn;
    }

    function opcoesDialogo(status) {
        const tecnico = status.modo_interface !== "simples";
        const opcoes = [
            {
                id: "diagnostico",
                label: "Diagnosticar",
                resposta: [
                    "Vou conferir os sinais principais sem alterar dados.",
                    "Depois eu mostro se banco, backup, PWA ou arquivos estaticos precisam de atencao.",
                ],
                acoes: ["testar_banco", "testar_backup", "revalidar_pwa", "revalidar_estaticos"],
            },
            {
                id: "velocidade",
                label: "Velocidade",
                resposta: [
                    "Vou focar em carregamento e rotas lentas.",
                    "Se a lentidao vier do banco, eu aviso em vez de mascarar o problema.",
                ],
                acoes: ["limpar_caches", "limpar_cache_rota_lenta", "revalidar_rota_lenta"],
            },
            {
                id: "relatorio",
                label: "Relatorio",
                resposta: [
                    "Vou preparar evidencias tecnicas para manutencao.",
                    "Use isso quando quiser trazer o problema aqui para o Codex.",
                ],
                acoes: ["gerar_pacote_codex", "gerar_backup_suporte"],
            },
        ];
        if (tecnico) {
            opcoes.push(
                {
                    id: "reparo",
                    label: "Reparo seguro",
                    resposta: [
                        "Vou executar apenas reparos reversiveis e de baixo risco.",
                        "Acoes sensiveis continuam bloqueadas ate confirmacao manual.",
                    ],
                    acoes: ["resolver_erros_com_checks_ok", "limpar_erros_resolvidos", "validar_ambiente"],
                    extras: ["auto_reparo", "simular"],
                },
                {
                    id: "sensivel",
                    label: "Sensivel",
                    resposta: [
                        "Aqui ficam acoes que podem alterar dados, enviar mensagem externa ou limpar evidencias.",
                        "Cada botao sensivel pede a frase de confirmacao do backend antes de executar.",
                    ],
                    acoes: ["registrar_incidente", "corrigir_classificacao_clientes", "desativar_planilhas_com_erro", "limpar_todos_erros", "enviar_relatorio_telegram"],
                }
            );
        }
        return opcoes;
    }

    function renderizarDialogoGuiado(status, body) {
        const diagnostico = status.diagnostico || {};
        const narrativa = status.narrativa || {};
        const plano = status.plano_acao || {};
        const linhas = Array.isArray(narrativa.linhas) && narrativa.linhas.length
            ? narrativa.linhas.slice(0, 3)
            : [diagnostico.frase || status.mensagem || "Estou lendo o sistema e preparando a melhor acao."];
        const escolhaAtual = estado.dialogoEscolha || "diagnostico";
        const opcoes = opcoesDialogo(status);
        const selecionada = opcoes.find((item) => item.id === escolhaAtual) || opcoes[0];

        const resumo = criarElemento("div", "auto-support-guided-summary");
        resumo.setAttribute("data-ok", status.ok === false ? "false" : "true");
        resumo.appendChild(criarElemento("p", "auto-support-message", diagnostico.titulo || "AutoSuporte IA"));
        resumo.appendChild(criarElemento("p", "auto-support-log-message", diagnostico.frase || status.mensagem || "Sistema em monitoramento."));
        body.appendChild(resumo);

        const chat = criarElemento("div", "auto-support-guided-chat");
        const bot = criarElemento("div", "auto-support-guided-message auto-support-guided-message--bot");
        bot.appendChild(criarElemento("p", "", "Estou analisando o que acontece agora."));
        linhas.forEach((linha) => bot.appendChild(criarElemento("p", "", linha)));
        if (plano.titulo) {
            bot.appendChild(criarElemento("p", "", `Proxima recomendacao: ${plano.titulo}.`));
        }
        chat.appendChild(bot);
        const user = criarElemento("div", "auto-support-guided-message auto-support-guided-message--user", selecionada.label);
        chat.appendChild(user);
        const resposta = criarElemento("div", "auto-support-guided-message auto-support-guided-message--bot");
        selecionada.resposta.forEach((linha) => resposta.appendChild(criarElemento("p", "", linha)));
        chat.appendChild(resposta);
        body.appendChild(chat);

        const opcoesBox = criarElemento("div", "auto-support-guided-options");
        opcoes.forEach((opcao) => {
            const btn = criarElemento("button", "auto-support-guided-option", opcao.label);
            btn.type = "button";
            btn.setAttribute("data-active", opcao.id === selecionada.id ? "true" : "false");
            btn.addEventListener("click", () => {
                estado.dialogoEscolha = opcao.id;
                renderizar();
            });
            opcoesBox.appendChild(btn);
        });
        body.appendChild(opcoesBox);

        const acoesBox = criarElemento("div", "auto-support-guided-actions");
        acoesBox.appendChild(criarElemento("p", "auto-support-section-title", "Botoes desta escolha"));
        if (Array.isArray(selecionada.extras) && selecionada.extras.includes("auto_reparo")) {
            const rodar = criarElemento("button", "auto-support-guided-action", estado.autonomiaRodando ? "Rodando..." : "Rodar auto-reparo");
            rodar.type = "button";
            rodar.disabled = estado.autonomiaRodando;
            rodar.addEventListener("click", executarAutonomia);
            acoesBox.appendChild(rodar);
        }
        if (Array.isArray(selecionada.extras) && selecionada.extras.includes("simular")) {
            const simular = criarElemento("button", "auto-support-guided-action", "Simular antes");
            simular.type = "button";
            simular.disabled = estado.autonomiaRodando;
            simular.addEventListener("click", () => executarAutonomia({ simular: true }));
            acoesBox.appendChild(simular);
        }
        selecionada.acoes.forEach((acao) => acoesBox.appendChild(criarBotaoAcaoGuiada(status, acao)));
        body.appendChild(acoesBox);

        const rodape = criarElemento("div", "auto-support-guided-footer");
        const abrir = criarElemento("a", "auto-support-primary-link", "Abrir tela completa");
        abrir.href = "/auto-suporte";
        const silenciar = criarElemento("button", "auto-support-primary-action", "Silenciar hoje");
        silenciar.type = "button";
        silenciar.addEventListener("click", ocultarPorHoje);
        rodape.appendChild(abrir);
        rodape.appendChild(silenciar);
        body.appendChild(rodape);

        if (estado.logs.length) {
            const logs = criarElemento("div", "auto-support-log");
            estado.logs.slice(0, 3).forEach((item) => {
                const log = criarElemento("div", "auto-support-log-item");
                log.setAttribute("data-ok", item.ok ? "true" : "false");
                log.appendChild(criarElemento("p", "auto-support-log-title", `${item.quando} - ${item.titulo}`));
                log.appendChild(criarElemento("p", "auto-support-log-message", item.mensagem));
                logs.appendChild(log);
            });
            body.appendChild(logs);
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
        const diagnostico = status.diagnostico || {};
        const tecnico = status.modo_interface !== "simples";
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
        headerText.appendChild(criarElemento("p", "auto-support-kicker", `[${(diagnostico.label || (status.ok === false ? "WARN" : "OK")).toUpperCase()}] IA GUIADA`));
        headerText.appendChild(criarElemento("h3", "auto-support-title", "AutoSuporte IA"));
        const close = criarElemento("button", "auto-support-close", "x");
        close.type = "button";
        close.addEventListener("click", fecharPainel);
        header.appendChild(headerText);
        header.appendChild(close);

        const body = criarElemento("div", "auto-support-body");
        renderizarDialogoGuiado(status, body);

        panel.appendChild(header);
        panel.appendChild(body);
        raiz.appendChild(panel);
        raiz.appendChild(bubble);
    }

    renderizar();
    carregarStatus({ abrirSeFalha: false });
    setInterval(() => carregarStatus({ abrirSeFalha: false }), 120000);
})();

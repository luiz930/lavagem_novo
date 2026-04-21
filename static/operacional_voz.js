(function () {
    const VOZ_ATIVA_KEY = "wagenPainelVozAtiva";
    const VOZ_ALERTAS_KEY = "wagenPainelVozAlertas";
    const VOZ_LIDER_KEY = "wagenPainelVozLider";
    const VOZ_TAB_ID_KEY = "wagenPainelVozTabId";
    const VOZ_ENDPOINT = "/api/operacional/voz";
    const POLLING_MS = 30000;
    const LIDER_TTL_MS = 45000;
    const STATUS_PADRAO =
        "A cada 10 minutos, o sistema avisa em portugues do Brasil quais veiculos continuam em atendimento e ha quantos minutos estao na operacao.";

    let servicosCache = [];
    let snapshotEm = Date.now();

    function obterOuCriarIdAba() {
        let tabId = sessionStorage.getItem(VOZ_TAB_ID_KEY);

        if (!tabId) {
            if (window.crypto && typeof window.crypto.randomUUID === "function") {
                tabId = window.crypto.randomUUID();
            } else {
                tabId = `voz-${Date.now()}-${Math.random().toString(16).slice(2)}`;
            }

            sessionStorage.setItem(VOZ_TAB_ID_KEY, tabId);
        }

        return tabId;
    }

    const abaId = obterOuCriarIdAba();

    function suportaVozOperacional() {
        return "speechSynthesis" in window && "SpeechSynthesisUtterance" in window;
    }

    function obterBotaoVoz() {
        return document.getElementById("vozToggleButton");
    }

    function obterBotaoTesteVoz() {
        return document.getElementById("vozTesteButton");
    }

    function obterStatusVoz() {
        return document.getElementById("vozStatusTexto");
    }

    function obterCardsServicos() {
        return Array.from(document.querySelectorAll(".js-servico-card"));
    }

    function vozOperacionalAtiva() {
        const salvo = localStorage.getItem(VOZ_ATIVA_KEY);
        return salvo === null ? true : salvo === "1";
    }

    function salvarEstadoVozOperacional(ativo) {
        localStorage.setItem(VOZ_ATIVA_KEY, ativo ? "1" : "0");
    }

    function obterHistoricoAlertas() {
        try {
            return JSON.parse(localStorage.getItem(VOZ_ALERTAS_KEY) || "{}");
        } catch (erro) {
            return {};
        }
    }

    function salvarHistoricoAlertas(mapa) {
        localStorage.setItem(VOZ_ALERTAS_KEY, JSON.stringify(mapa));
    }

    function limparHistoricoAlertasAntigos(idsAtivos) {
        const historico = obterHistoricoAlertas();
        const filtrado = {};

        idsAtivos.forEach((id) => {
            if (Object.prototype.hasOwnProperty.call(historico, id)) {
                filtrado[id] = historico[id];
            }
        });

        salvarHistoricoAlertas(filtrado);
        return filtrado;
    }

    function lerLiderancaAtual() {
        try {
            return JSON.parse(localStorage.getItem(VOZ_LIDER_KEY) || "null");
        } catch (erro) {
            return null;
        }
    }

    function renovarLideranca() {
        localStorage.setItem(
            VOZ_LIDER_KEY,
            JSON.stringify({
                tab_id: abaId,
                expira_em: Date.now() + LIDER_TTL_MS,
            })
        );
    }

    function estaAbaResponsavelPelosAvisos() {
        const lideranca = lerLiderancaAtual();

        if (
            !lideranca ||
            Number(lideranca.expira_em || 0) < Date.now() ||
            lideranca.tab_id === abaId
        ) {
            renovarLideranca();
            return true;
        }

        return false;
    }

    function obterVozPtBr() {
        const vozes = window.speechSynthesis.getVoices();
        return (
            vozes.find((voz) => (voz.lang || "").toLowerCase().startsWith("pt-br")) ||
            vozes.find((voz) => (voz.lang || "").toLowerCase().startsWith("pt")) ||
            null
        );
    }

    function falarAvisoOperacional(texto) {
        if (!suportaVozOperacional() || !vozOperacionalAtiva() || !texto) {
            return;
        }

        const mensagem = new SpeechSynthesisUtterance(texto);
        mensagem.lang = "pt-BR";
        mensagem.rate = 1;
        mensagem.pitch = 1;
        mensagem.volume = 1;

        const voz = obterVozPtBr();
        if (voz) {
            mensagem.voice = voz;
        }

        window.speechSynthesis.cancel();
        window.speechSynthesis.speak(mensagem);
    }

    function formatarTempoOperacional(minutos) {
        const total = Math.max(0, Number(minutos || 0));
        const horas = Math.floor(total / 60);
        const mins = total % 60;

        if (horas > 0) {
            return `${horas}h ${mins}min`;
        }

        return `${mins}min`;
    }

    function obterMinutosAtuais(item) {
        const base = Math.max(0, Number(item.minutos_em_andamento || item.baseMinutos || 0));
        const decorridos = Math.floor((Date.now() - snapshotEm) / 60000);
        return Math.max(0, base + decorridos);
    }

    function montarNomeVeiculo(item) {
        const placa = String(item.placa || "").trim();
        const modelo = String(item.modelo || "").trim();

        if (modelo && placa) {
            return `${modelo}, placa ${placa}`;
        }

        if (modelo) {
            return modelo;
        }

        return `placa ${placa || "sem identificacao"}`;
    }

    function obterServicosAtuais() {
        if (servicosCache.length) {
            return servicosCache.map((item) => ({
                ...item,
                minutos_em_andamento: obterMinutosAtuais(item),
            }));
        }

        return obterCardsServicos().map((card) => ({
            id: card.dataset.servicoId,
            placa: card.dataset.placa || "",
            modelo: card.dataset.modelo || "",
            servico: card.dataset.servico || "",
            minutos_em_andamento: obterMinutosAtuais({
                baseMinutos: Number(card.dataset.baseMinutos || 0),
            }),
        }));
    }

    function atualizarCardsPainel() {
        const cards = obterCardsServicos();

        if (!cards.length) {
            return;
        }

        const mapa = new Map(
            servicosCache.map((item) => [String(item.id), item])
        );

        cards.forEach((card) => {
            const servico = mapa.get(String(card.dataset.servicoId || ""));

            if (servico) {
                card.dataset.baseMinutos = String(
                    Math.max(0, Number(servico.minutos_em_andamento || 0))
                );

                if (servico.placa) {
                    card.dataset.placa = servico.placa;
                }

                if (servico.modelo) {
                    card.dataset.modelo = servico.modelo;
                }

                if (servico.servico) {
                    card.dataset.servico = servico.servico;
                }
            }

            const tempoEl = card.querySelector(".js-tempo-espera");
            if (tempoEl) {
                tempoEl.textContent = formatarTempoOperacional(
                    obterMinutosAtuais({
                        baseMinutos: Number(card.dataset.baseMinutos || 0),
                    })
                );
            }
        });
    }

    function atualizarStatusControlesVoz(textoExtra) {
        const botao = obterBotaoVoz();
        const botaoTeste = obterBotaoTesteVoz();
        const status = obterStatusVoz();

        if (!botao || !botaoTeste || !status) {
            return;
        }

        if (!suportaVozOperacional()) {
            botao.disabled = true;
            botaoTeste.disabled = true;
            botao.textContent = "Avisos por voz indisponiveis";
            status.textContent =
                "Este navegador nao oferece suporte a voz em portugues pelo recurso nativo.";
            return;
        }

        const ativa = vozOperacionalAtiva();
        const totalAtivos = obterServicosAtuais().length;
        const lider = estaAbaResponsavelPelosAvisos();

        botao.disabled = false;
        botaoTeste.disabled = false;
        botao.textContent = ativa
            ? "Avisos por voz: ativados"
            : "Avisos por voz: pausados";

        let texto = STATUS_PADRAO;

        if (!ativa) {
            texto = "Os avisos por voz estao pausados neste navegador.";
        } else if (totalAtivos === 0) {
            texto = "Nenhum atendimento em andamento no momento. O aviso sera retomado automaticamente quando houver carros em operacao.";
        } else if (!lider) {
            texto = "Os avisos por voz estao ativos, mas outra aba deste sistema esta responsavel pelos avisos para evitar duplicidade.";
        }

        if (textoExtra) {
            texto = `${texto} ${textoExtra}`.trim();
        }

        status.textContent = texto;
    }

    function verificarAlertasOperacionais() {
        if (!suportaVozOperacional() || !vozOperacionalAtiva()) {
            return;
        }

        const servicos = obterServicosAtuais();
        const idsAtivos = servicos.map((item) => String(item.id));

        if (!estaAbaResponsavelPelosAvisos()) {
            limparHistoricoAlertasAntigos(idsAtivos);
            return;
        }

        const historico = limparHistoricoAlertasAntigos(idsAtivos);
        const mensagens = [];

        servicos.forEach((item) => {
            const servicoId = String(item.id);
            const minutos = Math.max(0, Number(item.minutos_em_andamento || 0));
            const bloco = Math.floor(minutos / 10);
            const ultimoBloco = Number(historico[servicoId] || 0);

            if (bloco >= 1 && bloco > ultimoBloco) {
                mensagens.push(
                    `${montarNomeVeiculo(item)} esta em atendimento ha ${minutos} minutos.`
                );
                historico[servicoId] = bloco;
            }
        });

        salvarHistoricoAlertas(historico);

        if (mensagens.length) {
            falarAvisoOperacional(`Atencao. ${mensagens.join(" ")}`);
        }
    }

    async function atualizarSnapshotOperacional() {
        try {
            const resposta = await fetch(VOZ_ENDPOINT, {
                cache: "no-store",
                headers: {
                    "X-Requested-With": "fetch",
                },
            });

            if (!resposta.ok) {
                throw new Error(`HTTP ${resposta.status}`);
            }

            const payload = await resposta.json();
            servicosCache = Array.isArray(payload.servicos) ? payload.servicos : [];
            snapshotEm = Date.now();
            atualizarCardsPainel();
            atualizarStatusControlesVoz();
            verificarAlertasOperacionais();
        } catch (erro) {
            atualizarCardsPainel();
            atualizarStatusControlesVoz(
                "Nao foi possivel atualizar os dados operacionais agora."
            );
        }
    }

    function alternarAvisosVozOperacional() {
        const novoEstado = !vozOperacionalAtiva();
        salvarEstadoVozOperacional(novoEstado);
        atualizarStatusControlesVoz();

        if (novoEstado) {
            falarAvisoOperacional(
                "Avisos por voz ativados para os atendimentos operacionais."
            );
            verificarAlertasOperacionais();
        } else if (suportaVozOperacional()) {
            window.speechSynthesis.cancel();
        }
    }

    function testarAvisoVozOperacional() {
        if (!suportaVozOperacional()) {
            return;
        }

        falarAvisoOperacional(
            "Teste de voz operacional ativado com sucesso em qualquer tela do sistema."
        );
    }

    function conectarControlesVoz() {
        const botao = obterBotaoVoz();
        const botaoTeste = obterBotaoTesteVoz();

        if (botao && !botao.dataset.vozBind) {
            botao.dataset.vozBind = "1";
            botao.addEventListener("click", alternarAvisosVozOperacional);
        }

        if (botaoTeste && !botaoTeste.dataset.vozBind) {
            botaoTeste.dataset.vozBind = "1";
            botaoTeste.addEventListener("click", testarAvisoVozOperacional);
        }
    }

    function iniciarAvisosOperacionais() {
        conectarControlesVoz();
        atualizarCardsPainel();
        atualizarStatusControlesVoz();
        atualizarSnapshotOperacional();

        setInterval(() => {
            atualizarCardsPainel();
            atualizarStatusControlesVoz();
            verificarAlertasOperacionais();
        }, POLLING_MS);

        setInterval(() => {
            atualizarSnapshotOperacional();
        }, POLLING_MS);

        setInterval(() => {
            if (estaAbaResponsavelPelosAvisos()) {
                renovarLideranca();
            }
        }, 15000);
    }

    document.addEventListener("DOMContentLoaded", () => {
        iniciarAvisosOperacionais();
    });

    if (suportaVozOperacional()) {
        window.speechSynthesis.onvoiceschanged = () => {
            atualizarStatusControlesVoz();
        };
    }
})();

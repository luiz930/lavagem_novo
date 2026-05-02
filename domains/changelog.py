from __future__ import annotations

from collections import OrderedDict
from datetime import datetime
from pathlib import Path
import subprocess
import time


MESES_PT = [
    "Janeiro",
    "Fevereiro",
    "Marco",
    "Abril",
    "Maio",
    "Junho",
    "Julho",
    "Agosto",
    "Setembro",
    "Outubro",
    "Novembro",
    "Dezembro",
]

CHANGELOG_CACHE_TTL = 300
CHANGELOG_CACHE = {
    "carregado_em": 0.0,
    "repo_root": "",
    "payload": None,
}

COMMIT_CATEGORIES = [
    ("Banco e migracoes", ("supabase", "banco", "postgres", "sqlite", "migrac", "schema")),
    ("Interface e experiencia", ("layout", "hud", "home", "texto", "clima", "servico", "historico", "painel")),
    ("Performance e estabilidade", ("suaviza", "desacopla", "boot", "health check", "fallback", "polling", "estabilidade")),
    ("Seguranca e acesso", ("senha", "admin", "usuario", "acesso", "sessao", "csrf")),
    ("Arquivos e backup", ("backup", "google drive", "foto", "storage", "arquivo")),
    ("Deploy e hospedagem", ("render", "fly", "vps", "deploy", "cloudflare")),
    ("Produto e estrutura", ("produto", "multiempresa", "changelog", "foundation", "workflow", "template .env")),
]

MILESTONE_RULES = [
    ("Base inicial", ("refactor app.py", "refactor html", "encoding issues")),
    ("HUD e operacao", ("hud", "retornos", "historico", "painel")),
    ("Banco online", ("supabase", "banco online", "postgres", "migrac")),
    ("Backup e arquivos", ("backup", "google drive", "fotos no banco", "storage")),
    ("Performance", ("suaviza", "desacopla", "health check", "boot", "polling")),
    ("Produto e multiempresa", ("multiempresa", "produto", "foundation")),
    ("Deploy e VPS", ("deploy", "vps", "render", "fly")),
]

NOISE_SUBJECTS = {"add files via upload"}


def _run_git_command(repo_root, *args):
    try:
        resultado = subprocess.run(
            ["git", "-C", str(repo_root), *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=15,
            check=False,
        )
    except Exception:
        return ""

    if resultado.returncode != 0:
        return ""

    return (resultado.stdout or "").strip()


def _formatar_data_curta_iso(data_iso):
    try:
        data = datetime.strptime(str(data_iso or ""), "%Y-%m-%d")
        return data.strftime("%d/%m/%Y")
    except Exception:
        return str(data_iso or "-")


def _formatar_mes_ano_iso(data_iso):
    try:
        data = datetime.strptime(str(data_iso or ""), "%Y-%m-%d")
        return f"{MESES_PT[data.month - 1]} de {data.year}"
    except Exception:
        return str(data_iso or "-")


def _categorizar_commit(subject):
    texto = str(subject or "").strip()
    texto_casefold = texto.casefold()

    if texto_casefold in NOISE_SUBJECTS:
        return "Base e checkpoints"

    for categoria, palavras in COMMIT_CATEGORIES:
        if any(palavra in texto_casefold for palavra in palavras):
            return categoria

    return "Ajustes gerais"


def _parse_git_log(texto_git):
    entradas = []
    for linha in (texto_git or "").splitlines():
        partes = linha.split("\x1f", 2)
        if len(partes) != 3:
            continue

        commit_hash, data_iso, assunto = partes
        assunto = (assunto or "").strip()
        assunto_casefold = assunto.casefold()
        entradas.append(
            {
                "hash": commit_hash,
                "hash_curto": commit_hash[:7],
                "data": data_iso,
                "data_label": _formatar_data_curta_iso(data_iso),
                "assunto": assunto,
                "assunto_casefold": assunto_casefold,
                "categoria": _categorizar_commit(assunto),
                "ruido": assunto_casefold in NOISE_SUBJECTS,
            }
        )

    return entradas


def _build_milestones(entradas):
    relevantes = [item for item in entradas if not item.get("ruido")]
    if not relevantes:
        return []

    marcos = []
    hashes = set()

    def adicionar(item, destaque):
        commit_hash = item.get("hash")
        if not commit_hash or commit_hash in hashes:
            return
        hashes.add(commit_hash)
        payload = dict(item)
        payload["destaque"] = destaque
        marcos.append(payload)

    adicionar(relevantes[0], "Primeiro marco rastreado")

    for destaque, palavras in MILESTONE_RULES:
        for item in relevantes:
            if any(palavra in item["assunto_casefold"] for palavra in palavras):
                adicionar(item, destaque)
                break

    adicionar(relevantes[-1], "Estado mais recente")
    marcos.sort(key=lambda item: (item.get("data") or "", item.get("hash") or ""))
    return marcos[:12]


def _build_groups(entradas):
    grupos = OrderedDict()
    for item in reversed(entradas):
        periodo = str(item.get("data") or "")[:7]
        if periodo not in grupos:
            grupos[periodo] = {
                "periodo": periodo,
                "periodo_label": _formatar_mes_ano_iso(item.get("data")),
                "itens": [],
            }
        grupos[periodo]["itens"].append(item)

    return list(grupos.values())


def carregar_contexto_changelog(repo_root, versao_atual=""):
    repo_root = str(Path(repo_root).resolve())
    agora_ts = time.time()
    cache = CHANGELOG_CACHE.get("payload")

    if (
        cache
        and CHANGELOG_CACHE.get("repo_root") == repo_root
        and agora_ts - float(CHANGELOG_CACHE.get("carregado_em") or 0.0) < CHANGELOG_CACHE_TTL
    ):
        return dict(cache)

    texto_git = _run_git_command(
        repo_root,
        "log",
        "--reverse",
        "--pretty=format:%H%x1f%ad%x1f%s",
        "--date=short",
        "--",
        ".",
    )
    entradas = _parse_git_log(texto_git)
    branch_atual = _run_git_command(repo_root, "branch", "--show-current")
    hash_atual = _run_git_command(repo_root, "rev-parse", "--short", "HEAD")

    resumo = {
        "versao_atual": str(versao_atual or "").strip() or "Versao nao informada",
        "branch_atual": branch_atual or "main",
        "hash_atual": hash_atual or "-",
        "total_commits": len(entradas),
        "primeiro_commit_em": entradas[0]["data_label"] if entradas else "-",
        "ultimo_commit_em": entradas[-1]["data_label"] if entradas else "-",
        "tem_git": bool(entradas),
    }

    payload = {
        "resumo": resumo,
        "marcos": _build_milestones(entradas),
        "grupos": _build_groups(entradas),
        "gerado_em": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    }

    CHANGELOG_CACHE["carregado_em"] = agora_ts
    CHANGELOG_CACHE["repo_root"] = repo_root
    CHANGELOG_CACHE["payload"] = dict(payload)
    return payload

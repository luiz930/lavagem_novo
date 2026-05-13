from flask import Flask, render_template, request, redirect, session, jsonify, has_request_context
import csv
import json
import logging
import math
import sqlite3
import socket
from flask import g
from flask import send_from_directory
from copy import deepcopy
import base64
import mimetypes
from zoneinfo import ZoneInfo
import os
import shutil
import subprocess
import time
import hashlib
import re
import secrets
import string
import tempfile
import traceback
import zipfile
from threading import Thread
from io import BytesIO, StringIO
from threading import Lock, Thread
import unicodedata
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse
import bcrypt  # ðŸ‘ˆ se jÃ¡ adicionou
import pandas as pd
import requests
from werkzeug.utils import secure_filename
from werkzeug.exceptions import HTTPException
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from flask import send_file
from reportlab.lib.units import cm
from reportlab.platypus import Flowable
from reportlab.lib.utils import ImageReader
from xml.sax.saxutils import escape as xml_escape
from core.security import (
    append_security_headers,
    build_flask_security_config,
    extract_csrf_token,
    issue_csrf_token,
    should_enforce_csrf,
    validate_csrf_token,
)
from core.product_foundation import (
    build_brand_context,
    run_product_foundation_migrations,
)
from core.telemetry import registrar_evento_telemetria as registrar_evento_telemetria_core
from domains.clientes import (
    consultar_contatos_clientes_por_placas as consultar_contatos_clientes_por_placas_domain,
    consultar_estados_retorno as consultar_estados_retorno_domain,
    consultar_historico_sync_por_placa as consultar_historico_sync_por_placa_domain,
    consultar_registros_clientes as consultar_registros_clientes_domain,
    consultar_sincronizacao_cliente as consultar_sincronizacao_cliente_domain,
    consultar_sincronizacoes_clientes as consultar_sincronizacoes_clientes_domain,
    consultar_ultima_sincronizacao_cliente as consultar_ultima_sincronizacao_cliente_domain,
    consultar_ultimas_lavagens_sync as consultar_ultimas_lavagens_sync_domain,
    salvar_cliente_veiculo_cursor as salvar_cliente_veiculo_cursor_domain,
)
from domains.changelog import carregar_contexto_changelog as carregar_contexto_changelog_domain
from domains.documentos_fiscais import (
    calcular_totais_nota_fiscal,
    calcular_totais_orcamento,
    montar_prefill_nota_por_orcamento as montar_prefill_nota_por_orcamento_domain,
)
from domains.empresas import (
    PLANOS_LICENCA,
    STATUS_LICENCA,
    gerar_licenca_assinada as gerar_licenca_assinada_domain,
    garantir_licenca as garantir_licenca_domain,
    listar_empresas as listar_empresas_domain,
    montar_contexto_licenca as montar_contexto_licenca_domain,
    normalizar_plano as normalizar_plano_licenca_domain,
    normalizar_status_licenca as normalizar_status_licenca_domain,
    obter_empresa as obter_empresa_domain,
    obter_licenca as obter_licenca_domain,
    obter_uso_licenca as obter_uso_licenca_domain,
    plano_padrao as plano_padrao_licenca_domain,
    salvar_empresa as salvar_empresa_domain,
    salvar_licenca as salvar_licenca_domain,
    validar_licenca_assinada as validar_licenca_assinada_domain,
)
from domains.financeiro import (
    dias_do_periodo_financeiro,
    filtrar_registros_por_periodo,
    filtrar_servicos_por_periodo,
    montar_chave_cache_relatorios,
    montar_periodo_descricao,
    montar_ranking_equipe_relatorios as montar_ranking_equipe_relatorios_domain,
    montar_resumo_operacional_financeiro,
    normalizar_periodo_financeiro,
)
from domains.historico import (
    carregar_recursos_edicao_historico as carregar_recursos_edicao_historico_domain,
    excluir_dependencias_historico_servico as excluir_dependencias_historico_servico_domain,
    listar_nomes_checklist_por_servicos as listar_nomes_checklist_por_servicos_domain,
    substituir_checklist_servico as substituir_checklist_servico_domain,
)
from domains.pwa import (
    montar_manifesto_pwa,
    montar_status_pwa,
)
from domains.status_sistema import (
    ROTAS_CENTRAL_TECNICA,
    TABELAS_CENTRAL_TECNICA,
    causa_provavel_lentidao_rota,
    classificar_latencia_ms,
    classificar_tendencia_resposta_ms,
    enriquecer_metricas_tempo_resposta,
    rotulo_latencia_ms,
    rotulo_tendencia_resposta,
)
from domains.servicos import (
    consultar_historico_servicos as consultar_historico_servicos_domain,
    consultar_resumo_hud as consultar_resumo_hud_domain,
    consultar_servico_operacional as consultar_servico_operacional_domain,
    consultar_servicos_em_andamento as consultar_servicos_em_andamento_domain,
    consultar_servicos_em_andamento_voz as consultar_servicos_em_andamento_voz_domain,
    consultar_ultima_lavagem_local_por_placa as consultar_ultima_lavagem_local_por_placa_domain,
    consultar_ultimas_lavagens_locais as consultar_ultimas_lavagens_locais_domain,
    consultar_veiculo_por_placa as consultar_veiculo_por_placa_domain,
    atualizar_status_servico as atualizar_status_servico_domain,
)
from domains.sync_clientes import (
    alternar_sincronizacao_cliente as alternar_sincronizacao_cliente_domain,
    atualizar_status_sincronizacao_cliente as atualizar_status_sincronizacao_cliente_domain,
    criar_sincronizacao_cliente as criar_sincronizacao_cliente_domain,
    excluir_sincronizacao_cliente as excluir_sincronizacao_cliente_domain,
    salvar_historico_lavagens_sync as salvar_historico_lavagens_sync_domain,
)
from domains.tenant import normalize_empresa_id
from scripts.site_monitor import (
    CheckResult as SiteMonitorCheckResult,
    build_report as build_site_monitor_report,
    run_site_checks,
    send_telegram_message as send_site_monitor_telegram_message,
)

APP_LOGGER = logging.getLogger("wagen_estetica")


def log_info(*partes):
    APP_LOGGER.info(" ".join(str(parte) for parte in partes))

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build as google_build
    from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
    GOOGLE_DRIVE_DISPONIVEL = True
except Exception:
    service_account = None
    google_build = None
    MediaFileUpload = None
    MediaIoBaseDownload = None
    GOOGLE_DRIVE_DISPONIVEL = False

try:
    import psycopg2
    from psycopg2.extras import DictCursor
    POSTGRESQL_DISPONIVEL = True
except Exception:
    psycopg2 = None
    DictCursor = None
    POSTGRESQL_DISPONIVEL = False

try:
    from PIL import Image, ImageFile, ImageOps, UnidentifiedImageError
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    PILLOW_DISPONIVEL = True
except Exception:
    Image = None
    ImageFile = None
    ImageOps = None
    UnidentifiedImageError = Exception
    PILLOW_DISPONIVEL = False


def carregar_env_local(caminho=None):
    if caminho is None:
        caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

    if not os.path.exists(caminho):
        return

    try:
        with open(caminho, "r", encoding="utf-8") as arquivo:
            for linha in arquivo:
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue

                chave, valor = linha.split("=", 1)
                chave = chave.strip()
                if not chave or chave in os.environ:
                    continue

                valor = valor.strip().strip('"').strip("'")
                os.environ[chave] = valor
    except Exception:
        pass


carregar_env_local()

def caminho_env_local():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")


def ler_env_local(caminho=None):
    caminho = caminho or caminho_env_local()
    dados = {}
    if not os.path.exists(caminho):
        return dados

    try:
        with open(caminho, "r", encoding="utf-8") as arquivo:
            for linha in arquivo:
                linha = linha.strip()
                if not linha or linha.startswith("#") or "=" not in linha:
                    continue
                chave, valor = linha.split("=", 1)
                chave = chave.strip()
                if not chave:
                    continue
                dados[chave] = valor.strip().strip('"').strip("'")
    except Exception:
        pass

    return dados


def salvar_env_local(valores, caminho=None):
    caminho = caminho or caminho_env_local()
    dados = ler_env_local(caminho)

    for chave, valor in (valores or {}).items():
        if valor is None:
            dados.pop(chave, None)
            continue
        texto = str(valor).strip()
        if not texto:
            dados.pop(chave, None)
            continue
        dados[chave] = texto

    chaves_prioritarias = [
        "DATABASE_BACKEND",
        "DATABASE_URL",
        "SUPABASE_DB_PASSWORD",
        "SUPABASE_DATABASE_URL",
        "AUTO_MIGRAR_BANCO",
        "DATABASE_ONLINE_MIGRADO",
    ]
    linhas = []
    for chave in chaves_prioritarias:
        if chave in dados:
            linhas.append(f"{chave}={dados.pop(chave)}")

    for chave in sorted(dados):
        linhas.append(f"{chave}={dados[chave]}")

    with open(caminho, "w", encoding="utf-8") as arquivo:
        arquivo.write("\n".join(linhas).strip() + "\n")


DATABASE_URL_RAW = (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL") or "").strip()
SUPABASE_DB_PASSWORD = (os.environ.get("SUPABASE_DB_PASSWORD") or "").strip()
DATABASE_BACKEND_RAW = (
    os.environ.get("DATABASE_BACKEND")
    or os.environ.get("BACKEND_BANCO")
    or "postgres"
).strip().lower()
AUTO_MIGRAR_BANCO_RAW = (os.environ.get("AUTO_MIGRAR_BANCO") or "1").strip().lower()
DATABASE_ONLINE_MIGRADO_RAW = (os.environ.get("DATABASE_ONLINE_MIGRADO") or "0").strip().lower()
STRICT_ONLINE_DATABASE_RAW = (os.environ.get("STRICT_ONLINE_DATABASE") or "").strip().lower()
SESSION_COOKIE_SECURE_RAW = (os.environ.get("SESSION_COOKIE_SECURE") or "").strip().lower()
CSRF_PROTECTION_RAW = (os.environ.get("CSRF_PROTECTION") or "0").strip().lower()
FLASK_SECRET_KEY_RAW = (os.environ.get("FLASK_SECRET_KEY") or "").strip()
FLASK_SECRET_KEY_FALLBACK = ""
TELEMETRIA_ATIVA_RAW = (os.environ.get("TELEMETRIA_ATIVA") or "1").strip().lower()

UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
BACKUP_FOLDER = "backups"
os.makedirs(BACKUP_FOLDER, exist_ok=True)
DATABASE_FILE = "database_v2.db"
UPLOADS_ORFAOS_FOLDER = os.path.join(UPLOAD_FOLDER, "orfaos")
UPLOADS_THUMBS_FOLDER = os.path.join(UPLOAD_FOLDER, "thumbs")
UPLOADS_SERVICOS_FOLDER = os.path.join(UPLOAD_FOLDER, "servicos")
UPLOADS_PERFIS_FOLDER = os.path.join(UPLOAD_FOLDER, "perfis")
BACKUP_RETENCAO_PADRAO = 15
BACKUP_TIPO_PADRAO = "completo"
BACKUP_ARQUIVO_BANCO_ATUAL = "database_v2_atual.db"
BACKUP_ARQUIVO_POSTGRES_ATUAL = "database_postgres_atual.json"
BACKUP_ARQUIVO_POSTGRES_ATUAL_ZIP = "database_postgres_atual.zip"
BACKUP_ARQUIVO_MANIFESTO = "manifesto_backup.json"
BACKUP_VALIDACAO_CACHE_TTL = 90
BANCO_ONLINE_LOCK_FILE = ".database_online.lock"
AGENDA_RETORNO_MARCOS = (15, 30, 45)
AGENDA_RETORNO_LIMITE_ITENS = 18
STATUS_RETORNO_PADRAO = "pendente"
STATUS_RETORNO_OPCOES = [
    {"value": "acao_agora", "label": "Acao agora"},
    {"value": "todos", "label": "Todos"},
    {"value": "pendente", "label": "Pendentes"},
    {"value": "reagendado", "label": "Reagendados"},
    {"value": "contatado", "label": "Contatados"},
    {"value": "sem_interesse", "label": "Sem interesse"},
]
STATUS_RETORNO_LABELS = {
    "pendente": "Pendente",
    "contatado": "Contatado",
    "reagendado": "Reagendado",
    "sem_interesse": "Sem interesse",
}
FREQUENCIAS_BACKUP = [
    {"value": "diario", "label": "Diario"},
    {"value": "semanal", "label": "Semanal"},
    {"value": "mensal", "label": "Mensal"},
]
TIPOS_BACKUP = [
    {"value": "completo", "label": "Sistema completo"},
    {"value": "banco", "label": "Somente banco"},
]
CHECKLIST_RECUPERACAO_BACKUP = [
    "Validar a integridade do arquivo de backup antes de restaurar.",
    "Gerar um backup preventivo do ambiente atual antes de qualquer restauracao.",
    "Confirmar se o backup e do tipo esperado: banco ou sistema completo.",
    "Reiniciar a aplicacao e revisar login, painel, historico e configuracoes apos a restauracao.",
    "Conferir se a copia externa foi atualizada no Google Drive ou na pasta sincronizada.",
]
TABELAS_SISTEMA_ORDENADAS = [
    "notificacoes",
    "sincronizacoes_clientes",
    "usuarios",
    "clientes",
    "veiculos",
    "tipos_servico",
    "servicos",
    "adicionais",
    "servico_adicionais",
    "servico_cobrancas_extras",
    "fotos",
    "produtos_pneu",
    "checklist_itens",
    "servico_checklist",
    "historico_lavagens_sync",
    "retornos_clientes",
    "empresas",
    "licencas",
    "telemetria_eventos",
    "configuracao_empresa",
    "orcamentos",
    "orcamento_itens",
    "notas_fiscais",
    "nota_fiscal_itens",
    "integracao_fiscal",
    "configuracao_backup",
    "manutencao_arquivos",
    "auditoria",
]

TABELAS_COM_ATUALIZADO_EM_SYNC = [
    "notificacoes",
    "sincronizacoes_clientes",
    "usuarios",
    "clientes",
    "veiculos",
    "tipos_servico",
    "servicos",
    "adicionais",
    "servico_adicionais",
    "servico_cobrancas_extras",
    "fotos",
    "produtos_pneu",
    "checklist_itens",
    "servico_checklist",
    "historico_lavagens_sync",
    "retornos_clientes",
    "empresas",
    "licencas",
    "configuracao_empresa",
    "orcamentos",
    "orcamento_itens",
    "notas_fiscais",
    "nota_fiscal_itens",
    "integracao_fiscal",
    "configuracao_backup",
    "manutencao_arquivos",
    "auditoria",
]
FOTO_MAX_DIMENSAO = 1600
FOTO_QUALIDADE_JPEG = 82
FOTO_PERFIL_MAX_DIMENSAO = 640
FOTO_PERFIL_QUALIDADE_JPEG = 80
RETENCAO_ORFAOS_DIAS = 30
RETENCAO_THUMBS_DIAS = 7
IDADE_MINIMA_ORFAO_SEGUNDOS = 600

from datetime import datetime, date, time as dt_time

class ImagemRedonda(Flowable):
    def __init__(self, path, size=60):
        Flowable.__init__(self)
        self.img = ImageReader(path)
        self.size = size

    def draw(self):
        c = self.canv

        c.saveState()

        # ðŸ”¥ cria caminho circular
        path = c.beginPath()
        path.circle(self.size/2, self.size/2, self.size/2)

        # ðŸ”¥ aplica mÃ¡scara corretamente
        c.clipPath(path, stroke=0, fill=0)

        # ðŸ”¥ desenha imagem dentro do cÃ­rculo
        c.drawImage(self.img, 0, 0, width=self.size, height=self.size)

        c.restoreState()

class FontePlanilhaError(Exception):
    def __init__(self, mensagem, fatal=False):
        super().__init__(mensagem)
        self.fatal = fatal

def estimar_requisicoes_mensais(intervalo_minutos):
    try:
        intervalo = max(1, int(intervalo_minutos))
    except Exception:
        return None

    return int((30 * 24 * 60) / intervalo)

def montar_mensagem_sheety_bloqueado(status_code=None, intervalo_minutos=None):
    partes = []

    if status_code == 402:
        partes.append(
            "O Sheety recusou a leitura com 402 Payment Required. "
            "Isso indica bloqueio de plano ou limite de uso do Sheety, nao erro de login do sistema."
        )
    elif status_code in {401, 403}:
        partes.append(
            "O Sheety recusou a leitura por autenticacao/permissao. "
            "Se a API estiver protegida, sera preciso ajustar as credenciais no Sheety."
        )
    elif status_code == 404:
        partes.append(
            "O endpoint do Sheety nao foi encontrado. Verifique se a URL da API continua valida."
        )
    else:
        partes.append(
            "O Sheety bloqueou a leitura da planilha."
        )

    estimativa = estimar_requisicoes_mensais(intervalo_minutos)
    if estimativa:
        partes.append(
            (
                f"No intervalo atual de {intervalo_minutos} minutos, "
                f"a sincronizacao tenta cerca de {estimativa} leituras por mes."
            )
        )

        if estimativa > 200:
            partes.append(
                (
                    "No plano gratis do Sheety, a pagina de precos informa "
                    "200 requests por mes. "
                    "Para uso continuo, use um link direto da planilha "
                    "(Google Sheets/OneDrive/SharePoint) ou aumente o plano do Sheety."
                )
            )

    partes.append(
        (
            "Para parar de dar erro recorrente, a melhor opcao e trocar o "
            "link do Sheety pelo link direto/exportavel da planilha."
        )
    )

    return " ".join(partes)

def ler_planilha_sheety(url, intervalo_minutos=None):
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()

        data = response.json()

        # ðŸ”¥ pega a lista principal
        chave = list(data.keys())[0]
        lista = data[chave]

        df = pd.DataFrame(lista)

        # normaliza colunas
        df.columns = [str(col).strip().lower() for col in df.columns]

        return df

    except requests.HTTPError as e:
        status_code = getattr(getattr(e, "response", None), "status_code", None)

        if status_code in {401, 402, 403, 404}:
            raise FontePlanilhaError(
                montar_mensagem_sheety_bloqueado(status_code, intervalo_minutos),
                fatal=True,
            )

        raise FontePlanilhaError(f"Erro ao ler Sheety: {e}")
    except Exception as e:
        if isinstance(e, FontePlanilhaError):
            raise

        raise FontePlanilhaError(f"Erro ao ler Sheety: {e}")

def sanitizar_para_json(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    
    if isinstance(obj, date):
        return obj.strftime("%Y-%m-%d")
    
    if isinstance(obj, dt_time):  # ðŸ‘ˆ corrigido
        return obj.strftime("%H:%M:%S")
    
    if isinstance(obj, memoryview):
        obj = obj.tobytes()

    if isinstance(obj, (bytes, bytearray)):
        return {"__bytes_b64__": base64.b64encode(bytes(obj)).decode("ascii")}

    return obj


def desserializar_valor_json(obj):
    if (
        isinstance(obj, dict) and
        set(obj.keys()) == {"__bytes_b64__"} and
        obj.get("__bytes_b64__")
    ):
        try:
            return base64.b64decode(obj["__bytes_b64__"])
        except Exception:
            return b""
    return obj


def serializar_registro_snapshot(registro):
    registro_dict = dict(registro or {}) if not isinstance(registro, dict) else dict(registro)
    return {
        chave: sanitizar_para_json(valor)
        for chave, valor in registro_dict.items()
    }

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def agora():
    return datetime.now(ZoneInfo("America/Sao_Paulo"))


def normalizar_datetime_brasilia(valor):
    if not valor:
        return None

    if not isinstance(valor, datetime):
        return None

    timezone_brasilia = ZoneInfo("America/Sao_Paulo")
    if valor.tzinfo is None:
        return valor.replace(tzinfo=timezone_brasilia)

    try:
        return valor.astimezone(timezone_brasilia)
    except Exception:
        return valor

def caminho_banco_absoluto():
    return os.path.abspath(DATABASE_FILE)

def caminho_diretorio_backup():
    os.makedirs(BACKUP_FOLDER, exist_ok=True)
    return os.path.abspath(BACKUP_FOLDER)

def caminho_banco_postgres_temporario():
    return os.path.join(caminho_diretorio_backup(), BACKUP_ARQUIVO_POSTGRES_ATUAL)

def bool_config_ativo(valor):
    texto = str(valor or "").strip().lower()
    return texto in {"1", "true", "on", "sim", "yes"}

def normalizar_caminho_destino_externo(valor):
    texto = str(valor or "").strip().strip('"').strip()
    if not texto:
        return ""

    return os.path.abspath(os.path.expandvars(os.path.expanduser(texto)))

def normalizar_tipo_destino_backup(valor):
    tipo = str(valor or "pasta").strip().lower()
    if tipo in {"google_drive", "drive", "online"}:
        return "google_drive"
    return "pasta"

def carregar_credenciais_google_drive():
    if not GOOGLE_DRIVE_DISPONIVEL:
        return None, "Instale google-api-python-client e google-auth para usar o Google Drive."

    chaves = [
        "GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON",
        "GOOGLE_DRIVE_CREDENTIALS_JSON",
    ]
    dados_json = ""
    for chave in chaves:
        dados_json = (os.environ.get(chave) or "").strip()
        if dados_json:
            break

    if dados_json:
        try:
            credenciais_info = json.loads(dados_json)
            return credenciais_info, ""
        except Exception as e:
            return None, f"Credenciais JSON do Google Drive invalidas: {e}"

    caminho_env = (os.environ.get("GOOGLE_DRIVE_SERVICE_ACCOUNT_FILE") or "").strip()
    if caminho_env and os.path.isfile(caminho_env):
        try:
            with open(caminho_env, "r", encoding="utf-8") as arquivo:
                return json.load(arquivo), ""
        except Exception as e:
            return None, f"Arquivo de credenciais do Google Drive invalido: {e}"

    caminho_local = os.path.join(os.path.dirname(os.path.abspath(__file__)), "google_drive_service_account.json")
    if os.path.isfile(caminho_local):
        try:
            with open(caminho_local, "r", encoding="utf-8") as arquivo:
                return json.load(arquivo), ""
        except Exception as e:
            return None, f"Arquivo local de credenciais do Google Drive invalido: {e}"

    return None, (
        "Configure a variavel GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON ou o arquivo "
        "google_drive_service_account.json para autenticar no Google Drive."
    )

def obter_servico_google_drive():
    credenciais_info, erro = carregar_credenciais_google_drive()
    if erro:
        raise RuntimeError(erro)
    if not credenciais_info:
        raise RuntimeError("Credenciais do Google Drive nao configuradas.")

    credenciais = service_account.Credentials.from_service_account_info(
        credenciais_info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return google_build("drive", "v3", credentials=credenciais, cache_discovery=False)

def caminho_env_google_drive_fallback():
    return (os.environ.get("GOOGLE_DRIVE_FOLDER_ID") or "").strip()

def identificar_tipo_backup_google_drive(nome):
    return identificar_tipo_backup_por_nome(nome)

def formato_tempo_google_drive(modified_time):
    texto = str(modified_time or "").replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(texto)
        return dt.astimezone(ZoneInfo("America/Sao_Paulo")).isoformat(timespec="seconds")
    except Exception:
        return agora_iso()

def listar_pastas_sincronizadas_sugeridas():
    usuario_home = os.path.expanduser("~")
    candidatos = [
        os.environ.get("OneDrive", ""),
        os.environ.get("OneDriveCommercial", ""),
        os.environ.get("OneDriveConsumer", ""),
        os.path.join(usuario_home, "Google Drive"),
        os.path.join(usuario_home, "My Drive"),
        os.path.join(usuario_home, "Desktop", "Google Drive"),
        os.path.join(usuario_home, "Desktop", "My Drive"),
    ]
    encontrados = []

    for caminho in candidatos:
        caminho_normalizado = normalizar_caminho_destino_externo(caminho)
        if caminho_normalizado and os.path.isdir(caminho_normalizado):
            encontrados.append(caminho_normalizado)

    return sorted(set(encontrados))

def traduzir_sql_para_postgres(sql):
    texto = str(sql or "")
    texto = re.sub(
        r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
        "BIGSERIAL PRIMARY KEY",
        texto,
        flags=re.IGNORECASE,
    )
    texto = re.sub(r"\bBLOB\b", "BYTEA", texto, flags=re.IGNORECASE)
    return texto

class CursorCompat:
    def __init__(self, cursor, backend):
        self._cursor = cursor
        self.backend = backend
        self.lastrowid = getattr(cursor, "lastrowid", None)

    def _rollback_if_postgres(self):
        if self.backend != "postgres":
            return
        conn = getattr(self._cursor, "connection", None)
        if conn and hasattr(conn, "rollback"):
            try:
                conn.rollback()
            except Exception:
                pass

    def execute(self, sql, params=None):
        sql = str(sql or "")
        if self.backend == "postgres":
            sql = traduzir_sql_para_postgres(sql)
            parametros = tuple(params or ())
            sql_exec = re.sub(r"\?", "%s", sql)
            if sql_exec.lstrip().upper().startswith("INSERT") and "RETURNING" not in sql_exec.upper():
                sql_exec = f"{sql_exec} RETURNING id"
                try:
                    self._cursor.execute(sql_exec, parametros)
                    resultado = self._cursor.fetchone()
                    self.lastrowid = resultado[0] if resultado else None
                    return self
                except Exception:
                    self._rollback_if_postgres()
                    raise
            try:
                self._cursor.execute(sql_exec, parametros)
            except Exception:
                self._rollback_if_postgres()
                raise
            self.lastrowid = getattr(self._cursor, "lastrowid", None)
            return self

        self._cursor.execute(sql, params or ())
        self.lastrowid = getattr(self._cursor, "lastrowid", None)
        return self

    def executemany(self, sql, seq_of_params):
        sql = str(sql or "")
        if self.backend == "postgres":
            sql = traduzir_sql_para_postgres(sql)
            sql = re.sub(r"\?", "%s", sql)
        try:
            self._cursor.executemany(sql, seq_of_params)
        except Exception:
            self._rollback_if_postgres()
            raise
        self.lastrowid = getattr(self._cursor, "lastrowid", None)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        return self._cursor.close()

    def __iter__(self):
        return iter(self._cursor)

    def __getattr__(self, nome):
        return getattr(self._cursor, nome)

class ConexaoCompat:
    def __init__(self, conn, backend):
        self._conn = conn
        self.backend = backend

    def cursor(self):
        if self.backend == "postgres":
            return CursorCompat(self._conn.cursor(cursor_factory=DictCursor), self.backend)
        return CursorCompat(self._conn.cursor(), self.backend)

    def commit(self):
        return self._conn.commit()

    def close(self):
        return self._conn.close()

    def rollback(self):
        return self._conn.rollback()

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._conn.__exit__(exc_type, exc, tb)

    def __getattr__(self, nome):
        return getattr(self._conn, nome)

def normalizar_tipo_backup(valor):
    tipo = str(valor or BACKUP_TIPO_PADRAO).strip().lower()
    return tipo if tipo in {"banco", "completo"} else BACKUP_TIPO_PADRAO

def label_tipo_backup(valor):
    tipo = normalizar_tipo_backup(valor)
    return next(
        (item["label"] for item in TIPOS_BACKUP if item["value"] == tipo),
        "Sistema completo",
    )

def nome_arquivo_backup(datahora=None, tipo_backup=BACKUP_TIPO_PADRAO):
    datahora = datahora or agora()
    tipo = normalizar_tipo_backup(tipo_backup)
    prefixo = "sistema_completo" if tipo == "completo" else "database_v2"
    if tipo == "completo":
        extensao = ".zip"
    elif banco_online_ativo():
        extensao = ".zip"
    else:
        extensao = ".db"
    return f"{prefixo}_{datahora.strftime('%Y%m%d_%H%M%S')}{extensao}"

def nome_arquivo_backup_banco(datahora=None):
    return nome_arquivo_backup(datahora, "banco")

def configuracao_backup_padrao():
    return {
        "id": 1,
        "empresa_id": 1,
        "frequencia": "diario",
        "tipo_backup": BACKUP_TIPO_PADRAO,
        "retencao_arquivos": BACKUP_RETENCAO_PADRAO,
        "destino_externo_ativo": 0,
        "destino_externo_tipo": "pasta",
        "destino_externo_pasta": "",
        "destino_externo_drive_folder_id": "",
        "atualizado_em": "",
    }

def obter_configuracao_backup():
    def carregar(conn):
        c = conn.cursor()
        return selecionar_registro_administrativo_empresa_cursor(
            c,
            "configuracao_backup",
            empresa_atual_id(),
        )

    row = executar_leitura_resiliente(
        carregar,
        descricao="CONFIG BACKUP",
        padrao=None,
    )

    dados = configuracao_backup_padrao()
    if row:
        dados.update(row)

    frequencia = str(dados.get("frequencia") or "diario").strip().lower()
    dados["frequencia"] = frequencia if frequencia in {"diario", "semanal", "mensal"} else "diario"
    dados["tipo_backup"] = normalizar_tipo_backup(dados.get("tipo_backup"))
    dados["destino_externo_ativo"] = 1 if bool_config_ativo(dados.get("destino_externo_ativo")) else 0
    dados["destino_externo_tipo"] = normalizar_tipo_destino_backup(
        dados.get("destino_externo_tipo") or "pasta"
    )
    dados["destino_externo_pasta"] = normalizar_caminho_destino_externo(
        dados.get("destino_externo_pasta"),
    )
    dados["destino_externo_drive_folder_id"] = normalizar_texto_campo(
        dados.get("destino_externo_drive_folder_id")
    )

    try:
        retencao = int(dados.get("retencao_arquivos") or BACKUP_RETENCAO_PADRAO)
    except Exception:
        retencao = BACKUP_RETENCAO_PADRAO

    dados["retencao_arquivos"] = max(1, min(120, retencao))
    dados["frequencia_label"] = next(
        (item["label"] for item in FREQUENCIAS_BACKUP if item["value"] == dados["frequencia"]),
        "Diario",
    )
    dados["tipo_backup_label"] = label_tipo_backup(dados["tipo_backup"])
    return dados

def salvar_configuracao_backup_form(form):
    frequencia = str(form.get("frequencia") or "diario").strip().lower()
    if frequencia not in {"diario", "semanal", "mensal"}:
        frequencia = "diario"
    tipo_backup = normalizar_tipo_backup(form.get("tipo_backup"))
    destino_externo_ativo = 1 if bool_config_ativo(form.get("destino_externo_ativo")) else 0
    destino_externo_tipo = normalizar_tipo_destino_backup(form.get("destino_externo_tipo"))
    destino_externo_pasta = normalizar_caminho_destino_externo(
        form.get("destino_externo_pasta"),
    )
    destino_externo_drive_folder_id = normalizar_texto_campo(
        form.get("destino_externo_drive_folder_id")
    )

    if destino_externo_tipo == "google_drive":
        destino_externo_pasta = ""
    else:
        destino_externo_drive_folder_id = ""

    try:
        retencao = int(form.get("retencao_arquivos") or BACKUP_RETENCAO_PADRAO)
    except Exception:
        retencao = BACKUP_RETENCAO_PADRAO

    retencao = max(1, min(120, retencao))

    salvar_campos_registro_administrativo_empresa(
        "configuracao_backup",
        {
            "frequencia": frequencia,
            "tipo_backup": tipo_backup,
            "retencao_arquivos": retencao,
            "destino_externo_ativo": destino_externo_ativo,
            "destino_externo_tipo": destino_externo_tipo,
            "destino_externo_pasta": destino_externo_pasta,
            "destino_externo_drive_folder_id": destino_externo_drive_folder_id,
            "atualizado_em": agora_iso(),
        },
    )
    return obter_configuracao_backup()

def periodo_backup_coberto(frequencia, ultimo_dt, agora_atual):
    if not ultimo_dt:
        return False

    if frequencia == "mensal":
        return (ultimo_dt.year, ultimo_dt.month) == (agora_atual.year, agora_atual.month)

    if frequencia == "semanal":
        return ultimo_dt.isocalendar()[:2] == agora_atual.isocalendar()[:2]

    return ultimo_dt.date() == agora_atual.date()

def calcular_proximo_backup_programado(ultimo_dt, frequencia):
    if not ultimo_dt:
        return agora()

    if frequencia == "mensal":
        if ultimo_dt.month == 12:
            return ultimo_dt.replace(
                year=ultimo_dt.year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        return ultimo_dt.replace(
            month=ultimo_dt.month + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    if frequencia == "semanal":
        base = ultimo_dt - timedelta(days=ultimo_dt.weekday())
        return (base + timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)

    return (ultimo_dt + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

def formatar_tamanho_arquivo(tamanho_bytes):
    try:
        valor = float(tamanho_bytes or 0)
    except Exception:
        valor = 0

    if valor < 1024:
        return f"{int(valor)} B"
    if valor < 1024 ** 2:
        return f"{valor / 1024:.1f} KB"
    if valor < 1024 ** 3:
        return f"{valor / (1024 ** 2):.1f} MB"
    return f"{valor / (1024 ** 3):.1f} GB"

def caminho_backup_unico(datahora=None, sufixo="", tipo_backup=BACKUP_TIPO_PADRAO):
    nome_base_original = nome_arquivo_backup(datahora, tipo_backup)
    base_nome, extensao = os.path.splitext(nome_base_original)
    if sufixo:
        base_nome = f"{base_nome}_{sufixo}"

    pasta = caminho_diretorio_backup()
    destino = os.path.join(pasta, f"{base_nome}{extensao}")
    contador = 1

    while os.path.exists(destino):
        destino = os.path.join(pasta, f"{base_nome}_{contador}{extensao}")
        contador += 1

    return destino

def identificar_tipo_backup_por_nome(nome):
    texto = str(nome or "").strip()
    if texto.startswith("sistema_completo_") and texto.endswith(".zip"):
        return "completo"
    if texto.startswith("database_v2_") and texto.endswith((".db", ".zip")):
        return "banco"
    if texto.startswith("database_postgres_") and texto.endswith(".zip"):
        return "banco"
    return ""


def detectar_tipo_backup_por_estrutura(nome, nomes_internos=None):
    tipo_detectado = identificar_tipo_backup_por_nome(nome)
    if tipo_detectado:
        return tipo_detectado

    nomes = set(nomes_internos or [])
    if not nomes:
        return ""

    tem_banco = any(
        item in nomes
        for item in {
            BACKUP_ARQUIVO_POSTGRES_ATUAL,
            BACKUP_ARQUIVO_BANCO_ATUAL,
            "database_v2.db",
        }
    )
    tem_arquivos_sistema = any(item.startswith("static/uploads/") for item in nomes) or ".env" in nomes or ".flaskenv" in nomes

    if tem_banco and tem_arquivos_sistema:
        return "completo"
    if tem_banco:
        return "banco"
    return ""


def montar_manifesto_backup(tipo_backup, arquivos):
    tipo = normalizar_tipo_backup(tipo_backup)
    versao_atual = VERSAO_SISTEMA_PADRAO
    try:
        versao_atual = obter_versao_sistema()
    except Exception:
        pass

    return {
        "tipo_backup": tipo,
        "gerado_em": agora_iso(),
        "versao_sistema": versao_atual,
        "backend": "postgres" if banco_online_ativo() else "sqlite",
        "empresa_id": empresa_atual_id(),
        "arquivos": sorted({str(item or "").strip() for item in (arquivos or []) if str(item or "").strip()}),
    }


def chave_validacao_backup(caminho):
    try:
        stat = os.stat(caminho)
    except Exception:
        return ""
    return f"{normalizar_caminho_arquivo(caminho)}|{int(stat.st_mtime)}|{int(stat.st_size)}"


def validar_snapshot_backup_json(snapshot):
    if not isinstance(snapshot, dict):
        raise ValueError("Snapshot JSON invalido.")

    tabelas = snapshot.get("tabelas")
    if not isinstance(tabelas, dict):
        raise ValueError("Snapshot sem bloco de tabelas.")

    return len(tabelas)


def validar_backup_sqlite_arquivo(caminho):
    conn = sqlite3.connect(f"file:{caminho}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        c = conn.cursor()
        c.execute("PRAGMA quick_check")
        quick_check = (c.fetchone() or [""])[0]
        if str(quick_check or "").lower() != "ok":
            raise ValueError("PRAGMA quick_check retornou inconsistencias.")
        c.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
        total_tabelas = int((c.fetchone() or [0])[0] or 0)
        if total_tabelas <= 0:
            raise ValueError("Backup SQLite sem tabelas.")
        return total_tabelas
    finally:
        conn.close()


def validar_backup_zip_arquivo(caminho, nome_exibicao=""):
    with zipfile.ZipFile(caminho, "r") as arquivo_zip:
        nomes = set(arquivo_zip.namelist())
        if not nomes:
            raise ValueError("Arquivo ZIP vazio.")

        tipo_detectado = detectar_tipo_backup_por_estrutura(nome_exibicao or os.path.basename(caminho), nomes)
        detalhes = []
        manifesto = None
        if BACKUP_ARQUIVO_MANIFESTO in nomes:
            with arquivo_zip.open(BACKUP_ARQUIVO_MANIFESTO, "r") as fh:
                manifesto = json.loads(fh.read().decode("utf-8"))
            tipo_manifesto = normalizar_tipo_backup(manifesto.get("tipo_backup"))
            if tipo_manifesto in {"banco", "completo"}:
                tipo_detectado = tipo_manifesto
            detalhes.append("Manifesto encontrado e lido com sucesso.")
        else:
            detalhes.append("Manifesto nao encontrado; validacao feita pela estrutura do ZIP.")

        if tipo_detectado not in {"banco", "completo"}:
            raise ValueError("Nao foi possivel identificar o tipo do backup pelo ZIP.")

        possui_snapshot_json = BACKUP_ARQUIVO_POSTGRES_ATUAL in nomes
        possui_sqlite = "database_v2.db" in nomes or BACKUP_ARQUIVO_BANCO_ATUAL in nomes

        if possui_snapshot_json:
            with arquivo_zip.open(BACKUP_ARQUIVO_POSTGRES_ATUAL, "r") as fh:
                total_tabelas = validar_snapshot_backup_json(json.loads(fh.read().decode("utf-8")))
            detalhes.append(f"Snapshot JSON valido com {total_tabelas} tabela(s).")

        if possui_sqlite:
            detalhes.append("Arquivo SQLite de banco encontrado no ZIP.")

        if tipo_detectado == "banco" and not (possui_snapshot_json or possui_sqlite):
            raise ValueError("Backup de banco sem snapshot JSON nem arquivo SQLite.")

        if tipo_detectado == "completo":
            possui_uploads = any(item.startswith("static/uploads/") for item in nomes)
            if not (possui_snapshot_json or possui_sqlite):
                raise ValueError("Backup completo sem base de dados interna.")
            if not possui_uploads:
                detalhes.append("Backup completo sem uploads; restauracao de arquivos pode ser parcial.")
            else:
                detalhes.append("Uploads encontrados no pacote completo.")

        return {
            "ok": True,
            "tipo_backup": tipo_detectado,
            "tipo_backup_label": label_tipo_backup(tipo_detectado),
            "mensagem": "Backup validado com sucesso.",
            "detalhes": detalhes,
            "manifesto_encontrado": bool(manifesto),
            "total_arquivos": len(nomes),
        }


def validar_arquivo_backup_local(caminho, nome_exibicao="", usar_cache=True):
    caminho = normalizar_caminho_arquivo(caminho)
    if not caminho or not os.path.isfile(caminho):
        return {
            "ok": False,
            "tipo_backup": "",
            "tipo_backup_label": "",
            "mensagem": "Arquivo de backup nao encontrado.",
            "detalhes": [],
            "manifesto_encontrado": False,
            "total_arquivos": 0,
        }

    chave_cache = chave_validacao_backup(caminho)
    if usar_cache and cache_consulta_valido(BACKUP_VALIDACAO_CACHE, chave_cache, BACKUP_VALIDACAO_CACHE_TTL):
        return copiar_estrutura_cache(BACKUP_VALIDACAO_CACHE.get("resultado"))

    nome = nome_exibicao or os.path.basename(caminho)
    try:
        if str(caminho).lower().endswith(".zip"):
            resultado = validar_backup_zip_arquivo(caminho, nome)
        else:
            total_tabelas = validar_backup_sqlite_arquivo(caminho)
            resultado = {
                "ok": True,
                "tipo_backup": detectar_tipo_backup_por_estrutura(nome) or "banco",
                "tipo_backup_label": label_tipo_backup("banco"),
                "mensagem": "Backup SQLite validado com sucesso.",
                "detalhes": [f"Arquivo SQLite valido com {total_tabelas} tabela(s)."],
                "manifesto_encontrado": False,
                "total_arquivos": 1,
            }
    except Exception as erro:
        resultado = {
            "ok": False,
            "tipo_backup": detectar_tipo_backup_por_estrutura(nome),
            "tipo_backup_label": label_tipo_backup("banco") if str(caminho).lower().endswith(".db") else "",
            "mensagem": f"Falha na validacao do backup: {erro}",
            "detalhes": [],
            "manifesto_encontrado": False,
            "total_arquivos": 0,
        }

    resultado["arquivo"] = nome
    resultado["caminho"] = caminho

    if usar_cache and chave_cache:
        salvar_cache_consulta(BACKUP_VALIDACAO_CACHE, chave_cache, resultado)

    return resultado


def validar_backup_disponivel(item_backup):
    item = dict(item_backup or {})
    caminho = str(item.get("caminho") or "").strip()
    nome = str(item.get("nome") or "").strip()

    if caminho.startswith("drive://"):
        file_id = caminho.split("drive://", 1)[1].strip()
        if not file_id:
            return {
                "ok": False,
                "tipo_backup": item.get("tipo_backup") or "",
                "tipo_backup_label": item.get("tipo_backup_label") or "",
                "mensagem": "Arquivo do Google Drive sem identificador valido.",
                "detalhes": [],
                "manifesto_encontrado": False,
                "total_arquivos": 0,
            }
        arquivo_temp = tempfile.NamedTemporaryFile(
            prefix="validar_backup_drive_",
            suffix=os.path.splitext(nome)[1] or ".bak",
            delete=False,
        )
        arquivo_temp.close()
        try:
            baixar_arquivo_google_drive(file_id, arquivo_temp.name)
            return validar_arquivo_backup_local(arquivo_temp.name, nome_exibicao=nome, usar_cache=False)
        except Exception as erro:
            return {
                "ok": False,
                "tipo_backup": item.get("tipo_backup") or "",
                "tipo_backup_label": item.get("tipo_backup_label") or "",
                "mensagem": f"Falha ao baixar ou validar o backup do Google Drive: {erro}",
                "detalhes": [],
                "manifesto_encontrado": False,
                "total_arquivos": 0,
            }
        finally:
            remover_arquivo_se_existir(arquivo_temp.name)

    return validar_arquivo_backup_local(caminho, nome_exibicao=nome, usar_cache=True)

def listar_arquivos_backup_banco(tipo_backup=None):
    configuracao = obter_configuracao_backup()
    if (
        bool(int(configuracao.get("destino_externo_ativo") or 0))
        and normalizar_tipo_destino_backup(configuracao.get("destino_externo_tipo")) == "google_drive"
    ):
        backups_drive = listar_arquivos_backup_google_drive(configuracao, tipo_backup)
        if backups_drive:
            return backups_drive

    pasta = caminho_diretorio_backup()
    return listar_arquivos_backup_em_pasta(pasta, tipo_backup)

def listar_arquivos_backup_em_pasta(pasta, tipo_backup=None):
    backups = []
    filtro_tipo = normalizar_tipo_backup(tipo_backup) if tipo_backup else ""
    if not pasta or not os.path.isdir(pasta):
        return backups

    for nome in os.listdir(pasta):
        tipo_item = identificar_tipo_backup_por_nome(nome)
        if not tipo_item:
            continue
        if filtro_tipo and tipo_item != filtro_tipo:
            continue

        caminho = os.path.join(pasta, nome)
        if not os.path.isfile(caminho):
            continue

        stat = os.stat(caminho)
        modificado_dt = datetime.fromtimestamp(stat.st_mtime, ZoneInfo("America/Sao_Paulo"))
        backups.append({
            "nome": nome,
            "caminho": caminho,
            "origem": "local",
            "tipo_backup": tipo_item,
            "tipo_backup_label": label_tipo_backup(tipo_item),
            "modificado_em": stat.st_mtime,
            "modificado_dt": modificado_dt,
            "modificado_em_iso": modificado_dt.isoformat(timespec="seconds"),
            "modificado_em_fmt": formatar_datahora(modificado_dt.isoformat(timespec="seconds")),
            "tamanho_bytes": stat.st_size,
            "tamanho_fmt": formatar_tamanho_arquivo(stat.st_size),
        })

    backups.sort(key=lambda item: item["modificado_em"], reverse=True)
    return backups

def listar_arquivos_backup_destino_externo():
    configuracao = obter_configuracao_backup()
    if normalizar_tipo_destino_backup(configuracao.get("destino_externo_tipo")) == "google_drive":
        return listar_arquivos_backup_google_drive(configuracao)

    pasta = configuracao.get("destino_externo_pasta") or ""
    return listar_arquivos_backup_em_pasta(pasta)

def limpar_backups_antigos_em_pasta(pasta, tipo_backup=None):
    backups = listar_arquivos_backup_em_pasta(pasta, tipo_backup)
    retencao = obter_configuracao_backup()["retencao_arquivos"]
    removidos = 0

    for item in backups[retencao:]:
        try:
            os.remove(item["caminho"])
            removidos += 1
        except Exception:
            continue

    return removidos

def limpar_backups_antigos_banco(tipo_backup=None):
    return limpar_backups_antigos_em_pasta(caminho_diretorio_backup(), tipo_backup)

def preparar_destino_externo_backup(configuracao=None, criar=False):
    configuracao = configuracao or obter_configuracao_backup()
    if not bool(int(configuracao.get("destino_externo_ativo") or 0)):
        return ""

    if normalizar_tipo_destino_backup(configuracao.get("destino_externo_tipo")) == "google_drive":
        return obter_drive_folder_id_backup(configuracao)

    pasta = normalizar_caminho_destino_externo(configuracao.get("destino_externo_pasta"))
    if not pasta:
        return ""

    if criar:
        os.makedirs(pasta, exist_ok=True)

    return pasta

def pasta_escrevivel(pasta):
    if not pasta or not os.path.isdir(pasta):
        return False

    arquivo_teste = os.path.join(
        pasta,
        f".wagen_write_test_{secrets.token_hex(6)}.tmp",
    )

    try:
        with open(arquivo_teste, "w", encoding="utf-8") as fh:
            fh.write("ok")
        return True
    except Exception:
        return False
    finally:
        try:
            if os.path.exists(arquivo_teste):
                os.remove(arquivo_teste)
        except Exception:
            pass

def copiar_arquivo_para_destino(origem_path, destino_path):
    os.makedirs(os.path.dirname(destino_path), exist_ok=True)
    caminho_temp = os.path.join(
        tempfile.gettempdir(),
        f"{os.path.basename(destino_path)}.{secrets.token_hex(6)}.tmp",
    )

    if os.path.exists(caminho_temp):
        try:
            os.remove(caminho_temp)
        except Exception:
            pass

    shutil.copy2(origem_path, caminho_temp)
    shutil.copy2(caminho_temp, destino_path)
    try:
        os.remove(caminho_temp)
    except Exception:
        pass

def remover_arquivo_se_existir(caminho):
    if not caminho:
        return
    try:
        if os.path.isfile(caminho):
            os.remove(caminho)
    except Exception:
        pass

def obter_drive_folder_id_backup(configuracao=None):
    configuracao = configuracao or obter_configuracao_backup()
    folder_id = normalizar_texto_campo(configuracao.get("destino_externo_drive_folder_id"))
    if folder_id:
        return folder_id
    return normalizar_texto_campo(caminho_env_google_drive_fallback())

def google_drive_disponivel_para_backup():
    return GOOGLE_DRIVE_DISPONIVEL and service_account is not None and google_build is not None

def google_drive_pronto_para_backup():
    credenciais_info, _ = carregar_credenciais_google_drive()
    return bool(credenciais_info) and google_drive_disponivel_para_backup()

def listar_arquivos_backup_google_drive(configuracao=None, tipo_backup=None):
    configuracao = configuracao or obter_configuracao_backup()
    folder_id = obter_drive_folder_id_backup(configuracao)
    if not folder_id:
        return []
    if not google_drive_disponivel_para_backup():
        return []

    try:
        service = obter_servico_google_drive()
        filtros = [f"'{folder_id}' in parents", "trashed=false"]
        tipo_filtro = normalizar_tipo_backup(tipo_backup) if tipo_backup else ""
        if tipo_filtro:
            if tipo_filtro == "completo":
                filtros.append("(name contains 'sistema_completo_' or name contains 'database_postgres_' or name contains 'database_v2_')")
            else:
                filtros.append("(name contains 'database_v2_' or name contains 'database_postgres_')")

        resposta = service.files().list(
            q=" and ".join(filtros),
            fields="files(id,name,modifiedTime,size,mimeType)",
            pageSize=1000,
            orderBy="modifiedTime desc",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        ).execute()

        backups = []
        for item in resposta.get("files", []):
            nome = item.get("name") or ""
            tipo_item = identificar_tipo_backup_por_nome(nome)
            if tipo_filtro and tipo_item != tipo_filtro:
                continue

            modificado_iso = formato_tempo_google_drive(item.get("modifiedTime"))
            try:
                modificado_dt = datetime.fromisoformat(modificado_iso)
            except Exception:
                modificado_dt = agora()

            tamanho = int(float(item.get("size") or 0))
            backups.append({
                "nome": nome,
                "caminho": f"drive://{item.get('id')}",
                "origem": "google_drive",
                "google_drive_file_id": item.get("id"),
                "tipo_backup": tipo_item,
                "tipo_backup_label": label_tipo_backup(tipo_item),
                "modificado_em": modificado_dt.timestamp(),
                "modificado_dt": modificado_dt,
                "modificado_em_iso": modificado_iso,
                "modificado_em_fmt": formatar_datahora(modificado_iso),
                "tamanho_bytes": tamanho,
                "tamanho_fmt": formatar_tamanho_arquivo(tamanho),
            })

        backups.sort(key=lambda item: item["modificado_em"], reverse=True)
        return backups
    except Exception:
        return []

def upload_arquivo_google_drive(caminho_arquivo, configuracao=None, nome_remoto=None):
    configuracao = configuracao or obter_configuracao_backup()
    folder_id = obter_drive_folder_id_backup(configuracao)
    if not folder_id:
        raise RuntimeError("Informe o ID da pasta do Google Drive.")
    if not google_drive_disponivel_para_backup():
        raise RuntimeError("Instale as dependencias do Google Drive para usar esta opcao.")
    if not os.path.isfile(caminho_arquivo):
        raise FileNotFoundError("Arquivo de backup nao encontrado para upload.")

    service = obter_servico_google_drive()
    nome_remoto = nome_remoto or os.path.basename(caminho_arquivo)
    media = MediaFileUpload(caminho_arquivo, resumable=True)
    metadata = {
        "name": nome_remoto,
        "parents": [folder_id],
    }
    return service.files().create(
        body=metadata,
        media_body=media,
        fields="id,name,modifiedTime,size",
        supportsAllDrives=True,
    ).execute()

def excluir_arquivo_google_drive(file_id):
    if not file_id or not google_drive_disponivel_para_backup():
        return

    service = obter_servico_google_drive()
    service.files().delete(fileId=file_id, supportsAllDrives=True).execute()

def baixar_arquivo_google_drive(file_id, destino_path):
    if not file_id or not google_drive_disponivel_para_backup():
        raise RuntimeError("Google Drive nao configurado para download.")

    service = obter_servico_google_drive()
    request_drive = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    os.makedirs(os.path.dirname(destino_path), exist_ok=True)

    with open(destino_path, "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request_drive)
        concluido = False
        while not concluido:
            progresso, concluido = downloader.next_chunk()

def limpar_backups_antigos_google_drive(configuracao=None, tipo_backup=None):
    configuracao = configuracao or obter_configuracao_backup()
    backups = listar_arquivos_backup_google_drive(configuracao, tipo_backup)
    retencao = int(configuracao.get("retencao_arquivos") or BACKUP_RETENCAO_PADRAO)
    removidos = 0

    for item in backups[retencao:]:
        try:
            excluir_arquivo_google_drive(item.get("google_drive_file_id"))
            removidos += 1
        except Exception:
            continue

    return removidos

def gerar_snapshot_banco_para_arquivo(destino_path):
    if banco_online_ativo():
        gravar_snapshot_banco_em_arquivo(destino_path)
        return

    origem = None
    destino = None
    caminho_origem = caminho_banco_absoluto()

    if not os.path.exists(caminho_origem):
        raise FileNotFoundError("Banco principal nao encontrado para sincronizacao externa.")

    os.makedirs(os.path.dirname(destino_path), exist_ok=True)
    caminho_temp = f"{destino_path}.tmp"

    if os.path.exists(caminho_temp):
        try:
            os.remove(caminho_temp)
        except Exception:
            pass

    try:
        origem = sqlite3.connect(f"file:{caminho_origem}?mode=ro", uri=True)
        destino = sqlite3.connect(caminho_temp)
        with destino:
            origem.backup(destino)
    finally:
        try:
            if origem:
                origem.close()
        except Exception:
            pass
        try:
            if destino:
                destino.close()
        except Exception:
            pass

    os.replace(caminho_temp, destino_path)

def sincronizar_backup_destino_externo(caminho_backup=None, configuracao=None, incluir_snapshot=True):
    configuracao = configuracao or obter_configuracao_backup()
    ativo = bool(int(configuracao.get("destino_externo_ativo") or 0))
    tipo_destino = normalizar_tipo_destino_backup(configuracao.get("destino_externo_tipo"))
    alvo = preparar_destino_externo_backup(configuracao, criar=ativo)

    if not ativo:
        return {
            "ativo": False,
            "sucesso": False,
            "pasta": "",
            "mensagem": "Destino externo desativado.",
        }

    if not alvo:
        return {
            "ativo": True,
            "sucesso": False,
            "pasta": "",
            "mensagem": (
                "Selecione uma pasta sincronizada ou informe o ID da pasta do Google Drive."
            ),
        }

    if tipo_destino != "google_drive" and not pasta_escrevivel(alvo):
        return {
            "ativo": True,
            "sucesso": False,
            "pasta": alvo,
            "arquivos": [],
            "mensagem": "A pasta sincronizada nao esta gravavel neste momento.",
        }

    copiados = []
    aviso_snapshot = ""

    try:
        if tipo_destino == "google_drive":
            if not google_drive_pronto_para_backup():
                return {
                    "ativo": True,
                    "sucesso": False,
                    "pasta": alvo,
                    "arquivos": [],
                    "mensagem": (
                        "Google Drive configurado, mas as dependencias nao estao instaladas."
                    ),
                }

            if caminho_backup and os.path.exists(caminho_backup):
                upload_arquivo_google_drive(caminho_backup, configuracao)
                tipo_backup = identificar_tipo_backup_por_nome(os.path.basename(caminho_backup))
                limpar_backups_antigos_google_drive(configuracao, tipo_backup)
                copiados.append(os.path.basename(caminho_backup))
                remover_arquivo_se_existir(caminho_backup)

            if incluir_snapshot:
                caminho_snapshot = ""
                try:
                    with tempfile.NamedTemporaryFile(
                        suffix=f"_{secrets.token_hex(4)}.zip",
                        delete=False,
                    ) as temp_snapshot:
                        caminho_snapshot = temp_snapshot.name
                    gerar_snapshot_banco_para_arquivo(caminho_snapshot)
                    upload_arquivo_google_drive(
                        caminho_snapshot,
                        configuracao,
                        nome_remoto=os.path.basename(caminho_snapshot),
                    )
                    copiados.append(os.path.basename(caminho_snapshot))
                except Exception as e_snapshot:
                    aviso_snapshot = f" Aviso: snapshot nao enviado ({e_snapshot})."
                finally:
                    try:
                        if caminho_snapshot and os.path.exists(caminho_snapshot):
                            os.remove(caminho_snapshot)
                    except Exception:
                        pass

            limpar_backups_antigos_google_drive(configuracao)
        else:
            if caminho_backup and os.path.exists(caminho_backup):
                destino_backup = os.path.join(alvo, os.path.basename(caminho_backup))
                copiar_arquivo_para_destino(caminho_backup, destino_backup)
                tipo_backup = identificar_tipo_backup_por_nome(os.path.basename(caminho_backup))
                limpar_backups_antigos_em_pasta(alvo, tipo_backup)
                copiados.append(os.path.basename(destino_backup))

            if incluir_snapshot:
                destino_banco = os.path.join(
                    alvo,
                    BACKUP_ARQUIVO_POSTGRES_ATUAL_ZIP if banco_online_ativo() else BACKUP_ARQUIVO_BANCO_ATUAL,
                )
                gerar_snapshot_banco_para_arquivo(destino_banco)
                copiados.append(os.path.basename(destino_banco))

        return {
            "ativo": True,
            "sucesso": True,
            "pasta": alvo,
            "arquivos": copiados,
            "mensagem": f"Sincronizacao externa atualizada com sucesso.{aviso_snapshot}",
        }
    except Exception as e:
        if tipo_destino == "google_drive" and caminho_backup:
            remover_arquivo_se_existir(caminho_backup)
        return {
            "ativo": True,
            "sucesso": False,
            "pasta": alvo,
            "arquivos": copiados,
            "mensagem": f"Falha ao copiar para o {'Google Drive online' if tipo_destino == 'google_drive' else 'pasta sincronizada'}: {e}",
        }

def exportar_snapshot_banco_atual():
    conn = conectar()
    c = conn.cursor()
    snapshot = {
        "backend": "postgres" if banco_online_ativo() else "sqlite",
        "gerado_em": agora_iso(),
        "tabelas": {},
    }

    try:
        for tabela in TABELAS_SISTEMA_ORDENADAS:
            try:
                c.execute(f"SELECT * FROM {tabela} ORDER BY id")
                snapshot["tabelas"][tabela] = [
                    serializar_registro_snapshot(row)
                    for row in c.fetchall()
                ]
            except Exception:
                snapshot["tabelas"][tabela] = []
        return snapshot
    finally:
        try:
            conn.close()
        except Exception:
            pass

def gravar_snapshot_banco_em_arquivo(destino_path):
    os.makedirs(os.path.dirname(destino_path), exist_ok=True)
    caminho_temp = os.path.join(
        tempfile.gettempdir(),
        f"{os.path.basename(destino_path)}.{secrets.token_hex(6)}.tmp",
    )
    snapshot = exportar_snapshot_banco_atual()

    manifesto = montar_manifesto_backup("banco", [BACKUP_ARQUIVO_POSTGRES_ATUAL])

    with zipfile.ZipFile(
        caminho_temp,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as arquivo_zip:
        arquivo_zip.writestr(
            BACKUP_ARQUIVO_POSTGRES_ATUAL,
            json.dumps(snapshot, ensure_ascii=False, indent=2),
        )
        arquivo_zip.writestr(
            BACKUP_ARQUIVO_MANIFESTO,
            json.dumps(manifesto, ensure_ascii=False, indent=2),
        )

    shutil.copy2(caminho_temp, destino_path)
    try:
        os.remove(caminho_temp)
    except Exception:
        pass

def escrever_snapshot_banco_no_zip(arquivo_zip):
    snapshot = exportar_snapshot_banco_atual()
    arquivo_zip.writestr(
        BACKUP_ARQUIVO_POSTGRES_ATUAL,
        json.dumps(snapshot, ensure_ascii=False, indent=2),
    )

def limpar_tabelas_sistema(cursor, backend):
    tabelas = list(reversed(TABELAS_SISTEMA_ORDENADAS))
    if backend == "postgres":
        tabela_sql = ", ".join(tabelas)
        cursor.execute(f"TRUNCATE TABLE {tabela_sql} RESTART IDENTITY CASCADE")
        return

    for tabela in tabelas:
        try:
            cursor.execute(f"DELETE FROM {tabela}")
        except Exception:
            pass

def importar_snapshot_banco_json(caminho_json):
    with open(caminho_json, "r", encoding="utf-8") as fh:
        snapshot = json.load(fh)

    conn = conectar()
    c = conn.cursor()
    backend = "postgres" if banco_online_ativo() else "sqlite"

    try:
        limpar_tabelas_sistema(c, backend)
        tabelas = snapshot.get("tabelas") or {}
        for tabela in TABELAS_SISTEMA_ORDENADAS:
            registros = tabelas.get(tabela) or []
            if not registros:
                continue

            for registro in registros:
                colunas = list(registro.keys())
                valores = [
                    desserializar_valor_json(registro.get(coluna))
                    for coluna in colunas
                ]
                marcadores = ", ".join(["?"] * len(colunas))
                sql = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({marcadores})"
                c.execute(sql, valores)

        if backend == "postgres":
            for tabela in TABELAS_SISTEMA_ORDENADAS:
                try:
                    c.execute(f"SELECT COALESCE(MAX(id), 0) FROM {tabela}")
                    maior_id = c.fetchone()[0] or 0
                    c.execute(
                        f"SELECT setval(pg_get_serial_sequence('{tabela}', 'id'), %s, %s)",
                        (max(1, int(maior_id)), bool(maior_id)),
                    )
                except Exception:
                    continue

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

def importar_sqlite_para_banco_atual(caminho_sqlite):
    if not os.path.isfile(caminho_sqlite):
        raise FileNotFoundError("Arquivo SQLite de origem nao encontrado.")

    origem = sqlite3.connect(f"file:{caminho_sqlite}?mode=ro", uri=True)
    origem.row_factory = sqlite3.Row
    destino = conectar()

    try:
        for tabela in TABELAS_SISTEMA_ORDENADAS:
            if tabela.startswith("sincronizacao_"):
                continue
            sincronizar_tabela_incremental(origem, destino, tabela, origem_prevalece_em_empate=False)
    finally:
        try:
            origem.close()
        except Exception:
            pass
        try:
            destino.close()
        except Exception:
            pass

def listar_fontes_backup_completo():
    fontes = []

    if not banco_online_ativo():
        for nome_banco in ("database_v2.db", "database.db"):
            caminho_banco = os.path.abspath(nome_banco)
            if os.path.isfile(caminho_banco):
                fontes.append((caminho_banco, nome_banco))

    uploads_path = caminho_uploads_absoluto()
    if os.path.isdir(uploads_path):
        fontes.append((uploads_path, "static/uploads"))

    static_base = caminho_static_absoluto()
    if os.path.isdir(static_base):
        for nome in os.listdir(static_base):
            caminho = os.path.join(static_base, nome)
            if not os.path.isfile(caminho):
                continue
            if os.path.splitext(nome)[1].lower() not in {".xlsx", ".xls", ".csv"}:
                continue
            fontes.append((caminho, f"static/{nome}"))

    for nome_local in (".env", ".flaskenv"):
        caminho_local = os.path.abspath(nome_local)
        if os.path.isfile(caminho_local):
            fontes.append((caminho_local, nome_local))

    return fontes

def escrever_zip_backup_completo(destino_path):
    fontes = listar_fontes_backup_completo()
    if not fontes:
        raise ValueError("Nenhum dado elegivel foi encontrado para o backup completo.")

    arquivos_manifesto = []

    with zipfile.ZipFile(
        destino_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=6,
    ) as arquivo_zip:
        for origem_path, destino_rel in fontes:
            if os.path.isdir(origem_path):
                for raiz, _, arquivos in os.walk(origem_path):
                    for nome_arquivo in arquivos:
                        caminho_arquivo = os.path.join(raiz, nome_arquivo)
                        rel_interno = os.path.relpath(caminho_arquivo, origem_path).replace("\\", "/")
                        arcname = f"{destino_rel}/{rel_interno}"
                        arquivo_zip.write(caminho_arquivo, arcname)
                        arquivos_manifesto.append(arcname)
                continue

            arquivo_zip.write(origem_path, destino_rel)
            arquivos_manifesto.append(destino_rel)

        if banco_online_ativo():
            escrever_snapshot_banco_no_zip(arquivo_zip)
            arquivos_manifesto.append(BACKUP_ARQUIVO_POSTGRES_ATUAL)

        manifesto = montar_manifesto_backup("completo", arquivos_manifesto)

        arquivo_zip.writestr(
            BACKUP_ARQUIVO_MANIFESTO,
            json.dumps(manifesto, ensure_ascii=False, indent=2),
        )

def restaurar_arquivo_simples(caminho_origem, caminho_destino):
    os.makedirs(os.path.dirname(caminho_destino), exist_ok=True)
    shutil.copy2(caminho_origem, caminho_destino)

def restaurar_backup_completo_zip(caminho_backup_zip):
    with tempfile.TemporaryDirectory(prefix="restore_backup_") as pasta_temp:
        with zipfile.ZipFile(caminho_backup_zip, "r") as arquivo_zip:
            arquivo_zip.extractall(pasta_temp)

        snapshot_postgres = os.path.join(pasta_temp, BACKUP_ARQUIVO_POSTGRES_ATUAL)
        banco_extraido = os.path.join(pasta_temp, "database_v2.db")

        if banco_online_ativo() and os.path.isfile(snapshot_postgres):
            importar_snapshot_banco_json(snapshot_postgres)
        elif os.path.isfile(banco_extraido):
            origem_db = sqlite3.connect(f"file:{banco_extraido}?mode=ro", uri=True)
            destino_db = sqlite3.connect(caminho_banco_absoluto())
            try:
                with destino_db:
                    origem_db.backup(destino_db)
            finally:
                origem_db.close()
                destino_db.close()

        banco_legado = os.path.join(pasta_temp, "database.db")
        if os.path.isfile(banco_legado):
            restaurar_arquivo_simples(banco_legado, os.path.abspath("database.db"))

        uploads_extraidos = os.path.join(pasta_temp, "static", "uploads")
        if os.path.isdir(uploads_extraidos):
            uploads_destino = caminho_uploads_absoluto()
            if os.path.isdir(uploads_destino):
                shutil.rmtree(uploads_destino)
            shutil.copytree(uploads_extraidos, uploads_destino)

        static_extraido = os.path.join(pasta_temp, "static")
        if os.path.isdir(static_extraido):
            for nome in os.listdir(static_extraido):
                caminho_origem = os.path.join(static_extraido, nome)
                if os.path.isdir(caminho_origem):
                    continue
                if os.path.splitext(nome)[1].lower() not in {".xlsx", ".xls", ".csv"}:
                    continue
                restaurar_arquivo_simples(caminho_origem, os.path.join(caminho_static_absoluto(), nome))

        for nome_local in (".env", ".flaskenv"):
            caminho_origem = os.path.join(pasta_temp, nome_local)
            if os.path.isfile(caminho_origem):
                restaurar_arquivo_simples(caminho_origem, os.path.abspath(nome_local))

def obter_status_backup_banco():
    configuracao = obter_configuracao_backup()
    tipo_backup = configuracao["tipo_backup"]
    backups = listar_arquivos_backup_banco(tipo_backup)
    backups_externos = listar_arquivos_backup_destino_externo()
    ultimo = backups[0] if backups else None
    ultimo_externo = backups_externos[0] if backups_externos else None
    caminho_ultimo = ultimo["caminho"] if ultimo else ""
    proximo_backup_dt = (
        calcular_proximo_backup_programado(ultimo.get("modificado_dt"), configuracao["frequencia"])
        if ultimo else None
    )
    proximo_backup_iso = proximo_backup_dt.isoformat(timespec="seconds") if proximo_backup_dt else ""
    destino_externo_pasta = configuracao.get("destino_externo_pasta") or ""
    destino_externo_tipo = normalizar_tipo_destino_backup(configuracao.get("destino_externo_tipo"))
    destino_externo_drive_folder_id = normalizar_texto_campo(
        configuracao.get("destino_externo_drive_folder_id")
    )
    validacao_ultimo = {
        "ok": None,
        "mensagem": "Validacao sob demanda.",
        "detalhes": [],
    }
    if ultimo and str(ultimo.get("origem") or "") != "google_drive":
        validacao_ultimo = validar_arquivo_backup_local(
            ultimo.get("caminho"),
            nome_exibicao=ultimo.get("nome") or "",
            usar_cache=True,
        )

    return {
        "ativo": True,
        "backend": "postgres" if banco_online_ativo() else "sqlite",
        "backend_label": "Supabase / PostgreSQL" if banco_online_ativo() else "SQLite local",
        "pasta": caminho_diretorio_backup(),
        "retencao": configuracao["retencao_arquivos"],
        "frequencia": configuracao["frequencia"],
        "frequencia_label": configuracao["frequencia_label"],
        "tipo_backup": tipo_backup,
        "tipo_backup_label": configuracao["tipo_backup_label"],
        "quantidade": len(backups),
        "ultimo_backup": caminho_ultimo,
        "ultimo_backup_nome": os.path.basename(caminho_ultimo) if caminho_ultimo else "",
        "ultimo_backup_em": ultimo["modificado_em_iso"] if ultimo else "",
        "ultimo_backup_em_fmt": ultimo["modificado_em_fmt"] if ultimo else "Nenhum backup ainda",
        "ultimo_backup_tamanho": ultimo["tamanho_bytes"] if ultimo else 0,
        "ultimo_backup_tamanho_fmt": ultimo["tamanho_fmt"] if ultimo else "0 B",
        "proximo_backup_em": proximo_backup_iso,
        "proximo_backup_em_fmt": (
            formatar_datahora(proximo_backup_iso) if proximo_backup_iso else "Assim que a rotina rodar"
        ),
        "destino_externo_ativo": bool(int(configuracao.get("destino_externo_ativo") or 0)),
        "destino_externo_tipo": destino_externo_tipo,
        "destino_externo_tipo_label": (
            "Google Drive online" if destino_externo_tipo == "google_drive" else "Pasta sincronizada"
        ),
        "destino_externo_pasta": destino_externo_pasta,
        "destino_externo_drive_folder_id": destino_externo_drive_folder_id,
        "destino_externo_disponivel": (
            bool(destino_externo_drive_folder_id and google_drive_pronto_para_backup())
            if destino_externo_tipo == "google_drive"
            else bool(destino_externo_pasta and os.path.isdir(destino_externo_pasta))
        ),
        "destino_externo_escrevivel": (
            bool(google_drive_pronto_para_backup()) if destino_externo_tipo == "google_drive"
            else pasta_escrevivel(destino_externo_pasta)
        ),
        "destino_externo_quantidade": len(backups_externos),
        "ultimo_destino_externo_nome": ultimo_externo["nome"] if ultimo_externo else "",
        "ultimo_destino_externo_em_fmt": (
            ultimo_externo["modificado_em_fmt"] if ultimo_externo else "Nenhum envio externo ainda"
        ),
        "ultimo_backup_validacao_ok": validacao_ultimo.get("ok"),
        "ultimo_backup_validacao_mensagem": validacao_ultimo.get("mensagem"),
        "ultimo_backup_validacao_detalhes": validacao_ultimo.get("detalhes") or [],
    }

def status_backup_banco_padrao():
    configuracao = configuracao_backup_padrao()
    frequencia = str(configuracao.get("frequencia") or "diario").strip().lower()
    configuracao["frequencia"] = frequencia if frequencia in {"diario", "semanal", "mensal"} else "diario"
    configuracao["tipo_backup"] = normalizar_tipo_backup(configuracao.get("tipo_backup"))
    configuracao["frequencia_label"] = next(
        (item["label"] for item in FREQUENCIAS_BACKUP if item["value"] == configuracao["frequencia"]),
        "Diario",
    )
    configuracao["tipo_backup_label"] = label_tipo_backup(configuracao["tipo_backup"])
    destino_externo_tipo = normalizar_tipo_destino_backup(configuracao.get("destino_externo_tipo"))
    return {
        "ativo": True,
        "backend": "postgres" if banco_online_ativo() else "sqlite",
        "backend_label": "Supabase / PostgreSQL" if banco_online_ativo() else "SQLite local",
        "pasta": caminho_diretorio_backup(),
        "retencao": configuracao["retencao_arquivos"],
        "frequencia": configuracao["frequencia"],
        "frequencia_label": configuracao["frequencia_label"],
        "tipo_backup": configuracao["tipo_backup"],
        "tipo_backup_label": configuracao["tipo_backup_label"],
        "quantidade": 0,
        "ultimo_backup": "",
        "ultimo_backup_nome": "",
        "ultimo_backup_em": "",
        "ultimo_backup_em_fmt": "Nenhum backup ainda",
        "ultimo_backup_tamanho": 0,
        "ultimo_backup_tamanho_fmt": "0 B",
        "proximo_backup_em": "",
        "proximo_backup_em_fmt": "Assim que a rotina rodar",
        "destino_externo_ativo": bool(int(configuracao.get("destino_externo_ativo") or 0)),
        "destino_externo_tipo": destino_externo_tipo,
        "destino_externo_tipo_label": (
            "Google Drive online" if destino_externo_tipo == "google_drive" else "Pasta sincronizada"
        ),
        "destino_externo_pasta": configuracao.get("destino_externo_pasta") or "",
        "destino_externo_drive_folder_id": normalizar_texto_campo(
            configuracao.get("destino_externo_drive_folder_id")
        ),
        "destino_externo_disponivel": False,
        "destino_externo_escrevivel": False,
        "destino_externo_quantidade": 0,
        "ultimo_destino_externo_nome": "",
        "ultimo_destino_externo_em_fmt": "Nenhum envio externo ainda",
        "ultimo_backup_validacao_ok": None,
        "ultimo_backup_validacao_mensagem": "Validacao sob demanda.",
        "ultimo_backup_validacao_detalhes": [],
    }

def criar_backup_banco(force=False, tipo_backup=None):
    if not backup_lock.acquire(blocking=False):
        return False, "Backup ja esta em execucao.", None

    origem = None
    destino = None

    try:
        agora_atual = agora()
        configuracao = obter_configuracao_backup()
        tipo_efetivo = normalizar_tipo_backup(tipo_backup or configuracao["tipo_backup"])
        backups = listar_arquivos_backup_banco(tipo_efetivo)
        ultimo_backup = backups[0] if backups else None

        if (
            ultimo_backup and
            not force and
            periodo_backup_coberto(configuracao["frequencia"], ultimo_backup.get("modificado_dt"), agora_atual)
        ):
            sync_externo = sincronizar_backup_destino_externo(
                ultimo_backup["caminho"],
                configuracao=configuracao,
                incluir_snapshot=True,
            )
            mensagem_periodo = (
                f"Backup {configuracao['frequencia_label'].lower()} "
                f"{label_tipo_backup(tipo_efetivo).lower()} ja criado no periodo atual."
            )
            if sync_externo["ativo"]:
                if sync_externo["sucesso"]:
                    mensagem_periodo += " Copia externa atualizada."
                else:
                    mensagem_periodo += f" Aviso: {sync_externo['mensagem']}"
            return (
                True,
                mensagem_periodo,
                ultimo_backup["caminho"],
            )

        destino_path = caminho_backup_unico(agora_atual, tipo_backup=tipo_efetivo)

        if tipo_efetivo == "completo":
            escrever_zip_backup_completo(destino_path)
            limpar_backups_antigos_banco(tipo_efetivo)
            mensagem = "Backup completo do sistema criado com sucesso."
            sync_externo = sincronizar_backup_destino_externo(
                destino_path,
                configuracao=configuracao,
                incluir_snapshot=True,
            )
            if sync_externo["ativo"]:
                if sync_externo["sucesso"]:
                    mensagem += " Copia externa atualizada."
                else:
                    mensagem += f" Aviso: {sync_externo['mensagem']}"
            return True, mensagem, destino_path

        if banco_online_ativo():
            gravar_snapshot_banco_em_arquivo(destino_path)
        else:
            caminho_origem = caminho_banco_absoluto()
            if not os.path.exists(caminho_origem):
                return False, "Banco principal nao encontrado para backup.", None

            origem = sqlite3.connect(f"file:{caminho_origem}?mode=ro", uri=True)
            destino = sqlite3.connect(destino_path)

            with destino:
                origem.backup(destino)

        limpar_backups_antigos_banco(tipo_efetivo)
        mensagem = "Backup automatico do banco criado com sucesso."
        sync_externo = sincronizar_backup_destino_externo(
            destino_path,
            configuracao=configuracao,
            incluir_snapshot=True,
        )
        if sync_externo["ativo"]:
            if sync_externo["sucesso"]:
                mensagem += " Copia externa atualizada."
            else:
                mensagem += f" Aviso: {sync_externo['mensagem']}"
        return True, mensagem, destino_path
    except Exception as e:
        return False, f"Erro ao criar backup: {e}", None
    finally:
        try:
            if origem:
                origem.close()
        except Exception:
            pass
        try:
            if destino:
                destino.close()
        except Exception:
            pass
        backup_lock.release()

def restaurar_backup_banco(nome_arquivo_backup):
    backups_disponiveis = {item["nome"]: item for item in listar_arquivos_backup_banco()}
    selecionado = backups_disponiveis.get(str(nome_arquivo_backup or "").strip())

    if not selecionado:
        return False, "Backup selecionado nao foi encontrado."

    backup_drive_temp = None
    if str(selecionado.get("caminho") or "").startswith("drive://"):
        file_id = str(selecionado["caminho"]).split("drive://", 1)[1].strip()
        if file_id:
            try:
                backup_drive_temp = tempfile.NamedTemporaryFile(
                    prefix="restore_drive_",
                    suffix=os.path.splitext(selecionado["nome"])[1] or ".bak",
                    delete=False,
                )
                backup_drive_temp.close()
                baixar_arquivo_google_drive(file_id, backup_drive_temp.name)
                selecionado = dict(selecionado)
                selecionado["caminho"] = backup_drive_temp.name
            except Exception as erro:
                if backup_drive_temp:
                    remover_arquivo_se_existir(backup_drive_temp.name)
                return False, f"Nao foi possivel baixar o backup do Google Drive: {erro}"

    validacao = validar_arquivo_backup_local(
        selecionado.get("caminho"),
        nome_exibicao=selecionado.get("nome") or "",
        usar_cache=False,
    )
    if not validacao.get("ok"):
        if backup_drive_temp:
            remover_arquivo_se_existir(backup_drive_temp.name)
        return False, validacao.get("mensagem") or "O backup selecionado falhou na validacao."

    if not sync_lock.acquire(blocking=False):
        if backup_drive_temp:
            remover_arquivo_se_existir(backup_drive_temp.name)
        return False, "Existe uma sincronizacao em andamento. Tente restaurar novamente em alguns segundos."

    origem = None
    destino = None
    backup_preventivo = None
    restore_lock = False

    try:
        sucesso_backup, mensagem_backup, caminho_backup = criar_backup_banco(
            force=True,
            tipo_backup=selecionado["tipo_backup"],
        )
        if not sucesso_backup:
            return False, f"Nao foi possivel criar o backup preventivo antes da restauracao. {mensagem_backup}"

        backup_preventivo = caminho_backup

        if not backup_lock.acquire(blocking=False):
            return False, "Ja existe um backup ou restauracao em andamento. Tente novamente em instantes."

        restore_lock = True
        if selecionado["tipo_backup"] == "completo":
            restaurar_backup_completo_zip(selecionado["caminho"])
        elif banco_online_ativo():
            if selecionado["caminho"].lower().endswith(".zip"):
                with tempfile.TemporaryDirectory(prefix="restore_backup_") as pasta_temp:
                    with zipfile.ZipFile(selecionado["caminho"], "r") as arquivo_zip:
                        arquivo_zip.extractall(pasta_temp)

                    snapshot_postgres = os.path.join(pasta_temp, BACKUP_ARQUIVO_POSTGRES_ATUAL)
                    if os.path.isfile(snapshot_postgres):
                        importar_snapshot_banco_json(snapshot_postgres)
                    else:
                        banco_extraido = os.path.join(pasta_temp, "database_v2.db")
                        if os.path.isfile(banco_extraido):
                            importar_sqlite_para_banco_atual(banco_extraido)
                        else:
                            raise FileNotFoundError("Snapshot do banco nao encontrado no backup online.")
            else:
                importar_sqlite_para_banco_atual(selecionado["caminho"])
        else:
            origem = sqlite3.connect(f"file:{selecionado['caminho']}?mode=ro", uri=True)
            destino = sqlite3.connect(caminho_banco_absoluto())

            with destino:
                origem.backup(destino)

            origem.close()
            origem = None
            destino.close()
            destino = None

        init_db()
        sync_externo = sincronizar_backup_destino_externo(
            None,
            configuracao=obter_configuracao_backup(),
            incluir_snapshot=True,
        )

        nome_preventivo = os.path.basename(backup_preventivo) if backup_preventivo else ""
        mensagem = (
            f"Banco restaurado com sucesso usando {selecionado['nome']}. "
            f"Backup preventivo salvo em {nome_preventivo}."
        )
        if sync_externo["ativo"] and not sync_externo["sucesso"]:
            mensagem += f" Aviso: {sync_externo['mensagem']}"
        return (
            True,
            mensagem,
        )
    except Exception as e:
        return False, f"Erro ao restaurar backup: {e}"
    finally:
        try:
            if origem:
                origem.close()
        except Exception:
            pass
        try:
            if destino:
                destino.close()
        except Exception:
            pass
        if backup_drive_temp:
            try:
                if os.path.exists(backup_drive_temp.name):
                    os.remove(backup_drive_temp.name)
            except Exception:
                pass
        if restore_lock:
            try:
                backup_lock.release()
            except Exception:
                pass
        try:
            sync_lock.release()
        except Exception:
            pass

def usuario_sistema_interno():
    return {"id": None, "usuario": "sistema", "nome": "Sistema"}

def caminho_static_absoluto():
    return os.path.abspath("static")

def caminho_uploads_absoluto():
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    return os.path.abspath(UPLOAD_FOLDER)

def caminho_uploads_orfaos_absoluto():
    os.makedirs(UPLOADS_ORFAOS_FOLDER, exist_ok=True)
    return os.path.abspath(UPLOADS_ORFAOS_FOLDER)

def caminho_uploads_thumbs_absoluto():
    os.makedirs(UPLOADS_THUMBS_FOLDER, exist_ok=True)
    return os.path.abspath(UPLOADS_THUMBS_FOLDER)

def caminho_uploads_perfis_absoluto():
    os.makedirs(UPLOADS_PERFIS_FOLDER, exist_ok=True)
    return os.path.abspath(UPLOADS_PERFIS_FOLDER)

def caminho_uploads_servicos_diretorio(datahora=None):
    datahora = datahora or agora()
    pasta = os.path.join(
        caminho_uploads_absoluto(),
        "servicos",
        datahora.strftime("%Y"),
        datahora.strftime("%m"),
    )
    os.makedirs(pasta, exist_ok=True)
    return pasta

def normalizar_caminho_arquivo(caminho):
    texto = str(caminho or "").strip()
    if not texto:
        return ""

    if os.path.isabs(texto):
        return os.path.normpath(texto)

    return os.path.normpath(os.path.abspath(texto))

def caminho_relativo_static(caminho):
    texto = str(caminho or "").strip()
    if not texto:
        return ""

    texto_normalizado = texto.replace("\\", "/")
    if texto_normalizado.startswith("/static/"):
        return texto_normalizado.lstrip("/")
    if texto_normalizado.startswith("static/"):
        return texto_normalizado

    caminho_abs = normalizar_caminho_arquivo(texto)
    if not caminho_abs:
        return ""

    try:
        rel = os.path.relpath(caminho_abs, caminho_static_absoluto()).replace("\\", "/")
    except Exception:
        return ""

    if rel.startswith(".."):
        return ""

    return f"static/{rel}"

def arquivo_dentro_da_pasta(caminho_arquivo, pasta_base):
    try:
        return os.path.commonpath([normalizar_caminho_arquivo(caminho_arquivo), os.path.abspath(pasta_base)]) == os.path.abspath(pasta_base)
    except Exception:
        return False

def obter_iniciais_usuario(nome, usuario=""):
    texto = str(nome or usuario or "").strip()
    if not texto:
        return "US"

    partes = [item for item in re.split(r"\s+", texto) if item]
    if not partes:
        return "US"

    if len(partes) == 1:
        return partes[0][:2].upper()

    return (partes[0][:1] + partes[-1][:1]).upper()

def caminho_absoluto_usuario_foto(caminho):
    texto = str(caminho or "").strip()
    if not texto:
        return ""

    if os.path.isabs(texto):
        return os.path.normpath(texto)

    if texto.startswith("/static/"):
        texto = texto.lstrip("/")

    if texto.startswith("static/"):
        relativo = texto[len("static/"):].replace("/", os.sep)
        return os.path.normpath(os.path.join(caminho_static_absoluto(), relativo))

    return os.path.normpath(os.path.join(caminho_uploads_perfis_absoluto(), os.path.basename(texto)))

def caminho_relativo_usuario_foto(nome_arquivo):
    nome = os.path.basename(str(nome_arquivo or "").replace("\\", "/").strip())
    if not nome:
        return ""

    return f"static/uploads/perfis/{nome}"

def normalizar_registro_usuario_foto(caminho):
    texto = str(caminho or "").strip()
    if not texto:
        return ""

    rel_static = caminho_relativo_static(texto)
    if rel_static:
        nome_rel = rel_static.replace("\\", "/")
        if nome_rel.startswith("static/uploads/perfis/"):
            return nome_rel

    return caminho_relativo_usuario_foto(texto)

def url_foto_usuario(caminho, usuario_id=None):
    if usuario_id and str(caminho or "").strip():
        return f"/usuarios/{int(usuario_id)}/foto"
    return caminho_foto_para_url(caminho) if str(caminho or "").strip() else ""

def remover_foto_perfil_antiga(caminho):
    caminho_abs = caminho_absoluto_usuario_foto(caminho)
    if not caminho_abs or not os.path.exists(caminho_abs):
        return

    if not arquivo_dentro_da_pasta(caminho_abs, caminho_uploads_perfis_absoluto()):
        return

    try:
        os.remove(caminho_abs)
    except Exception:
        pass

def localizar_arquivo_em_subpastas(pasta_base, nome_arquivo):
    nome = os.path.basename(str(nome_arquivo or "").replace("\\", "/").strip())
    if not nome or not os.path.exists(pasta_base):
        return ""

    for raiz, _, arquivos in os.walk(pasta_base):
        if nome in arquivos:
            return os.path.join(raiz, nome)

    return ""

def reparar_registros_foto_perfil():
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT id, foto_perfil
        FROM usuarios
        WHERE foto_perfil IS NOT NULL AND TRIM(foto_perfil) <> ''
    """)
    usuarios = c.fetchall()
    atualizacoes = []

    for usuario in usuarios:
        registro_atual = str(usuario["foto_perfil"] or "").strip()
        registro_normalizado = normalizar_registro_usuario_foto(registro_atual)
        caminho_atual = caminho_absoluto_usuario_foto(registro_atual)
        caminho_normalizado = caminho_absoluto_usuario_foto(registro_normalizado)

        if caminho_normalizado and os.path.exists(caminho_normalizado):
            if registro_atual != registro_normalizado:
                atualizacoes.append((registro_normalizado, usuario["id"]))
            continue

        if caminho_atual and os.path.exists(caminho_atual):
            if registro_atual != registro_normalizado:
                atualizacoes.append((registro_normalizado, usuario["id"]))
            continue

        encontrado_em_orfaos = localizar_arquivo_em_subpastas(
            caminho_uploads_orfaos_absoluto(),
            registro_atual,
        )
        if not encontrado_em_orfaos:
            continue

        destino = caminho_absoluto_usuario_foto(registro_normalizado)
        if not destino:
            continue

        os.makedirs(os.path.dirname(destino), exist_ok=True)
        try:
            if os.path.normcase(os.path.normpath(encontrado_em_orfaos)) != os.path.normcase(
                os.path.normpath(destino)
            ):
                if os.path.exists(destino):
                    os.remove(destino)
                shutil.move(encontrado_em_orfaos, destino)
            atualizacoes.append((registro_normalizado, usuario["id"]))
        except Exception:
            continue

    if atualizacoes:
        c.executemany(
            "UPDATE usuarios SET foto_perfil=? WHERE id=?",
            atualizacoes,
        )
        conn.commit()

    conn.close()
    return len(atualizacoes)


def sincronizar_blobs_foto_perfil():
    conn = conectar()
    c = conn.cursor()
    try:
        c.execute(
            """
            SELECT id, foto_perfil, foto_perfil_blob, foto_perfil_mime_type, foto_perfil_arquivo_nome
            FROM usuarios
            WHERE foto_perfil IS NOT NULL AND TRIM(foto_perfil) <> ''
            """
        )
        usuarios = c.fetchall()
        for usuario in usuarios:
            if (
                usuario["foto_perfil_blob"]
                and str(usuario["foto_perfil_mime_type"] or "").strip()
                and str(usuario["foto_perfil_arquivo_nome"] or "").strip()
            ):
                continue

            caminho_abs = caminho_absoluto_usuario_foto(usuario["foto_perfil"])
            if not caminho_abs or not os.path.isfile(caminho_abs):
                continue

            try:
                c.execute(
                    """
                    UPDATE usuarios
                    SET foto_perfil_blob=?,
                        foto_perfil_mime_type=?,
                        foto_perfil_arquivo_nome=?
                    WHERE id=?
                    """,
                    (
                        ler_bytes_arquivo(caminho_abs),
                        detectar_mime_type_arquivo(caminho_abs),
                        os.path.basename(caminho_abs),
                        usuario["id"],
                    ),
                )
            except Exception:
                continue
        conn.commit()
    finally:
        conn.close()

def coletar_metricas_diretorio(pasta, ignorar_subpastas=None):
    base = os.path.abspath(pasta)
    ignorar_subpastas = [os.path.abspath(item) for item in (ignorar_subpastas or [])]
    total_arquivos = 0
    total_bytes = 0

    if not os.path.exists(base):
        return {"arquivos": 0, "bytes": 0, "tamanho_fmt": "0 B"}

    for raiz, dirs, arquivos in os.walk(base):
        raiz_abs = os.path.abspath(raiz)
        dirs[:] = [
            item for item in dirs
            if os.path.abspath(os.path.join(raiz, item)) not in ignorar_subpastas
        ]

        if raiz_abs in ignorar_subpastas:
            continue

        for nome in arquivos:
            caminho = os.path.join(raiz_abs, nome)
            try:
                total_bytes += os.path.getsize(caminho)
                total_arquivos += 1
            except Exception:
                continue

    return {
        "arquivos": total_arquivos,
        "bytes": total_bytes,
        "tamanho_fmt": formatar_tamanho_arquivo(total_bytes),
    }

def listar_arquivos_recursivos(pasta):
    base = os.path.abspath(pasta)
    arquivos = []

    if not os.path.exists(base):
        return arquivos

    for raiz, _, nomes in os.walk(base):
        for nome in nomes:
            caminho = os.path.join(raiz, nome)
            try:
                stat = os.stat(caminho)
            except Exception:
                continue

            arquivos.append({
                "caminho": caminho,
                "nome": nome,
                "modificado_em": stat.st_mtime,
                "tamanho_bytes": stat.st_size,
            })

    return arquivos

def remover_diretorios_vazios(base, preservar=None):
    base_abs = os.path.abspath(base)
    preservar = {os.path.abspath(item) for item in (preservar or [])}
    removidos = 0

    if not os.path.exists(base_abs):
        return removidos

    for raiz, dirs, _ in os.walk(base_abs, topdown=False):
        raiz_abs = os.path.abspath(raiz)
        if raiz_abs == base_abs or raiz_abs in preservar:
            continue

        try:
            if not os.listdir(raiz_abs):
                os.rmdir(raiz_abs)
                removidos += 1
        except Exception:
            continue

    return removidos

def obter_status_manutencao_arquivos_db():
    padrao = {
        "id": 1,
        "empresa_id": 1,
        "ultimo_executado_em": "",
        "ultima_mensagem": "",
        "ultimo_resultado_json": "",
    }

    def carregar(conn):
        c = conn.cursor()
        row = selecionar_registro_administrativo_empresa_cursor(
            c,
            "manutencao_arquivos",
            empresa_atual_id(),
        )
        return row if row else dict(padrao)

    return executar_leitura_resiliente(
        carregar,
        descricao="STATUS MANUTENCAO ARQUIVOS",
        padrao=padrao,
    )

def salvar_status_manutencao_arquivos(resultado, mensagem):
    salvar_campos_registro_administrativo_empresa(
        "manutencao_arquivos",
        {
            "ultimo_executado_em": agora_iso(),
            "ultima_mensagem": mensagem,
            "ultimo_resultado_json": json.dumps(
                resultado or {},
                ensure_ascii=False,
                default=sanitizar_para_json,
            ),
        },
    )

def obter_estatisticas_armazenamento():
    uploads_base = caminho_uploads_absoluto()
    orfaos_base = caminho_uploads_orfaos_absoluto()
    thumbs_base = caminho_uploads_thumbs_absoluto()
    backups_base = caminho_diretorio_backup()
    banco_path = caminho_banco_absoluto()

    uploads = coletar_metricas_diretorio(uploads_base, ignorar_subpastas=[orfaos_base, thumbs_base])
    orfaos = coletar_metricas_diretorio(orfaos_base)
    thumbs = coletar_metricas_diretorio(thumbs_base)
    backups = coletar_metricas_diretorio(backups_base)
    banco_bytes = os.path.getsize(banco_path) if os.path.exists(banco_path) else 0
    total_sistema = uploads["bytes"] + orfaos["bytes"] + thumbs["bytes"] + backups["bytes"] + banco_bytes
    disco = shutil.disk_usage(os.path.abspath("."))

    return {
        "uploads": uploads,
        "orfaos": orfaos,
        "thumbs": thumbs,
        "backups": backups,
        "banco": {
            "bytes": banco_bytes,
            "tamanho_fmt": formatar_tamanho_arquivo(banco_bytes),
        },
        "sistema": {
            "bytes": total_sistema,
            "tamanho_fmt": formatar_tamanho_arquivo(total_sistema),
        },
        "disco": {
            "total_bytes": disco.total,
            "livre_bytes": disco.free,
            "usado_bytes": disco.used,
            "total_fmt": formatar_tamanho_arquivo(disco.total),
            "livre_fmt": formatar_tamanho_arquivo(disco.free),
            "usado_fmt": formatar_tamanho_arquivo(disco.used),
        },
    }

def obter_status_arquivos():
    estado = obter_status_manutencao_arquivos_db()
    armazenamento = obter_estatisticas_armazenamento()
    resultado = {}

    try:
        resultado = json.loads(estado.get("ultimo_resultado_json") or "{}")
    except Exception:
        resultado = {}

    return {
        "ultimo_executado_em": estado.get("ultimo_executado_em") or "",
        "ultimo_executado_em_fmt": (
            formatar_datahora(estado.get("ultimo_executado_em"))
            if estado.get("ultimo_executado_em") else "Ainda nao executada"
        ),
        "ultima_mensagem": estado.get("ultima_mensagem") or "Nenhuma manutencao registrada ainda.",
        "ultimo_resultado": resultado,
        "armazenamento": armazenamento,
        "uploads_pasta": caminho_uploads_absoluto(),
        "orfaos_pasta": caminho_uploads_orfaos_absoluto(),
        "thumbs_pasta": caminho_uploads_thumbs_absoluto(),
    }

def status_arquivos_padrao():
    try:
        armazenamento = obter_estatisticas_armazenamento()
    except Exception:
        armazenamento = {
            "uploads": {"bytes": 0, "arquivos": 0, "tamanho_fmt": "0 B"},
            "orfaos": {"bytes": 0, "arquivos": 0, "tamanho_fmt": "0 B"},
            "thumbs": {"bytes": 0, "arquivos": 0, "tamanho_fmt": "0 B"},
            "backups": {"bytes": 0, "arquivos": 0, "tamanho_fmt": "0 B"},
            "banco": {"bytes": 0, "tamanho_fmt": "0 B"},
            "sistema": {"bytes": 0, "tamanho_fmt": "0 B"},
            "disco": {
                "total_bytes": 0,
                "livre_bytes": 0,
                "usado_bytes": 0,
                "total_fmt": "0 B",
                "livre_fmt": "0 B",
                "usado_fmt": "0 B",
            },
        }
    return {
        "ultimo_executado_em": "",
        "ultimo_executado_em_fmt": "Ainda nao executada",
        "ultima_mensagem": "Nenhuma manutencao registrada ainda.",
        "ultimo_resultado": {},
        "armazenamento": armazenamento,
        "uploads_pasta": caminho_uploads_absoluto(),
        "orfaos_pasta": caminho_uploads_orfaos_absoluto(),
        "thumbs_pasta": caminho_uploads_thumbs_absoluto(),
    }

def executar_manutencao_arquivos(force=False, registrar_log=True, usuario=None):
    if not maintenance_lock.acquire(blocking=False):
        return False, "Manutencao de arquivos ja esta em execucao.", {}

    try:
        estado = obter_status_manutencao_arquivos_db()
        ultimo_dt = interpretar_datahora_sistema(estado.get("ultimo_executado_em"))
        agora_atual = agora()

        if ultimo_dt and not force and ultimo_dt.date() == agora_atual.date():
            resultado = {}
            try:
                resultado = json.loads(estado.get("ultimo_resultado_json") or "{}")
            except Exception:
                resultado = {}
            return True, "Manutencao diaria ja executada hoje.", resultado

        uploads_base = caminho_uploads_absoluto()
        orfaos_base = caminho_uploads_orfaos_absoluto()
        thumbs_base = caminho_uploads_thumbs_absoluto()
        referenced = set()

        conn = conectar()
        c = conn.cursor()
        c.execute("SELECT caminho FROM fotos WHERE caminho IS NOT NULL AND TRIM(caminho) <> ''")
        for row in c.fetchall():
            caminho_ref = normalizar_caminho_arquivo(row["caminho"])
            if caminho_ref:
                referenced.add(caminho_ref)
        c.execute("""
            SELECT foto_perfil
            FROM usuarios
            WHERE foto_perfil IS NOT NULL AND TRIM(foto_perfil) <> ''
        """)
        for row in c.fetchall():
            caminho_ref = caminho_absoluto_usuario_foto(row["foto_perfil"])
            if caminho_ref:
                referenced.add(normalizar_caminho_arquivo(caminho_ref))
        conn.close()

        orfaos_movidos = 0
        thumbs_removidas = 0
        orfaos_removidos = 0

        for arquivo in listar_arquivos_recursivos(uploads_base):
            caminho_arquivo = arquivo["caminho"]
            if arquivo_dentro_da_pasta(caminho_arquivo, orfaos_base) or arquivo_dentro_da_pasta(caminho_arquivo, thumbs_base):
                continue

            if normalizar_caminho_arquivo(caminho_arquivo) in referenced:
                continue

            if (time.time() - arquivo["modificado_em"]) < IDADE_MINIMA_ORFAO_SEGUNDOS:
                continue

            destino_dir = os.path.join(orfaos_base, agora_atual.strftime("%Y-%m"))
            os.makedirs(destino_dir, exist_ok=True)
            destino = os.path.join(destino_dir, os.path.basename(caminho_arquivo))
            contador = 1
            while os.path.exists(destino):
                nome, ext = os.path.splitext(os.path.basename(caminho_arquivo))
                destino = os.path.join(destino_dir, f"{nome}_{contador}{ext}")
                contador += 1

            try:
                shutil.move(caminho_arquivo, destino)
                orfaos_movidos += 1
            except Exception:
                continue

        limite_orfaos = time.time() - (RETENCAO_ORFAOS_DIAS * 86400)
        for arquivo in listar_arquivos_recursivos(orfaos_base):
            if arquivo["modificado_em"] < limite_orfaos:
                try:
                    os.remove(arquivo["caminho"])
                    orfaos_removidos += 1
                except Exception:
                    continue

        limite_thumbs = time.time() - (RETENCAO_THUMBS_DIAS * 86400)
        for arquivo in listar_arquivos_recursivos(thumbs_base):
            if arquivo["modificado_em"] < limite_thumbs:
                try:
                    os.remove(arquivo["caminho"])
                    thumbs_removidas += 1
                except Exception:
                    continue

        diretorios_limpos = 0
        diretorios_limpos += remover_diretorios_vazios(uploads_base, preservar=[uploads_base, orfaos_base, thumbs_base])
        diretorios_limpos += remover_diretorios_vazios(orfaos_base, preservar=[orfaos_base])
        diretorios_limpos += remover_diretorios_vazios(thumbs_base, preservar=[thumbs_base])

        armazenamento = obter_estatisticas_armazenamento()
        resultado = {
            "orfaos_movidos": orfaos_movidos,
            "orfaos_removidos": orfaos_removidos,
            "thumbs_removidas": thumbs_removidas,
            "diretorios_limpos": diretorios_limpos,
            "uploads_ativos": armazenamento["uploads"]["arquivos"],
            "uploads_tamanho": armazenamento["uploads"]["tamanho_fmt"],
            "orfaos_tamanho": armazenamento["orfaos"]["tamanho_fmt"],
        }
        mensagem = (
            f"Manutencao concluida. Orfaos movidos: {orfaos_movidos} | "
            f"Orfaos removidos: {orfaos_removidos} | "
            f"Miniaturas limpas: {thumbs_removidas}."
        )
        salvar_status_manutencao_arquivos(resultado, mensagem)

        if registrar_log:
            registrar_auditoria(
                "manutencao_arquivos",
                "arquivos",
                detalhes=resultado,
                usuario=usuario or usuario_sistema_interno(),
            )

        return True, mensagem, resultado
    except Exception as e:
        mensagem = f"Erro na manutencao de arquivos: {e}"
        salvar_status_manutencao_arquivos({}, mensagem)
        return False, mensagem, {}
    finally:
        maintenance_lock.release()

def loop_worker_manutencao_arquivos():
    primeira_execucao = True
    while True:
        if primeira_execucao:
            primeira_execucao = False
            time.sleep(WORKER_MANUTENCAO_DELAY_INICIAL)
        try:
            sucesso, mensagem, _ = executar_manutencao_arquivos(
                force=False,
                registrar_log=False,
                usuario=usuario_sistema_interno(),
            )
            if sucesso and "concluida" in mensagem.lower():
                log_info(f"ARQUIVOS: {mensagem}")
        except Exception as e:
            log_info("ERRO WORKER ARQUIVOS:", e)

        time.sleep(3600)

def iniciar_worker_manutencao_arquivos():
    global maintenance_worker_iniciado

    if maintenance_worker_iniciado:
        return

    maintenance_worker_iniciado = True
    Thread(target=loop_worker_manutencao_arquivos, daemon=True).start()

def normalizar_periodo_auditoria(valor):
    valor = str(valor or "").strip().lower()
    if valor in {"hoje", "7dias", "30dias", "todos"}:
        return valor
    return "7dias"

def filtrar_inicio_periodo_auditoria(periodo, referencia=None):
    referencia = referencia or agora()
    if periodo == "todos":
        return None
    if periodo == "30dias":
        return referencia - timedelta(days=30)
    if periodo == "7dias":
        return referencia - timedelta(days=7)
    return referencia.replace(hour=0, minute=0, second=0, microsecond=0)

def formatar_acao_auditoria(acao):
    chave = normalizar_texto_campo(acao)
    if not chave:
        return "Acao"
    if chave in ACOES_AUDITORIA_LABELS:
        return ACOES_AUDITORIA_LABELS[chave]
    return chave.replace("_", " ").capitalize()

def carregar_contexto_auditoria(args):
    periodo = normalizar_periodo_auditoria(args.get("periodo"))
    usuario = normalizar_texto_campo(args.get("usuario"))
    placa = normalizar_texto_campo(args.get("placa")).upper()
    acao = normalizar_texto_campo(args.get("acao"))
    busca = normalizar_texto_campo(args.get("busca"))
    inicio_periodo = filtrar_inicio_periodo_auditoria(periodo)
    usuario_cache = str(session.get("usuario") or "")
    chave_cache = f"{usuario_cache}|{periodo}|{usuario}|{placa}|{acao}|{busca}"
    contexto_cache = obter_cache_consulta(
        AUDITORIA_CONTEXT_CACHE,
        chave_cache,
        AUDITORIA_CONTEXT_CACHE_TTL,
    )
    if contexto_cache is not None:
        return contexto_cache

    def carregar_auditoria_raw(conn):
        c = conn.cursor()
        filtros_sql = []
        params = []

        if usuario:
            filtros_sql.append("usuario = ?")
            params.append(usuario)
        if placa:
            filtros_sql.append("placa LIKE ?")
            params.append(f"%{placa}%")
        if acao:
            filtros_sql.append("acao = ?")
            params.append(acao)
        if inicio_periodo:
            filtros_sql.append("criado_em >= ?")
            params.append(inicio_periodo.isoformat(timespec='seconds'))
        if busca:
            filtros_sql.append("(detalhes_json LIKE ? OR usuario_nome LIKE ? OR entidade LIKE ?)")
            termo = f"%{busca}%"
            params.extend([termo, termo, termo])

        where = f"WHERE {' AND '.join(filtros_sql)}" if filtros_sql else ""
        c.execute(f"""
            SELECT *
            FROM auditoria
            {where}
            ORDER BY criado_em DESC, id DESC
            LIMIT 250
        """, params)
        registros = [dict(item) for item in c.fetchall()]

        c.execute("""
            SELECT DISTINCT usuario
            FROM auditoria
            WHERE COALESCE(NULLIF(usuario, ''), '') <> ''
            ORDER BY usuario
        """)
        usuarios = [row["usuario"] for row in c.fetchall()]

        c.execute("""
            SELECT DISTINCT acao
            FROM auditoria
            WHERE COALESCE(NULLIF(acao, ''), '') <> ''
            ORDER BY acao
        """)
        acoes = [row["acao"] for row in c.fetchall()]
        return {
            "registros": registros,
            "usuarios": usuarios,
            "acoes": acoes,
        }

    contexto_lido = executar_leitura_resiliente(
        carregar_auditoria_raw,
        descricao="AUDITORIA",
        padrao={"registros": [], "usuarios": [], "acoes": []},
    ) or {"registros": [], "usuarios": [], "acoes": []}
    registros = contexto_lido.get("registros", []) or []
    usuarios = contexto_lido.get("usuarios", []) or []
    acoes = contexto_lido.get("acoes", []) or []

    usuarios_unicos = set()
    placas_unicas = set()

    for item in registros:
        item["acao_label"] = formatar_acao_auditoria(item.get("acao"))
        item["usuario_exibicao"] = formatar_usuario_exibicao(
            item.get("usuario_nome"),
            item.get("usuario"),
            fallback="Sistema",
        )
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))
        item["entidade_label"] = normalizar_texto_campo(item.get("entidade")).replace("_", " ").capitalize() or "-"
        detalhe_dict = {}
        try:
            detalhe_dict = json.loads(item.get("detalhes_json") or "{}")
        except Exception:
            detalhe_dict = {}
        item["detalhes"] = detalhe_dict
        item["detalhes_pretty"] = json.dumps(detalhe_dict, ensure_ascii=False, indent=2) if detalhe_dict else ""
        if item.get("usuario"):
            usuarios_unicos.add(item["usuario"])
        if item.get("placa"):
            placas_unicas.add(item["placa"])

    resumo = {
        "total_eventos": len(registros),
        "usuarios_unicos": len(usuarios_unicos),
        "placas_unicas": len(placas_unicas),
        "ultimo_evento_em_fmt": registros[0]["criado_em_fmt"] if registros else "Nenhum evento",
    }

    contexto = {
        "registros": registros,
        "usuarios": usuarios,
        "acoes": [
            {"value": item, "label": formatar_acao_auditoria(item)}
            for item in acoes
        ],
        "filtros": {
            "periodo": periodo,
            "usuario": usuario,
            "placa": placa,
            "acao": acao,
            "busca": busca,
        },
        "periodos": PERIODOS_AUDITORIA,
        "resumo": resumo,
    }
    salvar_cache_consulta(AUDITORIA_CONTEXT_CACHE, chave_cache, contexto)
    return contexto

def loop_worker_backup_banco():
    primeira_execucao = True
    while True:
        if primeira_execucao:
            primeira_execucao = False
            time.sleep(WORKER_BACKUP_DELAY_INICIAL)
        try:
            sucesso, mensagem, caminho = criar_backup_banco(force=False)
            if caminho and "criado com sucesso" in mensagem.lower():
                log_info(f"BACKUP: {mensagem} -> {caminho}")
        except Exception as e:
            log_info("ERRO WORKER BACKUP:", e)

        time.sleep(3600)

def iniciar_worker_backup_banco():
    global backup_worker_iniciado

    if backup_worker_iniciado:
        return

    backup_worker_iniciado = True
    Thread(target=loop_worker_backup_banco, daemon=True).start()

def calcular_prioridade(entrada_iso, valor, tipo_nome):
    prioridade = 0

    try:
        entrada = datetime.fromisoformat(entrada_iso)
        tempo_espera = (agora() - entrada).total_seconds() / 3600  # horas

        # â±ï¸ tempo de espera
        if tempo_espera > 2:
            prioridade += 3
        elif tempo_espera > 1:
            prioridade += 2
        elif tempo_espera > 0.5:
            prioridade += 1

    except:
        pass

    # ðŸ’° valor do serviÃ§o
    try:
        if float(valor) >= 150:
            prioridade += 3
        elif float(valor) >= 80:
            prioridade += 2
        elif float(valor) >= 40:
            prioridade += 1
    except:
        pass

    # ðŸ§½ tipo de serviÃ§o
    if tipo_nome:
        tipo = tipo_nome.lower()

        if "completa" in tipo:
            prioridade += 2
        elif "simples" in tipo:
            prioridade += 1

    return prioridade

def calcular_prioridade_inteligente(servico):
    if not servico:
        return 0

    prioridade = calcular_prioridade(
        servico.get("entrada"),
        servico.get("valor", 0),
        servico.get("tipo_nome", ""),
    )

    try:
        prioridade += int(servico.get("prioridade") or 0)
    except Exception:
        pass

    observacoes = normalizar_texto_comparacao(servico.get("observacoes", ""))

    if "urgente" in observacoes or "prioridade" in observacoes:
        prioridade += 2

    return prioridade

# ðŸ“ CONFIG UPLOAD
UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ðŸ” SEGURANÃ‡A UPLOAD
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "heic", "heif"}
ALLOWED_SPREADSHEET_EXTENSIONS = {"csv", "tsv", "xls", "xlsx"}

def arquivo_permitido(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

def arquivo_planilha_permitido(filename):
    return (
        "." in filename and
        filename.rsplit(".", 1)[1].lower() in ALLOWED_SPREADSHEET_EXTENSIONS
    )

def adicionar_coluna_se_preciso(cursor, tabela, definicao_coluna):
    backend = getattr(cursor, "backend", None)
    nome_coluna = str(definicao_coluna or "").strip().split()[0].strip('"')
    if not nome_coluna:
        return

    try:
        if backend == "postgres":
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name = ?
                LIMIT 1
                """,
                (tabela,),
            )
            if not cursor.fetchone():
                return
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = ?
                  AND column_name = ?
                LIMIT 1
                """,
                (tabela, nome_coluna),
            )
            if cursor.fetchone():
                return
        else:
            cursor.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", (tabela,))
            if not cursor.fetchone():
                return
            cursor.execute(f"PRAGMA table_info({tabela})")
            colunas = {str(row["name"] if hasattr(row, "keys") else row[1]) for row in cursor.fetchall()}
            if nome_coluna in colunas:
                return
    except Exception:
        pass

    sql = f"ALTER TABLE {tabela} ADD COLUMN {definicao_coluna}"
    if backend == "postgres":
        sql = f"ALTER TABLE {tabela} ADD COLUMN IF NOT EXISTS {definicao_coluna}"

    try:
        cursor.execute(sql)
    except Exception:
        if backend != "postgres":
            return
        conn = getattr(cursor, "_cursor", None)
        conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
        if conn and hasattr(conn, "rollback"):
            try:
                conn.rollback()
            except Exception:
                pass
        pass

app = Flask(__name__)
app.config.update(
    build_flask_security_config(
        FLASK_SECRET_KEY_RAW or FLASK_SECRET_KEY_FALLBACK,
        secure_cookie=SESSION_COOKIE_SECURE_RAW in {"1", "true", "yes", "on"},
    )
)
app.config["SESSION_TYPE"] = "filesystem"
app.secret_key = app.config["SECRET_KEY"]

VERSAO_SISTEMA_PADRAO = "1.0.0"
APP_VERSION = f"Versao: {VERSAO_SISTEMA_PADRAO}"
VERSOES_SISTEMA_LEGADAS = {
    "0.7.5-alpha (Em Desenvolvimento)",
    "0.9.5-beta (Em Desenvolvimento)",
    "0.10.0-beta",
    "0.11.0",
    "0.11.1",
    "0.11.2",
    "0.11.3",
    "0.11.4",
    "0.11.5",
    "0.11.6",
}
MESES_CURTOS_PT = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
PERIODOS_FINANCEIRO = [
    {"value": "hoje", "label": "Hoje"},
    {"value": "7dias", "label": "7 dias"},
    {"value": "30dias", "label": "30 dias"},
    {"value": "mes", "label": "Mes atual"},
]
MAPA_PERIODOS_FINANCEIRO = {
    "hoje": "hoje",
    "7dias": "ultimos 7 dias",
    "30dias": "ultimos 30 dias",
    "mes": "mes atual",
}
PERIODOS_AUDITORIA = [
    {"value": "hoje", "label": "Hoje"},
    {"value": "7dias", "label": "7 dias"},
    {"value": "30dias", "label": "30 dias"},
    {"value": "todos", "label": "Tudo"},
]
ACOES_AUDITORIA_LABELS = {
    "abriu_checklist_finalizacao": "Abriu checklist de finalizacao",
    "adicionou_fotos_detalhe": "Adicionou fotos de detalhe",
    "alterou_propria_senha": "Alterou a propria senha",
    "atualizou_foto_perfil": "Atualizou foto de perfil",
    "atualizou_emitente_fiscal": "Atualizou emitente fiscal",
    "atualizou_integracao_fiscal": "Atualizou integracao fiscal",
    "configurou_backup": "Configurou rotina de backup",
    "criou_usuario": "Criou usuario",
    "atualizou_versao_sistema": "Atualizou versao do sistema",
    "finalizou_atendimento": "Finalizou atendimento",
    "gerou_backup_manual": "Gerou backup manual",
    "gerou_nota_fiscal": "Gerou nota fiscal",
    "iniciou_atendimento": "Iniciou atendimento",
    "manutencao_arquivos": "Executou manutencao de arquivos",
    "marcou_retorno_como_contatado": "Marcou retorno como contatado",
    "marcou_retorno_sem_interesse": "Marcou retorno como sem interesse",
    "registrou_emissao_nota_fiscal": "Registrou emissao manual da nota",
    "reativou_retorno": "Reativou retorno",
    "reagendou_retorno": "Reagendou retorno",
    "redefiniu_senha_usuario": "Redefiniu senha de usuario",
    "restaurou_backup": "Restaurou backup",
    "salvou_observacao_retorno": "Salvou observacao de retorno",
    "salvou_operacional": "Salvou dados operacionais",
}
TIPOS_INTEGRACAO_FISCAL = [
    {"value": "manual", "label": "Manual / sem integracao"},
    {"value": "nfse_nacional", "label": "NFS-e Padrao Nacional"},
    {"value": "prefeitura_api", "label": "API da prefeitura / provedor municipal"},
    {"value": "nfe_sefaz", "label": "NF-e / SEFAZ estadual"},
]
AMBIENTES_INTEGRACAO_FISCAL = [
    {"value": "homologacao", "label": "Homologacao"},
    {"value": "producao", "label": "Producao"},
]
AUTENTICACAO_INTEGRACAO_FISCAL = [
    {"value": "nenhuma", "label": "Sem autenticacao"},
    {"value": "token", "label": "Token / Bearer"},
    {"value": "basic", "label": "Usuario e senha"},
    {"value": "oauth2", "label": "OAuth2 / Client Credentials"},
    {"value": "certificado", "label": "Certificado digital"},
]
TIPOS_CERTIFICADO_FISCAL = [
    {"value": "nenhum", "label": "Sem certificado"},
    {"value": "a1", "label": "A1 / arquivo"},
    {"value": "a3", "label": "A3 / dispositivo"},
]
SENHA_PADRAO_ADMIN_LEGADA = "admin123"
SENHA_MINIMO_CARACTERES = 10
MAX_TENTATIVAS_LOGIN = 5
MINUTOS_BLOQUEIO_LOGIN = 15

ITENS_CHECKLIST_PADRAO = [
    "Aspiracao do interior concluida",
    "Painel e console conferidos",
    "Vidros revisados e sem marcas",
    "Rodas verificadas e limpas",
    "Tapetes posicionados corretamente",
]

CAMPOS_SINCRONIZACAO_CLIENTES = [
    {"key": "nome", "label": "Cliente", "required": False},
    {"key": "modelo", "label": "Carro", "required": False},
    {"key": "cor", "label": "Cor", "required": False},
    {"key": "placa", "label": "Placa", "required": True},
    {"key": "servico", "label": "Servico", "required": False},
]

INTERVALOS_SINCRONIZACAO = [
    {"value": 15, "label": "15 minutos"},
    {"value": 30, "label": "30 minutos"},
    {"value": 60, "label": "1 hora"},
    {"value": 180, "label": "3 horas"},
    {"value": 360, "label": "6 horas"},
    {"value": 720, "label": "12 horas"},
    {"value": 1440, "label": "24 horas"},
]

MAPA_INTERVALOS_SINCRONIZACAO = {
    item["value"]: item["label"] for item in INTERVALOS_SINCRONIZACAO
}

MAPA_LABEL_CAMPOS_SYNC = {
    item["key"]: item["label"] for item in CAMPOS_SINCRONIZACAO_CLIENTES
}

ALIASES_CAMPOS_SYNC = {
    "placa": ["placa"],
    "nome": ["nome", "cliente"],
    "modelo": ["modelo", "carro", "veiculo", "veÃ­culo"],
    "cor": ["cor"],
    "servico": ["servico", "serviÃ§o", "servi?o", "servi o", "tipo servico", "tipo", "lavagem"],
    "data": ["data", "data lavagem", "dia", "data servico"],
}


def normalizar_versao_sistema(valor):
    texto = normalizar_texto_campo(valor)
    if not texto:
        return VERSAO_SISTEMA_PADRAO
    texto = re.sub(r"(?i)^\s*vers(?:ao|ão)\s*:\s*", "", texto).strip()
    if texto in VERSOES_SISTEMA_LEGADAS:
        return VERSAO_SISTEMA_PADRAO
    return texto or VERSAO_SISTEMA_PADRAO


def formatar_versao_sistema(valor):
    return f"Versao: {normalizar_versao_sistema(valor)}"


def row_para_dict(row):
    if not row:
        return {}
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "keys"):
        return {chave: row[chave] for chave in row.keys()}
    return {}


def csrf_protection_ativa():
    return bool_config_ativo(CSRF_PROTECTION_RAW)


def telemetria_ativa():
    return not (str(TELEMETRIA_ATIVA_RAW or "").strip().lower() in {"0", "false", "no", "off"})


def empresa_atual_id():
    if has_request_context():
        try:
            return int(session.get("empresa_id") or 1)
        except Exception:
            return 1
    return 1


def selecionar_configuracao_empresa_cursor(cursor, empresa_id=None):
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())

    try:
        cursor.execute(
            "SELECT * FROM configuracao_empresa WHERE empresa_id=? ORDER BY id LIMIT 1",
            (empresa_id,),
        )
        row = cursor.fetchone()
        if row:
            return row_para_dict(row)
    except Exception:
        pass

    if empresa_id == 1:
        try:
            cursor.execute("SELECT * FROM configuracao_empresa WHERE id=1")
            row = cursor.fetchone()
            if row:
                dados = row_para_dict(row)
                empresa_row = normalize_empresa_id(dados.get("empresa_id") or 1)
                if empresa_row == 1:
                    return dados
        except Exception:
            pass

    return None


def salvar_campos_configuracao_empresa(campos, empresa_id=None):
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())
    payload = dict(campos or {})
    payload["empresa_id"] = empresa_id
    payload["atualizado_em"] = payload.get("atualizado_em") or agora_iso()

    conn = conectar()
    c = conn.cursor()
    if any(str(chave).startswith("auto_teste_") for chave in payload):
        garantir_colunas_auto_teste_configuracao_empresa(c)
    atual = selecionar_configuracao_empresa_cursor(c, empresa_id)

    if atual and atual.get("id"):
        colunas = list(payload.keys())
        valores = [payload[coluna] for coluna in colunas]
        sql = (
            "UPDATE configuracao_empresa SET "
            + ", ".join(f"{coluna}=?" for coluna in colunas)
            + " WHERE id=?"
        )
        c.execute(sql, tuple(valores + [atual["id"]]))
    else:
        colunas = list(payload.keys())
        valores = [payload[coluna] for coluna in colunas]
        sql = (
            "INSERT INTO configuracao_empresa ("
            + ", ".join(colunas)
            + ") VALUES ("
            + ", ".join(["?"] * len(colunas))
            + ")"
        )
        c.execute(sql, tuple(valores))

    conn.commit()
    conn.close()
    limpar_cache_configuracao_empresa()
    return payload


def garantir_colunas_auto_teste_configuracao_empresa(cursor):
    colunas = [
        "auto_teste_ativo INTEGER DEFAULT 0",
        "auto_teste_site_url TEXT",
        "auto_teste_intervalo_horas INTEGER DEFAULT 2",
        "auto_teste_telegram_bot_token TEXT",
        "auto_teste_telegram_bot_nick TEXT",
        "auto_teste_telegram_chat_id TEXT",
        "auto_teste_ultimo_status TEXT",
        "auto_teste_ultimo_relatorio TEXT",
        "auto_teste_ultimo_teste_em TEXT",
    ]
    for coluna in colunas:
        adicionar_coluna_se_preciso(cursor, "configuracao_empresa", coluna)


def selecionar_registro_administrativo_empresa_cursor(cursor, tabela, empresa_id=None):
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())

    try:
        cursor.execute(
            f"SELECT * FROM {tabela} WHERE empresa_id=? ORDER BY id LIMIT 1",
            (empresa_id,),
        )
        row = cursor.fetchone()
        if row:
            return row_para_dict(row)
    except Exception:
        pass

    if empresa_id == 1:
        try:
            cursor.execute(f"SELECT * FROM {tabela} WHERE id=1")
            row = cursor.fetchone()
            if row:
                dados = row_para_dict(row)
                empresa_row = normalize_empresa_id(dados.get("empresa_id") or 1)
                if empresa_row == 1:
                    return dados
        except Exception:
            pass

    return None


def salvar_campos_registro_administrativo_empresa(tabela, campos, empresa_id=None):
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())
    payload = dict(campos or {})
    payload["empresa_id"] = empresa_id

    conn = conectar()
    c = conn.cursor()
    atual = selecionar_registro_administrativo_empresa_cursor(c, tabela, empresa_id)

    if atual and atual.get("id"):
        colunas = list(payload.keys())
        valores = [payload[coluna] for coluna in colunas]
        sql = (
            f"UPDATE {tabela} SET "
            + ", ".join(f"{coluna}=?" for coluna in colunas)
            + " WHERE id=?"
        )
        c.execute(sql, tuple(valores + [atual["id"]]))
    else:
        colunas = list(payload.keys())
        valores = [payload[coluna] for coluna in colunas]
        sql = (
            f"INSERT INTO {tabela} ("
            + ", ".join(colunas)
            + ") VALUES ("
            + ", ".join(["?"] * len(colunas))
            + ")"
        )
        c.execute(sql, tuple(valores))

    conn.commit()
    conn.close()
    return payload


def contexto_produto_padrao():
    return build_brand_context(
        {
            "marca_nome": "Wagen Estetica Automotiva",
            "marca_subtitulo": "Gestao Estetica",
            "marca_logo_url": "",
            "marca_favicon_url": "",
            "marca_cor_primaria": "#facc15",
            "marca_cor_secundaria": "#111827",
            "marca_cor_fundo": "#0b0b0b",
            "marca_cor_superficie": "#111827",
            "marca_cor_texto": "#f9fafb",
            "site_titulo": "Gestao Estetica",
            "site_rodape_texto": "Desenvolvido por Luiz Henrique | Qualquer Erro Contate o Desenvolvedor | Wagen Estetica Automotiva | Direitos Reservados.",
            "login_titulo_publico": "Acesso ao sistema",
            "login_subtitulo_publico": "Entre no sistema",
            "login_botao_texto": "Entrar",
            "home_busca_placeholder": "Digite a placa",
            "home_busca_botao_texto": "Buscar",
            "home_estado_inicial_titulo": "Digite uma placa para comecar",
            "storage_provider": "database",
            "licenca_plano": "starter",
            "licenca_status": "trial",
            "onboarding_concluido": 0,
            "whitelabel_ativo": 0,
        },
        {},
    )


def campos_contexto_produto_sql(incluir_blobs=False):
    campos = [
        "id",
        "empresa_id",
        "marca_nome",
        "marca_subtitulo",
        "marca_logo_url",
        "CASE WHEN marca_logo_blob IS NOT NULL THEN 1 ELSE 0 END AS marca_logo_tem_blob",
        "marca_favicon_url",
        "CASE WHEN marca_favicon_blob IS NOT NULL THEN 1 ELSE 0 END AS marca_favicon_tem_blob",
        "marca_cor_primaria",
        "marca_cor_secundaria",
        "marca_cor_fundo",
        "marca_cor_superficie",
        "marca_cor_texto",
        "site_titulo",
        "site_rodape_texto",
        "login_titulo_publico",
        "login_subtitulo_publico",
        "login_botao_texto",
        "home_busca_placeholder",
        "home_busca_botao_texto",
        "home_estado_inicial_titulo",
        "whitelabel_ativo",
        "storage_provider",
        "licenca_plano",
        "licenca_status",
        "onboarding_concluido",
    ]
    if incluir_blobs:
        campos.extend([
            "marca_logo_blob",
            "marca_logo_mime_type",
            "marca_logo_arquivo_nome",
            "marca_favicon_blob",
            "marca_favicon_mime_type",
            "marca_favicon_arquivo_nome",
        ])
    return ",\n                ".join(campos)


def carregar_dados_contexto_produto(empresa_id=None, incluir_blobs=False):
    padrao = {"config": {}, "empresa": {}}
    if not INIT_DB_EXECUTADO:
        return padrao

    empresa_id_atual = normalize_empresa_id(empresa_id or empresa_atual_id())
    campos_sql = campos_contexto_produto_sql(incluir_blobs=incluir_blobs)

    def carregar(conn):
        c = conn.cursor()
        def carregar_configuracao(esquema_estrito=True):
            if esquema_estrito:
                c.execute(
                    """
                    SELECT
                        """
                    + campos_sql
                    + """
                    FROM configuracao_empresa
                    WHERE empresa_id=?
                    ORDER BY id
                    LIMIT 1
                    """,
                    (empresa_id_atual,),
                )
            else:
                c.execute(
                    """
                    SELECT *
                    FROM configuracao_empresa
                    WHERE empresa_id=?
                    ORDER BY id
                    LIMIT 1
                    """,
                    (empresa_id_atual,),
                )
            return row_para_dict(c.fetchone())

        try:
            config = carregar_configuracao(esquema_estrito=True)
        except Exception:
            config = carregar_configuracao(esquema_estrito=False)

        if not config and empresa_id_atual == 1:
            try:
                c.execute(
                    """
                    SELECT
                        """
                    + campos_sql
                    + """
                    FROM configuracao_empresa
                    WHERE id=1
                    """
                )
                config = row_para_dict(c.fetchone())
            except Exception:
                c.execute(
                    """
                    SELECT *
                    FROM configuracao_empresa
                    WHERE id=1
                    """
                )
                config = row_para_dict(c.fetchone())
        if config and "marca_logo_tem_blob" not in config:
            config["marca_logo_tem_blob"] = 1 if config.get("marca_logo_blob") else 0
        if config and "marca_favicon_tem_blob" not in config:
            config["marca_favicon_tem_blob"] = 1 if config.get("marca_favicon_blob") else 0

        empresa_id = int(config.get("empresa_id") or 1)
        empresa = {}
        try:
            c.execute("SELECT * FROM empresas WHERE id=?", (empresa_id,))
            empresa = row_para_dict(c.fetchone())
        except Exception:
            empresa = {}

        return {"config": config, "empresa": empresa}

    return executar_leitura_resiliente(
        carregar,
        descricao="CONTEXTO PRODUTO",
        padrao=padrao,
    )


def carregar_contexto_produto():
    dados = carregar_dados_contexto_produto()
    if not dados.get("config") and not dados.get("empresa"):
        return contexto_produto_padrao()
    return build_brand_context(dados.get("config"), dados.get("empresa"))


def registrar_evento_telemetria_app(evento, categoria="sistema", payload=None, severidade="info", usuario_row=None):
    if not telemetria_ativa():
        return False

    usuario_id = None
    usuario = None
    empresa_id = empresa_atual_id()

    if usuario_row:
        try:
            usuario_id = usuario_row["id"]
        except Exception:
            usuario_id = None
        try:
            usuario = usuario_row["usuario"]
        except Exception:
            usuario = None
        try:
            empresa_id = int(usuario_row["empresa_id"] or empresa_id)
        except Exception:
            pass
    elif has_request_context():
        usuario_id = session.get("usuario_id")
        usuario = session.get("usuario")

    ip = request.headers.get("X-Forwarded-For", request.remote_addr) if has_request_context() else None
    user_agent = request.headers.get("User-Agent", "")[:255] if has_request_context() else None

    return registrar_evento_telemetria_core(
        conectar,
        empresa_id=empresa_id,
        usuario_id=usuario_id,
        usuario=usuario,
        categoria=categoria,
        evento=evento,
        severidade=severidade,
        payload=payload,
        ip=ip,
        user_agent=user_agent,
    )


def obter_versao_sistema(permitir_sem_sessao=False):
    if not INIT_DB_EXECUTADO:
        return APP_VERSION

    if has_request_context() and not session.get("usuario") and not permitir_sem_sessao:
        return APP_VERSION

    empresa_id_atual = normalize_empresa_id(empresa_atual_id())
    agora_cache_ts = time.time()
    if (
        VERSAO_SISTEMA_CACHE.get("resultado")
        and VERSAO_SISTEMA_CACHE.get("empresa_id") == empresa_id_atual
        and agora_cache_ts - float(VERSAO_SISTEMA_CACHE.get("testado_em") or 0.0) < VERSAO_SISTEMA_CACHE_TTL
    ):
        return VERSAO_SISTEMA_CACHE["resultado"]

    try:
        conn = conectar()
        c = conn.cursor()
        c.execute(
            "SELECT versao_sistema FROM configuracao_empresa WHERE empresa_id=? ORDER BY id LIMIT 1",
            (empresa_id_atual,),
        )
        item = c.fetchone()
        if not item and empresa_id_atual == 1:
            c.execute("SELECT versao_sistema FROM configuracao_empresa WHERE id=1")
            item = c.fetchone()
        conn.close()

        if item:
            try:
                valor = item["versao_sistema"]
            except Exception:
                valor = item[0] if item else ""
            resultado = formatar_versao_sistema(valor)
            VERSAO_SISTEMA_CACHE["testado_em"] = agora_cache_ts
            VERSAO_SISTEMA_CACHE["empresa_id"] = empresa_id_atual
            VERSAO_SISTEMA_CACHE["resultado"] = resultado
            return resultado
    except Exception:
        pass

    return APP_VERSION


def obter_contexto_licenca_empresa_cached(empresa_id=None, force=False):
    empresa_id_cache = normalize_empresa_id(empresa_id or empresa_atual_id())
    agora_cache_ts = time.time()
    if (
        not force
        and TEMPLATE_LICENCA_CACHE.get("resultado") is not None
        and TEMPLATE_LICENCA_CACHE.get("empresa_id") == empresa_id_cache
        and agora_cache_ts - float(TEMPLATE_LICENCA_CACHE.get("testado_em") or 0.0) < TEMPLATE_LICENCA_CACHE_TTL
    ):
        return deepcopy(TEMPLATE_LICENCA_CACHE["resultado"])

    licenca = carregar_contexto_licenca_empresa_seguro(empresa_id_cache)
    TEMPLATE_LICENCA_CACHE["testado_em"] = agora_cache_ts
    TEMPLATE_LICENCA_CACHE["empresa_id"] = empresa_id_cache
    TEMPLATE_LICENCA_CACHE["resultado"] = deepcopy(licenca)
    return licenca


@app.context_processor
def inject_global_template_context():
    empresa_id_cache = empresa_atual_id() if session.get("usuario") else 0
    agora_cache_ts = time.time()
    produto = None
    if (
        TEMPLATE_PRODUTO_CACHE.get("resultado") is not None
        and TEMPLATE_PRODUTO_CACHE.get("empresa_id") == empresa_id_cache
        and agora_cache_ts - float(TEMPLATE_PRODUTO_CACHE.get("testado_em") or 0.0) < TEMPLATE_PRODUTO_CACHE_TTL
    ):
        produto = deepcopy(TEMPLATE_PRODUTO_CACHE["resultado"])
    if produto is None:
        produto = carregar_contexto_produto()
        TEMPLATE_PRODUTO_CACHE["testado_em"] = agora_cache_ts
        TEMPLATE_PRODUTO_CACHE["empresa_id"] = empresa_id_cache
        TEMPLATE_PRODUTO_CACHE["resultado"] = deepcopy(produto)
    produto["brand_primary_rgb"] = cor_hex_para_rgb_css(produto.get("brand_primary_color"), "#facc15")
    produto["brand_secondary_rgb"] = cor_hex_para_rgb_css(produto.get("brand_secondary_color"), "#111827")
    produto["brand_background_rgb"] = cor_hex_para_rgb_css(produto.get("brand_background_color"), "#0b0b0b")
    produto["brand_surface_rgb"] = cor_hex_para_rgb_css(produto.get("brand_surface_color"), "#111827")
    produto["brand_text_rgb"] = cor_hex_para_rgb_css(produto.get("brand_text_color"), "#f9fafb")
    licenca_atual = {}
    if session.get("usuario"):
        licenca_atual = obter_contexto_licenca_empresa_cached(empresa_id_cache)
    auto_suporte_perfil = perfil_auto_suporte() if session.get("usuario") else ""
    return {
        "app_version": obter_versao_sistema(),
        "csrf_token": lambda: issue_csrf_token(session),
        "licenca_atual": licenca_atual,
        "pode_gerenciar_empresas": usuario_gerencia_empresas() if session.get("usuario") else False,
        "auto_suporte_disponivel": bool(auto_suporte_perfil),
        "auto_suporte_perfil": auto_suporte_perfil,
        "pagina_menu_habilitada": pagina_menu_habilitada,
        "hud_usuario_config": obter_configuracao_hud_usuario() if session.get("usuario") else configuracao_hud_usuario_padrao(),
        **produto,
    }


sync_lock = Lock()
sync_worker_iniciado = False
sync_bancos_lock = Lock()
sync_bancos_worker_iniciado = False
backup_lock = Lock()
backup_worker_iniciado = False
maintenance_lock = Lock()
maintenance_worker_iniciado = False
auto_teste_lock = Lock()
auto_teste_worker_iniciado = False
ULTIMO_ERRO_PRODUCAO = {
    "quando": "",
    "endpoint": "",
    "path": "",
    "tipo": "",
    "mensagem": "",
}
ERROS_PRODUCAO_ARQUIVO = os.path.join("logs", "erros_producao.json")
ERROS_PRODUCAO_LIMITE = 80
ERROS_PRODUCAO_LOCK = Lock()
AUTO_SUPORTE_HISTORICO_ARQUIVO = os.path.join("logs", "auto_suporte_historico.json")
AUTO_SUPORTE_ESTADO_ARQUIVO = os.path.join("logs", "auto_suporte_estado.json")
AUTO_SUPORTE_HISTORICO_LIMITE = 80
AUTO_SUPORTE_LOCK = Lock()
POSTGRES_CONNECT_TIMEOUT = 3
POSTGRES_RETRY_TENTATIVAS = 2
POSTGRES_RETRY_DELAY = 0.25
BANCO_ONLINE_FALHAS_ROTAS = {}
ESTABILIDADE_ALERTAS_CACHE = {}
ESTABILIDADE_ALERTA_INTERVALO = 600
ROTAS_MONITORADAS_RESPOSTA = {"/", "/clientes", "/painel", "/configuracoes", "/financeiro", "/status-sistema"}
METRICAS_TEMPO_RESPOSTA = {
    rota: {
        "rota": rota,
        "ultimo_ms": 0,
        "anterior_ms": 0,
        "media_ms": 0,
        "max_ms": 0,
        "amostras": 0,
        "status": "",
        "ultima_medicao": "",
        "classe": "rapido",
        "tendencia": "estavel",
        "tendencia_label": "Estavel",
        "alerta_2s": False,
        "pioras_consecutivas": 0,
    }
    for rota in ROTAS_MONITORADAS_RESPOSTA
}
SQL_METRICAS_CONSULTAS = []
SQL_METRICAS_LIMITE = 120
SQL_METRICAS_LOCK = Lock()
init_db_lock = Lock()
INIT_DB_EXECUTADO = False
bootstrap_init_thread_started = False
schema_bootstrap_thread_started = False
WORKER_SYNC_DELAY_INICIAL = 90
WORKER_SYNC_BANCOS_DELAY_INICIAL = 360
WORKER_MANUTENCAO_DELAY_INICIAL = 600
WORKER_BACKUP_DELAY_INICIAL = 900
WORKER_AUTO_TESTE_DELAY_INICIAL = 180
HUD_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
HUD_CACHE_TTL = 45
HOME_SNAPSHOT_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
HOME_SNAPSHOT_CACHE_TTL = 45
NOTIFICACOES_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
NOTIFICACOES_CACHE_TTL = 60
ANIVERSARIO_NOTIFICACOES_CACHE = {
    "testado_em": 0.0,
    "chave": "",
}
ANIVERSARIO_NOTIFICACOES_CACHE_TTL = 900
STATUS_SYNC_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
STATUS_SYNC_CACHE_TTL = 60
SYNC_TOKEN_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
SYNC_TOKEN_CACHE_TTL = 60
RETORNOS_HUD_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
RETORNOS_HUD_CACHE_TTL = 60
AGENDA_RETORNO_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
AGENDA_RETORNO_CACHE_TTL = 90
VOZ_CACHE = {
    "testado_em": 0.0,
    "usuario": "",
    "resultado": None,
}
VOZ_CACHE_TTL = 120
CLIENTES_CONTEXT_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
CLIENTES_CONTEXT_CACHE_TTL = 45
BASE_DADOS_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
BASE_DADOS_CACHE_TTL = 45
HISTORICO_CONTEXT_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
HISTORICO_CONTEXT_CACHE_TTL = 45
PAINEL_CONTEXT_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
PAINEL_CONTEXT_CACHE_TTL = 20
RELATORIOS_CONTEXT_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
    "entradas": {},
}
RELATORIOS_CONTEXT_CACHE_TTL = 45
CLIENTES_LISTA_INICIAL_LIMITE = 80
AUDITORIA_CONTEXT_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
AUDITORIA_CONTEXT_CACHE_TTL = 45
PRODUTOS_PNEU_CACHE = {
    "testado_em": 0.0,
    "resultado": None,
}
PRODUTOS_PNEU_CACHE_TTL = 300
CLIMA_CACHE = {
    "testado_em": 0.0,
    "resultado": None,
}
CLIMA_CACHE_TTL = 600
ULTIMO_SYNC_FONTES_SOB_DEMANDA_TS = time.time()
SYNC_FONTES_SOB_DEMANDA_INTERVALO = 600
ULTIMA_PREPARACAO_INTERFACE_TS = 0.0
PREPARACAO_INTERFACE_INTERVALO = 45
USUARIO_SESSAO_SYNC_TTL = 60
BANCO_ONLINE_STATUS_CACHE = {
    "testado_em": 0.0,
    "resultado": {},
}
BANCO_ONLINE_STATUS_CACHE_TTL = 90
BANCO_ONLINE_TABELAS_CACHE = {
    "testado_em": 0.0,
    "dsn": "",
    "resultado": None,
}
BANCO_ONLINE_TABELAS_CACHE_TTL = 180
TEMPLATE_PRODUTO_CACHE = {
    "testado_em": 0.0,
    "empresa_id": 0,
    "resultado": None,
}
TEMPLATE_PRODUTO_CACHE_TTL = 60
TEMPLATE_LICENCA_CACHE = {
    "testado_em": 0.0,
    "empresa_id": 0,
    "resultado": None,
}
TEMPLATE_LICENCA_CACHE_TTL = 45
CONFIG_EMPRESA_CACHE = {
    "testado_em": 0.0,
    "empresa_id": 0,
    "resultado": None,
}
CONFIG_EMPRESA_CACHE_TTL = 60
VERSAO_SISTEMA_CACHE = {
    "testado_em": 0.0,
    "empresa_id": 0,
    "resultado": "",
}
VERSAO_SISTEMA_CACHE_TTL = 60
CONFIG_HUD_USUARIO_CACHE = {
    "testado_em": 0.0,
    "usuario_id": 0,
    "resultado": None,
}
CONFIG_HUD_USUARIO_CACHE_TTL = 60
PAGINAS_MENU_CACHE = {
    "testado_em": 0.0,
    "empresa_id": 0,
    "resultado": set(),
}
PAGINAS_MENU_CACHE_TTL = 60
BACKUP_VALIDACAO_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
AUTO_SUPORTE_STATUS_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
AUTO_SUPORTE_STATUS_CACHE_TTL = 45
AUTO_SUPORTE_PACOTE_CACHE = {
    "testado_em": 0.0,
    "chave": "",
    "resultado": None,
}
AUTO_SUPORTE_PACOTE_CACHE_TTL = 60
SCHEMA_BANCO_ONLINE_GARANTIDO = False
SCHEMA_SQLITE_LOCAL_GARANTIDO = False
BANCO_ONLINE_BLOQUEADO_ATE_TS = 0.0


class BancoOnlineObrigatorioErro(RuntimeError):
    pass


def limpar_caches_interface():
    HUD_CACHE["testado_em"] = 0.0
    HUD_CACHE["usuario"] = ""
    HUD_CACHE["resultado"] = None
    HOME_SNAPSHOT_CACHE["testado_em"] = 0.0
    HOME_SNAPSHOT_CACHE["usuario"] = ""
    HOME_SNAPSHOT_CACHE["resultado"] = None
    NOTIFICACOES_CACHE["testado_em"] = 0.0
    NOTIFICACOES_CACHE["usuario"] = ""
    NOTIFICACOES_CACHE["resultado"] = None
    STATUS_SYNC_CACHE["testado_em"] = 0.0
    STATUS_SYNC_CACHE["usuario"] = ""
    STATUS_SYNC_CACHE["resultado"] = None
    SYNC_TOKEN_CACHE["testado_em"] = 0.0
    SYNC_TOKEN_CACHE["usuario"] = ""
    SYNC_TOKEN_CACHE["resultado"] = None
    RETORNOS_HUD_CACHE["testado_em"] = 0.0
    RETORNOS_HUD_CACHE["usuario"] = ""
    RETORNOS_HUD_CACHE["resultado"] = None
    AGENDA_RETORNO_CACHE["testado_em"] = 0.0
    AGENDA_RETORNO_CACHE["usuario"] = ""
    AGENDA_RETORNO_CACHE["resultado"] = None
    VOZ_CACHE["testado_em"] = 0.0
    VOZ_CACHE["usuario"] = ""
    VOZ_CACHE["resultado"] = None
    CLIENTES_CONTEXT_CACHE["testado_em"] = 0.0
    CLIENTES_CONTEXT_CACHE["chave"] = ""
    CLIENTES_CONTEXT_CACHE["resultado"] = None
    BASE_DADOS_CACHE["testado_em"] = 0.0
    BASE_DADOS_CACHE["chave"] = ""
    BASE_DADOS_CACHE["resultado"] = None
    HISTORICO_CONTEXT_CACHE["testado_em"] = 0.0
    HISTORICO_CONTEXT_CACHE["chave"] = ""
    HISTORICO_CONTEXT_CACHE["resultado"] = None
    PAINEL_CONTEXT_CACHE["testado_em"] = 0.0
    PAINEL_CONTEXT_CACHE["chave"] = ""
    PAINEL_CONTEXT_CACHE["resultado"] = None
    RELATORIOS_CONTEXT_CACHE["testado_em"] = 0.0
    RELATORIOS_CONTEXT_CACHE["chave"] = ""
    RELATORIOS_CONTEXT_CACHE["resultado"] = None
    RELATORIOS_CONTEXT_CACHE["entradas"] = {}
    AUDITORIA_CONTEXT_CACHE["testado_em"] = 0.0
    AUDITORIA_CONTEXT_CACHE["chave"] = ""
    AUDITORIA_CONTEXT_CACHE["resultado"] = None
    PRODUTOS_PNEU_CACHE["testado_em"] = 0.0
    PRODUTOS_PNEU_CACHE["resultado"] = None
    BACKUP_VALIDACAO_CACHE["testado_em"] = 0.0
    BACKUP_VALIDACAO_CACHE["chave"] = ""
    BACKUP_VALIDACAO_CACHE["resultado"] = None
    limpar_cache_auto_suporte()
    TEMPLATE_PRODUTO_CACHE["testado_em"] = 0.0
    TEMPLATE_PRODUTO_CACHE["empresa_id"] = 0
    TEMPLATE_PRODUTO_CACHE["resultado"] = None
    TEMPLATE_LICENCA_CACHE["testado_em"] = 0.0
    TEMPLATE_LICENCA_CACHE["empresa_id"] = 0
    TEMPLATE_LICENCA_CACHE["resultado"] = None
    CONFIG_EMPRESA_CACHE["testado_em"] = 0.0
    CONFIG_EMPRESA_CACHE["empresa_id"] = 0
    CONFIG_EMPRESA_CACHE["resultado"] = None
    VERSAO_SISTEMA_CACHE["testado_em"] = 0.0
    VERSAO_SISTEMA_CACHE["empresa_id"] = 0
    VERSAO_SISTEMA_CACHE["resultado"] = ""
    CONFIG_HUD_USUARIO_CACHE["testado_em"] = 0.0
    CONFIG_HUD_USUARIO_CACHE["usuario_id"] = 0
    CONFIG_HUD_USUARIO_CACHE["resultado"] = None
    PAGINAS_MENU_CACHE["testado_em"] = 0.0
    PAGINAS_MENU_CACHE["empresa_id"] = 0
    PAGINAS_MENU_CACHE["resultado"] = set()


def limpar_cache_configuracao_empresa():
    CONFIG_EMPRESA_CACHE["testado_em"] = 0.0
    CONFIG_EMPRESA_CACHE["empresa_id"] = 0
    CONFIG_EMPRESA_CACHE["resultado"] = None
    TEMPLATE_LICENCA_CACHE["testado_em"] = 0.0
    TEMPLATE_LICENCA_CACHE["empresa_id"] = 0
    TEMPLATE_LICENCA_CACHE["resultado"] = None
    VERSAO_SISTEMA_CACHE["testado_em"] = 0.0
    VERSAO_SISTEMA_CACHE["empresa_id"] = 0
    VERSAO_SISTEMA_CACHE["resultado"] = ""
    PAGINAS_MENU_CACHE["testado_em"] = 0.0
    PAGINAS_MENU_CACHE["empresa_id"] = 0
    PAGINAS_MENU_CACHE["resultado"] = set()


def limpar_cache_clientes():
    CLIENTES_CONTEXT_CACHE["testado_em"] = 0.0
    CLIENTES_CONTEXT_CACHE["chave"] = ""
    CLIENTES_CONTEXT_CACHE["resultado"] = None
    BASE_DADOS_CACHE["testado_em"] = 0.0
    BASE_DADOS_CACHE["chave"] = ""
    BASE_DADOS_CACHE["resultado"] = None


def limpar_caches_operacionais_leves():
    HUD_CACHE["testado_em"] = 0.0
    HUD_CACHE["usuario"] = ""
    HUD_CACHE["resultado"] = None
    HOME_SNAPSHOT_CACHE["testado_em"] = 0.0
    HOME_SNAPSHOT_CACHE["usuario"] = ""
    HOME_SNAPSHOT_CACHE["resultado"] = None
    STATUS_SYNC_CACHE["testado_em"] = 0.0
    STATUS_SYNC_CACHE["usuario"] = ""
    STATUS_SYNC_CACHE["resultado"] = None
    SYNC_TOKEN_CACHE["testado_em"] = 0.0
    SYNC_TOKEN_CACHE["usuario"] = ""
    SYNC_TOKEN_CACHE["resultado"] = None


def limpar_cache_painel():
    PAINEL_CONTEXT_CACHE["testado_em"] = 0.0
    PAINEL_CONTEXT_CACHE["chave"] = ""
    PAINEL_CONTEXT_CACHE["resultado"] = None


def chave_cache_auto_suporte():
    if not has_request_context():
        return "sistema|desenvolvedor|1"
    return "|".join([
        str(session.get("usuario") or ""),
        str(session.get("usuario_perfil") or ""),
        str(session.get("empresa_id") or empresa_atual_id() or ""),
        str(AUTO_SUPORTE_HISTORICO_ARQUIVO),
        str(AUTO_SUPORTE_ESTADO_ARQUIVO),
        str(ERROS_PRODUCAO_ARQUIVO),
    ])


def limpar_cache_auto_suporte():
    AUTO_SUPORTE_STATUS_CACHE["testado_em"] = 0.0
    AUTO_SUPORTE_STATUS_CACHE["chave"] = ""
    AUTO_SUPORTE_STATUS_CACHE["resultado"] = None
    AUTO_SUPORTE_PACOTE_CACHE["testado_em"] = 0.0
    AUTO_SUPORTE_PACOTE_CACHE["chave"] = ""
    AUTO_SUPORTE_PACOTE_CACHE["resultado"] = None


def limpar_cache_banco_online():
    BANCO_ONLINE_STATUS_CACHE["testado_em"] = 0.0
    BANCO_ONLINE_STATUS_CACHE["resultado"] = {}
    BANCO_ONLINE_TABELAS_CACHE["testado_em"] = 0.0
    BANCO_ONLINE_TABELAS_CACHE["dsn"] = ""
    BANCO_ONLINE_TABELAS_CACHE["resultado"] = None
BANCO_ONLINE_ULTIMO_LOG = {"mensagem": "", "testado_em": 0.0}
schema_sqlite_local_lock = Lock()


def atualizar_configuracao_banco_runtime(database_url=None, senha=None, backend=None, auto_migrar=None, migrado=None):
    global DATABASE_URL_RAW, SUPABASE_DB_PASSWORD, DATABASE_BACKEND_RAW
    global AUTO_MIGRAR_BANCO_RAW, DATABASE_ONLINE_MIGRADO_RAW
    global SCHEMA_BANCO_ONLINE_GARANTIDO

    if database_url is not None:
        DATABASE_URL_RAW = str(database_url).strip()
        os.environ["DATABASE_URL"] = DATABASE_URL_RAW
        os.environ["SUPABASE_DATABASE_URL"] = DATABASE_URL_RAW

    if senha is not None:
        SUPABASE_DB_PASSWORD = str(senha).strip()
        os.environ["SUPABASE_DB_PASSWORD"] = SUPABASE_DB_PASSWORD

    if backend is not None:
        DATABASE_BACKEND_RAW = str(backend).strip().lower()
        os.environ["DATABASE_BACKEND"] = DATABASE_BACKEND_RAW

    if auto_migrar is not None:
        AUTO_MIGRAR_BANCO_RAW = "1" if bool_config_ativo(auto_migrar) else "0"
        os.environ["AUTO_MIGRAR_BANCO"] = AUTO_MIGRAR_BANCO_RAW

    if migrado is not None:
        DATABASE_ONLINE_MIGRADO_RAW = "1" if bool_config_ativo(migrado) else "0"
        os.environ["DATABASE_ONLINE_MIGRADO"] = DATABASE_ONLINE_MIGRADO_RAW

    BANCO_ONLINE_STATUS_CACHE["testado_em"] = 0.0
    BANCO_ONLINE_STATUS_CACHE["resultado"] = {}
    SCHEMA_BANCO_ONLINE_GARANTIDO = False


def modo_banco_preferido():
    modo = str(DATABASE_BACKEND_RAW or "").strip().lower()
    if modo in {"sqlite", "local"}:
        return "sqlite"
    if modo in {"postgres", "supabase", "online"}:
        return "postgres"
    if banco_online_travado_localmente() or migracao_online_ja_realizada():
        return "postgres"
    return "postgres"


def ambiente_render():
    return any(
        str(os.environ.get(chave) or "").strip()
        for chave in (
            "RENDER",
            "RENDER_SERVICE_ID",
            "RENDER_EXTERNAL_URL",
            "IS_RENDER",
        )
    )


def ambiente_fly():
    return any(
        str(os.environ.get(chave) or "").strip()
        for chave in (
            "FLY_APP_NAME",
            "FLY_ALLOC_ID",
            "FLY_REGION",
            "IS_FLY",
        )
    )


def ambiente_hospedado_gerenciado():
    return ambiente_render() or ambiente_fly()


def banco_online_estritamente_obrigatorio():
    if STRICT_ONLINE_DATABASE_RAW in {"1", "true", "yes", "on"}:
        return True
    return ambiente_render() and modo_banco_preferido() == "postgres"


def migracao_online_automatica_ativa():
    return bool_config_ativo(AUTO_MIGRAR_BANCO_RAW)


def migracao_online_ja_realizada():
    return bool_config_ativo(DATABASE_ONLINE_MIGRADO_RAW)


def caminho_trava_banco_online():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), BANCO_ONLINE_LOCK_FILE)


def banco_online_travado_localmente():
    return os.path.exists(caminho_trava_banco_online())


def travar_banco_online_localmente(ativar=True, motivo=""):
    caminho = caminho_trava_banco_online()

    if ativar:
        conteudo = [
            f"ativado_em={datetime.now().isoformat(timespec='seconds')}",
            f"motivo={str(motivo or '').strip() or 'banco online em uso'}",
            f"backend={DATABASE_BACKEND_RAW or 'postgres'}",
        ]
        try:
            with open(caminho, "w", encoding="utf-8") as arquivo:
                arquivo.write("\n".join(conteudo) + "\n")
        except Exception:
            pass
        return True

    try:
        if os.path.exists(caminho):
            os.remove(caminho)
    except Exception:
        pass
    return False


def url_postgres_ajustada():
    url = DATABASE_URL_RAW
    if not url:
        return ""

    partes = desmontar_url_postgres(url)
    if not partes.get("host"):
        return ""
    senha = SUPABASE_DB_PASSWORD or partes.get("senha") or ""
    if not senha and "[YOUR-PASSWORD]" in url:
        return ""

    usuario = quote(partes.get("usuario") or "postgres")
    host = partes.get("host") or ""
    porta = str(
        int(str(partes.get("porta") or "5432").strip())
        if str(partes.get("porta") or "5432").strip().isdigit()
        else 5432
    )
    banco = partes.get("database") or "postgres"
    url = f"postgresql://{usuario}:{quote(senha)}@{host}:{porta}/{banco}"

    if "sslmode=" not in url:
        separador = "&" if "?" in url else "?"
        url = f"{url}{separador}sslmode=require"

    if "connect_timeout=" not in url:
        separador = "&" if "?" in url else "?"
        url = f"{url}{separador}connect_timeout={POSTGRES_CONNECT_TIMEOUT}"

    return url


def banco_online_configurado():
    return modo_banco_preferido() == "postgres" and bool(DATABASE_URL_RAW)


def mascarar_url_postgres(url):
    try:
        partes = desmontar_url_postgres(url)
    except Exception:
        return ""

    if not partes.get("scheme") or not partes.get("host"):
        return ""

    usuario = partes.get("usuario") or "postgres"
    host = partes.get("host") or ""
    porta = f":{partes.get('porta')}" if partes.get("porta") else ""
    banco = partes.get("database") or "postgres"
    return f"{partes.get('scheme') or 'postgresql'}://{usuario}:***@{host}{porta}/{banco}"


def diagnosticar_banco_online(force=False):
    global BANCO_ONLINE_BLOQUEADO_ATE_TS
    agora_ts = time.time()
    cache = BANCO_ONLINE_STATUS_CACHE.get("resultado") or {}
    testado_em = float(BANCO_ONLINE_STATUS_CACHE.get("testado_em") or 0.0)
    if (
        not force
        and BANCO_ONLINE_BLOQUEADO_ATE_TS > agora_ts
        and cache
    ):
        return dict(cache)
    if (
        not force
        and cache
        and agora_ts - testado_em < BANCO_ONLINE_STATUS_CACHE_TTL
    ):
        return dict(cache)

    backend_preferido = modo_banco_preferido()
    resultado = {
        "configurado": banco_online_configurado(),
        "ativo": False,
        "conectado": False,
        "backend": backend_preferido,
        "backend_label": "Supabase / PostgreSQL" if backend_preferido == "postgres" else "SQLite local",
        "mensagem": (
            "Banco online obrigatorio, mas a connection string ainda nao foi configurada."
            if backend_preferido == "postgres"
            else "Modo local selecionado."
        ),
        "url_masked": mascarar_url_postgres(url_postgres_ajustada()) if banco_online_configurado() else "",
        "host": "",
        "porta": "",
        "database": "",
        "usuario": "",
    }

    if (
        not force
        and BANCO_ONLINE_BLOQUEADO_ATE_TS > agora_ts
        and banco_online_configurado()
        and not cache
    ):
        restante = max(1, int(BANCO_ONLINE_BLOQUEADO_ATE_TS - agora_ts))
        resultado["mensagem"] = (
            "Banco online em respiro temporario por excesso de conexoes. "
            f"Nova tentativa automatica em cerca de {restante}s."
        )
        BANCO_ONLINE_STATUS_CACHE["testado_em"] = agora_ts
        BANCO_ONLINE_STATUS_CACHE["resultado"] = dict(resultado)
        return resultado

    if not banco_online_configurado():
        if banco_online_travado_localmente():
            resultado["mensagem"] = (
                "Banco online ja estava travado neste ambiente, mas a connection string nao foi carregada. "
                "O sistema nao vai cair para SQLite para evitar perda de dados."
            )
        BANCO_ONLINE_STATUS_CACHE["testado_em"] = agora_ts
        BANCO_ONLINE_STATUS_CACHE["resultado"] = dict(resultado)
        return resultado

    if not POSTGRESQL_DISPONIVEL:
        resultado["mensagem"] = (
            "Banco online configurado, mas o driver PostgreSQL nao esta instalado."
        )
        BANCO_ONLINE_STATUS_CACHE["testado_em"] = agora_ts
        BANCO_ONLINE_STATUS_CACHE["resultado"] = dict(resultado)
        return resultado

    dsn = url_postgres_ajustada()
    if not dsn:
        resultado["mensagem"] = (
            "A conexao do Supabase ainda esta incompleta. Verifique a senha e a URL."
        )
        BANCO_ONLINE_STATUS_CACHE["testado_em"] = agora_ts
        BANCO_ONLINE_STATUS_CACHE["resultado"] = dict(resultado)
        return resultado

    resultado.update({
        "host": desmontar_url_postgres(dsn).get("host") or "",
        "porta": str(desmontar_url_postgres(dsn).get("porta") or 5432),
        "database": desmontar_url_postgres(dsn).get("database") or "",
        "usuario": desmontar_url_postgres(dsn).get("usuario") or "",
        "url_masked": mascarar_url_postgres(dsn),
    })

    try:
        conn = conectar_postgres_com_fallback(dsn)
        c = conn.cursor()
        c.execute("SELECT current_database(), current_user")
        row = c.fetchone() or ("", "")
        conn.close()
        resultado.update({
            "ativo": True,
            "conectado": True,
            "mensagem": "Conexao com o banco online estabelecida com sucesso.",
            "database_real": row[0] if len(row) > 0 else resultado["database"],
            "usuario_real": row[1] if len(row) > 1 else resultado["usuario"],
        })
        travar_banco_online_localmente(True, "Banco online validado com sucesso")
    except Exception as e:
        texto_erro = str(e)
        if "Network is unreachable" in texto_erro or "could not translate host name" in texto_erro:
            resultado["mensagem"] = (
                f"Falha ao conectar no banco online: {texto_erro}"
                "\nA conexao direta da Supabase usa IPv6. Se seu servidor nao tem IPv6, "
                "use a connection string do Session pooler no campo de URL completa ou "
                "habilite o add-on IPv4 no Supabase."
            )
        else:
            resultado["mensagem"] = f"Falha ao conectar no banco online: {texto_erro}"
        if erro_limite_conexoes_banco_online(texto_erro):
            BANCO_ONLINE_BLOQUEADO_ATE_TS = max(BANCO_ONLINE_BLOQUEADO_ATE_TS, agora_ts + 45)
            resultado["mensagem"] += " O pool online atingiu o limite de conexoes e vai entrar em respiro temporario."

    BANCO_ONLINE_STATUS_CACHE["testado_em"] = agora_ts
    BANCO_ONLINE_STATUS_CACHE["resultado"] = dict(resultado)
    return resultado


def banco_online_ativo():
    return bool(diagnosticar_banco_online().get("conectado"))


def garantir_schema_banco_online(force=False):
    global SCHEMA_BANCO_ONLINE_GARANTIDO

    if not banco_online_ativo():
        return False
    if SCHEMA_BANCO_ONLINE_GARANTIDO and not force:
        return True

    try:
        criar_todas_tabelas()
        atualizar_banco()
        SCHEMA_BANCO_ONLINE_GARANTIDO = True
        return True
    except Exception as e:
        SCHEMA_BANCO_ONLINE_GARANTIDO = False
        log_info("AVISO: nao foi possivel garantir o schema online:", e)
        return False


def garantir_init_db(force=False):
    global INIT_DB_EXECUTADO
    global SCHEMA_BANCO_ONLINE_GARANTIDO

    if INIT_DB_EXECUTADO and not force:
        return True

    with init_db_lock:
        if INIT_DB_EXECUTADO and not force:
            return True

        init_db()
        INIT_DB_EXECUTADO = True
        if modo_banco_preferido() == "postgres" and banco_online_ativo():
            SCHEMA_BANCO_ONLINE_GARANTIDO = True
        return True


def _bootstrap_init_db_assincrono():
    try:
        garantir_init_db()
    except Exception as e:
        log_info("AVISO BOOTSTRAP INIT_DB:", e)


def iniciar_bootstrap_init_db():
    global bootstrap_init_thread_started

    if bootstrap_init_thread_started:
        return False

    bootstrap_init_thread_started = True
    Thread(target=_bootstrap_init_db_assincrono, daemon=True).start()
    return True


def _bootstrap_schema_online_assincrono():
    global schema_bootstrap_thread_started
    try:
        garantir_schema_banco_online()
    except Exception as e:
        log_info("AVISO BOOTSTRAP SCHEMA:", e)
    finally:
        if not SCHEMA_BANCO_ONLINE_GARANTIDO:
            schema_bootstrap_thread_started = False


def iniciar_bootstrap_schema_online():
    global schema_bootstrap_thread_started

    if schema_bootstrap_thread_started or SCHEMA_BANCO_ONLINE_GARANTIDO:
        return False

    schema_bootstrap_thread_started = True
    Thread(target=_bootstrap_schema_online_assincrono, daemon=True).start()
    return True


def desmontar_url_postgres(url):
    texto = str(url or "").strip()
    if not texto:
        return {
            "scheme": "postgresql",
            "host": "",
            "porta": "5432",
            "database": "postgres",
            "usuario": "postgres",
            "senha": "",
        }

    partes = urlparse(texto)
    host = (partes.hostname or "").strip()
    porta = str(partes.port or 5432) if host else "5432"
    database = (partes.path or "").lstrip("/").strip() or "postgres"
    usuario = (partes.username or "postgres").strip() or "postgres"
    senha = (partes.password or "").strip()
    host_invalido = not host or host.lower() in {"postgres", "postgresql"} or "." not in host
    database_invalido = not database or "@" in database or database.lower().startswith("postgresql")

    if host_invalido or database_invalido:
        ultimo_arroba = texto.rfind("@")
        trecho = texto[ultimo_arroba + 1 :] if ultimo_arroba >= 0 else texto
        trecho = trecho.lstrip("/")

        padrao = re.match(
            r"(?P<host>[A-Za-z0-9.-]+\.[A-Za-z]{2,})(?::(?P<porta>\d+))?/(?P<database>[^?#/]+)",
            trecho,
        )
        if padrao:
            host = padrao.group("host") or host
            porta = padrao.group("porta") or porta or "5432"
            database = (padrao.group("database") or database).split(":", 1)[0].strip() or database
        else:
            padrao_host = re.search(r"([A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+\.[A-Za-z]{2,})", texto)
            if padrao_host:
                host = padrao_host.group(1)

            banco_match = re.search(r"/([^/?#]+)", trecho)
            if banco_match:
                database = banco_match.group(1).split(":", 1)[0].strip() or database

    if not host:
        return {
            "scheme": partes.scheme or "postgresql",
            "host": "",
            "porta": porta or "5432",
            "database": database or "postgres",
            "usuario": usuario or "postgres",
            "senha": senha,
        }

    return {
        "scheme": partes.scheme or "postgresql",
        "host": host,
        "porta": str(int(str(porta).strip()) if str(porta).strip().isdigit() else 5432),
        "database": database or "postgres",
        "usuario": usuario or "postgres",
        "senha": senha,
    }


def listar_ipv4_host(host, porta):
    ips = []
    try:
        infos = socket.getaddrinfo(
            host,
            int(str(porta).strip()) if str(porta).strip().isdigit() else 5432,
            socket.AF_INET,
            socket.SOCK_STREAM,
        )
    except Exception:
        return ips
    for info in infos:
        ip = (info[4] or ("",))[0]
        if ip and ip not in ips:
            ips.append(ip)
    return ips


def conectar_postgres_com_fallback(dsn):
    partes = desmontar_url_postgres(dsn)
    host = partes.get("host") or ""
    porta = partes.get("porta") or "5432"
    tentativas = []
    if host:
        tentativas.extend(listar_ipv4_host(host, porta))

    ultimo_erro = None
    for hostaddr in tentativas:
        try:
            return conectar_postgres_com_retry(dsn, hostaddr=hostaddr)
        except Exception as erro:
            ultimo_erro = erro

    try:
        return conectar_postgres_com_retry(dsn)
    except Exception as erro:
        if ultimo_erro is not None:
            raise ultimo_erro
        raise erro


def erro_transitorio_conexao_postgres(erro):
    texto = str(erro or "").lower()
    sinais = (
        "timeout",
        "timed out",
        "connection refused",
        "could not connect",
        "could not translate host",
        "server closed the connection",
        "ssl syscall",
        "network is unreachable",
        "too many connections",
        "remaining connection slots",
        "connection reset",
    )
    return any(sinal in texto for sinal in sinais)


def conectar_postgres_com_retry(dsn, **kwargs):
    ultimo_erro = None
    tentativas = max(1, int(POSTGRES_RETRY_TENTATIVAS or 1))
    for tentativa in range(tentativas):
        try:
            return psycopg2.connect(dsn, **kwargs)
        except Exception as erro:
            ultimo_erro = erro
            if tentativa >= tentativas - 1 or not erro_transitorio_conexao_postgres(erro):
                break
            time.sleep(float(POSTGRES_RETRY_DELAY or 0))
    raise ultimo_erro


def quebrar_url_postgres(url):
    partes = desmontar_url_postgres(url)
    return {
        "host": partes.get("host") or "",
        "porta": partes.get("porta") or "5432",
        "database": partes.get("database") or "postgres",
        "usuario": partes.get("usuario") or "postgres",
    }


def montar_url_postgres(host, porta, database, usuario, senha):
    host = normalizar_texto_campo(host)
    database = normalizar_texto_campo(database) or "postgres"
    usuario = normalizar_texto_campo(usuario) or "postgres"
    senha = str(senha or "").strip()
    porta = str(converter_inteiro(porta, 5432) or 5432)

    if not host:
        return ""

    return f"postgresql://{quote(usuario)}:{quote(senha)}@{host}:{porta}/{database}"


def obter_configuracao_banco_form(status=None):
    status = dict(status or diagnosticar_banco_online())
    # Preserve exactly what was saved in the environment/config form.
    # The adjusted DSN is only for runtime connection attempts.
    origem = DATABASE_URL_RAW or url_postgres_ajustada()
    partes = quebrar_url_postgres(origem)
    partes.update({
        "modo": modo_banco_preferido(),
        "url_masked": status.get("url_masked") or "",
        "url_completa": origem,
        "senha_preenchida": bool(SUPABASE_DB_PASSWORD),
        "auto_migrar_banco": migracao_online_automatica_ativa(),
        "migracao_online_ja_realizada": bool_config_ativo(DATABASE_ONLINE_MIGRADO_RAW) or bool(status.get("conectado")),
    })
    return partes


def salvar_configuracao_banco_form(form):
    modo = str(form.get("database_backend") or "sqlite").strip().lower()
    if modo not in {"sqlite", "postgres"}:
        modo = "sqlite"

    modo_anterior = modo_banco_preferido()
    url_anterior = url_postgres_ajustada() or DATABASE_URL_RAW
    configuracao_atual = obter_configuracao_banco_form()
    url_completa = normalizar_texto_campo(form.get("database_url"))
    host = normalizar_texto_campo(form.get("database_host")) or configuracao_atual.get("host") or ""
    porta = normalizar_texto_campo(form.get("database_port")) or configuracao_atual.get("porta") or "5432"
    database = normalizar_texto_campo(form.get("database_name")) or configuracao_atual.get("database") or "postgres"
    usuario = normalizar_texto_campo(form.get("database_user")) or configuracao_atual.get("usuario") or "postgres"
    senha = form.get("database_password") or SUPABASE_DB_PASSWORD
    auto_migrar = bool_config_ativo(form.get("migrar_automaticamente", "1"))

    url_atual = DATABASE_URL_RAW
    senha_atual = SUPABASE_DB_PASSWORD

    if modo == "postgres":
        if url_completa:
            partes_url = desmontar_url_postgres(url_completa)
            if not partes_url.get("host"):
                raise ValueError(
                    "Cole a connection string completa do Supabase ou do Session pooler."
                )
            if "[YOUR-PASSWORD]" in url_completa:
                raise ValueError(
                    "Cole a connection string completa com a senha real do banco."
                )
            host = partes_url.get("host") or host
            porta = partes_url.get("porta") or porta
            database = partes_url.get("database") or database
            usuario = partes_url.get("usuario") or usuario
            senha = partes_url.get("senha") or senha
            url_atual = url_completa
            senha_atual = str(senha or partes_url.get("senha") or "").strip()
        else:
            if host.lower() in {"postgres", "postgresql"} or "." not in host:
                host = configuracao_atual.get("host") or host

            if not host or not senha:
                raise ValueError("Preencha a connection string completa ou o host do banco online.")
            url_atual = montar_url_postgres(host, porta, database, usuario, senha)
            senha_atual = str(senha).strip()

    atual_migrado = "0" if url_atual != url_anterior or modo != modo_anterior else DATABASE_ONLINE_MIGRADO_RAW
    atualizar_configuracao_banco_runtime(
        url_atual,
        senha_atual,
        modo,
        auto_migrar=auto_migrar,
        migrado=atual_migrado,
    )
    salvar_env_local({
        "DATABASE_BACKEND": modo,
        "DATABASE_URL": url_atual,
        "SUPABASE_DB_PASSWORD": senha_atual,
        "SUPABASE_DATABASE_URL": url_atual,
        "AUTO_MIGRAR_BANCO": "1" if auto_migrar else "0",
        "DATABASE_ONLINE_MIGRADO": "0" if url_atual != url_anterior or modo != modo_anterior else DATABASE_ONLINE_MIGRADO_RAW,
    })

    status = diagnosticar_banco_online(force=True)
    migracao_automatica = False
    mensagem_migracao = ""
    if status.get("conectado") and modo == "postgres":
        garantir_schema_banco_online(force=True)
        travar_banco_online_localmente(True, "Banco online configurado e validado")
        if auto_migrar and modo_anterior != "postgres":
            try:
                importar_sqlite_para_banco_atual(caminho_banco_absoluto())
                salvar_env_local({
                    "DATABASE_ONLINE_MIGRADO": "1",
                })
                atualizar_configuracao_banco_runtime(migrado="1")
                travar_banco_online_localmente(True, "Migracao local para o banco online concluida")
                migracao_automatica = True
                mensagem_migracao = " Dados do banco local migrados automaticamente para o Supabase."
            except Exception as e:
                log_info("AVISO: falha na migracao automatica para o banco online:", e)
                mensagem_migracao = (
                    " A conexao foi salva, mas a migracao automatica nao concluiu. "
                    "Use o botao de migracao manual."
                )

    status_final = obter_status_banco_online()
    status_final["migracao_automatica"] = migracao_automatica
    status_final["mensagem_migracao"] = mensagem_migracao
    limpar_caches_operacionais_leves()
    return status_final


def obter_status_banco_online():
    status = diagnosticar_banco_online()
    status["modo"] = modo_banco_preferido()
    status["modo_label"] = "Supabase / PostgreSQL" if status["modo"] == "postgres" else "SQLite local"
    status["dsn_masked"] = status.get("url_masked") or ""
    status["backend"] = "postgres" if status.get("conectado") else "sqlite"
    status["backend_label"] = "Supabase / PostgreSQL" if status.get("conectado") else "SQLite local"
    return status


def listar_tabelas_banco_online(status=None):
    status = dict(status or obter_status_banco_online())
    resultado = {
        "disponivel": False,
        "mensagem": status.get("mensagem") or "Banco online indisponivel.",
        "database": status.get("database_real") or status.get("database") or "",
        "usuario": status.get("usuario_real") or status.get("usuario") or "",
        "tabelas": [],
        "quantidade": 0,
    }

    if not status.get("conectado"):
        return resultado

    dsn = url_postgres_ajustada()
    if not dsn:
        resultado["mensagem"] = "A connection string do banco online nao esta completa."
        return resultado

    agora_ts = time.time()
    cache_resultado = BANCO_ONLINE_TABELAS_CACHE.get("resultado")
    if (
        cache_resultado is not None
        and str(BANCO_ONLINE_TABELAS_CACHE.get("dsn") or "") == dsn
        and agora_ts - float(BANCO_ONLINE_TABELAS_CACHE.get("testado_em") or 0.0) < BANCO_ONLINE_TABELAS_CACHE_TTL
    ):
        return dict(cache_resultado)

    conn = None
    try:
        conn = conectar_postgres_com_fallback(dsn)
        c = conn.cursor()
        c.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name
            """
        )
        tabelas = []
        for row in c.fetchall() or []:
            schema = row[0] if len(row) > 0 else ""
            nome = row[1] if len(row) > 1 else ""
            if nome:
                tabelas.append(
                    {
                        "schema": schema or "public",
                        "nome": nome,
                        "rotulo": f"{schema}.{nome}" if schema and schema != "public" else nome,
                    }
                )
        resultado.update(
            {
                "disponivel": True,
                "mensagem": "Tabelas carregadas do banco online.",
                "tabelas": tabelas,
                "quantidade": len(tabelas),
            }
        )
        BANCO_ONLINE_TABELAS_CACHE["testado_em"] = agora_ts
        BANCO_ONLINE_TABELAS_CACHE["dsn"] = dsn
        BANCO_ONLINE_TABELAS_CACHE["resultado"] = dict(resultado)
    except Exception as erro:
        resultado["mensagem"] = f"Nao foi possivel listar as tabelas online: {erro}"
        BANCO_ONLINE_TABELAS_CACHE["testado_em"] = agora_ts
        BANCO_ONLINE_TABELAS_CACHE["dsn"] = dsn
        BANCO_ONLINE_TABELAS_CACHE["resultado"] = dict(resultado)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    return resultado

def conectar():
    global BANCO_ONLINE_BLOQUEADO_ATE_TS

    if modo_banco_preferido() == "postgres":
        dsn = url_postgres_ajustada()
        if not dsn:
            if banco_online_estritamente_obrigatorio():
                raise BancoOnlineObrigatorioErro(
                    "Banco online obrigatorio neste ambiente, mas a connection string nao foi carregada."
                )
            garantir_schema_sqlite_local_minima()
            conn = sqlite3.connect(DATABASE_FILE)
            conn.row_factory = sqlite3.Row
            return ConexaoCompat(conn, "sqlite")

        if (
            not banco_online_estritamente_obrigatorio()
            and BANCO_ONLINE_BLOQUEADO_ATE_TS > time.time()
        ):
            garantir_schema_sqlite_local_minima()
            conn = sqlite3.connect(DATABASE_FILE)
            conn.row_factory = sqlite3.Row
            return ConexaoCompat(conn, "sqlite")

        try:
            conn = conectar_postgres_com_fallback(dsn)
            BANCO_ONLINE_BLOQUEADO_ATE_TS = 0.0
            return ConexaoCompat(conn, "postgres")
        except Exception as e:
            if erro_limite_conexoes_banco_online(e):
                BANCO_ONLINE_BLOQUEADO_ATE_TS = time.time() + 120
            BANCO_ONLINE_STATUS_CACHE["testado_em"] = time.time()
            mensagem_erro = f"Falha ao abrir conexao online: {e}"
            if erro_limite_conexoes_banco_online(e):
                mensagem_erro += " Banco online em respiro temporario."
            BANCO_ONLINE_STATUS_CACHE["resultado"] = {
                "configurado": True,
                "ativo": False,
                "conectado": False,
                "backend": "postgres",
                "backend_label": "Supabase / PostgreSQL",
                "mensagem": mensagem_erro,
                "url_masked": mascarar_url_postgres(dsn),
            }
            registrar_log_banco_online(
                BANCO_ONLINE_STATUS_CACHE["resultado"]["mensagem"],
                intervalo_segundos=120 if erro_limite_conexoes_banco_online(e) else 60,
            )
            registrar_falha_banco_online_request(e, descricao="banco_online_conectar")
            if banco_online_estritamente_obrigatorio():
                raise BancoOnlineObrigatorioErro(
                    "Banco online obrigatorio neste ambiente e a conexao falhou: "
                    f"{BANCO_ONLINE_STATUS_CACHE['resultado']['mensagem']}"
                ) from e
            garantir_schema_sqlite_local_minima()
            conn = sqlite3.connect(DATABASE_FILE)
            conn.row_factory = sqlite3.Row
            return ConexaoCompat(conn, "sqlite")

    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row  # ðŸ”¥ ESSENCIAL
    return ConexaoCompat(conn, "sqlite")


def conectar_banco_local_forcado():
    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row
    return ConexaoCompat(conn, "sqlite")


def erro_limite_conexoes_banco_online(erro):
    texto = normalizar_texto_comparacao(str(erro or ""))
    sinais = (
        "maxclientsinsessionmode",
        "max clients reached",
        "too many clients",
        "remaining connection slots",
        "pool_size",
    )
    return any(sinal in texto for sinal in sinais)


def registrar_log_banco_online(mensagem, intervalo_segundos=60):
    global BANCO_ONLINE_ULTIMO_LOG

    texto = str(mensagem or "").strip()
    if not texto:
        return

    agora_ts = time.time()
    ultimo_texto = str(BANCO_ONLINE_ULTIMO_LOG.get("mensagem") or "")
    ultimo_em = float(BANCO_ONLINE_ULTIMO_LOG.get("testado_em") or 0.0)
    if texto == ultimo_texto and (agora_ts - ultimo_em) < max(5, int(intervalo_segundos or 60)):
        return

    BANCO_ONLINE_ULTIMO_LOG["mensagem"] = texto
    BANCO_ONLINE_ULTIMO_LOG["testado_em"] = agora_ts
    log_info("ERRO:", texto)


def erro_transitorio_leitura_banco(erro):
    if erro_limite_conexoes_banco_online(erro):
        return True

    texto = normalizar_texto_comparacao(str(erro or ""))
    sinais = (
        "deadlock detected",
        "lock timeout",
        "could not obtain lock",
        "statement timeout",
        "database is locked",
        "canceling statement due to lock timeout",
    )
    return any(sinal in texto for sinal in sinais)


def mensagem_erro_login_servidor(erro):
    texto = normalizar_texto_comparacao(str(erro or ""))

    sinais_banco_online = (
        "banco online obrigatorio",
        "connection string",
        "falha ao abrir conexao online",
        "falha ao conectar no banco online",
        "password authentication failed",
        "could not connect to server",
        "connection refused",
        "timeout expired",
    )
    if any(sinal in texto for sinal in sinais_banco_online):
        return (
            "Falha ao acessar o banco configurado no servidor. "
            "Revise DATABASE_BACKEND, SUPABASE_DATABASE_URL e STRICT_ONLINE_DATABASE."
        )

    sinais_estrutura = (
        "no such table",
        "undefined table",
        "does not exist",
        "no such column",
        "undefined column",
    )
    if any(sinal in texto for sinal in sinais_estrutura):
        return (
            "O banco do servidor ainda nao foi preparado por completo. "
            "Execute a inicializacao/migracao antes do primeiro login."
        )

    return "Falha interna ao autenticar. Verifique os logs e a configuracao do servidor."


def excecao_relacionada_banco(erro):
    if isinstance(erro, BancoOnlineObrigatorioErro):
        return True
    if psycopg2 is not None:
        try:
            if isinstance(erro, psycopg2.Error):
                return True
        except Exception:
            pass
    texto = normalizar_texto_comparacao(str(erro or ""))
    sinais = (
        "banco online obrigatorio",
        "connection string",
        "falha ao abrir conexao online",
        "falha ao conectar no banco online",
        "password authentication failed",
        "could not connect to server",
        "connection refused",
        "timeout expired",
        "server closed the connection",
        "connection already closed",
        "connection timed out",
        "too many clients",
        "remaining connection slots",
        "sslmode",
        "undefined table",
        "undefined column",
        "no such table",
        "no such column",
        "does not exist",
    )
    return any(sinal in texto for sinal in sinais)


def copiar_valor_padrao(padrao):
    if isinstance(padrao, dict):
        return dict(padrao)
    if isinstance(padrao, list):
        return list(padrao)
    if isinstance(padrao, set):
        return set(padrao)
    return padrao


def copiar_estrutura_cache(valor):
    try:
        return deepcopy(valor)
    except Exception:
        return copiar_valor_padrao(valor)


def cache_consulta_valido(cache, chave, ttl):
    if cache.get("resultado") is None:
        return False
    if str(cache.get("chave") or "") != str(chave or ""):
        return False
    return time.time() - float(cache.get("testado_em") or 0.0) < ttl


def obter_cache_consulta(cache, chave, ttl):
    entradas = cache.get("entradas")
    if isinstance(entradas, dict):
        entrada = entradas.get(str(chave or ""))
        if not entrada:
            return None
        if time.time() - float(entrada.get("testado_em") or 0.0) >= ttl:
            return None
        cache["testado_em"] = entrada.get("testado_em") or 0.0
        cache["chave"] = str(chave or "")
        cache["resultado"] = entrada.get("resultado")
        return copiar_estrutura_cache(entrada.get("resultado"))

    if not cache_consulta_valido(cache, chave, ttl):
        return None
    return copiar_estrutura_cache(cache.get("resultado"))


def salvar_cache_consulta(cache, chave, resultado):
    chave_normalizada = str(chave or "")
    entradas = cache.get("entradas")
    if isinstance(entradas, dict):
        entradas[chave_normalizada] = {
            "testado_em": time.time(),
            "resultado": copiar_estrutura_cache(resultado),
        }
        agora_cache = entradas[chave_normalizada]["testado_em"]
        cache["testado_em"] = agora_cache
        cache["chave"] = chave_normalizada
        cache["resultado"] = copiar_estrutura_cache(resultado)
        return

    cache["testado_em"] = time.time()
    cache["chave"] = chave_normalizada
    cache["resultado"] = copiar_estrutura_cache(resultado)


def registrar_metrica_consulta_sql(pagina, nome, tempo_ms, origem="banco", detalhes="", cache_hit=False):
    try:
        pagina = normalizar_texto_campo(pagina) or "-"
        nome = normalizar_texto_campo(nome) or "consulta"
        tempo_ms = int(max(0, tempo_ms or 0))
        registro = {
            "pagina": pagina,
            "nome": nome,
            "tempo_ms": tempo_ms,
            "origem": normalizar_texto_campo(origem) or "banco",
            "detalhes": normalizar_texto_campo(detalhes),
            "cache_hit": bool(cache_hit),
            "classe": classificar_latencia_ms(tempo_ms),
            "quando": agora_iso(),
        }
        with SQL_METRICAS_LOCK:
            SQL_METRICAS_CONSULTAS.insert(0, registro)
            del SQL_METRICAS_CONSULTAS[SQL_METRICAS_LIMITE:]
    except Exception:
        pass


def medir_consulta_sql(pagina, nome, func, origem="banco", detalhes=""):
    inicio = time.perf_counter()
    try:
        return func()
    finally:
        registrar_metrica_consulta_sql(
            pagina,
            nome,
            int((time.perf_counter() - inicio) * 1000),
            origem=origem,
            detalhes=detalhes,
        )


def obter_metricas_consultas_sql(limite=40):
    with SQL_METRICAS_LOCK:
        registros = [dict(item) for item in SQL_METRICAS_CONSULTAS[: int(limite or 40)]]

    agregados = {}
    paginas = {}
    for item in registros:
        chave = f"{item.get('pagina')}|{item.get('nome')}"
        resumo = agregados.setdefault(
            chave,
            {
                "pagina": item.get("pagina"),
                "nome": item.get("nome"),
                "amostras": 0,
                "ultimo_ms": 0,
                "max_ms": 0,
                "media_ms": 0,
                "total_ms": 0,
                "origem": item.get("origem"),
                "cache_hits": 0,
                "ultima_medicao": item.get("quando"),
            },
        )
        if not resumo["amostras"]:
            resumo["ultimo_ms"] = item.get("tempo_ms") or 0
            resumo["ultima_medicao"] = item.get("quando")
        resumo["amostras"] += 1
        resumo["max_ms"] = max(int(resumo.get("max_ms") or 0), int(item.get("tempo_ms") or 0))
        resumo["total_ms"] += int(item.get("tempo_ms") or 0)
        resumo["cache_hits"] += 1 if item.get("cache_hit") else 0
        resumo["media_ms"] = int(resumo["total_ms"] / resumo["amostras"]) if resumo["amostras"] else 0
        resumo["classe"] = classificar_latencia_ms(resumo["ultimo_ms"])

        pagina_chave = item.get("pagina") or "-"
        pagina = paginas.setdefault(
            pagina_chave,
            {
                "pagina": pagina_chave,
                "amostras": 0,
                "ultimo_ms": 0,
                "max_ms": 0,
                "media_ms": 0,
                "total_ms": 0,
                "cache_hits": 0,
                "gargalo": "",
                "gargalo_ms": 0,
                "ultima_medicao": item.get("quando"),
            },
        )
        tempo_item = int(item.get("tempo_ms") or 0)
        if not pagina["amostras"]:
            pagina["ultimo_ms"] = tempo_item
            pagina["ultima_medicao"] = item.get("quando")
        pagina["amostras"] += 1
        pagina["max_ms"] = max(int(pagina.get("max_ms") or 0), tempo_item)
        pagina["total_ms"] += tempo_item
        pagina["cache_hits"] += 1 if item.get("cache_hit") else 0
        pagina["media_ms"] = int(pagina["total_ms"] / pagina["amostras"]) if pagina["amostras"] else 0
        if tempo_item >= int(pagina.get("gargalo_ms") or 0):
            pagina["gargalo"] = item.get("nome") or "consulta"
            pagina["gargalo_ms"] = tempo_item

    ranking = sorted(agregados.values(), key=lambda item: int(item.get("max_ms") or 0), reverse=True)
    por_pagina = sorted(paginas.values(), key=lambda item: int(item.get("max_ms") or 0), reverse=True)
    for item in por_pagina:
        item["classe"] = classificar_latencia_ms(item.get("max_ms") or 0)
    return {
        "recentes": registros,
        "ranking": ranking[:20],
        "por_pagina": por_pagina[:12],
        "total_registros": len(registros),
    }


def conectar_somente_leitura():
    conn = conectar()
    if getattr(conn, "backend", "") != "postgres":
        return conn

    try:
        conn.rollback()
    except Exception:
        pass

    try:
        conn.set_session(readonly=True, autocommit=True)
    except Exception:
        try:
            conn.autocommit = True
        except Exception:
            pass
    try:
        cursor = conn.cursor()
        cursor.execute("SET lock_timeout TO '750ms'")
        cursor.execute("SET statement_timeout TO '2500ms'")
        cursor.close()
    except Exception:
        pass
    return conn


def executar_leitura_resiliente(operacao, descricao="", padrao=None, permitir_fallback_local=None):
    if permitir_fallback_local is None:
        permitir_fallback_local = not banco_online_estritamente_obrigatorio()

    conn = None
    try:
        conn = conectar_somente_leitura()
        return operacao(conn)
    except Exception as erro:
        mensagem = f"{descricao}: {erro}" if descricao else str(erro)
        if erro_limite_conexoes_banco_online(erro):
            registrar_log_banco_online(mensagem, intervalo_segundos=30)
        else:
            log_info("AVISO LEITURA:", mensagem)

        if permitir_fallback_local:
            conn_local = None
            try:
                garantir_schema_sqlite_local_minima(force=True)
                conn_local = conectar_banco_local_forcado()
                return operacao(conn_local)
            except Exception as erro_local:
                mensagem_local = f"{descricao}: {erro_local}" if descricao else str(erro_local)
                log_info("AVISO LEITURA LOCAL:", mensagem_local)
            finally:
                try:
                    if conn_local:
                        conn_local.close()
                except Exception:
                    pass

        if padrao is not None or erro_transitorio_leitura_banco(erro):
            return copiar_valor_padrao(padrao)
        raise
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def garantir_schema_sqlite_local_minima(force=False):
    global SCHEMA_SQLITE_LOCAL_GARANTIDO

    if SCHEMA_SQLITE_LOCAL_GARANTIDO and not force:
        return True

    with schema_sqlite_local_lock:
        if SCHEMA_SQLITE_LOCAL_GARANTIDO and not force:
            return True

        conn = conectar_banco_local_forcado()
        c = conn.cursor()
        try:
            c.execute("""
            CREATE TABLE IF NOT EXISTS clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                nome TEXT NOT NULL,
                telefone TEXT,
                placa_principal TEXT,
                data_nascimento TEXT
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS veiculos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                placa TEXT NOT NULL,
                modelo TEXT,
                cor TEXT,
                cliente_id INTEGER,
                status_atendimento TEXT DEFAULT 'SEM_ATENDIMENTO',
                atendimento_ativo INTEGER DEFAULT 0,
                ultima_entrada TEXT,
                ultima_entrega TEXT,
                FOREIGN KEY(cliente_id) REFERENCES clientes(id)
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS servicos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                veiculo_id INTEGER,
                tipo_id INTEGER,
                valor REAL,
                entrada TEXT,
                entrega_prevista TEXT,
                entrega TEXT,
                status TEXT,
                prioridade INTEGER DEFAULT 0,
                observacoes TEXT,
                origem TEXT,
                guarita TEXT,
                pneu TEXT,
                cera TEXT,
                hidro_lataria TEXT,
                hidro_vidros TEXT,
                valor_adicional REAL DEFAULT 0,
                criado_por_usuario TEXT,
                criado_por_nome TEXT,
                etapa_atual TEXT DEFAULT 'LAVAGEM',
                etapa_atual_iniciada_em TEXT,
                lavagem_iniciada_em TEXT,
                finalizacao_iniciada_em TEXT,
                lavagem_segundos INTEGER DEFAULT 0,
                finalizacao_segundos INTEGER DEFAULT 0,
                operacional_por_usuario TEXT,
                operacional_por_nome TEXT,
                finalizado_por_usuario TEXT,
                finalizado_por_nome TEXT,
                perfil_cliente_atendimento TEXT DEFAULT 'NOVO'
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS fotos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                servico_id INTEGER,
                tipo TEXT,
                caminho TEXT,
                usuario TEXT,
                usuario_nome TEXT,
                tamanho_bytes INTEGER,
                largura INTEGER,
                altura INTEGER,
                arquivo_blob BLOB,
                mime_type TEXT,
                arquivo_nome TEXT,
                criado_em TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS configuracao_empresa (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                empresa_id INTEGER DEFAULT 1,
                versao_sistema TEXT,
                clima_ativo INTEGER DEFAULT 1,
                clima_api_url TEXT,
                clima_local_label TEXT,
                clima_latitude REAL,
                clima_longitude REAL,
                clima_timezone TEXT,
                clima_timeout_segundos INTEGER DEFAULT 8,
                marca_nome TEXT,
                marca_subtitulo TEXT,
                marca_logo_url TEXT,
                marca_logo_blob BLOB,
                marca_logo_mime_type TEXT,
                marca_logo_arquivo_nome TEXT,
                marca_favicon_url TEXT,
                marca_favicon_blob BLOB,
                marca_favicon_mime_type TEXT,
                marca_favicon_arquivo_nome TEXT,
                marca_cor_primaria TEXT,
                marca_cor_secundaria TEXT,
                marca_cor_fundo TEXT,
                marca_cor_superficie TEXT,
                marca_cor_texto TEXT,
                site_titulo TEXT,
                site_rodape_texto TEXT,
                login_titulo_publico TEXT,
                login_subtitulo_publico TEXT,
                login_botao_texto TEXT,
                home_busca_placeholder TEXT,
                home_busca_botao_texto TEXT,
                home_estado_inicial_titulo TEXT,
                whitelabel_ativo INTEGER DEFAULT 0,
                storage_provider TEXT,
                licenca_plano TEXT,
                licenca_status TEXT,
                onboarding_concluido INTEGER DEFAULT 0,
                paginas_menu_desabilitadas_json TEXT,
                auto_teste_ativo INTEGER DEFAULT 0,
                auto_teste_site_url TEXT,
                auto_teste_intervalo_horas INTEGER DEFAULT 2,
                auto_teste_telegram_bot_token TEXT,
                auto_teste_telegram_bot_nick TEXT,
                auto_teste_telegram_chat_id TEXT,
                auto_teste_ultimo_status TEXT,
                auto_teste_ultimo_relatorio TEXT,
                auto_teste_ultimo_teste_em TEXT,
                atualizado_em TEXT
            )
            """)
            c.execute("""
            CREATE TABLE IF NOT EXISTS sincronizacoes_clientes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                empresa_id INTEGER DEFAULT 1,
                nome TEXT,
                url TEXT NOT NULL,
                intervalo_minutos INTEGER NOT NULL DEFAULT 60,
                campo_placa TEXT NOT NULL,
                campo_nome TEXT,
                campo_telefone TEXT,
                campo_modelo TEXT,
                campo_cor TEXT,
                campo_servico TEXT,
                campo_data TEXT,
                ativo INTEGER NOT NULL DEFAULT 1,
                ultimo_sync_em TEXT,
                proximo_sync_em TEXT,
                ultimo_status TEXT,
                ultima_mensagem TEXT,
                ultimo_hash TEXT,
                colunas_ultima_sync TEXT,
                excluido_em TEXT,
                excluido_por TEXT,
                criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
                atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """)

            adicionar_coluna_se_preciso(c, "clientes", "placa_principal TEXT")
            adicionar_coluna_se_preciso(c, "clientes", "data_nascimento TEXT")
            adicionar_coluna_se_preciso(c, "veiculos", "status_atendimento TEXT DEFAULT 'SEM_ATENDIMENTO'")
            adicionar_coluna_se_preciso(c, "veiculos", "atendimento_ativo INTEGER DEFAULT 0")
            adicionar_coluna_se_preciso(c, "veiculos", "ultima_entrada TEXT")
            adicionar_coluna_se_preciso(c, "veiculos", "ultima_entrega TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "entrega_prevista TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "valor_adicional REAL DEFAULT 0")
            adicionar_coluna_se_preciso(c, "servicos", "origem TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "guarita TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "pneu TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "cera TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "hidro_lataria TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "hidro_vidros TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "criado_por_usuario TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "criado_por_nome TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "etapa_atual TEXT DEFAULT 'LAVAGEM'")
            adicionar_coluna_se_preciso(c, "servicos", "etapa_atual_iniciada_em TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "lavagem_iniciada_em TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "finalizacao_iniciada_em TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "lavagem_segundos INTEGER DEFAULT 0")
            adicionar_coluna_se_preciso(c, "servicos", "finalizacao_segundos INTEGER DEFAULT 0")
            adicionar_coluna_se_preciso(c, "servicos", "operacional_por_usuario TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "operacional_por_nome TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "finalizado_por_usuario TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "finalizado_por_nome TEXT")
            adicionar_coluna_se_preciso(c, "servicos", "perfil_cliente_atendimento TEXT DEFAULT 'NOVO'")
            adicionar_coluna_se_preciso(c, "fotos", "arquivo_blob BLOB")
            adicionar_coluna_se_preciso(c, "fotos", "mime_type TEXT")
            adicionar_coluna_se_preciso(c, "fotos", "arquivo_nome TEXT")
            adicionar_coluna_se_preciso(c, "notificacoes", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "notificacoes", "categoria TEXT")
            adicionar_coluna_se_preciso(c, "notificacoes", "referencia TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "versao_sistema TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_ativo INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_api_url TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_local_label TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_latitude REAL")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_longitude REAL")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_timezone TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_timeout_segundos INTEGER DEFAULT 8")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_nome TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_subtitulo TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_url TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_blob BLOB")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_mime_type TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_arquivo_nome TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_url TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_blob BLOB")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_mime_type TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_arquivo_nome TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_primaria TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_secundaria TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_fundo TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_superficie TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_texto TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "site_titulo TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "site_rodape_texto TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "login_titulo_publico TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "login_subtitulo_publico TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "login_botao_texto TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "home_busca_placeholder TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "home_busca_botao_texto TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "home_estado_inicial_titulo TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "whitelabel_ativo INTEGER DEFAULT 0")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "storage_provider TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "licenca_plano TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "licenca_status TEXT")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "onboarding_concluido INTEGER DEFAULT 0")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "paginas_menu_desabilitadas_json TEXT")
            adicionar_coluna_se_preciso(c, "clientes", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "veiculos", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "servicos", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "fotos", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "configuracao_empresa", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "orcamentos", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "notas_fiscais", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "licencas", "codigo_licenca TEXT")
            adicionar_coluna_se_preciso(c, "licencas", "assinatura TEXT")
            adicionar_coluna_se_preciso(c, "licencas", "payload_json TEXT")
            adicionar_coluna_se_preciso(c, "licencas", "emitida_em TEXT")
            adicionar_coluna_se_preciso(c, "licencas", "renovada_em TEXT")
            adicionar_coluna_se_preciso(c, "licencas", "ultimo_status_validacao TEXT")
            adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "empresa_id INTEGER DEFAULT 1")
            adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "ultimo_hash TEXT")
            adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "colunas_ultima_sync TEXT")
            adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "criado_em TEXT")
            adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "atualizado_em TEXT")
            adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "excluido_em TEXT")
            adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "excluido_por TEXT")
            c.execute("UPDATE clientes SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE veiculos SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE servicos SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE fotos SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE configuracao_empresa SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE orcamentos SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE notas_fiscais SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE sincronizacoes_clientes SET empresa_id=1 WHERE empresa_id IS NULL")
            c.execute("UPDATE notificacoes SET empresa_id=1 WHERE empresa_id IS NULL")
            conn.commit()
            SCHEMA_SQLITE_LOCAL_GARANTIDO = True
            return True
        finally:
            conn.close()


def conectar_banco_online_forcado():
    dsn = url_postgres_ajustada()
    if not dsn:
        raise BancoOnlineObrigatorioErro("Banco online configurado, mas a connection string esta incompleta.")
    try:
        conn = conectar_postgres_com_fallback(dsn)
    except Exception as erro:
        raise BancoOnlineObrigatorioErro(f"Falha ao conectar no banco online: {erro}") from erro
    return ConexaoCompat(conn, "postgres")


def obter_colunas_tabela(conn, tabela):
    try:
        cursor = conn.cursor()
        if getattr(conn, "backend", "") == "postgres":
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = ?
                ORDER BY ordinal_position
                """,
                (tabela,),
            )
            return [normalizar_texto_campo(row[0] if isinstance(row, tuple) else row["column_name"]) for row in cursor.fetchall()]

        cursor.execute(f"PRAGMA table_info({tabela})")
        colunas = []
        for row in cursor.fetchall():
            if isinstance(row, dict):
                colunas.append(normalizar_texto_campo(row.get("name")))
            else:
                colunas.append(normalizar_texto_campo(row[1] if len(row) > 1 else ""))
        return [coluna for coluna in colunas if coluna]
    except Exception:
        return []


def hash_registro_sync(registro):
    dados = {}
    for chave, valor in dict(registro or {}).items():
        if chave == "atualizado_em":
            continue
        dados[str(chave)] = sanitizar_para_json(valor)

    texto = json.dumps(dados, ensure_ascii=False, sort_keys=True, default=sanitizar_para_json)
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def obter_momento_registro_sync(registro):
    if not registro:
        return None

    registro_dict = dict(registro) if not isinstance(registro, dict) else dict(registro)
    for chave in ("atualizado_em", "updated_em", "updated_at", "criado_em", "created_at"):
        valor = registro_dict.get(chave)
        if not valor:
            continue

        momento = interpretar_datahora_sistema(valor)
        if momento:
            if momento.tzinfo is None:
                momento = momento.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
            return momento

    return None


def obter_status_operacional_sync(registro):
    if not registro:
        return ""

    registro_dict = dict(registro) if not isinstance(registro, dict) else registro
    for chave in ("status", "status_atendimento"):
        valor = normalizar_texto_campo(registro_dict.get(chave)).upper()
        if valor:
            return valor
    return ""


def nivel_restricao_usuario_sync(registro):
    if not registro:
        return 0

    registro_dict = dict(registro) if not isinstance(registro, dict) else registro
    nivel = 0

    ativo = registro_dict.get("ativo")
    if ativo is not None and not bool(int(ativo or 0)):
        nivel = max(nivel, 2)

    bloqueado_ate = interpretar_datahora_sistema(registro_dict.get("bloqueado_ate"))
    if bloqueado_ate and bloqueado_ate > agora():
        nivel = max(nivel, 3)

    obrigatoria = registro_dict.get("senha_alteracao_obrigatoria")
    if obrigatoria is not None and bool(int(obrigatoria or 0)):
        nivel = max(nivel, 1)

    return nivel


def decidir_atualizar_registro_sync(
    registro_origem,
    registro_destino,
    origem_prevalece_em_empate=False,
    tabela=None,
):
    hash_origem = hash_registro_sync(registro_origem)
    hash_destino = hash_registro_sync(registro_destino)

    if hash_origem == hash_destino:
        return False

    if tabela in {"servicos", "veiculos"}:
        status_origem = obter_status_operacional_sync(registro_origem)
        status_destino = obter_status_operacional_sync(registro_destino)

        if status_origem == "FINALIZADO" and status_destino != "FINALIZADO":
            return True
        if status_destino == "FINALIZADO" and status_origem != "FINALIZADO":
            return False

        if status_origem == "EM ANDAMENTO" and status_destino == "FINALIZADO":
            return False
        if status_destino == "EM ANDAMENTO" and status_origem == "FINALIZADO":
            return True

    if tabela == "usuarios":
        nivel_origem = nivel_restricao_usuario_sync(registro_origem)
        nivel_destino = nivel_restricao_usuario_sync(registro_destino)

        if nivel_origem != nivel_destino:
            return nivel_origem > nivel_destino

    momento_origem = obter_momento_registro_sync(registro_origem)
    momento_destino = obter_momento_registro_sync(registro_destino)

    if momento_origem and momento_destino:
        if momento_origem > momento_destino:
            return True
        if momento_origem < momento_destino:
            return False
        return origem_prevalece_em_empate

    if momento_origem and not momento_destino:
        return True
    if momento_destino and not momento_origem:
        return False

    return origem_prevalece_em_empate


def tabela_existe_no_backend(conn, tabela):
    try:
        cursor = conn.cursor()
        if getattr(conn, "backend", "") == "postgres":
            cursor.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = ?
                LIMIT 1
                """,
                (tabela,),
            )
            return cursor.fetchone() is not None

        cursor.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (tabela,),
        )
        return cursor.fetchone() is not None
    except Exception:
        return False


def ajustar_sequence_tabela_postgres(conn, tabela):
    if getattr(conn, "backend", "") != "postgres":
        return

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COALESCE(MAX(id), 0) FROM {tabela}")
        maior_id = cursor.fetchone()[0] or 0
        cursor.execute(
            f"SELECT setval(pg_get_serial_sequence('{tabela}', 'id'), %s, %s)",
            (max(1, int(maior_id)), bool(maior_id)),
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass


def garantir_atualizado_em_sync(cursor):
    backend = getattr(cursor, "backend", "")

    if backend == "postgres":
        aplicar_triggers_sync_no_boot = bool_config_ativo(
            os.environ.get("APLICAR_TRIGGERS_SYNC_NO_BOOT", "")
        )

        if not aplicar_triggers_sync_no_boot:
            for tabela in TABELAS_COM_ATUALIZADO_EM_SYNC:
                adicionar_coluna_se_preciso(cursor, tabela, "atualizado_em TEXT")
            return

        try:
            cursor.execute("""
                CREATE OR REPLACE FUNCTION public.atualizar_atualizado_em_generico()
                RETURNS trigger
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    NEW.atualizado_em := to_char(
                        timezone('America/Sao_Paulo', now()),
                        'YYYY-MM-DD"T"HH24:MI:SS'
                    ) || '-03:00';
                    RETURN NEW;
                END;
                $$;
            """)
        except Exception:
            try:
                conn = getattr(cursor, "_cursor", None)
                conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
                if conn and hasattr(conn, "rollback"):
                    conn.rollback()
            except Exception:
                pass

        for tabela in TABELAS_COM_ATUALIZADO_EM_SYNC:
            adicionar_coluna_se_preciso(cursor, tabela, "atualizado_em TEXT")
            try:
                cursor.execute(
                    f"UPDATE {tabela} SET atualizado_em = COALESCE(NULLIF(atualizado_em, ''), ?)",
                    (agora_iso(),),
                )
            except Exception:
                try:
                    conn = getattr(cursor, "_cursor", None)
                    conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
                    if conn and hasattr(conn, "rollback"):
                        conn.rollback()
                except Exception:
                    pass

            try:
                cursor.execute(f"DROP TRIGGER IF EXISTS trg_{tabela}_atualizado_em ON {tabela}")
            except Exception:
                try:
                    conn = getattr(cursor, "_cursor", None)
                    conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
                    if conn and hasattr(conn, "rollback"):
                        conn.rollback()
                except Exception:
                    pass

            try:
                cursor.execute(f"""
                    CREATE TRIGGER trg_{tabela}_atualizado_em
                    BEFORE INSERT OR UPDATE ON {tabela}
                    FOR EACH ROW
                    EXECUTE FUNCTION public.atualizar_atualizado_em_generico()
                """)
            except Exception:
                try:
                    conn = getattr(cursor, "_cursor", None)
                    conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
                    if conn and hasattr(conn, "rollback"):
                        conn.rollback()
                except Exception:
                    pass
        return

    for tabela in TABELAS_COM_ATUALIZADO_EM_SYNC:
        adicionar_coluna_se_preciso(cursor, tabela, "atualizado_em TEXT")
        try:
            cursor.execute(
                f"UPDATE {tabela} SET atualizado_em = COALESCE(NULLIF(atualizado_em, ''), ?)",
                (agora_iso(),),
            )
        except Exception:
            try:
                conn = getattr(cursor, "_cursor", None)
                conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
                if conn and hasattr(conn, "rollback"):
                    conn.rollback()
            except Exception:
                pass

        try:
            cursor.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS trg_{tabela}_ai_atualizado_em
                AFTER INSERT ON {tabela}
                FOR EACH ROW
                BEGIN
                    UPDATE {tabela}
                    SET atualizado_em = COALESCE(NEW.atualizado_em, CURRENT_TIMESTAMP)
                    WHERE id = NEW.id;
                END;
                """
            )
        except Exception:
            try:
                conn = getattr(cursor, "_cursor", None)
                conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
                if conn and hasattr(conn, "rollback"):
                    conn.rollback()
            except Exception:
                pass

        try:
            cursor.execute(
                f"""
                CREATE TRIGGER IF NOT EXISTS trg_{tabela}_au_atualizado_em
                AFTER UPDATE ON {tabela}
                FOR EACH ROW
                BEGIN
                    UPDATE {tabela}
                    SET atualizado_em = CURRENT_TIMESTAMP
                    WHERE id = NEW.id;
                END;
                """
            )
        except Exception:
            try:
                conn = getattr(cursor, "_cursor", None)
                conn = getattr(conn, "connection", None) or getattr(cursor, "connection", None)
                if conn and hasattr(conn, "rollback"):
                    conn.rollback()
            except Exception:
                pass


CHAVES_UNICAS_SYNC = {
    "veiculos": ("placa",),
    "usuarios": ("usuario",),
    "retornos_clientes": ("placa",),
    "orcamentos": ("numero",),
    "notas_fiscais": ("rps_numero",),
}


def encontrar_registro_destino_por_chave_unica(cursor_destino, tabela, registro_dict):
    campos = CHAVES_UNICAS_SYNC.get(tabela) or ()
    if not campos:
        return None

    condicoes = []
    parametros = []

    for campo in campos:
        valor = registro_dict.get(campo)
        if valor is None or str(valor).strip() == "":
            return None

        if campo == "placa":
            condicoes.append("TRIM(UPPER(placa))=TRIM(UPPER(?))")
            parametros.append(normalizar_texto_campo(valor).upper())
        elif campo == "usuario":
            condicoes.append("TRIM(LOWER(usuario))=TRIM(LOWER(?))")
            parametros.append(normalizar_texto_campo(valor).lower())
        else:
            condicoes.append(f"{campo}=?")
            parametros.append(valor)

    try:
        cursor_destino.execute(
            f"SELECT * FROM {tabela} WHERE {' AND '.join(condicoes)} LIMIT 1",
            tuple(parametros),
        )
        return cursor_destino.fetchone()
    except Exception:
        return None


def executar_update_sync_seguro(cursor_destino, tabela, colunas_update, valores_update, id_destino):
    if not colunas_update or id_destino is None:
        return False

    sql_update = (
        f"UPDATE {tabela} SET "
        + ", ".join(f"{coluna}=?" for coluna in colunas_update)
        + " WHERE id=?"
    )

    try:
        cursor_destino.execute("SAVEPOINT sync_update_registro")
    except Exception:
        pass

    try:
        cursor_destino.execute(sql_update, tuple(valores_update) + (id_destino,))
        try:
            cursor_destino.execute("RELEASE SAVEPOINT sync_update_registro")
        except Exception:
            pass
        return True
    except Exception:
        try:
            cursor_destino.execute("ROLLBACK TO SAVEPOINT sync_update_registro")
            cursor_destino.execute("RELEASE SAVEPOINT sync_update_registro")
        except Exception:
            pass
        return False


def sincronizar_tabela_incremental(origem_conn, destino_conn, tabela, origem_prevalece_em_empate=False):
    if not tabela_existe_no_backend(origem_conn, tabela) or not tabela_existe_no_backend(destino_conn, tabela):
        return {"lidos": 0, "inseridos": 0, "atualizados": 0, "ignorados": 0}

    colunas_destino = obter_colunas_tabela(destino_conn, tabela)
    if not colunas_destino:
        return {"lidos": 0, "inseridos": 0, "atualizados": 0, "ignorados": 0}

    colunas_destino_set = set(colunas_destino)
    cursor_origem = origem_conn.cursor()
    cursor_destino = destino_conn.cursor()
    cursor_origem.execute(f"SELECT * FROM {tabela} ORDER BY id")
    registros = cursor_origem.fetchall()

    estatisticas = {"lidos": 0, "inseridos": 0, "atualizados": 0, "ignorados": 0}

    for registro in registros:
        estatisticas["lidos"] += 1
        registro_dict = dict(registro) if not isinstance(registro, dict) else dict(registro)
        if not registro_dict:
            estatisticas["ignorados"] += 1
            continue

        colunas = [coluna for coluna in registro_dict.keys() if coluna in colunas_destino_set]
        if not colunas:
            estatisticas["ignorados"] += 1
            continue

        valores_originais = [registro_dict.get(coluna) for coluna in colunas]
        if getattr(destino_conn, "backend", "") == "postgres":
            valores = [
                normalizar_valor_importacao_pg(tabela, coluna, valor)
                for coluna, valor in zip(colunas, valores_originais)
            ]
        else:
            valores = valores_originais

        id_registro = registro_dict.get("id")
        existe = False
        if id_registro is not None:
            try:
                cursor_destino.execute(f"SELECT 1 FROM {tabela} WHERE id=?", (id_registro,))
                existe = cursor_destino.fetchone() is not None
            except Exception:
                existe = False

        if existe:
            try:
                cursor_destino.execute(f"SELECT * FROM {tabela} WHERE id=?", (id_registro,))
                registro_destino = cursor_destino.fetchone()
            except Exception:
                registro_destino = None

            if registro_destino is None:
                estatisticas["ignorados"] += 1
                continue

            if not decidir_atualizar_registro_sync(
                registro_dict,
                registro_destino,
                origem_prevalece_em_empate=origem_prevalece_em_empate,
                tabela=tabela,
            ):
                estatisticas["ignorados"] += 1
                continue

            colunas_update = [coluna for coluna in colunas if coluna != "id"]
            valores_update = [registro_dict.get(coluna) for coluna in colunas_update]
            if getattr(destino_conn, "backend", "") == "postgres":
                valores_update = [
                    normalizar_valor_importacao_pg(tabela, coluna, valor)
                    for coluna, valor in zip(colunas_update, valores_update)
                ]

            if executar_update_sync_seguro(
                cursor_destino,
                tabela,
                colunas_update,
                valores_update,
                id_registro,
            ):
                estatisticas["atualizados"] += 1
            else:
                estatisticas["ignorados"] += 1
            continue

        registro_destino = encontrar_registro_destino_por_chave_unica(
            cursor_destino,
            tabela,
            registro_dict,
        )
        if registro_destino is not None:
            if not decidir_atualizar_registro_sync(
                registro_dict,
                registro_destino,
                origem_prevalece_em_empate=origem_prevalece_em_empate,
                tabela=tabela,
            ):
                estatisticas["ignorados"] += 1
                continue

            id_destino = registro_destino["id"] if hasattr(registro_destino, "__getitem__") else None
            colunas_update = [coluna for coluna in colunas if coluna != "id"]
            valores_update = [registro_dict.get(coluna) for coluna in colunas_update]
            if getattr(destino_conn, "backend", "") == "postgres":
                valores_update = [
                    normalizar_valor_importacao_pg(tabela, coluna, valor)
                    for coluna, valor in zip(colunas_update, valores_update)
                ]

            if executar_update_sync_seguro(
                cursor_destino,
                tabela,
                colunas_update,
                valores_update,
                id_destino,
            ):
                estatisticas["atualizados"] += 1
            else:
                estatisticas["ignorados"] += 1
            continue

        sql_insert = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({', '.join(['?'] * len(colunas))})"
        try:
            cursor_destino.execute("SAVEPOINT sync_insert_registro")
        except Exception:
            pass

        try:
            cursor_destino.execute(sql_insert, valores)
            try:
                cursor_destino.execute("RELEASE SAVEPOINT sync_insert_registro")
            except Exception:
                pass
            estatisticas["inseridos"] += 1
        except Exception:
            try:
                cursor_destino.execute("ROLLBACK TO SAVEPOINT sync_insert_registro")
                cursor_destino.execute("RELEASE SAVEPOINT sync_insert_registro")
            except Exception:
                pass

            registro_destino = encontrar_registro_destino_por_chave_unica(
                cursor_destino,
                tabela,
                registro_dict,
            )
            if registro_destino is None:
                estatisticas["ignorados"] += 1
                continue

            if not decidir_atualizar_registro_sync(
                registro_dict,
                registro_destino,
                origem_prevalece_em_empate=origem_prevalece_em_empate,
                tabela=tabela,
            ):
                estatisticas["ignorados"] += 1
                continue

            id_destino = registro_destino["id"] if hasattr(registro_destino, "__getitem__") else None
            colunas_update = [coluna for coluna in colunas if coluna != "id"]
            valores_update = [registro_dict.get(coluna) for coluna in colunas_update]
            if getattr(destino_conn, "backend", "") == "postgres":
                valores_update = [
                    normalizar_valor_importacao_pg(tabela, coluna, valor)
                    for coluna, valor in zip(colunas_update, valores_update)
                ]

            if executar_update_sync_seguro(
                cursor_destino,
                tabela,
                colunas_update,
                valores_update,
                id_destino,
            ):
                estatisticas["atualizados"] += 1
            else:
                estatisticas["ignorados"] += 1

    try:
        destino_conn.commit()
    except Exception:
        try:
            destino_conn.rollback()
        except Exception:
            pass
        raise

    ajustar_sequence_tabela_postgres(destino_conn, tabela)
    return estatisticas


def sincronizar_bancos_incremental(force=False):
    if not banco_online_configurado():
        return {"ativo": False, "conectado": False, "mensagem": "Banco online nao configurado."}

    status = diagnosticar_banco_online(force=force)
    if not status.get("conectado"):
        return {
            "ativo": False,
            "conectado": False,
            "mensagem": status.get("mensagem") or "Banco online indisponivel.",
        }

    origem_online = conectar_banco_online_forcado()
    destino_local = conectar_banco_local_forcado()

    resumo = {
        "ativo": True,
        "conectado": True,
        "mensagem": "Sincronizacao incremental concluida.",
        "tabelas": {},
    }

    try:
        for tabela in TABELAS_SISTEMA_ORDENADAS:
            if tabela.startswith("sincronizacao_"):
                continue

            resultado_online_local = sincronizar_tabela_incremental(
                origem_online,
                destino_local,
                tabela,
                origem_prevalece_em_empate=True,
            )
            resultado_local_online = sincronizar_tabela_incremental(
                destino_local,
                origem_online,
                tabela,
                origem_prevalece_em_empate=False,
            )
            resumo["tabelas"][tabela] = {
                "online_para_local": resultado_online_local,
                "local_para_online": resultado_local_online,
            }
        return resumo
    finally:
        try:
            origem_online.close()
        except Exception:
            pass
        try:
            destino_local.close()
        except Exception:
            pass

def salvar_notificacao(mensagem, tipo="info", empresa_id=None, categoria=None, referencia=None):
    global NOTIFICACOES_CACHE
    global HUD_CACHE
    conn = None
    try:
        empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())
        conn = conectar()
        c = conn.cursor()

        c.execute("""
            INSERT INTO notificacoes (empresa_id, mensagem, tipo, categoria, referencia, criada_em)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (empresa_id, mensagem, tipo, categoria, referencia, agora_iso()))

        conn.commit()
        NOTIFICACOES_CACHE["testado_em"] = 0.0
        NOTIFICACOES_CACHE["resultado"] = None
        HUD_CACHE["testado_em"] = 0.0
        HUD_CACHE["resultado"] = None

    except Exception as e:
        log_info("ERRO AO SALVAR NOTIFICACAO:", e)

    finally:
        try:
            conn.close()
        except:
            pass

def garantir_notificacoes_aniversario(empresa_id=None, force=False):
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())
    hoje = agora().date()
    chave_cache = f"{empresa_id}|{hoje.isoformat()}"
    agora_ts = time.time()
    if (
        not force
        and ANIVERSARIO_NOTIFICACOES_CACHE.get("chave") == chave_cache
        and agora_ts - float(ANIVERSARIO_NOTIFICACOES_CACHE.get("testado_em") or 0.0) < ANIVERSARIO_NOTIFICACOES_CACHE_TTL
    ):
        return 0

    conn = None
    inseridas = 0
    try:
        conn = conectar()
        c = conn.cursor()
        c.execute(
            """
            SELECT id, nome, telefone, data_nascimento
            FROM clientes
            WHERE empresa_id=?
              AND data_nascimento IS NOT NULL
              AND TRIM(data_nascimento) <> ''
            """,
            (empresa_id,),
        )
        clientes = [dict(row) for row in c.fetchall()]
        for cliente in clientes:
            data_nascimento = interpretar_data_nascimento(cliente.get("data_nascimento"))
            if not data_nascimento:
                continue

            aniversario_ano = construir_data_aniversario_ano(data_nascimento, hoje.year)
            dias_restantes = (aniversario_ano - hoje).days
            if dias_restantes not in {0, 1}:
                continue

            categoria = "aniversario_hoje" if dias_restantes == 0 else "aniversario_amanha"
            referencia = f"{categoria}:{cliente.get('id')}:{aniversario_ano.isoformat()}"
            c.execute(
                """
                SELECT 1
                FROM notificacoes
                WHERE empresa_id=? AND categoria=? AND referencia=?
                LIMIT 1
                """,
                (empresa_id, categoria, referencia),
            )
            if c.fetchone():
                continue

            mensagem = montar_mensagem_notificacao_aniversario(
                cliente.get("nome"),
                data_nascimento,
                dias_antecedencia=dias_restantes,
            )
            c.execute(
                """
                INSERT INTO notificacoes (empresa_id, mensagem, tipo, categoria, referencia, criada_em)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (empresa_id, mensagem, "info", categoria, referencia, agora_iso()),
            )
            inseridas += 1

        conn.commit()
        ANIVERSARIO_NOTIFICACOES_CACHE["chave"] = chave_cache
        ANIVERSARIO_NOTIFICACOES_CACHE["testado_em"] = agora_ts
        if inseridas:
            NOTIFICACOES_CACHE["testado_em"] = 0.0
            NOTIFICACOES_CACHE["resultado"] = None
            HUD_CACHE["testado_em"] = 0.0
            HUD_CACHE["resultado"] = None
        return inseridas
    except Exception as e:
        log_info("AVISO ANIVERSARIOS:", e)
        return 0
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def atualizar_banco():
    conn = conectar()
    c = conn.cursor()

    def senha_usa_bcrypt_local(valor):
        texto = str(valor or "")
        return texto.startswith("$2a$") or texto.startswith("$2b$") or texto.startswith("$2y$")

    def verificar_senha_local(senha_digitada, senha_salva):
        senha_digitada = str(senha_digitada or "")
        senha_salva = str(senha_salva or "")

        if not senha_salva:
            return False

        if senha_usa_bcrypt_local(senha_salva):
            try:
                return bcrypt.checkpw(senha_digitada.encode(), senha_salva.encode())
            except Exception:
                return False

        return senha_digitada == senha_salva

    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "ultimo_hash TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "colunas_ultima_sync TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "campo_servico TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "campo_data TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "excluido_em TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "excluido_por TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "origem TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "guarita TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "pneu TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "cera TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "hidro_lataria TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "hidro_vidros TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "valor_adicional REAL DEFAULT 0")
    adicionar_coluna_se_preciso(c, "servicos", "entrega_prevista TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "criado_por_usuario TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "criado_por_nome TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "etapa_atual TEXT DEFAULT 'LAVAGEM'")
    adicionar_coluna_se_preciso(c, "servicos", "etapa_atual_iniciada_em TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "lavagem_iniciada_em TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "finalizacao_iniciada_em TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "lavagem_segundos INTEGER DEFAULT 0")
    adicionar_coluna_se_preciso(c, "servicos", "finalizacao_segundos INTEGER DEFAULT 0")
    adicionar_coluna_se_preciso(c, "servicos", "operacional_por_usuario TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "operacional_por_nome TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "finalizado_por_usuario TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "finalizado_por_nome TEXT")
    adicionar_coluna_se_preciso(c, "fotos", "usuario TEXT")
    adicionar_coluna_se_preciso(c, "fotos", "usuario_nome TEXT")
    adicionar_coluna_se_preciso(c, "fotos", "tamanho_bytes INTEGER")
    adicionar_coluna_se_preciso(c, "fotos", "largura INTEGER")
    adicionar_coluna_se_preciso(c, "fotos", "altura INTEGER")
    adicionar_coluna_se_preciso(c, "fotos", "arquivo_blob BLOB")
    adicionar_coluna_se_preciso(c, "fotos", "mime_type TEXT")
    adicionar_coluna_se_preciso(c, "fotos", "arquivo_nome TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "nome TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "perfil TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "ativo INTEGER")
    adicionar_coluna_se_preciso(c, "usuarios", "criado_em TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "tentativas_login INTEGER")
    adicionar_coluna_se_preciso(c, "usuarios", "bloqueado_ate TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "ultimo_login_em TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "senha_alteracao_obrigatoria INTEGER")
    adicionar_coluna_se_preciso(c, "usuarios", "senha_atualizada_em TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "foto_perfil TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "foto_perfil_blob BLOB")
    adicionar_coluna_se_preciso(c, "usuarios", "foto_perfil_mime_type TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "foto_perfil_arquivo_nome TEXT")
    adicionar_coluna_se_preciso(c, "usuarios", "hud_config_json TEXT")
    adicionar_coluna_se_preciso(c, "clientes", "placa_principal TEXT")
    adicionar_coluna_se_preciso(c, "clientes", "data_nascimento TEXT")
    adicionar_coluna_se_preciso(c, "clientes", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "veiculos", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "veiculos", "status_atendimento TEXT DEFAULT 'SEM_ATENDIMENTO'")
    adicionar_coluna_se_preciso(c, "veiculos", "atendimento_ativo INTEGER DEFAULT 0")
    adicionar_coluna_se_preciso(c, "veiculos", "ultima_entrada TEXT")
    adicionar_coluna_se_preciso(c, "veiculos", "ultima_entrega TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "servicos", "perfil_cliente_atendimento TEXT DEFAULT 'NOVO'")
    adicionar_coluna_se_preciso(c, "fotos", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "notificacoes", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "notificacoes", "categoria TEXT")
    adicionar_coluna_se_preciso(c, "notificacoes", "referencia TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "versao_sistema TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_ativo INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_api_url TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_local_label TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_latitude REAL")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_longitude REAL")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_timezone TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "clima_timeout_segundos INTEGER DEFAULT 8")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_nome TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_subtitulo TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_url TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_blob BLOB")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_mime_type TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_logo_arquivo_nome TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_url TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_blob BLOB")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_mime_type TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_favicon_arquivo_nome TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_primaria TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_secundaria TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_fundo TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_superficie TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "marca_cor_texto TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "site_titulo TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "site_rodape_texto TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "login_titulo_publico TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "login_subtitulo_publico TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "login_botao_texto TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "home_busca_placeholder TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "home_busca_botao_texto TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "home_estado_inicial_titulo TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "whitelabel_ativo INTEGER DEFAULT 0")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "storage_provider TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "licenca_plano TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "licenca_status TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "onboarding_concluido INTEGER DEFAULT 0")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "paginas_menu_desabilitadas_json TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_ativo INTEGER DEFAULT 0")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_site_url TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_intervalo_horas INTEGER DEFAULT 2")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_telegram_bot_token TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_telegram_bot_nick TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_telegram_chat_id TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_ultimo_status TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_ultimo_relatorio TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_empresa", "auto_teste_ultimo_teste_em TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "frequencia TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "tipo_backup TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "retencao_arquivos INTEGER")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "destino_externo_ativo INTEGER")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "destino_externo_tipo TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "destino_externo_pasta TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "destino_externo_drive_folder_id TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "atualizado_em TEXT")
    adicionar_coluna_se_preciso(c, "manutencao_arquivos", "ultimo_executado_em TEXT")
    adicionar_coluna_se_preciso(c, "manutencao_arquivos", "ultima_mensagem TEXT")
    adicionar_coluna_se_preciso(c, "manutencao_arquivos", "ultimo_resultado_json TEXT")
    adicionar_coluna_se_preciso(c, "integracao_fiscal", "token_api TEXT")
    adicionar_coluna_se_preciso(c, "orcamentos", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "notas_fiscais", "empresa_id INTEGER DEFAULT 1")
    adicionar_coluna_se_preciso(c, "licencas", "codigo_licenca TEXT")
    adicionar_coluna_se_preciso(c, "licencas", "assinatura TEXT")
    adicionar_coluna_se_preciso(c, "licencas", "payload_json TEXT")
    adicionar_coluna_se_preciso(c, "licencas", "emitida_em TEXT")
    adicionar_coluna_se_preciso(c, "licencas", "renovada_em TEXT")
    adicionar_coluna_se_preciso(c, "licencas", "ultimo_status_validacao TEXT")
    c.execute("UPDATE clientes SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE veiculos SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE servicos SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE fotos SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE configuracao_empresa SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE sincronizacoes_clientes SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE notificacoes SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE orcamentos SET empresa_id=1 WHERE empresa_id IS NULL")
    c.execute("UPDATE notas_fiscais SET empresa_id=1 WHERE empresa_id IS NULL")

    c.execute("""
        UPDATE usuarios
        SET nome=COALESCE(NULLIF(nome, ''), usuario)
    """)
    c.execute("""
        UPDATE configuracao_backup
        SET tipo_backup=COALESCE(NULLIF(tipo_backup, ''), 'completo')
    """)
    c.execute("""
        UPDATE configuracao_backup
        SET destino_externo_ativo=COALESCE(destino_externo_ativo, 0)
    """)
    c.execute("""
        UPDATE usuarios
        SET perfil=COALESCE(NULLIF(perfil, ''), CASE WHEN usuario='admin' THEN 'admin' ELSE 'funcionario' END)
    """)
    c.execute("""
        UPDATE usuarios
        SET ativo=COALESCE(ativo, 1)
    """)
    c.execute("""
        UPDATE usuarios
        SET criado_em=COALESCE(criado_em, ?)
    """, (agora().isoformat(timespec="seconds"),))
    c.execute("""
        UPDATE usuarios
        SET tentativas_login=COALESCE(tentativas_login, 0)
    """)
    c.execute("""
        UPDATE usuarios
        SET senha_alteracao_obrigatoria=COALESCE(senha_alteracao_obrigatoria, 0)
    """)
    c.execute("""
        UPDATE usuarios
        SET senha_atualizada_em=COALESCE(senha_atualizada_em, criado_em, ?)
    """, (agora().isoformat(timespec="seconds"),))

    # O backfill de blobs antigos pode consumir muita memoria/tempo em hospedagem.
    # Novos uploads ja salvam os bytes no banco; os antigos so entram nesse fluxo
    # quando o ambiente explicitamente permitir.
    if bool_config_ativo(os.environ.get("BACKFILL_FOTOS_NO_BOOT", "")):
        try:
            c.execute("""
                SELECT id, caminho, arquivo_blob, mime_type, arquivo_nome
                FROM fotos
                WHERE caminho IS NOT NULL AND TRIM(caminho) <> ''
            """)
            for foto in c.fetchall():
                caminho_abs = caminho_absoluto_foto_servico(foto["caminho"])
                if not caminho_abs or not os.path.isfile(caminho_abs):
                    continue

                blob_atual = foto["arquivo_blob"]
                mime_atual = str(foto["mime_type"] or "").strip()
                nome_atual = str(foto["arquivo_nome"] or "").strip()
                if blob_atual and mime_atual and nome_atual:
                    continue

                try:
                    blob = blob_atual or ler_bytes_arquivo(caminho_abs)
                    mime_type = mime_atual or detectar_mime_type_arquivo(caminho_abs)
                    arquivo_nome = nome_atual or os.path.basename(caminho_abs)
                    c.execute(
                        """
                        UPDATE fotos
                        SET arquivo_blob=?,
                            mime_type=?,
                            arquivo_nome=?
                        WHERE id=?
                        """,
                        (blob, mime_type, arquivo_nome, foto["id"]),
                    )
                except Exception:
                    continue
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass

    c.execute("""
        UPDATE clientes
        SET placa_principal=COALESCE(NULLIF(placa_principal, ''), NULL)
    """)

    try:
        c.execute("""
            SELECT cliente_id, placa
            FROM veiculos
            WHERE cliente_id IS NOT NULL
            ORDER BY cliente_id ASC, id DESC
        """)
        clientes_com_placa = {}
        for row in c.fetchall():
            cliente_id = row["cliente_id"]
            if cliente_id and cliente_id not in clientes_com_placa:
                clientes_com_placa[cliente_id] = normalizar_texto_campo(row["placa"]).upper()

        for cliente_id, placa in clientes_com_placa.items():
            if placa:
                c.execute(
                    "UPDATE clientes SET placa_principal=? WHERE id=?",
                    (placa, cliente_id),
                )
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    try:
        c.execute("""
            SELECT id
            FROM veiculos
            ORDER BY id ASC
        """)
        veiculos_ids = [row["id"] for row in c.fetchall()]

        for veiculo_id in veiculos_ids:
            c.execute("""
                SELECT status, entrada, entrega
                FROM servicos
                WHERE veiculo_id=?
                ORDER BY id DESC
                LIMIT 1
            """, (veiculo_id,))
            servico = c.fetchone()

            if servico:
                status = normalizar_texto_campo(servico["status"]).upper() or "SEM_ATENDIMENTO"
                entrada = servico["entrada"]
                entrega = servico["entrega"]
                ativo = 1 if status == "EM ANDAMENTO" else 0
            else:
                status = "SEM_ATENDIMENTO"
                entrada = None
                entrega = None
                ativo = 0

            c.execute("""
                UPDATE veiculos
                SET status_atendimento=?,
                    atendimento_ativo=?,
                    ultima_entrada=?,
                    ultima_entrega=?
                WHERE id=?
            """, (status, ativo, entrada, entrega, veiculo_id))
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass

    c.execute("""
        SELECT id, usuario, senha, senha_alteracao_obrigatoria
        FROM usuarios
    """)
    usuarios = c.fetchall()
    for usuario in usuarios:
        precisa_troca = False
        senha_salva = usuario["senha"]

        if not senha_usa_bcrypt_local(senha_salva):
            precisa_troca = True

        if (
            str(usuario["usuario"] or "").strip().lower() == "admin" and
            verificar_senha_local(SENHA_PADRAO_ADMIN_LEGADA, senha_salva)
        ):
            precisa_troca = True

        if precisa_troca and not int(usuario["senha_alteracao_obrigatoria"] or 0):
            c.execute(
                "UPDATE usuarios SET senha_alteracao_obrigatoria=1 WHERE id=?",
                (usuario["id"],)
            )

    c.execute("""
    CREATE TABLE IF NOT EXISTS historico_lavagens_sync (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sync_id INTEGER NOT NULL,
        placa TEXT NOT NULL,
        cliente TEXT,
        carro TEXT,
        cor TEXT,
        servico TEXT,
        data_lavagem TEXT,
        data_original TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(sync_id) REFERENCES sincronizacoes_clientes(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS retornos_clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        placa TEXT NOT NULL,
        status TEXT DEFAULT 'pendente',
        observacao TEXT,
        proximo_contato_em TEXT,
        ultimo_contato_em TEXT,
        ultima_acao TEXT,
        reagendado_dias INTEGER,
        usuario TEXT,
        usuario_nome TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS configuracao_empresa (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        razao_social TEXT,
        nome_fantasia TEXT,
        cnpj TEXT,
        inscricao_municipal TEXT,
        inscricao_estadual TEXT,
        regime_tributario TEXT,
        email TEXT,
        telefone TEXT,
        endereco TEXT,
        numero TEXT,
        complemento TEXT,
        bairro TEXT,
        cidade TEXT,
        uf TEXT,
        cep TEXT,
        codigo_servico_padrao TEXT,
        aliquota_padrao REAL DEFAULT 0,
        versao_sistema TEXT,
        clima_ativo INTEGER DEFAULT 1,
        clima_api_url TEXT,
        clima_local_label TEXT,
        clima_latitude REAL,
        clima_longitude REAL,
        clima_timezone TEXT,
        clima_timeout_segundos INTEGER DEFAULT 8,
        atualizado_em TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS orcamentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER DEFAULT 1,
        numero INTEGER UNIQUE,
        cliente_nome TEXT NOT NULL,
        cliente_documento TEXT,
        email TEXT,
        telefone TEXT,
        placa TEXT,
        modelo TEXT,
        validade_dias INTEGER DEFAULT 7,
        forma_pagamento TEXT,
        observacoes TEXT,
        subtotal REAL DEFAULT 0,
        desconto REAL DEFAULT 0,
        total REAL DEFAULT 0,
        status TEXT DEFAULT 'GERADO',
        empresa_snapshot TEXT,
        criado_em TEXT,
        atualizado_em TEXT,
        usuario TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS orcamento_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        orcamento_id INTEGER NOT NULL,
        descricao TEXT NOT NULL,
        quantidade REAL DEFAULT 1,
        valor_unitario REAL DEFAULT 0,
        valor_total REAL DEFAULT 0,
        ordem INTEGER DEFAULT 0,
        FOREIGN KEY(orcamento_id) REFERENCES orcamentos(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS notas_fiscais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER DEFAULT 1,
        rps_numero INTEGER UNIQUE,
        numero_nota TEXT,
        serie TEXT,
        ambiente TEXT,
        tipo_documento TEXT DEFAULT 'NFS-e',
        status TEXT DEFAULT 'RASCUNHO',
        cliente_nome TEXT NOT NULL,
        cliente_documento TEXT,
        email TEXT,
        telefone TEXT,
        placa TEXT,
        modelo TEXT,
        endereco TEXT,
        numero_endereco TEXT,
        complemento TEXT,
        bairro TEXT,
        cidade TEXT,
        uf TEXT,
        cep TEXT,
        codigo_servico TEXT,
        discriminacao TEXT,
        observacoes TEXT,
        aliquota_iss REAL DEFAULT 0,
        valor_servicos REAL DEFAULT 0,
        desconto REAL DEFAULT 0,
        valor_iss REAL DEFAULT 0,
        valor_total REAL DEFAULT 0,
        empresa_snapshot TEXT,
        criado_em TEXT,
        atualizado_em TEXT,
        usuario TEXT,
        origem_orcamento_id INTEGER,
        FOREIGN KEY(origem_orcamento_id) REFERENCES orcamentos(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS nota_fiscal_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nota_fiscal_id INTEGER NOT NULL,
        descricao TEXT NOT NULL,
        quantidade REAL DEFAULT 1,
        valor_unitario REAL DEFAULT 0,
        valor_total REAL DEFAULT 0,
        ordem INTEGER DEFAULT 0,
        FOREIGN KEY(nota_fiscal_id) REFERENCES notas_fiscais(id)
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_historico_sync_placa ON historico_lavagens_sync(placa)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_historico_sync_data ON historico_lavagens_sync(data_lavagem)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_retornos_clientes_placa ON retornos_clientes(placa)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_retornos_clientes_status ON retornos_clientes(status, proximo_contato_em)")

    conn.commit()
    try:
        reparar_registros_foto_perfil()
    except Exception:
        pass
    try:
        sincronizar_blobs_foto_perfil()
    except Exception:
        pass
    conn.close()

def init_db():
    try:
        garantir_schema_sqlite_local_minima(force=True)
    except Exception as e:
        log_info("AVISO SCHEMA SQLITE LOCAL:", e)

    if modo_banco_preferido() == "postgres":
        status_boot = diagnosticar_banco_online(force=True)
        if not status_boot.get("conectado"):
            if banco_online_estritamente_obrigatorio():
                raise BancoOnlineObrigatorioErro(
                    "Banco online obrigatorio no boot, mas a conexao nao foi estabelecida. "
                    f"Detalhe: {status_boot.get('mensagem') or 'sem detalhes'}"
                )
            log_info(
                "AVISO: banco online indisponivel no boot. "
                "O sistema vai iniciar em modo local e sincronizar quando a rede voltar."
            )
    criar_todas_tabelas()
    atualizar_banco()
    criar_itens_checklist_padrao()
    criar_admin()
    aplicar_migracoes_fundacao_produto()


def aplicar_migracoes_fundacao_produto():
    conn = conectar()
    try:
        run_product_foundation_migrations(
            conn,
            adicionar_coluna_se_preciso,
            agora_iso,
            print_func=print,
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass

def criar_todas_tabelas():
    conn = conectar()
    c = conn.cursor()

    # ðŸ”” NOTIFICAÃ‡Ã•ES
    c.execute("""
    CREATE TABLE IF NOT EXISTS notificacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER DEFAULT 1,
        mensagem TEXT,
        tipo TEXT,
        categoria TEXT,
        referencia TEXT,
        lida INTEGER DEFAULT 0,
        criada_em TEXT
    )
    """)

    # ðŸ”„ SINCRONIZAÃ‡Ã•ES
    c.execute("""
    CREATE TABLE IF NOT EXISTS sincronizacoes_clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        url TEXT NOT NULL,
        intervalo_minutos INTEGER NOT NULL DEFAULT 60,
        campo_placa TEXT NOT NULL,
        campo_nome TEXT,
        campo_telefone TEXT,
        campo_modelo TEXT,
        campo_cor TEXT,
        campo_servico TEXT,
        campo_data TEXT,
        ativo INTEGER NOT NULL DEFAULT 1,
        ultimo_sync_em TEXT,
        proximo_sync_em TEXT,
        ultimo_status TEXT,
        ultima_mensagem TEXT,
        ultimo_hash TEXT,
        colunas_ultima_sync TEXT,
        excluido_em TEXT,
        excluido_por TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # ðŸ” USUÃRIOS
    c.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT UNIQUE,
        senha TEXT,
        nome TEXT,
        perfil TEXT,
        ativo INTEGER DEFAULT 1,
        criado_em TEXT,
        tentativas_login INTEGER DEFAULT 0,
        bloqueado_ate TEXT,
        ultimo_login_em TEXT,
        senha_alteracao_obrigatoria INTEGER DEFAULT 0,
        senha_atualizada_em TEXT,
        foto_perfil TEXT,
        foto_perfil_blob BLOB,
        foto_perfil_mime_type TEXT,
        foto_perfil_arquivo_nome TEXT,
        hud_config_json TEXT
    )
    """)

    # ðŸ‘¤ CLIENTES
    c.execute("""
    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        telefone TEXT,
        placa_principal TEXT,
        data_nascimento TEXT
    )
    """)

    # ðŸš— VEÃCULOS
    c.execute("""
    CREATE TABLE IF NOT EXISTS veiculos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        placa TEXT NOT NULL,
        modelo TEXT,
        cor TEXT,
        cliente_id INTEGER,
        status_atendimento TEXT DEFAULT 'SEM_ATENDIMENTO',
        atendimento_ativo INTEGER DEFAULT 0,
        ultima_entrada TEXT,
        ultima_entrega TEXT,
        FOREIGN KEY(cliente_id) REFERENCES clientes(id)
    )
    """)

    # ðŸ”” NOTIFICAÃ‡Ã•ES
    c.execute("""
    CREATE TABLE IF NOT EXISTS notificacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER DEFAULT 1,
        mensagem TEXT,
        tipo TEXT,
        categoria TEXT,
        referencia TEXT,
        lida INTEGER DEFAULT 0,
        criada_em TEXT
    )
    """)

    # ðŸ”„ SINCRONIZAÃ‡Ã•ES
    c.execute("""
    CREATE TABLE IF NOT EXISTS sincronizacoes_clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        url TEXT NOT NULL,
        intervalo_minutos INTEGER NOT NULL DEFAULT 60,
        campo_placa TEXT NOT NULL,
        campo_nome TEXT,
        campo_telefone TEXT,
        campo_modelo TEXT,
        campo_cor TEXT,
        campo_servico TEXT,
        campo_data TEXT,
        ativo INTEGER NOT NULL DEFAULT 1,
        ultimo_sync_em TEXT,
        proximo_sync_em TEXT,
        ultimo_status TEXT,
        ultima_mensagem TEXT,
        ultimo_hash TEXT,
        colunas_ultima_sync TEXT,
        excluido_em TEXT,
        excluido_por TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # ðŸ”¥ TIPOS DE SERVIÃ‡O
    c.execute("""
    CREATE TABLE IF NOT EXISTS tipos_servico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        valor REAL
    )
    """)

    # ðŸ”¥ SERVIÃ‡OS
    c.execute("""
    CREATE TABLE IF NOT EXISTS servicos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        veiculo_id INTEGER,
        tipo_id INTEGER,
        valor REAL,
        entrada TEXT,
        entrega_prevista TEXT,
        entrega TEXT,
        status TEXT,
        prioridade INTEGER DEFAULT 0,
        observacoes TEXT,
        origem TEXT,
        guarita TEXT,
        pneu TEXT,
        cera TEXT,
        hidro_lataria TEXT,
        hidro_vidros TEXT,
        valor_adicional REAL DEFAULT 0,
        criado_por_usuario TEXT,
        criado_por_nome TEXT,
        perfil_cliente_atendimento TEXT DEFAULT 'NOVO',
        etapa_atual TEXT DEFAULT 'LAVAGEM',
        etapa_atual_iniciada_em TEXT,
        lavagem_iniciada_em TEXT,
        finalizacao_iniciada_em TEXT,
        lavagem_segundos INTEGER DEFAULT 0,
        finalizacao_segundos INTEGER DEFAULT 0,
        operacional_por_usuario TEXT,
        operacional_por_nome TEXT,
        finalizado_por_usuario TEXT,
        finalizado_por_nome TEXT,
        FOREIGN KEY(veiculo_id) REFERENCES veiculos(id),
        FOREIGN KEY(tipo_id) REFERENCES tipos_servico(id)
    )
    """)

    # ðŸ”¥ ADICIONAIS
    c.execute("""
    CREATE TABLE IF NOT EXISTS adicionais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT
    )
    """)

    # ðŸ”¥ RELAÃ‡ÃƒO SERVIÃ‡O â†” ADICIONAIS
    c.execute("""
    CREATE TABLE IF NOT EXISTS servico_adicionais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        servico_id INTEGER,
        adicional_id INTEGER,
        FOREIGN KEY(servico_id) REFERENCES servicos(id),
        FOREIGN KEY(adicional_id) REFERENCES adicionais(id)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS servico_cobrancas_extras (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        servico_id INTEGER NOT NULL,
        descricao TEXT NOT NULL,
        valor REAL NOT NULL DEFAULT 0,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        criado_por_usuario TEXT,
        criado_por_nome TEXT,
        FOREIGN KEY(servico_id) REFERENCES servicos(id)
    )
    """)

    # ðŸ”¥ FOTOS (melhorado)
    c.execute("""
    CREATE TABLE IF NOT EXISTS fotos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        servico_id INTEGER,
        tipo TEXT,
        caminho TEXT,
        usuario TEXT,
        usuario_nome TEXT,
        tamanho_bytes INTEGER,
        largura INTEGER,
        altura INTEGER,
        arquivo_blob BLOB,
        mime_type TEXT,
        arquivo_nome TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(servico_id) REFERENCES servicos(id)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS produtos_pneu (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nome TEXT
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS checklist_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        ativo INTEGER NOT NULL DEFAULT 1,
        ordem INTEGER NOT NULL DEFAULT 0,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS servico_checklist (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        servico_id INTEGER NOT NULL,
        item_id INTEGER,
        item_nome TEXT NOT NULL,
        marcado INTEGER NOT NULL DEFAULT 1,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(servico_id) REFERENCES servicos(id),
        FOREIGN KEY(item_id) REFERENCES checklist_itens(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS auditoria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario_id INTEGER,
        usuario TEXT,
        usuario_nome TEXT,
        acao TEXT NOT NULL,
        entidade TEXT NOT NULL,
        entidade_id INTEGER,
        placa TEXT,
        detalhes_json TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS historico_lavagens_sync (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sync_id INTEGER NOT NULL,
        placa TEXT NOT NULL,
        cliente TEXT,
        carro TEXT,
        cor TEXT,
        servico TEXT,
        data_lavagem TEXT,
        data_original TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(sync_id) REFERENCES sincronizacoes_clientes(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS retornos_clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        placa TEXT NOT NULL,
        status TEXT DEFAULT 'pendente',
        observacao TEXT,
        proximo_contato_em TEXT,
        ultimo_contato_em TEXT,
        ultima_acao TEXT,
        reagendado_dias INTEGER,
        usuario TEXT,
        usuario_nome TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # âš¡ ÃNDICES (performance)
    c.execute("""
    CREATE TABLE IF NOT EXISTS configuracao_empresa (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        razao_social TEXT,
        nome_fantasia TEXT,
        cnpj TEXT,
        inscricao_municipal TEXT,
        inscricao_estadual TEXT,
        regime_tributario TEXT,
        email TEXT,
        telefone TEXT,
        endereco TEXT,
        numero TEXT,
        complemento TEXT,
        bairro TEXT,
        cidade TEXT,
        uf TEXT,
        cep TEXT,
        codigo_servico_padrao TEXT,
        aliquota_padrao REAL DEFAULT 0,
        versao_sistema TEXT,
        clima_ativo INTEGER DEFAULT 1,
        clima_api_url TEXT,
        clima_local_label TEXT,
        clima_latitude REAL,
        clima_longitude REAL,
        clima_timezone TEXT,
        clima_timeout_segundos INTEGER DEFAULT 8,
        atualizado_em TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS orcamentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER DEFAULT 1,
        numero INTEGER UNIQUE,
        cliente_nome TEXT NOT NULL,
        cliente_documento TEXT,
        email TEXT,
        telefone TEXT,
        placa TEXT,
        modelo TEXT,
        validade_dias INTEGER DEFAULT 7,
        forma_pagamento TEXT,
        observacoes TEXT,
        subtotal REAL DEFAULT 0,
        desconto REAL DEFAULT 0,
        total REAL DEFAULT 0,
        status TEXT DEFAULT 'GERADO',
        empresa_snapshot TEXT,
        criado_em TEXT,
        atualizado_em TEXT,
        usuario TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS orcamento_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        orcamento_id INTEGER NOT NULL,
        descricao TEXT NOT NULL,
        quantidade REAL DEFAULT 1,
        valor_unitario REAL DEFAULT 0,
        valor_total REAL DEFAULT 0,
        ordem INTEGER DEFAULT 0,
        FOREIGN KEY(orcamento_id) REFERENCES orcamentos(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS notas_fiscais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER DEFAULT 1,
        rps_numero INTEGER UNIQUE,
        numero_nota TEXT,
        serie TEXT,
        ambiente TEXT,
        tipo_documento TEXT DEFAULT 'NFS-e',
        status TEXT DEFAULT 'RASCUNHO',
        cliente_nome TEXT NOT NULL,
        cliente_documento TEXT,
        email TEXT,
        telefone TEXT,
        placa TEXT,
        modelo TEXT,
        endereco TEXT,
        numero_endereco TEXT,
        complemento TEXT,
        bairro TEXT,
        cidade TEXT,
        uf TEXT,
        cep TEXT,
        codigo_servico TEXT,
        discriminacao TEXT,
        observacoes TEXT,
        aliquota_iss REAL DEFAULT 0,
        valor_servicos REAL DEFAULT 0,
        desconto REAL DEFAULT 0,
        valor_iss REAL DEFAULT 0,
        valor_total REAL DEFAULT 0,
        empresa_snapshot TEXT,
        criado_em TEXT,
        atualizado_em TEXT,
        usuario TEXT,
        origem_orcamento_id INTEGER,
        FOREIGN KEY(origem_orcamento_id) REFERENCES orcamentos(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS nota_fiscal_itens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nota_fiscal_id INTEGER NOT NULL,
        descricao TEXT NOT NULL,
        quantidade REAL DEFAULT 1,
        valor_unitario REAL DEFAULT 0,
        valor_total REAL DEFAULT 0,
        ordem INTEGER DEFAULT 0,
        FOREIGN KEY(nota_fiscal_id) REFERENCES notas_fiscais(id)
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS integracao_fiscal (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER NOT NULL DEFAULT 1,
        tipo_integracao TEXT DEFAULT 'manual',
        provedor_nome TEXT,
        ambiente TEXT DEFAULT 'homologacao',
        municipio_codigo_ibge TEXT,
        municipio_nome TEXT,
        uf TEXT,
        endpoint_emissao TEXT,
        endpoint_consulta TEXT,
        endpoint_cancelamento TEXT,
        autenticacao_tipo TEXT DEFAULT 'nenhuma',
        usuario_api TEXT,
        senha_api TEXT,
        client_id TEXT,
        client_secret TEXT,
        token_api TEXT,
        token_url TEXT,
        certificado_tipo TEXT DEFAULT 'nenhum',
        certificado_arquivo TEXT,
        certificado_senha TEXT,
        serie_rps TEXT,
        serie_nfe TEXT,
        ativo INTEGER DEFAULT 0,
        ultimo_status TEXT,
        ultima_mensagem TEXT,
        atualizado_em TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS configuracao_backup (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER NOT NULL DEFAULT 1,
        frequencia TEXT DEFAULT 'diario',
        tipo_backup TEXT DEFAULT 'completo',
        retencao_arquivos INTEGER DEFAULT 15,
        destino_externo_ativo INTEGER DEFAULT 0,
        destino_externo_tipo TEXT DEFAULT 'pasta',
        destino_externo_pasta TEXT,
        destino_externo_drive_folder_id TEXT,
        atualizado_em TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS manutencao_arquivos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        empresa_id INTEGER NOT NULL DEFAULT 1,
        ultimo_executado_em TEXT,
        ultima_mensagem TEXT,
        ultimo_resultado_json TEXT
    )
    """)

    # Salva as tabelas antes dos indices para que um timeout no Postgres
    # nao reverta toda a inicializacao do schema.
    conn.commit()

    criar_indices_no_boot = (
        getattr(conn, "backend", "") != "postgres"
        or bool_config_ativo(os.environ.get("CRIAR_INDICES_NO_BOOT", ""))
    )

    if not criar_indices_no_boot:
        garantir_atualizado_em_sync(c)
        conn.commit()
        conn.close()
        return

    def criar_indice_seguro(sql_indice):
        try:
            if getattr(conn, "backend", "") == "postgres":
                try:
                    c.execute("SET statement_timeout TO 0")
                except Exception:
                    pass
            c.execute(sql_indice)
            conn.commit()
            return True
        except Exception as e:
            mensagem = str(e or "").lower()
            if "statement timeout" in mensagem or "querycanceled" in mensagem:
                log_info(f"AVISO INDICE: {sql_indice} pulado por timeout do banco online.")
                try:
                    conn.rollback()
                except Exception:
                    pass
                return False
            raise

    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_servico_status ON servicos(status)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_servico_entrada ON servicos(entrada)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_veiculo_placa ON veiculos(placa)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_servico_checklist_servico ON servico_checklist(servico_id)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_servico_cobrancas_extras_servico ON servico_cobrancas_extras(servico_id)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_fotos_servico_tipo ON fotos(servico_id, tipo)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_auditoria_entidade ON auditoria(entidade, entidade_id)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_auditoria_criado_em ON auditoria(criado_em)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_historico_sync_placa ON historico_lavagens_sync(placa)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_historico_sync_data ON historico_lavagens_sync(data_lavagem)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_retornos_clientes_placa ON retornos_clientes(placa)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_retornos_clientes_status ON retornos_clientes(status, proximo_contato_em)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_orcamentos_numero ON orcamentos(numero)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_orcamentos_criado_em ON orcamentos(criado_em)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_orcamento_itens_orcamento ON orcamento_itens(orcamento_id)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_notas_fiscais_rps ON notas_fiscais(rps_numero)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_notas_fiscais_criado_em ON notas_fiscais(criado_em)")
    criar_indice_seguro("CREATE INDEX IF NOT EXISTS idx_nota_fiscal_itens_nota ON nota_fiscal_itens(nota_fiscal_id)")
    criar_indice_seguro(
        """
        CREATE INDEX IF NOT EXISTS idx_sync_clientes_proximo
        ON sincronizacoes_clientes(ativo, proximo_sync_em)
        """
    )

    garantir_atualizado_em_sync(c)

    conn.commit()
    conn.close()

def agora_iso():
    return agora().isoformat(timespec="seconds")

def somar_minutos_iso(minutos):
    return (agora() + timedelta(minutes=minutos)).isoformat(timespec="seconds")

def formatar_datahora(valor_iso):
    if not valor_iso:
        return "Nunca"

    try:
        return datetime.fromisoformat(valor_iso).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return valor_iso

def definir_feedback_clientes(tipo, mensagem):
    session["clientes_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_base_dados(tipo, mensagem):
    session["base_dados_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_index(tipo, mensagem):
    session["index_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_painel(tipo, mensagem):
    session["painel_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_historico(tipo, mensagem):
    session["historico_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_itens(tipo, mensagem):
    session["itens_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_por_destino(destino, tipo, mensagem):
    destino = normalizar_texto_campo(destino)
    if destino.startswith("/painel"):
        definir_feedback_painel(tipo, mensagem)
        return
    if destino.startswith("/clientes"):
        definir_feedback_clientes(tipo, mensagem)
        return
    if destino.startswith("/?") or destino == "/":
        definir_feedback_index(tipo, mensagem)
        return
    definir_feedback_historico(tipo, mensagem)

def definir_feedback_checklist(tipo, mensagem):
    session["checklist_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_configuracoes(tipo, mensagem):
    session["configuracoes_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_orcamento(tipo, mensagem):
    session["orcamento_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_nota_fiscal(tipo, mensagem):
    session["nota_fiscal_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def definir_feedback_retornos(tipo, mensagem):
    session["retornos_feedback"] = {"tipo": tipo, "mensagem": mensagem}

def montar_mensagem_publica_cadastro_veiculo(resultado):
    placa = normalizar_texto_campo((resultado or {}).get("placa")).upper()
    return f"Cadastro da placa {placa or '-'} salvo com sucesso."

def registrar_alertas_espelho_planilha_cadastro(resultado):
    espelho = (resultado or {}).get("espelho_planilha") or {}
    falhas = espelho.get("falhas") or []
    if not falhas:
        return

    placa = normalizar_texto_campo((resultado or {}).get("placa")).upper() or "-"
    primeira_falha = falhas[0] or {}
    log_info(
        "AVISO ESPELHO PLANILHA CADASTRO:",
        {
            "placa": placa,
            "total_falhas": len(falhas),
            "primeira_falha": primeira_falha,
        },
    )

def limpar_preview_sincronizacao():
    session.pop("clientes_sync_preview", None)

def limpar_feedback_base_dados():
    session.pop("base_dados_feedback", None)

def senha_hash_bcrypt(valor):
    return bcrypt.hashpw(str(valor or "").encode(), bcrypt.gensalt()).decode()

def senha_usa_bcrypt(valor):
    texto = str(valor or "")
    return texto.startswith("$2a$") or texto.startswith("$2b$") or texto.startswith("$2y$")

def verificar_senha_usuario(senha_digitada, senha_salva):
    senha_digitada = str(senha_digitada or "")
    senha_salva = str(senha_salva or "")

    if not senha_salva:
        return False

    if senha_usa_bcrypt(senha_salva):
        try:
            return bcrypt.checkpw(senha_digitada.encode(), senha_salva.encode())
        except Exception:
            return False

    return senha_digitada == senha_salva


def buscar_usuario_para_alteracao_senha(cursor, usuario_id=None, usuario_login=""):
    if usuario_id:
        cursor.execute("SELECT * FROM usuarios WHERE id=?", (usuario_id,))
        usuario = cursor.fetchone()
        if usuario:
            return usuario

    usuario_login = normalizar_texto_campo(usuario_login)
    if usuario_login:
        cursor.execute("SELECT * FROM usuarios WHERE usuario=?", (usuario_login,))
        return cursor.fetchone()

    return None


def gerar_senha_temporaria_segura(comprimento=18):
    alfabeto = string.ascii_letters + string.digits + "!@#$%*+-_?"
    return "".join(secrets.choice(alfabeto) for _ in range(max(14, int(comprimento or 18))))

def validar_forca_senha(senha, usuario=""):
    senha = str(senha or "")
    usuario = str(usuario or "").strip().lower()

    if len(senha) < SENHA_MINIMO_CARACTERES:
        return f"A senha precisa ter pelo menos {SENHA_MINIMO_CARACTERES} caracteres."
    if usuario and usuario in senha.lower():
        return "A senha nao pode conter o proprio login."
    if not re.search(r"[a-z]", senha):
        return "A senha precisa ter pelo menos uma letra minuscula."
    if not re.search(r"[A-Z]", senha):
        return "A senha precisa ter pelo menos uma letra maiuscula."
    if not re.search(r"\d", senha):
        return "A senha precisa ter pelo menos um numero."
    if not re.search(r"[^A-Za-z0-9]", senha):
        return "A senha precisa ter pelo menos um simbolo."
    if senha == SENHA_PADRAO_ADMIN_LEGADA:
        return "Essa senha nao pode ser usada."
    return None

def senha_padrao_admin_ativa(usuario_row):
    if not usuario_row:
        return False
    if str(usuario_row["usuario"] or "").strip().lower() != "admin":
        return False
    return verificar_senha_usuario(SENHA_PADRAO_ADMIN_LEGADA, usuario_row["senha"])

def usuario_precisa_trocar_senha(usuario_row):
    if not usuario_row:
        return False

    try:
        obrigatoria = bool(int(usuario_row["senha_alteracao_obrigatoria"] or 0))
    except Exception:
        obrigatoria = False

    senha_salva = str(usuario_row["senha"] or "")
    if not senha_usa_bcrypt(senha_salva):
        return True

    if senha_padrao_admin_ativa(usuario_row):
        return True

    return obrigatoria

def usuario_bloqueado_ate(usuario_row):
    if not usuario_row:
        return None

    bloqueado_ate = interpretar_datahora_sistema(usuario_row["bloqueado_ate"])
    if not bloqueado_ate:
        return None

    return bloqueado_ate if bloqueado_ate > agora() else None

def registrar_falha_login(c, usuario_row):
    tentativas = int(usuario_row["tentativas_login"] or 0) + 1
    bloqueado_ate = None

    if tentativas >= MAX_TENTATIVAS_LOGIN:
        bloqueado_ate = (agora() + timedelta(minutes=MINUTOS_BLOQUEIO_LOGIN)).isoformat(timespec="seconds")
        tentativas = 0

    c.execute(
        "UPDATE usuarios SET tentativas_login=?, bloqueado_ate=? WHERE id=?",
        (tentativas, bloqueado_ate, usuario_row["id"])
    )
    return bloqueado_ate

def limpar_status_login_usuario(c, usuario_id, registrar_login=False):
    atualizado_em = agora_iso() if registrar_login else None
    c.execute(
        """
        UPDATE usuarios
        SET tentativas_login=0,
            bloqueado_ate=?,
            ultimo_login_em=COALESCE(?, ultimo_login_em)
        WHERE id=?
        """,
        (None, atualizado_em, usuario_id)
    )

def preencher_sessao_usuario(usuario_row, limpar=True):
    if limpar:
        session.clear()
    perfil_usuario = normalizar_perfil_usuario(
        usuario_row["perfil"] or (
            "admin" if usuario_row["usuario"] == "admin" else "funcionario"
        )
    )
    session["usuario"] = usuario_row["usuario"]
    session["usuario_id"] = usuario_row["id"]
    session["usuario_nome"] = (usuario_row["nome"] or usuario_row["usuario"])
    session["usuario_iniciais"] = obter_iniciais_usuario(
        usuario_row["nome"],
        usuario_row["usuario"],
    )
    session["usuario_foto"] = str(usuario_row["foto_perfil"] or "").strip()
    session["usuario_foto_url"] = url_foto_usuario(
        usuario_row["foto_perfil"],
        usuario_row["id"],
    )
    try:
        session["usuario_hud_config_json"] = str(usuario_row["hud_config_json"] or "").strip()
    except Exception:
        session["usuario_hud_config_json"] = ""
    empresa_sessao_atual = session.get("empresa_id") if not limpar else None
    if empresa_sessao_atual and perfil_usuario in {"admin", "desenvolvedor"}:
        try:
            session["empresa_id"] = int(empresa_sessao_atual or 1)
        except Exception:
            session["empresa_id"] = 1
    else:
        try:
            session["empresa_id"] = int(usuario_row["empresa_id"] or 1)
        except Exception:
            session["empresa_id"] = 1
    session["usuario_perfil"] = perfil_usuario
    session["senha_alteracao_obrigatoria"] = usuario_precisa_trocar_senha(usuario_row)
    session["usuario_sync_em"] = time.time()
    session.permanent = True

def sincronizar_sessao_usuario(force=False):
    if not session.get("usuario"):
        return

    if (
        not force and
        session.get("usuario_id") and
        session.get("usuario_nome") and
        session.get("usuario_perfil") and
        "usuario_iniciais" in session and
        "usuario_foto_url" in session and
        "senha_alteracao_obrigatoria" in session and
        not str(session.get("usuario_foto_url") or "").startswith("/static/uploads/perfis/") and
        float(session.get("usuario_sync_em") or 0.0) > 0 and
        (time.time() - float(session.get("usuario_sync_em") or 0.0) < USUARIO_SESSAO_SYNC_TTL)
    ):
        return

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM usuarios WHERE usuario=?", (session.get("usuario"),))
    usuario = c.fetchone()
    conn.close()

    if usuario:
        preencher_sessao_usuario(usuario, limpar=False)


def sincronizar_sessao_usuario_seguro(force=False, contexto="sessao"):
    try:
        sincronizar_sessao_usuario(force=force)
        return True
    except Exception as erro:
        log_info(f"AVISO SINCRONIZACAO USUARIO {contexto}:", erro)
        return False

def usuario_admin():
    return (
        session.get("usuario_perfil") == "admin" or
        session.get("usuario") == "admin"
    )

def normalizar_perfil_usuario(valor):
    perfil = str(valor or "").strip().lower()
    if perfil == "desenvolvedor":
        return "desenvolvedor"
    if perfil == "admin":
        return "admin"
    return "funcionario"

def rotulo_perfil_usuario(valor):
    perfil = normalizar_perfil_usuario(valor)
    if perfil == "desenvolvedor":
        return "Desenvolvedor"
    if perfil == "admin":
        return "Administrador"
    return "Funcionario"

def usuario_desenvolvedor():
    return normalizar_perfil_usuario(session.get("usuario_perfil")) == "desenvolvedor"

def perfil_auto_suporte():
    if usuario_desenvolvedor():
        return "desenvolvedor"
    if usuario_admin():
        return "administrador"
    return ""

def usuario_auto_suporte_tecnico():
    return perfil_auto_suporte() == "desenvolvedor"

def usuario_gerencia_acessos():
    return usuario_admin() or usuario_desenvolvedor()

def usuario_gerencia_banco_online():
    return usuario_admin() or usuario_desenvolvedor()

def usuario_gerencia_configuracao_sistema():
    return usuario_admin() or usuario_desenvolvedor()


def usuario_gerencia_empresas():
    return usuario_desenvolvedor()


def normalizar_slug_empresa(valor, fallback="empresa"):
    texto = normalizar_texto_comparacao(valor or fallback)
    texto = re.sub(r"[^a-z0-9]+", "-", texto).strip("-")
    return texto or "empresa"


def carregar_contexto_licenca_empresa(empresa_id=None):
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())
    mes_prefixo = agora().strftime("%Y-%m")
    conn = conectar()
    c = conn.cursor()
    licenca = garantir_licenca_domain(c, empresa_id, agora_iso())
    uso = obter_uso_licenca_domain(c, empresa_id, mes_prefixo)
    conn.commit()
    conn.close()
    return montar_contexto_licenca_domain(licenca, uso, hoje=agora().date())


def segredo_assinatura_licenca():
    segredo = os.getenv("LICENSE_SIGNING_SECRET")
    if segredo:
        return segredo
    return os.getenv("SECRET_KEY") or os.getenv("FLASK_SECRET_KEY") or app.secret_key or ""


def recurso_liberado_por_plano(recurso, empresa_id=None):
    licenca = carregar_contexto_licenca_empresa_seguro(empresa_id)
    recursos = (PLANOS_LICENCA.get(licenca.get("codigo_plano")) or {}).get("recursos") or {}
    return bool(recursos.get(recurso))


def bloquear_recurso_plano(recurso, mensagem, feedback_func=definir_feedback_configuracoes, destino="/configuracoes"):
    if recurso_liberado_por_plano(recurso):
        return None
    feedback_func("erro", mensagem)
    return redirect(destino)


PAGINAS_MENU_CONFIGURAVEIS = [
    {
        "id": "painel",
        "grupo": "Operacao",
        "titulo": "Painel",
        "descricao": "Quadro operacional dos atendimentos em andamento.",
        "endpoints": {"painel", "servico", "editar_servico", "editar_servico_inline", "excluir_servico", "atualizar_status_servico_legado", "salvar_operacional_painel", "trocar_etapa_servico_painel", "adicionar_cobranca_extra_painel", "checklist_servico", "finalizar", "detalhe", "prioridade"},
    },
    {
        "id": "clientes",
        "grupo": "Operacao",
        "titulo": "Clientes",
        "descricao": "Cadastro, importacao e sincronizacao de clientes e veiculos.",
        "endpoints": {"clientes", "importar_local", "preview_sincronizacao_clientes", "cancelar_preview_sincronizacao_clientes", "adicionar_sincronizacao_clientes", "salvar_sincronizacao_clientes", "executar_sync_clientes", "alternar_sync_clientes", "excluir_sync_clientes", "cadastrar", "editar_cliente", "buscar_cliente_api"},
    },
    {
        "id": "historico",
        "grupo": "Operacao",
        "titulo": "Historico",
        "descricao": "Historico de servicos finalizados, fotos, reabertura e exclusao.",
        "endpoints": {"pagina_historico", "editar_atendimento_historico", "enviar_fotos_historico", "excluir_foto_historico", "reabrir_atendimento_historico", "excluir_atendimento_historico"},
    },
    {
        "id": "retornos",
        "grupo": "Operacao",
        "titulo": "Retornos",
        "descricao": "Agenda e acompanhamento de retornos dos clientes.",
        "endpoints": {"pagina_retornos", "atualizar_retorno"},
    },
    {
        "id": "servicos",
        "grupo": "Operacao",
        "titulo": "Servicos",
        "descricao": "Cadastro dos tipos de servico usados nos atendimentos.",
        "endpoints": {"cadastrar_servico"},
    },
    {
        "id": "checklist",
        "grupo": "Operacao",
        "titulo": "Itens Checklist",
        "descricao": "Itens do checklist de finalizacao dos servicos.",
        "endpoints": {"itens_checklist", "alternar_item_checklist", "excluir_item_checklist"},
    },
    {
        "id": "pneus",
        "grupo": "Operacao",
        "titulo": "Pneus",
        "descricao": "Cadastro de medidas e opcoes de pneus.",
        "endpoints": {"cadastrar_pneu"},
    },
    {
        "id": "relatorios",
        "grupo": "Gestao",
        "titulo": "Relatorios",
        "descricao": "Financeiro, ranking, indicadores e exportacao CSV.",
        "endpoints": {"financeiro", "exportar_relatorios_csv"},
    },
    {
        "id": "orcamentos",
        "grupo": "Gestao",
        "titulo": "Orcamentos",
        "descricao": "Criacao, consulta e PDF de orcamentos.",
        "endpoints": {"pagina_orcamento", "gerar_orcamento", "baixar_orcamento_pdf"},
    },
    {
        "id": "nota_fiscal",
        "grupo": "Gestao",
        "titulo": "Emissao de nota fiscal",
        "descricao": "Dados fiscais, integracao, emissao e registro de notas.",
        "endpoints": {"pagina_nota_fiscal", "salvar_emitente_nota_fiscal", "salvar_integracao_nota_fiscal", "gerar_nota_fiscal", "baixar_nota_fiscal_pdf", "registrar_emissao_nota_fiscal"},
    },
    {
        "id": "clima",
        "grupo": "Apoio / Administracao",
        "titulo": "Clima",
        "descricao": "Tela de clima e recomendacao para operacao.",
        "endpoints": {"clima"},
    },
    {
        "id": "auditoria",
        "grupo": "Apoio / Administracao",
        "titulo": "Auditoria",
        "descricao": "Consulta de logs e rastreio de alteracoes.",
        "endpoints": {"pagina_auditoria"},
    },
    {
        "id": "changelog",
        "grupo": "Apoio / Administracao",
        "titulo": "Changelog",
        "descricao": "Historico de versoes e mudancas publicadas.",
        "endpoints": {"pagina_changelog"},
    },
    {
        "id": "empresas",
        "grupo": "Apoio / Administracao",
        "titulo": "Empresas",
        "descricao": "Cadastro de empresas, licencas e troca de empresa ativa.",
        "endpoints": {"pagina_empresas", "salvar_empresa_admin", "gerar_licenca_empresa_admin", "renovar_licenca_empresa_admin", "trocar_empresa_ativa"},
    },
    {
        "id": "diagnostico",
        "grupo": "Apoio / Administracao",
        "titulo": "Diagnostico",
        "descricao": "Validacao do ambiente, checklist e backup de suporte.",
        "endpoints": {"pagina_diagnostico", "validar_diagnostico", "exportar_diagnostico_json", "gerar_backup_suporte"},
    },
    {
        "id": "status_sistema",
        "grupo": "Apoio / Administracao",
        "titulo": "Status do sistema",
        "descricao": "Resumo simples de saude operacional para o dono.",
        "endpoints": {"pagina_status_sistema", "status_sistema_json"},
    },
    {
        "id": "auto_suporte",
        "grupo": "Apoio / Administracao",
        "titulo": "AutoSuporte",
        "descricao": "Central do bot, historico, diagnostico inteligente e reparos seguros.",
        "endpoints": {"pagina_auto_suporte", "auto_suporte_json", "pagina_auto_suporte_acao", "api_auto_suporte_status", "api_auto_suporte_acao", "api_auto_suporte_pacote_codex"},
    },
    {
        "id": "configuracoes_site",
        "grupo": "Apoio / Administracao",
        "titulo": "Configuracoes do site",
        "descricao": "Marca, logo, cores, titulo publico e white-label.",
        "endpoints": {"configuracoes_site"},
    },
]
PAGINAS_MENU_IDS = {item["id"] for item in PAGINAS_MENU_CONFIGURAVEIS}
ENDPOINTS_PAGINAS_MENU = {
    endpoint: item["id"]
    for item in PAGINAS_MENU_CONFIGURAVEIS
    for endpoint in item["endpoints"]
}


def normalizar_paginas_menu_desabilitadas(valor):
    if not valor:
        return set()
    dados = valor
    if isinstance(valor, str):
        try:
            dados = json.loads(valor)
        except Exception:
            dados = [parte.strip() for parte in valor.split(",")]
    if isinstance(dados, dict):
        dados = dados.keys()
    if not isinstance(dados, (list, tuple, set)):
        return set()
    return {
        str(item or "").strip().lower()
        for item in dados
        if str(item or "").strip().lower() in PAGINAS_MENU_IDS
    }


def obter_paginas_menu_desabilitadas(force=False):
    empresa_id_cache = empresa_atual_id() if has_request_context() and session.get("usuario") else 1
    agora_cache_ts = time.time()
    if (
        not force
        and PAGINAS_MENU_CACHE.get("empresa_id") == empresa_id_cache
        and agora_cache_ts - float(PAGINAS_MENU_CACHE.get("testado_em") or 0.0) < PAGINAS_MENU_CACHE_TTL
    ):
        return set(PAGINAS_MENU_CACHE.get("resultado") or set())

    try:
        config = obter_configuracao_empresa()
        resultado = normalizar_paginas_menu_desabilitadas(config.get("paginas_menu_desabilitadas_json"))
    except Exception:
        resultado = set()

    PAGINAS_MENU_CACHE["testado_em"] = agora_cache_ts
    PAGINAS_MENU_CACHE["empresa_id"] = empresa_id_cache
    PAGINAS_MENU_CACHE["resultado"] = set(resultado)
    return resultado


def pagina_menu_habilitada(pagina_id):
    pagina_id = str(pagina_id or "").strip().lower()
    if not pagina_id:
        return True
    return pagina_id not in obter_paginas_menu_desabilitadas()


def montar_paginas_menu_configuracao(desabilitadas=None, force=False):
    if desabilitadas is None:
        desabilitadas = obter_paginas_menu_desabilitadas(force=force)
    else:
        desabilitadas = set(desabilitadas)
    return [
        {
            **item,
            "habilitada": item["id"] not in desabilitadas,
        }
        for item in PAGINAS_MENU_CONFIGURAVEIS
    ]


def salvar_paginas_menu_configuracao_form(form):
    habilitadas = {
        str(item or "").strip().lower()
        for item in form.getlist("paginas_habilitadas")
        if str(item or "").strip().lower() in PAGINAS_MENU_IDS
    }
    desabilitadas = sorted(PAGINAS_MENU_IDS - habilitadas)
    salvar_campos_configuracao_empresa({
        "paginas_menu_desabilitadas_json": json.dumps(desabilitadas, ensure_ascii=False),
    })
    limpar_caches_interface()
    return desabilitadas


HUD_USUARIO_ITENS_CONFIGURAVEIS = [
    {
        "id": "cabecalho",
        "titulo": "Avatar, notificacoes e agenda",
        "descricao": "Mostra foto do usuario, sino de notificacoes e agenda de retornos no topo.",
    },
    {
        "id": "data_hora",
        "titulo": "Data e hora",
        "descricao": "Mostra data e relogio no HUD.",
    },
    {
        "id": "banco_online",
        "titulo": "Banco online",
        "descricao": "Mostra status do Supabase/PostgreSQL.",
    },
    {
        "id": "financeiro",
        "titulo": "Resumo financeiro",
        "descricao": "Mostra faturamento, ticket e valores do dia.",
    },
    {
        "id": "operacional",
        "titulo": "Operacao",
        "descricao": "Mostra atendimentos em andamento e atrasados.",
    },
    {
        "id": "retornos",
        "titulo": "Retornos",
        "descricao": "Mostra resumo comercial de retornos.",
    },
    {
        "id": "entregas",
        "titulo": "Entregas combinadas",
        "descricao": "Mostra avisos de entrega prevista e atraso.",
    },
    {
        "id": "clima",
        "titulo": "Clima",
        "descricao": "Mostra clima e sugestao de lavagem.",
    },
    {
        "id": "usuario",
        "titulo": "Usuario",
        "descricao": "Mostra o nome do usuario logado no HUD expandido.",
    },
    {
        "id": "versao",
        "titulo": "Versao",
        "descricao": "Mostra a versao atual do sistema.",
    },
]
HUD_USUARIO_ITEM_IDS = {item["id"] for item in HUD_USUARIO_ITENS_CONFIGURAVEIS}


def configuracao_hud_usuario_padrao():
    return {
        "hud_ativo": True,
        "itens_habilitados": sorted(HUD_USUARIO_ITEM_IDS),
    }


def normalizar_configuracao_hud_usuario(valor):
    config = configuracao_hud_usuario_padrao()
    if not valor:
        return config
    dados = valor
    if isinstance(valor, str):
        try:
            dados = json.loads(valor)
        except Exception:
            return config
    if not isinstance(dados, dict):
        return config

    if "hud_ativo" in dados:
        config["hud_ativo"] = bool_config_ativo(dados.get("hud_ativo"))

    itens = dados.get("itens_habilitados")
    if isinstance(itens, dict):
        itens = [chave for chave, ativo in itens.items() if bool_config_ativo(ativo)]
    if isinstance(itens, (list, tuple, set)):
        itens_normalizados = {
            str(item or "").strip().lower()
            for item in itens
            if str(item or "").strip().lower() in HUD_USUARIO_ITEM_IDS
        }
        config["itens_habilitados"] = sorted(itens_normalizados)

    return config


def obter_configuracao_hud_usuario(usuario_id=None, force=False):
    usuario_id = int(usuario_id or session.get("usuario_id") or 0)
    if not usuario_id:
        return configuracao_hud_usuario_padrao()

    agora_cache_ts = time.time()
    if (
        not force
        and CONFIG_HUD_USUARIO_CACHE.get("resultado") is not None
        and CONFIG_HUD_USUARIO_CACHE.get("usuario_id") == usuario_id
        and agora_cache_ts - float(CONFIG_HUD_USUARIO_CACHE.get("testado_em") or 0.0) < CONFIG_HUD_USUARIO_CACHE_TTL
    ):
        return deepcopy(CONFIG_HUD_USUARIO_CACHE["resultado"])

    valor = session.get("usuario_hud_config_json") if int(session.get("usuario_id") or 0) == usuario_id else ""
    if not valor:
        try:
            conn = conectar()
            c = conn.cursor()
            c.execute("SELECT hud_config_json FROM usuarios WHERE id=?", (usuario_id,))
            row = c.fetchone()
            conn.close()
            if row:
                try:
                    valor = row["hud_config_json"]
                except Exception:
                    valor = row[0]
        except Exception:
            valor = ""

    config = normalizar_configuracao_hud_usuario(valor)
    CONFIG_HUD_USUARIO_CACHE["testado_em"] = agora_cache_ts
    CONFIG_HUD_USUARIO_CACHE["usuario_id"] = usuario_id
    CONFIG_HUD_USUARIO_CACHE["resultado"] = deepcopy(config)
    return config


def montar_itens_hud_configuracao_usuario(config=None, force=False):
    config = config or obter_configuracao_hud_usuario(force=force)
    habilitados = set(config.get("itens_habilitados") or [])
    return [
        {
            **item,
            "habilitado": item["id"] in habilitados,
        }
        for item in HUD_USUARIO_ITENS_CONFIGURAVEIS
    ]


def salvar_configuracao_hud_usuario_form(form):
    usuario_id = int(session.get("usuario_id") or 0)
    if not usuario_id:
        raise ValueError("Usuario logado nao identificado.")

    itens_habilitados = {
        str(item or "").strip().lower()
        for item in form.getlist("hud_itens_habilitados")
        if str(item or "").strip().lower() in HUD_USUARIO_ITEM_IDS
    }
    config = {
        "hud_ativo": bool_config_ativo(form.get("hud_ativo")),
        "itens_habilitados": sorted(itens_habilitados),
    }
    payload_json = json.dumps(config, ensure_ascii=False)

    conn = conectar()
    c = conn.cursor()
    c.execute("UPDATE usuarios SET hud_config_json=? WHERE id=?", (payload_json, usuario_id))
    conn.commit()
    conn.close()

    session["usuario_hud_config_json"] = payload_json
    limpar_caches_interface()
    return config


def montar_dados_licenca_form(form):
    plano_codigo = normalizar_plano_licenca_domain(form.get("plano_codigo"))
    plano = plano_padrao_licenca_domain(plano_codigo)
    return {
        "codigo_plano": plano_codigo,
        "status": normalizar_status_licenca_domain(form.get("licenca_status") or form.get("status")),
        "limite_usuarios": converter_inteiro(form.get("limite_usuarios"), plano["limite_usuarios"]),
        "limite_atendimentos_mes": converter_inteiro(form.get("limite_atendimentos_mes"), plano["limite_atendimentos_mes"]),
        "limite_unidades": converter_inteiro(form.get("limite_unidades"), plano["limite_unidades"]),
        "limite_storage_mb": converter_inteiro(form.get("limite_storage_mb"), plano["limite_storage_mb"]),
        "validade_em": normalizar_texto_campo(form.get("validade_em")),
        "recursos": plano["recursos"],
    }


def carregar_contexto_licenca_empresa_seguro(empresa_id=None):
    try:
        return carregar_contexto_licenca_empresa(empresa_id)
    except Exception:
        plano = plano_padrao_licenca_domain("starter")
        return {
            "codigo_plano": "starter",
            "plano_label": plano["label"],
            "status": "trial",
            "status_label": "Trial",
            "validade_em": "",
            "dias_restantes": None,
            "limite_usuarios": plano["limite_usuarios"],
            "limite_atendimentos_mes": plano["limite_atendimentos_mes"],
            "usuarios_ativos": 0,
            "atendimentos_mes": 0,
            "bloqueada": False,
            "aviso": False,
            "excedeu_usuarios": False,
            "excedeu_atendimentos": False,
            "codigo_licenca": "",
            "assinatura": "",
            "assinatura_resumo": "",
            "emitida_em": "",
            "renovada_em": "",
            "ultimo_status_validacao": "",
            "recursos": plano["recursos"],
        }


def bloquear_criacao_usuario_por_licenca():
    licenca = obter_contexto_licenca_empresa_cached()
    return licenca.get("bloqueada") or licenca.get("excedeu_usuarios")


def bloquear_criacao_atendimento_por_licenca():
    licenca = obter_contexto_licenca_empresa_cached()
    return licenca.get("bloqueada") or licenca.get("excedeu_atendimentos")


def endpoint_liberado_com_licenca_bloqueada(endpoint):
    if not endpoint:
        return False
    if endpoint == "static" or endpoint.startswith("api_sync") or endpoint.startswith("servir_"):
        return True
    return endpoint in {
        "login",
        "logout",
        "healthz",
        "offline",
        "api_pwa_status",
        "configuracoes",
        "pagina_empresas",
        "salvar_empresa_admin",
        "gerar_licenca_empresa_admin",
        "renovar_licenca_empresa_admin",
        "trocar_empresa_ativa",
        "pagina_diagnostico",
        "validar_diagnostico",
        "exportar_diagnostico_json",
        "gerar_backup_suporte",
        "atualizar_minha_senha",
        "salvar_configuracao_banco",
        "testar_configuracao_banco",
        "migrar_banco_para_supabase",
    }


def formatar_taxa_percentual(parte, total):
    if not total:
        return "0%"
    return f"{round((float(parte) / float(total)) * 100)}%"


def consultar_documentos_relatorios_cursor(cursor, empresa_id):
    try:
        cursor.execute(
            """
            SELECT id, numero, cliente_nome, placa, modelo, total, status, criado_em, usuario
            FROM orcamentos
            WHERE empresa_id=?
            ORDER BY numero DESC
            """,
            (empresa_id,),
        )
    except Exception:
        cursor.execute(
            """
            SELECT id, numero, cliente_nome, placa, modelo, total, status, criado_em, usuario
            FROM orcamentos
            ORDER BY numero DESC
            """
        )
    orcamentos = [dict(item) for item in cursor.fetchall()]

    try:
        cursor.execute(
            """
            SELECT id, rps_numero, numero_nota, serie, ambiente, status, cliente_nome, placa, modelo, valor_total, criado_em, usuario
            FROM notas_fiscais
            WHERE empresa_id=?
            ORDER BY rps_numero DESC
            """,
            (empresa_id,),
        )
    except Exception:
        cursor.execute(
            """
            SELECT id, rps_numero, numero_nota, serie, ambiente, status, cliente_nome, placa, modelo, valor_total, criado_em, usuario
            FROM notas_fiscais
            ORDER BY rps_numero DESC
            """
        )
    notas = [dict(item) for item in cursor.fetchall()]
    return orcamentos, notas


def normalizar_orcamentos_relatorios(registros):
    resultado = []
    for item in registros or []:
        item = dict(item)
        criado_em_dt = interpretar_datahora_sistema(item.get("criado_em"))
        item["criado_em_dt"] = criado_em_dt
        item["numero_formatado"] = formatar_numero_documento(item.get("numero"))
        item["valor_num"] = converter_valor_numerico(item.get("total"))
        item["valor_exibicao"] = formatar_valor_monetario(item["valor_num"])
        item["cliente_nome"] = item.get("cliente_nome") or "Sem cliente"
        item["placa"] = item.get("placa") or "-"
        item["modelo"] = item.get("modelo") or ""
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))
        resultado.append(item)
    resultado.sort(key=lambda row: row.get("criado_em_dt") or datetime.min.replace(tzinfo=ZoneInfo("America/Sao_Paulo")), reverse=True)
    return resultado


def normalizar_notas_relatorios(registros):
    resultado = []
    for item in registros or []:
        item = dict(item)
        criado_em_dt = interpretar_datahora_sistema(item.get("criado_em"))
        item["criado_em_dt"] = criado_em_dt
        item["rps_formatado"] = formatar_numero_documento(item.get("rps_numero"))
        item["valor_num"] = converter_valor_numerico(item.get("valor_total"))
        item["valor_exibicao"] = formatar_valor_monetario(item["valor_num"])
        item["cliente_nome"] = item.get("cliente_nome") or "Sem cliente"
        item["placa"] = item.get("placa") or "-"
        item["modelo"] = item.get("modelo") or ""
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))
        item["emitida"] = bool(normalizar_texto_campo(item.get("numero_nota")) or "EMITIDA" in str(item.get("status") or "").upper())
        resultado.append(item)
    resultado.sort(key=lambda row: row.get("criado_em_dt") or datetime.min.replace(tzinfo=ZoneInfo("America/Sao_Paulo")), reverse=True)
    return resultado


def construir_linhas_csv_relatorio_servicos(servicos):
    linhas = []
    for item in servicos or []:
        entrega_prevista = interpretar_datahora_sistema(item.get("entrega_prevista"))
        entrega_real = item.get("entrega_dt") or interpretar_datahora_sistema(item.get("entrega"))
        status_prazo = ""
        if entrega_prevista and entrega_real:
            status_prazo = "No prazo" if entrega_real <= entrega_prevista else "Fora do prazo"
        linhas.append(
            {
                "Entrega": item.get("entrega_exibicao") or formatar_datahora(item.get("entrega")),
                "Entrega prevista": entrega_prevista.strftime("%d/%m/%Y %H:%M") if entrega_prevista else "-",
                "Status prazo": status_prazo or "-",
                "Cliente": item.get("cliente_nome") or "Sem cliente",
                "Perfil cliente": item.get("perfil_cliente_atendimento_exibicao") or perfil_cliente_atendimento_exibicao(item.get("perfil_cliente_atendimento")),
                "Placa": item.get("placa") or "-",
                "Modelo": item.get("modelo") or "",
                "Servico": item.get("tipo_nome") or "Servico",
                "Valor": formatar_valor_monetario(item.get("valor_num")),
                "Lavagem": formatar_duracao_segundos(item.get("lavagem_segundos") or 0),
                "Finalizacao": formatar_duracao_segundos(item.get("finalizacao_segundos") or 0),
                "Responsavel": normalizar_texto_campo(item.get("finalizado_por_nome")) or normalizar_texto_campo(item.get("operacional_por_nome")) or "-",
            }
        )
    return linhas


def construir_linhas_csv_relatorio_documentos(orcamentos, notas):
    linhas = []
    for item in orcamentos or []:
        linhas.append(
            {
                "Tipo": "Orcamento",
                "Numero": item.get("numero_formatado") or "-",
                "Cliente": item.get("cliente_nome") or "Sem cliente",
                "Placa": item.get("placa") or "-",
                "Valor": item.get("valor_exibicao") or formatar_valor_monetario(item.get("valor_num")),
                "Status": item.get("status") or "-",
                "Data": item.get("criado_em_fmt") or formatar_datahora(item.get("criado_em")),
                "Usuario": item.get("usuario") or "-",
            }
        )
    for item in notas or []:
        linhas.append(
            {
                "Tipo": "Nota fiscal",
                "Numero": item.get("numero_nota") or item.get("rps_formatado") or "-",
                "Cliente": item.get("cliente_nome") or "Sem cliente",
                "Placa": item.get("placa") or "-",
                "Valor": item.get("valor_exibicao") or formatar_valor_monetario(item.get("valor_num")),
                "Status": item.get("status") or "-",
                "Data": item.get("criado_em_fmt") or formatar_datahora(item.get("criado_em")),
                "Usuario": item.get("usuario") or "-",
            }
        )
    return linhas


def montar_csv_resposta(nome_arquivo, linhas):
    buffer = StringIO()
    if linhas:
        campos = list(linhas[0].keys())
        writer = csv.DictWriter(buffer, fieldnames=campos)
        writer.writeheader()
        writer.writerows(linhas)
    else:
        buffer.write("Sem dados\r\n")

    output = BytesIO(buffer.getvalue().encode("utf-8-sig"))
    output.seek(0)
    return send_file(
        output,
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=nome_arquivo,
    )


def carregar_contexto_relatorios(periodo_atual=None, detalhado=False):
    periodo_atual = normalizar_periodo_financeiro(periodo_atual)
    detalhado = bool(detalhado)
    empresa_id = empresa_atual_id()
    chave_cache = montar_chave_cache_relatorios(empresa_id, periodo_atual, detalhado)
    contexto_cache = obter_cache_consulta(
        RELATORIOS_CONTEXT_CACHE,
        chave_cache,
        RELATORIOS_CONTEXT_CACHE_TTL,
    )
    if contexto_cache is not None:
        registrar_metrica_consulta_sql("/financeiro", "snapshot_relatorios", 0, origem="cache", cache_hit=True)
        return contexto_cache
    agora_atual = agora()
    hoje = agora_atual.date()
    ano_atual = hoje.year
    mes_atual = hoje.month

    def carregar_relatorios_raw(conn):
        c = conn.cursor()
        servicos = medir_consulta_sql(
            "/financeiro",
            "servicos_financeiro_join",
            lambda: (
                c.execute(
                    """
                    SELECT
                        servicos.*,
                        tipos_servico.nome AS tipo_nome,
                        veiculos.placa,
                        veiculos.modelo,
                        clientes.nome AS cliente_nome
                    FROM servicos
                    LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
                    LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id AND veiculos.empresa_id=?
                    LEFT JOIN clientes ON veiculos.cliente_id = clientes.id AND clientes.empresa_id=?
                    WHERE servicos.empresa_id=?
                    ORDER BY servicos.id DESC
                    """,
                    (empresa_id, empresa_id, empresa_id),
                ),
                [dict(row) for row in c.fetchall()],
            )[1],
            detalhes="servicos + tipos + veiculos + clientes",
        )
        enriquecer_perfil_cliente_atendimento(servicos)
        if detalhado:
            orcamentos, notas = medir_consulta_sql(
                "/financeiro",
                "documentos_financeiros",
                lambda: consultar_documentos_relatorios_cursor(c, empresa_id),
                detalhes="orcamentos + notas_fiscais",
            )
        else:
            orcamentos, notas = [], []
        return {
            "servicos_raw": servicos,
            "orcamentos_raw": orcamentos,
            "notas_raw": notas,
        }

    contexto_lido = medir_consulta_sql(
        "/financeiro",
        "leitura_contexto_financeiro",
        lambda: executar_leitura_resiliente(
            carregar_relatorios_raw,
            descricao="RELATORIOS",
            padrao={"servicos_raw": [], "orcamentos_raw": [], "notas_raw": []},
        ) or {"servicos_raw": [], "orcamentos_raw": [], "notas_raw": []},
        origem="banco",
        detalhes="servicos + documentos",
    )
    servicos_raw = contexto_lido.get("servicos_raw", []) or []
    orcamentos_raw = contexto_lido.get("orcamentos_raw", []) or []
    notas_raw = contexto_lido.get("notas_raw", []) or []
    inicio_agregacao = time.perf_counter()

    periodo_label = MAPA_PERIODOS_FINANCEIRO[periodo_atual]
    finalizados = []
    servicos_em_andamento = []

    for item in servicos_raw:
        entrega_prevista_dt = interpretar_datahora_sistema(item.get("entrega_prevista"))
        item["entrega_prevista_dt"] = entrega_prevista_dt
        item["entrega_prevista_vencida"] = bool(
            entrega_prevista_dt and entrega_prevista_dt < agora_atual
        )

        if item.get("status") == "EM ANDAMENTO":
            servicos_em_andamento.append(item)

        if item.get("status") != "FINALIZADO":
            continue

        entrega = interpretar_datahora_sistema(item.get("entrega"))
        if not entrega:
            continue

        valor_num = converter_valor_numerico(item.get("valor"))
        item["entrega_dt"] = entrega
        item["valor_num"] = valor_num
        item["valor_exibicao"] = formatar_valor_monetario(valor_num)
        item["tipo_nome"] = item.get("tipo_nome") or "Servico"
        item["cliente_nome"] = item.get("cliente_nome") or "Sem cliente"
        item["placa"] = item.get("placa") or "-"
        item["modelo"] = item.get("modelo") or ""
        item["entrega_exibicao"] = entrega.strftime("%d/%m/%Y %H:%M")
        finalizados.append(item)

    finalizados.sort(key=lambda item: item["entrega_dt"], reverse=True)
    orcamentos = normalizar_orcamentos_relatorios(orcamentos_raw)
    notas = normalizar_notas_relatorios(notas_raw)

    finalizados_hoje = [item for item in finalizados if item["entrega_dt"].date() == hoje]
    finalizados_mes = [
        item
        for item in finalizados
        if item["entrega_dt"].year == ano_atual and item["entrega_dt"].month == mes_atual
    ]
    finalizados_periodo = filtrar_servicos_por_periodo(finalizados, periodo_atual, hoje)
    orcamentos_periodo = filtrar_registros_por_periodo(orcamentos, periodo_atual, hoje, "criado_em_dt")
    notas_periodo = filtrar_registros_por_periodo(notas, periodo_atual, hoje, "criado_em_dt")

    total_hoje = sum(item["valor_num"] for item in finalizados_hoje)
    total_mes = sum(item["valor_num"] for item in finalizados_mes)
    total_geral = sum(item["valor_num"] for item in finalizados)
    quantidade_hoje = len(finalizados_hoje)
    quantidade_mes = len(finalizados_mes)
    quantidade_geral = len(finalizados)
    quantidade_periodo = len(finalizados_periodo)
    ticket_geral = total_geral / quantidade_geral if quantidade_geral else 0
    total_periodo = sum(item["valor_num"] for item in finalizados_periodo)
    ticket_periodo = total_periodo / quantidade_periodo if quantidade_periodo else 0
    media_periodo = total_periodo / dias_do_periodo_financeiro(periodo_atual, hoje)

    base_ranking = finalizados_periodo
    ranking_servicos = {}
    for item in base_ranking:
        nome_servico = item["tipo_nome"]
        resumo = ranking_servicos.setdefault(
            nome_servico,
            {"nome": nome_servico, "quantidade": 0, "valor_total": 0.0},
        )
        resumo["quantidade"] += 1
        resumo["valor_total"] += item["valor_num"]

    ranking_faturamento = sorted(
        ranking_servicos.values(),
        key=lambda item: (item["valor_total"], item["quantidade"]),
        reverse=True,
    )
    ranking_quantidade = sorted(
        ranking_servicos.values(),
        key=lambda item: (item["quantidade"], item["valor_total"]),
        reverse=True,
    )
    referencia_ranking = ranking_faturamento[0]["valor_total"] if ranking_faturamento else 0
    referencia_quantidade = ranking_quantidade[0]["quantidade"] if ranking_quantidade else 0
    for item in ranking_faturamento:
        item["valor_exibicao"] = formatar_valor_monetario(item["valor_total"])
        item["ticket_exibicao"] = formatar_valor_monetario(
            item["valor_total"] / item["quantidade"] if item["quantidade"] else 0
        )
        item["percentual"] = round((item["valor_total"] / referencia_ranking) * 100) if referencia_ranking else 0

    ranking_quantidade_formatado = []
    for item in ranking_quantidade[:5]:
        ranking_quantidade_formatado.append(
            {
                "nome": item["nome"],
                "quantidade": item["quantidade"],
                "valor_exibicao": formatar_valor_monetario(item["valor_total"]),
                "percentual": round((item["quantidade"] / referencia_quantidade) * 100) if referencia_quantidade else 0,
            }
        )

    ultimos_7_dias = []
    totais_por_data = {}
    for deslocamento in range(6, -1, -1):
        dia = hoje - timedelta(days=deslocamento)
        totais_por_data[dia] = 0.0
    for item in finalizados:
        data_entrega = item["entrega_dt"].date()
        if data_entrega in totais_por_data:
            totais_por_data[data_entrega] += item["valor_num"]
    referencia_7_dias = max(totais_por_data.values(), default=0)
    for data_ref, total_dia in totais_por_data.items():
        ultimos_7_dias.append(
            {
                "label": data_ref.strftime("%d/%m"),
                "valor": total_dia,
                "valor_exibicao": formatar_valor_monetario(total_dia),
                "percentual": round((total_dia / referencia_7_dias) * 100) if referencia_7_dias else 0,
            }
        )

    ultimos_6_meses = []
    totais_por_mes = []
    for deslocamento in range(5, -1, -1):
        ano_ref = ano_atual
        mes_ref = mes_atual - deslocamento
        while mes_ref <= 0:
            mes_ref += 12
            ano_ref -= 1
        total_mes_ref = sum(
            item["valor_num"]
            for item in finalizados
            if item["entrega_dt"].year == ano_ref and item["entrega_dt"].month == mes_ref
        )
        totais_por_mes.append(total_mes_ref)
        ultimos_6_meses.append(
            {
                "label": f"{MESES_CURTOS_PT[mes_ref - 1]}/{str(ano_ref)[-2:]}",
                "valor": total_mes_ref,
                "valor_exibicao": formatar_valor_monetario(total_mes_ref),
            }
        )
    referencia_6_meses = max(totais_por_mes, default=0)
    for item in ultimos_6_meses:
        item["percentual"] = round((item["valor"] / referencia_6_meses) * 100) if referencia_6_meses else 0

    entregas_com_previsao = 0
    entregas_no_prazo = 0
    entregas_fora_prazo = 0
    tempos_lavagem = []
    tempos_finalizacao = []
    tempos_ciclo = []
    for item in finalizados_periodo:
        lavagem_segundos = max(0, int(item.get("lavagem_segundos") or 0))
        finalizacao_segundos = max(0, int(item.get("finalizacao_segundos") or 0))
        ciclo_segundos = lavagem_segundos + finalizacao_segundos
        if lavagem_segundos:
            tempos_lavagem.append(lavagem_segundos)
        if finalizacao_segundos:
            tempos_finalizacao.append(finalizacao_segundos)
        if ciclo_segundos:
            tempos_ciclo.append(ciclo_segundos)
        prevista = item.get("entrega_prevista_dt")
        entrega_real = item.get("entrega_dt")
        if prevista and entrega_real:
            entregas_com_previsao += 1
            if entrega_real <= prevista:
                entregas_no_prazo += 1
            else:
                entregas_fora_prazo += 1

    resumo_fluxo_aberto = montar_resumo_fluxo_atendimento(servicos_em_andamento)
    ranking_equipe = montar_ranking_equipe_relatorios_domain(
        finalizados_periodo,
        normalizar_texto_campo,
        converter_valor_numerico,
        formatar_valor_monetario,
        formatar_duracao_segundos,
    )
    resumo_retornos = medir_consulta_sql(
        "/financeiro",
        "resumo_retornos_financeiro",
        lambda: obter_resumo_retornos_hud(
            str(session.get("usuario") or ""),
            time.time(),
            agora_atual,
            somente_cache=True,
        ),
        origem="memoria",
        detalhes="retornos hud cache",
    )
    orcamentos_valor_total = sum(item["valor_num"] for item in orcamentos_periodo)
    notas_emitidas_periodo = [item for item in notas_periodo if item.get("emitida")]
    notas_pendentes_periodo = [item for item in notas_periodo if not item.get("emitida")]
    valor_fiscal_periodo = sum(item["valor_num"] for item in notas_emitidas_periodo)
    novos_periodo = [
        item for item in finalizados_periodo
        if normalizar_perfil_cliente_atendimento(item.get("perfil_cliente_atendimento")) == "NOVO"
    ]
    retornos_periodo = [
        item for item in finalizados_periodo
        if normalizar_perfil_cliente_atendimento(item.get("perfil_cliente_atendimento")) == "RETORNO"
    ]

    resumo_operacional = montar_resumo_operacional_financeiro(
        tempos_lavagem,
        tempos_finalizacao,
        tempos_ciclo,
        entregas_no_prazo,
        entregas_com_previsao,
        entregas_fora_prazo,
        resumo_fluxo_aberto,
        formatar_duracao_segundos,
        formatar_taxa_percentual,
    )
    resumo_comercial = {
        "orcamentos_total": len(orcamentos_periodo),
        "orcamentos_valor_exibicao": formatar_valor_monetario(orcamentos_valor_total),
        "notas_emitidas_total": len(notas_emitidas_periodo),
        "notas_pendentes_total": len(notas_pendentes_periodo),
        "notas_valor_exibicao": formatar_valor_monetario(valor_fiscal_periodo),
        "novos_total": len(novos_periodo),
        "retornos_total": len(retornos_periodo),
        "novos_valor_exibicao": formatar_valor_monetario(sum(item["valor_num"] for item in novos_periodo)),
        "retornos_valor_exibicao": formatar_valor_monetario(sum(item["valor_num"] for item in retornos_periodo)),
        "retornos_acao_agora": resumo_retornos["acao_agora"],
        "retornos_reagendados": resumo_retornos["reagendados_vencidos"],
        "retornos_contatados_hoje": resumo_retornos["contatados_hoje"],
    }
    periodo_descricao = montar_periodo_descricao(periodo_atual)

    contexto = {
        "periodo_atual": periodo_atual,
        "periodo_label": periodo_label,
        "periodo_descricao": periodo_descricao,
        "periodos_financeiro": PERIODOS_FINANCEIRO,
        "total_periodo": formatar_valor_monetario(total_periodo),
        "quantidade_periodo": quantidade_periodo,
        "ticket_periodo": formatar_valor_monetario(ticket_periodo),
        "media_periodo": formatar_valor_monetario(media_periodo),
        "total_hoje": formatar_valor_monetario(total_hoje),
        "total_mes": formatar_valor_monetario(total_mes),
        "total_geral": formatar_valor_monetario(total_geral),
        "quantidade_hoje": quantidade_hoje,
        "quantidade_mes": quantidade_mes,
        "quantidade_geral": quantidade_geral,
        "ticket_geral": formatar_valor_monetario(ticket_geral),
        "em_andamento": len(servicos_em_andamento),
        "ranking_faturamento": ranking_faturamento[:5],
        "ranking_quantidade": ranking_quantidade_formatado,
        "ultimos_7_dias": ultimos_7_dias,
        "ultimos_6_meses": ultimos_6_meses,
        "ultimos_finalizados": finalizados_periodo[:8],
        "servico_campeao": ranking_faturamento[0] if ranking_faturamento else None,
        "referencia_ranking_periodo": periodo_label,
        "resumo_operacional": resumo_operacional,
        "resumo_comercial": resumo_comercial,
        "ranking_equipe": ranking_equipe,
        "orcamentos_periodo": orcamentos_periodo[:5],
        "notas_periodo": notas_periodo[:5],
        "finalizados_periodo_raw": finalizados_periodo,
        "orcamentos_periodo_raw": orcamentos_periodo,
        "notas_periodo_raw": notas_periodo,
        "relatorio_detalhado": detalhado,
    }
    registrar_metrica_consulta_sql(
        "/financeiro",
        "agregacao_contexto_financeiro",
        int((time.perf_counter() - inicio_agregacao) * 1000),
        origem="memoria",
        detalhes=f"servicos={len(servicos_raw)}",
    )
    salvar_cache_consulta(RELATORIOS_CONTEXT_CACHE, chave_cache, contexto)
    return contexto

def obter_label_intervalo_sincronizacao(minutos):
    return MAPA_INTERVALOS_SINCRONIZACAO.get(minutos, f"{minutos} min")

def formatar_tempo_restante(valor_iso):
    if not valor_iso:
        return "Sem agendamento"

    try:
        referencia = interpretar_datahora_sistema(valor_iso)
        if not referencia:
            return "Horario invalido"
        diferenca = int((referencia - agora()).total_seconds())
    except Exception:
        return "Horario invalido"

    if diferenca <= 0:
        return "Executando agora"

    dias, resto = divmod(diferenca, 86400)
    horas, resto = divmod(resto, 3600)
    minutos, segundos = divmod(resto, 60)
    partes = []

    if dias:
        partes.append(f"{dias}d")
    if horas:
        partes.append(f"{horas}h")
    if minutos:
        partes.append(f"{minutos}min")
    if not partes:
        partes.append(f"{segundos}s")

    return "Falta " + " ".join(partes[:3])

def formatar_duracao_segundos(segundos):
    total = max(0, int(segundos or 0))
    dias, resto = divmod(total, 86400)
    horas, resto = divmod(resto, 3600)
    minutos, segundos = divmod(resto, 60)
    partes = []

    if dias:
        partes.append(f"{dias}d")
    if horas:
        partes.append(f"{horas}h")
    if minutos:
        partes.append(f"{minutos}min")
    if not partes:
        partes.append(f"{segundos}s")

    return " ".join(partes[:3])


ETAPAS_OPERACIONAIS_VALIDAS = {"LAVAGEM", "FINALIZACAO"}


def normalizar_etapa_operacional(valor, fallback="LAVAGEM"):
    texto = normalizar_texto_campo(valor).upper()
    texto = texto.replace("FINALIZAÃ‡ÃƒO", "FINALIZACAO")
    if texto not in ETAPAS_OPERACIONAIS_VALIDAS:
        return fallback
    return texto


def _campo_segundos_etapa(etapa):
    return "lavagem_segundos" if etapa == "LAVAGEM" else "finalizacao_segundos"


def _campo_inicio_primeiro_etapa(etapa):
    return "lavagem_iniciada_em" if etapa == "LAVAGEM" else "finalizacao_iniciada_em"


def obter_inicio_etapa_servico(servico, etapa, incluir_entrada=False):
    servico = dict(servico or {})
    candidatos = [servico.get("etapa_atual_iniciada_em")]
    status_em_andamento = normalizar_texto_campo(servico.get("status")).upper() == "EM ANDAMENTO"

    if status_em_andamento and not normalizar_texto_campo(servico.get("etapa_atual_iniciada_em")):
        candidatos.append(servico.get(_campo_inicio_primeiro_etapa(etapa)))

    if status_em_andamento and incluir_entrada and etapa == "LAVAGEM":
        candidatos.append(servico.get("entrada"))

    for valor in candidatos:
        inicio = interpretar_datahora_sistema(valor)
        inicio = normalizar_datetime_brasilia(inicio)
        if inicio:
            return inicio

    return None


def atualizar_campos_servico(cursor, servico_id, updates, empresa_id=None):
    if not updates:
        return

    colunas = []
    parametros = []

    for chave, valor in updates.items():
        colunas.append(f"{chave}=?")
        parametros.append(valor)

    sql = f"UPDATE servicos SET {', '.join(colunas)} WHERE id=?"
    parametros.append(servico_id)
    if empresa_id is not None:
        sql += " AND empresa_id=?"
        parametros.append(normalize_empresa_id(empresa_id))
    cursor.execute(sql, tuple(parametros))


def calcular_segundos_etapa_servico(servico, etapa, referencia=None):
    servico = dict(servico or {})
    referencia = referencia or agora()
    etapa = normalizar_etapa_operacional(etapa)
    campo_segundos = _campo_segundos_etapa(etapa)
    total = converter_inteiro(servico.get(campo_segundos), 0)
    etapa_atual = normalizar_etapa_operacional(servico.get("etapa_atual"), fallback="LAVAGEM")
    etapa_iniciada_em = obter_inicio_etapa_servico(
        servico,
        etapa_atual,
        incluir_entrada=True,
    )

    if etapa_iniciada_em and etapa_atual == etapa:
        total += max(0, int((referencia - etapa_iniciada_em).total_seconds()))

    return max(0, total)


def enriquecer_etapas_operacionais_servico(servico, referencia=None):
    referencia = referencia or agora()
    etapa_atual = normalizar_etapa_operacional(servico.get("etapa_atual"), fallback="LAVAGEM")
    etapa_atual_iniciada = obter_inicio_etapa_servico(
        servico,
        etapa_atual,
        incluir_entrada=True,
    )

    servico["etapa_atual_normalizada"] = etapa_atual
    servico["etapa_atual_exibicao"] = "Lavagem" if etapa_atual == "LAVAGEM" else "Finalizacao"
    servico["etapa_atual_iniciada_em_iso"] = (
        etapa_atual_iniciada.isoformat(timespec="seconds")
        if etapa_atual_iniciada else ""
    )
    servico["lavagem_segundos_base"] = converter_inteiro(servico.get("lavagem_segundos"), 0)
    servico["finalizacao_segundos_base"] = converter_inteiro(servico.get("finalizacao_segundos"), 0)
    servico["lavagem_segundos_total"] = calcular_segundos_etapa_servico(servico, "LAVAGEM", referencia=referencia)
    servico["finalizacao_segundos_total"] = calcular_segundos_etapa_servico(servico, "FINALIZACAO", referencia=referencia)
    servico["lavagem_tempo_exibicao"] = formatar_duracao_segundos(servico["lavagem_segundos_total"])
    servico["finalizacao_tempo_exibicao"] = formatar_duracao_segundos(servico["finalizacao_segundos_total"])
    servico["proxima_etapa"] = "FINALIZACAO" if etapa_atual == "LAVAGEM" else "LAVAGEM"
    servico["proxima_etapa_exibicao"] = "Finalizacao" if servico["proxima_etapa"] == "FINALIZACAO" else "Lavagem"
    servico["botao_trocar_etapa"] = (
        "Ir para finalizacao"
        if servico["proxima_etapa"] == "FINALIZACAO"
        else "Voltar para lavagem"
    )
    return servico


def registrar_transicao_etapa_servico(cursor, servico, etapa_destino, instante=None):
    if not servico:
        return None

    servico_dict = dict(servico)
    instante = normalizar_datetime_brasilia(instante or agora())
    instante_iso = instante.isoformat(timespec="seconds")
    etapa_atual = normalizar_etapa_operacional(servico_dict.get("etapa_atual"), fallback="LAVAGEM")
    etapa_destino = normalizar_etapa_operacional(etapa_destino, fallback=etapa_atual)
    updates = {}

    if etapa_atual == etapa_destino:
        if not normalizar_texto_campo(servico_dict.get("etapa_atual_iniciada_em")):
            updates["etapa_atual_iniciada_em"] = instante_iso
        campo_inicio_destino = _campo_inicio_primeiro_etapa(etapa_destino)
        if not normalizar_texto_campo(servico_dict.get(campo_inicio_destino)):
            updates[campo_inicio_destino] = instante_iso
        atualizar_campos_servico(
            cursor,
            servico_dict["id"],
            updates,
            empresa_id=servico_dict.get("empresa_id"),
        )
        servico_dict.update(updates)
        return servico_dict

    etapa_iniciada_em = obter_inicio_etapa_servico(
        servico_dict,
        etapa_atual,
        incluir_entrada=True,
    )

    if etapa_iniciada_em:
        campo_segundos_atual = _campo_segundos_etapa(etapa_atual)
        base_atual = converter_inteiro(servico_dict.get(campo_segundos_atual), 0)
        updates[campo_segundos_atual] = base_atual + max(
            0,
            int((instante - etapa_iniciada_em).total_seconds()),
        )

    updates["etapa_atual"] = etapa_destino
    updates["etapa_atual_iniciada_em"] = instante_iso
    campo_inicio_destino = _campo_inicio_primeiro_etapa(etapa_destino)
    if not normalizar_texto_campo(servico_dict.get(campo_inicio_destino)):
        updates[campo_inicio_destino] = instante_iso

    atualizar_campos_servico(
        cursor,
        servico_dict["id"],
        updates,
        empresa_id=servico_dict.get("empresa_id"),
    )
    servico_dict.update(updates)
    return servico_dict


def consolidar_tempo_etapa_atual_servico(cursor, servico, instante=None, etapa_final=None):
    if not servico:
        return None

    servico_dict = dict(servico)
    instante = normalizar_datetime_brasilia(instante or agora())
    etapa_atual = normalizar_etapa_operacional(servico_dict.get("etapa_atual"), fallback="LAVAGEM")
    etapa_iniciada_em = obter_inicio_etapa_servico(
        servico_dict,
        etapa_atual,
        incluir_entrada=True,
    )
    updates = {}

    if etapa_iniciada_em:
        campo_segundos_atual = _campo_segundos_etapa(etapa_atual)
        base_atual = converter_inteiro(servico_dict.get(campo_segundos_atual), 0)
        updates[campo_segundos_atual] = base_atual + max(
            0,
            int((instante - etapa_iniciada_em).total_seconds()),
        )

    if etapa_final:
        etapa_final = normalizar_etapa_operacional(etapa_final, fallback=etapa_atual)
        updates["etapa_atual"] = etapa_final
        campo_inicio_final = _campo_inicio_primeiro_etapa(etapa_final)
        if not normalizar_texto_campo(servico_dict.get(campo_inicio_final)):
            updates[campo_inicio_final] = instante.isoformat(timespec="seconds")

    updates["etapa_atual_iniciada_em"] = None

    atualizar_campos_servico(
        cursor,
        servico_dict["id"],
        updates,
        empresa_id=servico_dict.get("empresa_id"),
    )
    servico_dict.update(updates)
    return servico_dict


def aplicar_fluxo_etapa_atendimento_em_edicao(cursor, servico, status_destino, etapa_destino, instante=None):
    if not servico:
        return None

    servico_dict = dict(servico)
    instante = normalizar_datetime_brasilia(instante or agora())
    status_destino = normalizar_texto_campo(status_destino).upper() or "EM ANDAMENTO"
    etapa_atual = normalizar_etapa_operacional(servico_dict.get("etapa_atual"), fallback="LAVAGEM")
    etapa_destino = normalizar_etapa_operacional(etapa_destino, fallback=etapa_atual)

    if status_destino == "FINALIZADO":
        return consolidar_tempo_etapa_atual_servico(
            cursor,
            servico_dict,
            instante=instante,
            etapa_final="FINALIZACAO",
        )

    status_atual = normalizar_texto_campo(servico_dict.get("status")).upper()
    if status_atual == "FINALIZADO":
        instante_iso = instante.isoformat(timespec="seconds")
        updates = {
            "etapa_atual": etapa_destino,
            "etapa_atual_iniciada_em": instante_iso,
        }
        campo_inicio_destino = _campo_inicio_primeiro_etapa(etapa_destino)
        if not normalizar_texto_campo(servico_dict.get(campo_inicio_destino)):
            updates[campo_inicio_destino] = instante_iso
        atualizar_campos_servico(
            cursor,
            servico_dict["id"],
            updates,
            empresa_id=servico_dict.get("empresa_id"),
        )
        servico_dict.update(updates)
        return servico_dict

    return registrar_transicao_etapa_servico(
        cursor,
        servico_dict,
        etapa_destino,
        instante=instante,
    )


def ordenar_servicos_fluxo_atendimento(servicos):
    def chave(servico):
        entrega_dt = servico.get("entrega_prevista_dt")
        entrega_ts = entrega_dt.timestamp() if entrega_dt else float("inf")
        return (
            0 if servico.get("entrega_prevista_vencida") else 1,
            entrega_ts,
            -converter_inteiro(servico.get("prioridade_ia"), 0),
            -converter_inteiro(servico.get("tempo_espera_minutos"), 0),
            str(servico.get("placa") or ""),
        )

    return sorted(servicos or [], key=chave)


def montar_resumo_fluxo_atendimento(servicos):
    resumo = {
        "total": 0,
        "lavagem": 0,
        "finalizacao": 0,
        "novos": 0,
        "retornos": 0,
        "com_horario": 0,
        "sem_horario": 0,
        "atrasados": 0,
        "proxima_entrega_exibicao": "Sem horario combinado",
    }
    proxima_entrega = None

    for servico in servicos or []:
        resumo["total"] += 1
        etapa = normalizar_etapa_operacional(servico.get("etapa_atual"), fallback="LAVAGEM")
        if etapa == "FINALIZACAO":
            resumo["finalizacao"] += 1
        else:
            resumo["lavagem"] += 1

        perfil = normalizar_perfil_cliente_atendimento(
            servico.get("perfil_cliente_atendimento"),
            fallback="NOVO",
        )
        if perfil == "RETORNO":
            resumo["retornos"] += 1
        else:
            resumo["novos"] += 1

        if servico.get("entrega_prevista_dt"):
            resumo["com_horario"] += 1
            if servico.get("entrega_prevista_vencida"):
                resumo["atrasados"] += 1
            entrega_dt = servico["entrega_prevista_dt"]
            if proxima_entrega is None or entrega_dt < proxima_entrega:
                proxima_entrega = entrega_dt
        else:
            resumo["sem_horario"] += 1

    if proxima_entrega:
        resumo["proxima_entrega_exibicao"] = proxima_entrega.strftime("%d/%m/%Y %H:%M")

    return resumo

def interpretar_hora_brasilia(valor_hora, referencia=None):
    texto = normalizar_texto_campo(valor_hora)
    if not texto:
        return None

    referencia = referencia or agora()

    for formato in ("%H:%M", "%H:%M:%S"):
        try:
            hora = datetime.strptime(texto, formato).time()
            datahora = datetime.combine(
                referencia.date(),
                hora,
                tzinfo=ZoneInfo("America/Sao_Paulo"),
            )
            if datahora < referencia:
                datahora += timedelta(days=1)
            return datahora
        except Exception:
            continue

    return None

def formatar_contagem_regressiva(valor_iso, referencia=None):
    datahora = interpretar_datahora_sistema(valor_iso)
    if not datahora:
        return "Sem horario combinado"

    referencia = referencia or agora()
    if datahora.tzinfo is None:
        datahora = datahora.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))

    diferenca = int((datahora - referencia).total_seconds())
    if diferenca <= 0:
        return f"Atraso de {formatar_duracao_segundos(abs(diferenca))}"

    return f"Falta {formatar_duracao_segundos(diferenca)}"

def interpretar_datahora_sistema(valor):
    if not valor:
        return None

    if isinstance(valor, datetime):
        return normalizar_datetime_brasilia(valor)

    texto = str(valor).strip()

    for parser in (
        lambda item: datetime.fromisoformat(item),
        lambda item: datetime.strptime(item, "%d/%m/%Y %H:%M"),
    ):
        try:
            return normalizar_datetime_brasilia(parser(texto))
        except Exception:
            continue

    return None

def normalizar_datahora_formulario(valor, obrigatoria=False):
    texto = normalizar_texto_campo(valor)
    if not texto:
        if obrigatoria:
            raise ValueError("Informe uma data e hora validas.")
        return None

    datahora = interpretar_datahora_sistema(texto)
    if not datahora:
        raise ValueError("Informe uma data e hora validas.")

    if datahora.tzinfo is None:
        datahora = datahora.replace(tzinfo=ZoneInfo("America/Sao_Paulo"))
    return datahora.isoformat(timespec="seconds")

def formatar_datahora_input(valor):
    datahora = interpretar_datahora_sistema(valor)
    if not datahora:
        return ""
    return datahora.strftime("%Y-%m-%dT%H:%M")

def interpretar_data_nascimento(valor):
    if not valor:
        return None

    if isinstance(valor, datetime):
        return valor.date()

    if isinstance(valor, date):
        return valor

    texto = str(valor).strip()
    for formato in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(texto, formato).date()
        except Exception:
            continue
    return None

def normalizar_data_nascimento(valor):
    data_nascimento = interpretar_data_nascimento(valor)
    return data_nascimento.isoformat() if data_nascimento else None

def formatar_data_nascimento_input(valor):
    data_nascimento = interpretar_data_nascimento(valor)
    return data_nascimento.isoformat() if data_nascimento else ""

def formatar_data_nascimento_exibicao(valor):
    data_nascimento = interpretar_data_nascimento(valor)
    return data_nascimento.strftime("%d/%m/%Y") if data_nascimento else ""

def normalizar_perfil_cliente_atendimento(valor, fallback="NOVO"):
    texto = normalizar_texto_campo(valor).upper()
    if texto in {"NOVO", "RETORNO"}:
        return texto
    texto_fallback = normalizar_texto_campo(fallback).upper()
    return texto_fallback if texto_fallback in {"NOVO", "RETORNO"} else "NOVO"

def perfil_cliente_atendimento_exibicao(valor):
    return "Novo cadastro" if normalizar_perfil_cliente_atendimento(valor) == "NOVO" else "Cliente retornando"

def chave_marcadores_cadastro_novo():
    return "cadastros_novos_para_atendimento"

def registrar_cadastro_novo_para_atendimento(placa):
    placa_normalizada = normalizar_texto_campo(placa).upper()
    if not placa_normalizada:
        return

    marcadores = dict(session.get(chave_marcadores_cadastro_novo()) or {})
    agora_ts = time.time()
    marcadores = {
        item_placa: item_ts
        for item_placa, item_ts in marcadores.items()
        if agora_ts - converter_valor_numerico(item_ts) < 7200
    }
    marcadores[placa_normalizada] = agora_ts
    session[chave_marcadores_cadastro_novo()] = marcadores

def remover_cadastro_novo_para_atendimento(placa):
    placa_normalizada = normalizar_texto_campo(placa).upper()
    marcadores = dict(session.get(chave_marcadores_cadastro_novo()) or {})
    if placa_normalizada in marcadores:
        marcadores.pop(placa_normalizada, None)
        session[chave_marcadores_cadastro_novo()] = marcadores

def consumir_cadastro_novo_para_atendimento(placa):
    placa_normalizada = normalizar_texto_campo(placa).upper()
    marcadores = dict(session.get(chave_marcadores_cadastro_novo()) or {})
    marcado_em = converter_valor_numerico(marcadores.pop(placa_normalizada, 0))
    session[chave_marcadores_cadastro_novo()] = marcadores
    return bool(marcado_em and time.time() - marcado_em < 7200)


def contar_atendimentos_anteriores_veiculo(cursor, empresa_id, veiculo_id):
    cursor.execute(
        """
        SELECT COUNT(*) AS total
        FROM servicos
        WHERE empresa_id=?
          AND veiculo_id=?
        """,
        (empresa_id, veiculo_id),
    )
    linha = cursor.fetchone()
    try:
        return int(linha["total"] or 0)
    except Exception:
        return int((linha or [0])[0] or 0)


def classificar_perfil_cliente_atendimento(cursor, empresa_id, veiculo_id, placa):
    cadastro_criado_agora = consumir_cadastro_novo_para_atendimento(placa)
    atendimentos_anteriores = contar_atendimentos_anteriores_veiculo(cursor, empresa_id, veiculo_id)
    if cadastro_criado_agora and atendimentos_anteriores <= 0:
        return "NOVO", "cliente cadastrado agora sem atendimento anterior", atendimentos_anteriores
    return "RETORNO", "placa ja tinha cadastro ou atendimento anterior", atendimentos_anteriores

def enriquecer_perfil_cliente_atendimento(servicos):
    primeiro_servico_por_veiculo = {}
    for item in sorted(servicos or [], key=lambda row: converter_inteiro(row.get("id"), 0)):
        veiculo_id = converter_inteiro(item.get("veiculo_id"), 0)
        if veiculo_id and veiculo_id not in primeiro_servico_por_veiculo:
            primeiro_servico_por_veiculo[veiculo_id] = converter_inteiro(item.get("id"), 0)

    for item in servicos or []:
        veiculo_id = converter_inteiro(item.get("veiculo_id"), 0)
        servico_id = converter_inteiro(item.get("id"), 0)
        fallback = "NOVO"
        if veiculo_id and primeiro_servico_por_veiculo.get(veiculo_id) != servico_id:
            fallback = "RETORNO"
        perfil = normalizar_perfil_cliente_atendimento(
            item.get("perfil_cliente_atendimento"),
            fallback=fallback,
        )
        item["perfil_cliente_atendimento"] = perfil
        item["perfil_cliente_atendimento_exibicao"] = perfil_cliente_atendimento_exibicao(perfil)
    return servicos

def construir_data_aniversario_ano(data_nascimento, ano):
    try:
        return date(ano, data_nascimento.month, data_nascimento.day)
    except ValueError:
        if data_nascimento.month == 2 and data_nascimento.day == 29:
            return date(ano, 2, 28)
        raise

def montar_mensagem_notificacao_aniversario(cliente_nome, data_nascimento, dias_antecedencia=0):
    nome = normalizar_texto_campo(cliente_nome) or "Cliente"
    data_fmt = data_nascimento.strftime("%d/%m")
    if dias_antecedencia == 1:
        return (
            f"Aniversario amanha: {nome} ({data_fmt}).\n"
            f"Sugestao de mensagem:\n"
            f"Oi {nome}, tudo bem? Passando para te desejar um feliz aniversario adiantado. "
            f"Amanha queremos te presentear com uma condicao especial nos servicos da Wagen. "
            f"Se quiser, ja deixamos seu horario reservado."
        )
    return (
        f"Aniversario hoje: {nome} ({data_fmt}).\n"
        f"Sugestao de mensagem:\n"
        f"Oi {nome}, feliz aniversario! Preparamos uma condicao especial para voce comemorar com o carro em dia. "
        f"Se quiser, te envio as opcoes com desconto e ja separo um horario."
    )

def formatar_valor_monetario(valor):
    numero = converter_valor_numerico(valor)

    return f"{numero:.2f}"

def converter_valor_numerico(valor):
    try:
        texto = str(valor or "").strip()
        if not texto:
            return 0.0

        texto = re.sub(r"[^0-9,.-]", "", texto)
        if "," in texto and "." in texto:
            if texto.rfind(",") > texto.rfind("."):
                texto = texto.replace(".", "").replace(",", ".")
            else:
                texto = texto.replace(",", "")
        else:
            texto = texto.replace(",", ".")

        return float(texto)
    except Exception:
        return 0.0

def normalizar_valor_importacao_pg(tabela, coluna, valor):
    if valor is None:
        return None

    texto_coluna = str(coluna or "").strip().lower()
    tabela = str(tabela or "").strip().lower()

    colunas_float = {
        "valor", "valor_unitario", "valor_total", "subtotal", "desconto",
        "total", "aliquota_padrao", "aliquota_iss", "valor_servicos",
        "valor_iss", "valor_adicional",
    }
    colunas_int = {
        "id", "validade_dias", "ordem", "ativo", "prioridade",
        "tentativas_login", "retencao_arquivos", "destino_externo_ativo",
        "intervalo_minutos", "rps_numero", "veiculo_id", "tipo_id",
        "sync_id", "usuario_id",
    }

    if tabela == "tipos_servico" and texto_coluna == "valor":
        return converter_valor_numerico(valor)

    if tabela == "orcamentos" and texto_coluna == "numero":
        return converter_inteiro(valor)

    if tabela == "notas_fiscais" and texto_coluna == "rps_numero":
        return converter_inteiro(valor)

    if texto_coluna in colunas_float:
        return converter_valor_numerico(valor)

    if texto_coluna in colunas_int:
        return converter_inteiro(valor)

    return valor

def converter_inteiro(valor, padrao=0):
    try:
        return int(str(valor or "").strip())
    except Exception:
        return padrao

def formatar_valor_brl(valor):
    numero = converter_valor_numerico(valor)
    texto = f"{numero:,.2f}"
    return texto.replace(",", "X").replace(".", ",").replace("X", ".")

def normalizar_documento_fiscal(valor):
    return re.sub(r"\D", "", str(valor or ""))

def formatar_documento_fiscal(valor):
    documento = normalizar_documento_fiscal(valor)

    if len(documento) == 11:
        return (
            f"{documento[:3]}.{documento[3:6]}.{documento[6:9]}-"
            f"{documento[9:11]}"
        )

    if len(documento) == 14:
        return (
            f"{documento[:2]}.{documento[2:5]}.{documento[5:8]}/"
            f"{documento[8:12]}-{documento[12:14]}"
        )

    return str(valor or "").strip()

def formatar_cep(valor):
    cep = re.sub(r"\D", "", str(valor or ""))

    if len(cep) == 8:
        return f"{cep[:5]}-{cep[5:]}"

    return str(valor or "").strip()

def formatar_numero_documento(numero, tamanho=6):
    try:
        return str(int(numero)).zfill(tamanho)
    except Exception:
        return str(numero or "-")

def normalizar_cor_hex(valor, padrao):
    texto = normalizar_texto_campo(valor)
    if not texto:
        return padrao
    if not texto.startswith("#"):
        texto = f"#{texto}"
    if not re.fullmatch(r"#[0-9A-Fa-f]{6}", texto):
        return padrao
    return texto.lower()

def cor_hex_para_rgb_css(valor, padrao):
    cor = normalizar_cor_hex(valor, padrao).lstrip("#")
    return f"{int(cor[0:2], 16)}, {int(cor[2:4], 16)}, {int(cor[4:6], 16)}"

def empresa_snapshot_padrao():
    return {
        "razao_social": "",
        "nome_fantasia": "",
        "cnpj": "",
        "inscricao_municipal": "",
        "inscricao_estadual": "",
        "regime_tributario": "",
        "email": "",
        "telefone": "",
        "endereco": "",
        "numero": "",
        "complemento": "",
        "bairro": "",
        "cidade": "",
        "uf": "",
        "cep": "",
        "codigo_servico_padrao": "",
        "aliquota_padrao": 0.0,
        "versao_sistema": VERSAO_SISTEMA_PADRAO,
        "clima_ativo": 1,
        "clima_api_url": "https://api.open-meteo.com/v1/forecast",
        "clima_local_label": "Cachoeirinha / RS",
        "clima_latitude": -29.68,
        "clima_longitude": -51.13,
        "clima_timezone": "America/Sao_Paulo",
        "clima_timeout_segundos": 8,
        "marca_nome": "Wagen Estetica Automotiva",
        "marca_subtitulo": "Gestao Estetica",
        "marca_logo_url": "",
        "marca_favicon_url": "",
        "marca_cor_primaria": "#facc15",
        "marca_cor_secundaria": "#111827",
        "marca_cor_fundo": "#0b0b0b",
        "marca_cor_superficie": "#111827",
        "marca_cor_texto": "#f9fafb",
        "site_titulo": "Gestao Estetica",
        "site_rodape_texto": "Desenvolvido por Luiz Henrique | Qualquer Erro Contate o Desenvolvedor | Wagen Estetica Automotiva | Direitos Reservados.",
        "login_titulo_publico": "Acesso ao sistema",
        "login_subtitulo_publico": "Entre no sistema",
        "login_botao_texto": "Entrar",
        "home_busca_placeholder": "Digite a placa",
        "home_busca_botao_texto": "Buscar",
        "home_estado_inicial_titulo": "Digite uma placa para comecar",
        "paginas_menu_desabilitadas_json": "[]",
        "auto_teste_ativo": 0,
        "auto_teste_site_url": "https://wagenestetica.duckdns.org",
        "auto_teste_intervalo_horas": 2,
        "auto_teste_telegram_bot_token": "",
        "auto_teste_telegram_bot_nick": "@wagenesteticabot",
        "auto_teste_telegram_chat_id": "",
        "auto_teste_ultimo_status": "",
        "auto_teste_ultimo_relatorio": "",
        "auto_teste_ultimo_teste_em": "",
    }


def mascarar_token_telegram(token):
    token = normalizar_texto_campo(token)
    if not token:
        return "Nao configurado"
    if len(token) <= 12:
        return "***"
    return f"{token[:8]}...{token[-6:]}"


def normalizar_bot_telegram(valor):
    valor = normalizar_texto_campo(valor)
    if not valor:
        return "@wagenesteticabot"
    return valor if valor.startswith("@") else f"@{valor}"


def resolver_chat_id_telegram(token, chat_id_atual=""):
    chat_id_atual = normalizar_texto_campo(chat_id_atual)
    if chat_id_atual:
        return chat_id_atual

    token = normalizar_texto_campo(token)
    if not token:
        return ""

    resposta = requests.get(
        f"https://api.telegram.org/bot{token}/getUpdates",
        timeout=12,
    )
    resposta.raise_for_status()
    payload = resposta.json()

    for update in reversed(payload.get("result") or []):
        mensagem = update.get("message") or update.get("edited_message") or {}
        chat = mensagem.get("chat") or {}
        chat_id = chat.get("id")
        if chat_id:
            return str(chat_id)

    return ""


def obter_configuracao_empresa(force=False):
    empresa_id_cache = normalize_empresa_id(empresa_atual_id())
    agora_cache_ts = time.time()
    if (
        not force
        and CONFIG_EMPRESA_CACHE.get("resultado") is not None
        and CONFIG_EMPRESA_CACHE.get("empresa_id") == empresa_id_cache
        and agora_cache_ts - float(CONFIG_EMPRESA_CACHE.get("testado_em") or 0.0) < CONFIG_EMPRESA_CACHE_TTL
    ):
        return deepcopy(CONFIG_EMPRESA_CACHE["resultado"])

    def carregar(conn):
        c = conn.cursor()
        return selecionar_configuracao_empresa_cursor(c, empresa_id_cache)

    item = executar_leitura_resiliente(
        carregar,
        descricao="CONFIG EMPRESA",
        padrao=None,
    )

    dados = empresa_snapshot_padrao()

    if item:
        dados.update(item)

    dados["versao_sistema"] = normalizar_versao_sistema(dados.get("versao_sistema"))
    dados["versao_sistema_label"] = formatar_versao_sistema(dados.get("versao_sistema"))
    dados["cnpj_formatado"] = formatar_documento_fiscal(dados.get("cnpj"))
    dados["cep_formatado"] = formatar_cep(dados.get("cep"))
    dados["aliquota_padrao"] = converter_valor_numerico(dados.get("aliquota_padrao"))
    dados["clima_ativo"] = bool(int(dados.get("clima_ativo") or 0))
    dados["clima_api_url"] = normalizar_texto_campo(dados.get("clima_api_url")) or "https://api.open-meteo.com/v1/forecast"
    dados["clima_local_label"] = normalizar_texto_campo(dados.get("clima_local_label")) or "Cachoeirinha / RS"
    dados["clima_latitude"] = converter_valor_numerico(dados.get("clima_latitude"))
    dados["clima_longitude"] = converter_valor_numerico(dados.get("clima_longitude"))
    dados["clima_timezone"] = normalizar_texto_campo(dados.get("clima_timezone")) or "America/Sao_Paulo"
    dados["clima_timeout_segundos"] = max(3, min(20, converter_inteiro(dados.get("clima_timeout_segundos"), 8)))
    dados["marca_nome"] = normalizar_texto_campo(dados.get("marca_nome")) or "Wagen Estetica Automotiva"
    dados["marca_subtitulo"] = normalizar_texto_campo(dados.get("marca_subtitulo")) or "Gestao Estetica"
    dados["marca_logo_url"] = normalizar_texto_campo(dados.get("marca_logo_url"))
    dados["marca_favicon_url"] = normalizar_texto_campo(dados.get("marca_favicon_url"))
    dados["marca_cor_primaria"] = normalizar_cor_hex(dados.get("marca_cor_primaria"), "#facc15")
    dados["marca_cor_secundaria"] = normalizar_cor_hex(dados.get("marca_cor_secundaria"), "#111827")
    dados["marca_cor_fundo"] = normalizar_cor_hex(dados.get("marca_cor_fundo"), "#0b0b0b")
    dados["marca_cor_superficie"] = normalizar_cor_hex(dados.get("marca_cor_superficie"), "#111827")
    dados["marca_cor_texto"] = normalizar_cor_hex(dados.get("marca_cor_texto"), "#f9fafb")
    dados["site_titulo"] = normalizar_texto_campo(dados.get("site_titulo")) or "Gestao Estetica"
    dados["site_rodape_texto"] = normalizar_texto_campo(dados.get("site_rodape_texto")) or "Desenvolvido por Luiz Henrique | Qualquer Erro Contate o Desenvolvedor | Wagen Estetica Automotiva | Direitos Reservados."
    dados["login_titulo_publico"] = normalizar_texto_campo(dados.get("login_titulo_publico")) or "Acesso ao sistema"
    dados["login_subtitulo_publico"] = normalizar_texto_campo(dados.get("login_subtitulo_publico")) or "Entre no sistema"
    dados["login_botao_texto"] = normalizar_texto_campo(dados.get("login_botao_texto")) or "Entrar"
    dados["home_busca_placeholder"] = normalizar_texto_campo(dados.get("home_busca_placeholder")) or "Digite a placa"
    dados["home_busca_botao_texto"] = normalizar_texto_campo(dados.get("home_busca_botao_texto")) or "Buscar"
    dados["home_estado_inicial_titulo"] = normalizar_texto_campo(dados.get("home_estado_inicial_titulo")) or "Digite uma placa para comecar"
    dados["paginas_menu_desabilitadas_json"] = normalizar_texto_campo(dados.get("paginas_menu_desabilitadas_json")) or "[]"
    dados["auto_teste_ativo"] = bool(int(dados.get("auto_teste_ativo") or 0))
    dados["auto_teste_site_url"] = normalizar_texto_campo(dados.get("auto_teste_site_url")) or "https://wagenestetica.duckdns.org"
    dados["auto_teste_intervalo_horas"] = max(1, min(24, converter_inteiro(dados.get("auto_teste_intervalo_horas"), 2)))
    dados["auto_teste_telegram_bot_token"] = normalizar_texto_campo(dados.get("auto_teste_telegram_bot_token"))
    dados["auto_teste_telegram_bot_nick"] = normalizar_texto_campo(dados.get("auto_teste_telegram_bot_nick")) or "@wagenesteticabot"
    dados["auto_teste_telegram_chat_id"] = normalizar_texto_campo(dados.get("auto_teste_telegram_chat_id"))
    dados["auto_teste_ultimo_status"] = normalizar_texto_campo(dados.get("auto_teste_ultimo_status"))
    dados["auto_teste_ultimo_relatorio"] = normalizar_texto_campo(dados.get("auto_teste_ultimo_relatorio"))
    dados["auto_teste_ultimo_teste_em"] = normalizar_texto_campo(dados.get("auto_teste_ultimo_teste_em"))
    dados["auto_teste_ultimo_teste_em_fmt"] = formatar_datahora(dados.get("auto_teste_ultimo_teste_em")) if dados.get("auto_teste_ultimo_teste_em") else "Ainda nao testado"
    dados["auto_teste_telegram_token_configurado"] = bool(dados["auto_teste_telegram_bot_token"])
    dados["auto_teste_telegram_token_masked"] = mascarar_token_telegram(dados["auto_teste_telegram_bot_token"])
    CONFIG_EMPRESA_CACHE["testado_em"] = agora_cache_ts
    CONFIG_EMPRESA_CACHE["empresa_id"] = empresa_id_cache
    CONFIG_EMPRESA_CACHE["resultado"] = deepcopy(dados)
    return dados

def salvar_configuracao_versao_form(form):
    versao = normalizar_versao_sistema(form.get("versao_sistema"))
    salvar_campos_configuracao_empresa({
        "versao_sistema": versao,
    })
    limpar_cache_configuracao_empresa()

    return versao


def montar_configuracao_auto_teste_form(form):
    atual = obter_configuracao_empresa()
    token_informado = normalizar_texto_campo(form.get("auto_teste_telegram_bot_token"))
    remover_token = bool_config_ativo(form.get("remover_auto_teste_token"))
    token = "" if remover_token else (token_informado or atual.get("auto_teste_telegram_bot_token") or "")
    chat_id = normalizar_texto_campo(form.get("auto_teste_telegram_chat_id"))
    bot_nick = normalizar_bot_telegram(form.get("auto_teste_telegram_bot_nick"))
    site_url = normalizar_texto_campo(form.get("auto_teste_site_url")) or "https://wagenestetica.duckdns.org"
    intervalo = max(1, min(24, converter_inteiro(form.get("auto_teste_intervalo_horas"), 2)))

    if not site_url.startswith(("https://", "http://")):
        raise ValueError("Informe a URL completa do site, com https://.")

    return {
        "auto_teste_ativo": 1 if bool_config_ativo(form.get("auto_teste_ativo")) else 0,
        "auto_teste_site_url": site_url.rstrip("/"),
        "auto_teste_intervalo_horas": intervalo,
        "auto_teste_telegram_bot_token": token,
        "auto_teste_telegram_bot_nick": bot_nick,
        "auto_teste_telegram_chat_id": chat_id,
    }


def salvar_configuracao_auto_teste_form(form):
    payload = montar_configuracao_auto_teste_form(form)
    salvar_campos_configuracao_empresa(payload)
    limpar_cache_configuracao_empresa()
    return obter_configuracao_empresa(force=True)


def salvar_resultado_auto_teste(status, relatorio, chat_id=None):
    campos = {
        "auto_teste_ultimo_status": normalizar_texto_campo(status),
        "auto_teste_ultimo_relatorio": normalizar_texto_campo(relatorio),
        "auto_teste_ultimo_teste_em": agora_iso(),
    }
    if chat_id:
        campos["auto_teste_telegram_chat_id"] = normalizar_texto_campo(chat_id)
    salvar_campos_configuracao_empresa(campos)
    limpar_cache_configuracao_empresa()


def salvar_resultado_auto_teste_seguro(status, relatorio, chat_id=None):
    try:
        salvar_resultado_auto_teste(status, relatorio, chat_id=chat_id)
    except Exception as erro:
        log_info("ERRO AO SALVAR RESULTADO AUTO TESTE:", erro)


def run_site_checks_interno(timeout=5):
    rotas = [
        ("Site HTTPS", "/", {200, 302}),
        ("Login", "/login", {200}),
        ("Clientes", "/clientes", {200, 302}),
        ("Status do sistema", "/status-sistema", {200, 302}),
        ("Manifest PWA", "/site.webmanifest", {200}),
        ("Service Worker", "/sw.js", {200}),
        ("Status PWA", "/api/pwa/status", {200}),
    ]
    resultados = []

    with app.test_client() as client:
        for nome, caminho, status_esperados in rotas:
            inicio = time.perf_counter()
            try:
                resposta = client.get(caminho)
                elapsed_ms = int((time.perf_counter() - inicio) * 1000)
                status = int(resposta.status_code)
                mensagem = f"HTTP {status}"
                location = resposta.headers.get("Location")
                if location:
                    mensagem += f" -> {location}"
                resultados.append(
                    SiteMonitorCheckResult(
                        name=nome,
                        ok=status in status_esperados,
                        status=status,
                        elapsed_ms=elapsed_ms,
                        message=mensagem,
                    )
                )
            except Exception as erro:
                elapsed_ms = int((time.perf_counter() - inicio) * 1000)
                resultados.append(
                    SiteMonitorCheckResult(
                        name=nome,
                        ok=False,
                        status=None,
                        elapsed_ms=elapsed_ms,
                        message=str(erro),
                    )
                )

    return resultados


def check_auto_teste_banco_online():
    inicio = time.perf_counter()
    try:
        status = obter_status_banco_online()
        elapsed_ms = int((time.perf_counter() - inicio) * 1000)
        conectado = bool(status.get("conectado"))
        mensagem = status.get("mensagem") or status.get("backend_label") or "Banco online nao validado."
        return SiteMonitorCheckResult(
            name="Banco online",
            ok=conectado,
            status=200 if conectado else None,
            elapsed_ms=elapsed_ms,
            message=mensagem,
        )
    except Exception as erro:
        elapsed_ms = int((time.perf_counter() - inicio) * 1000)
        registrar_ultimo_erro_producao(erro, descricao="auto_teste_banco_online")
        return SiteMonitorCheckResult(
            name="Banco online",
            ok=False,
            status=None,
            elapsed_ms=elapsed_ms,
            message=str(erro),
        )


def executar_auto_teste_site(configuracao=None, enviar_telegram=True):
    configuracao = configuracao or obter_configuracao_empresa(force=True)
    site_url = configuracao.get("auto_teste_site_url") or "https://wagenestetica.duckdns.org"
    token = normalizar_texto_campo(configuracao.get("auto_teste_telegram_bot_token"))
    chat_id = normalizar_texto_campo(configuracao.get("auto_teste_telegram_chat_id"))

    if has_request_context():
        resultados = run_site_checks_interno()
    else:
        resultados = run_site_checks(site_url, 6)
    resultados.append(check_auto_teste_banco_online())
    relatorio = build_site_monitor_report(site_url, resultados)
    status = "ok" if all(item.ok for item in resultados) else "falha"

    if enviar_telegram:
        if not token:
            raise ValueError("Informe o token do bot do Telegram.")
        chat_id = resolver_chat_id_telegram(token, chat_id)
        if not chat_id:
            raise ValueError("Nao encontrei o chat_id. Envie /start para o bot no Telegram e teste novamente.")
        send_site_monitor_telegram_message(token, chat_id, relatorio, 15)

    salvar_resultado_auto_teste_seguro(status, relatorio, chat_id=chat_id)
    return {
        "status": status,
        "ok": status == "ok",
        "relatorio": relatorio,
        "chat_id": chat_id,
    }


def auto_teste_deve_executar(configuracao):
    if not configuracao.get("auto_teste_ativo"):
        return False
    if not configuracao.get("auto_teste_telegram_bot_token"):
        return False

    ultimo = normalizar_texto_campo(configuracao.get("auto_teste_ultimo_teste_em"))
    if not ultimo:
        return True

    try:
        ultimo_dt = datetime.fromisoformat(ultimo)
    except Exception:
        return True

    intervalo_horas = max(1, min(24, converter_inteiro(configuracao.get("auto_teste_intervalo_horas"), 2)))
    return (agora() - ultimo_dt).total_seconds() >= intervalo_horas * 3600


def loop_worker_auto_teste():
    primeira_execucao = True
    while True:
        if primeira_execucao:
            primeira_execucao = False
            time.sleep(WORKER_AUTO_TESTE_DELAY_INICIAL)

        if auto_teste_lock.acquire(blocking=False):
            try:
                configuracao = obter_configuracao_empresa(force=True)
                if auto_teste_deve_executar(configuracao):
                    resultado = executar_auto_teste_site(configuracao, enviar_telegram=True)
                    log_info(f"AUTO TESTE: {resultado['status']}")
            except Exception as erro:
                salvar_resultado_auto_teste_seguro("erro", f"Erro no auto teste: {erro}")
                log_info("ERRO WORKER AUTO TESTE:", erro)
            finally:
                auto_teste_lock.release()

        time.sleep(300)


def iniciar_worker_auto_teste():
    global auto_teste_worker_iniciado

    if auto_teste_worker_iniciado:
        return

    auto_teste_worker_iniciado = True
    Thread(target=loop_worker_auto_teste, daemon=True).start()


def limpar_cache_clima():
    CLIMA_CACHE["testado_em"] = 0.0
    CLIMA_CACHE["resultado"] = None


def normalizar_url_clima_api(url):
    url = normalizar_texto_campo(url) or "https://api.open-meteo.com/v1/forecast"
    try:
        partes = urlparse(url)
    except Exception:
        return url

    if "open-meteo.com" not in normalizar_texto_comparacao(partes.netloc):
        return url

    caminho = partes.path or "/v1/forecast"
    params = parse_qs(partes.query or "", keep_blank_values=True)

    params["latitude"] = ["{latitude}"]
    params["longitude"] = ["{longitude}"]
    params["timezone"] = ["{timezone}"]

    current_vals = [valor for valor in params.get("current", []) if valor]
    if current_vals:
        current_itens = []
        for valor in current_vals:
            current_itens.extend([item.strip() for item in str(valor).split(",") if item.strip()])
    else:
        current_itens = []

    for item in ("temperature_2m", "weather_code"):
        if item not in current_itens:
            current_itens.append(item)

    params["current"] = [",".join(current_itens)]
    params.pop("current_weather", None)

    query = urlencode(params, doseq=True)
    query = (
        query
        .replace("%7Blatitude%7D", "{latitude}")
        .replace("%7Blongitude%7D", "{longitude}")
        .replace("%7Btimezone%7D", "{timezone}")
    )
    return urlunparse((
        partes.scheme or "https",
        partes.netloc or "api.open-meteo.com",
        caminho,
        "",
        query,
        "",
    ))


def obter_valor_json_caminho(payload, caminho):
    atual = payload

    for parte in caminho:
        if isinstance(parte, int):
            if not isinstance(atual, list) or parte >= len(atual):
                return None
            atual = atual[parte]
            continue

        if not isinstance(atual, dict):
            return None
        atual = atual.get(parte)

    return atual


def converter_temperatura_clima(valor):
    if valor is None or valor == "":
        return None

    try:
        return round(float(str(valor).replace(",", ".")), 1)
    except Exception:
        return None


def extrair_temperatura_clima(payload):
    caminhos = [
        ("current", "temperature_2m"),
        ("current", "temperature"),
        ("current", "temp_c"),
        ("current", "temp"),
        ("current_weather", "temperature"),
        ("main", "temp"),
        ("temperature",),
        ("temp",),
        ("data", "temperature"),
        ("data", "temp"),
        ("currently", "temperature"),
        ("current_observation", "temp_c"),
    ]

    for caminho in caminhos:
        temperatura = converter_temperatura_clima(obter_valor_json_caminho(payload, caminho))
        if temperatura is not None:
            return temperatura

    hourly = payload.get("hourly") if isinstance(payload, dict) else {}
    if isinstance(hourly, dict):
        for chave in ("temperature_2m", "temperature", "temp"):
            valores = hourly.get(chave)
            if isinstance(valores, list) and valores:
                temperatura = converter_temperatura_clima(valores[0])
                if temperatura is not None:
                    return temperatura

    return None


def extrair_codigo_clima(payload):
    caminhos = [
        ("current", "weather_code"),
        ("current_weather", "weathercode"),
        ("current", "weathercode"),
        ("weather_code",),
        ("weathercode",),
        ("data", "weather_code"),
        ("weather", 0, "id"),
        ("current", "condition", "code"),
    ]

    for caminho in caminhos:
        valor = obter_valor_json_caminho(payload, caminho)
        try:
            if valor is not None and valor != "":
                return int(float(valor))
        except Exception:
            continue

    hourly = payload.get("hourly") if isinstance(payload, dict) else {}
    if isinstance(hourly, dict):
        for chave in ("weather_code", "weathercode"):
            valores = hourly.get(chave)
            if isinstance(valores, list) and valores:
                try:
                    return int(float(valores[0]))
                except Exception:
                    pass

    return None


def extrair_texto_clima(payload):
    caminhos = [
        ("current", "condition", "text"),
        ("current", "weather", "description"),
        ("weather", 0, "main"),
        ("weather", 0, "description"),
        ("current", "summary"),
        ("current_weather", "summary"),
        ("description",),
        ("summary",),
        ("condition",),
        ("data", "condition"),
        ("data", "summary"),
    ]

    for caminho in caminhos:
        valor = normalizar_texto_campo(obter_valor_json_caminho(payload, caminho))
        if valor:
            return valor

    return ""


def montar_resultado_clima_normalizado(payload):
    if not isinstance(payload, dict):
        return None

    temperatura = extrair_temperatura_clima(payload)
    codigo = extrair_codigo_clima(payload)
    texto = extrair_texto_clima(payload)
    texto_cmp = normalizar_texto_comparacao(texto)

    if temperatura is None and codigo is None and not texto:
        return None

    if any(termo in texto_cmp for termo in ("chuva", "rain", "storm", "drizzle", "shower", "trovo")) or (codigo is not None and codigo >= 61):
        icone = "\U0001F327\uFE0F"
        clima = texto or "Chuva"
        sugestao = "\U0001F4A1 Lavagem interna"
    elif any(termo in texto_cmp for termo in ("nublado", "cloud", "overcast", "encoberto")) or (codigo is not None and 4 <= codigo < 61):
        icone = "\u2601\uFE0F"
        clima = texto or "Nublado"
        sugestao = "\U0001F4A1 Lavagem simples"
    elif any(termo in texto_cmp for termo in ("sol", "sun", "clear", "limpo", "ensolar")) or (codigo is not None and codigo <= 3):
        icone = "\u2600\uFE0F"
        clima = texto or "Tempo limpo"
        sugestao = "\U0001F4A1 Lavagem completa"
    elif any(termo in texto_cmp for termo in ("fog", "mist", "nebl", "haze")):
        icone = "\U0001F32B\uFE0F"
        clima = texto or "Neblina"
        sugestao = "\U0001F4A1 Consulte a previsao completa."
    else:
        icone = "\u26C5"
        clima = texto or "Clima carregado"
        sugestao = "\U0001F4A1 Consulte a previsao completa."

    return {
        "clima": clima,
        "temp": temperatura if temperatura is not None else "--",
        "icone": icone,
        "sugestao": sugestao,
    }


def obter_resultado_clima_api(permitir_rede=True):
    configuracao = obter_configuracao_empresa()
    fallback = {
        "clima": "Clima indisponivel",
        "temp": "--",
        "icone": "\u26A0\uFE0F",
        "sugestao": "\U0001F4A1 Consulte o radar do clima.",
    }

    if not configuracao.get("clima_ativo"):
        return {
            "clima": "Clima desativado",
            "temp": "--",
            "icone": "\u23F8\uFE0F",
            "sugestao": "\U0001F4A1 Ative o clima nas configuracoes.",
        }

    agora_ts = time.time()
    cache = CLIMA_CACHE.get("resultado")
    ultimo_teste = float(CLIMA_CACHE.get("testado_em") or 0.0)

    if cache and agora_ts - ultimo_teste < CLIMA_CACHE_TTL:
        return dict(cache)

    if not permitir_rede:
        return {
            "clima": "Carregando clima",
            "temp": "",
            "icone": "\U0001F324\uFE0F",
            "sugestao": "",
            "deferido": True,
        }

    try:
        sessao_http = requests.Session()
        sessao_http.trust_env = False
        headers = {
            "User-Agent": "WagenEstetica/1.0",
            "Accept": "application/json",
        }
        clima_api_url = normalizar_url_clima_api(configuracao.get("clima_api_url"))
        clima_latitude = configuracao.get("clima_latitude")
        clima_longitude = configuracao.get("clima_longitude")
        clima_timezone = quote(
            normalizar_texto_campo(configuracao.get("clima_timezone")) or "America/Sao_Paulo",
            safe="",
        )
        timeout_segundos = max(
            3,
            min(20, converter_inteiro(configuracao.get("clima_timeout_segundos"), 8)),
        )
        url_configurada = (
            clima_api_url
            .replace("{latitude}", str(clima_latitude))
            .replace("{longitude}", str(clima_longitude))
            .replace("{timezone}", clima_timezone)
        )
        urls = []
        if url_configurada:
            urls.append(url_configurada)

        urls.extend(
            [
                (
                    "https://api.open-meteo.com/v1/forecast"
                    f"?latitude={clima_latitude}&longitude={clima_longitude}"
                    "&current=temperature_2m,weather_code"
                    "&hourly=temperature_2m"
                    f"&timezone={clima_timezone}"
                    "&forecast_days=1"
                ),
                (
                    "https://api.open-meteo.com/v1/forecast"
                    f"?latitude={clima_latitude}&longitude={clima_longitude}"
                    "&current_weather=true"
                    f"&timezone={clima_timezone}"
                    "&forecast_days=1"
                ),
            ]
        )
        urls_unicas = []
        vistos = set()
        for item_url in urls:
            texto_url = str(item_url or "").strip()
            if not texto_url or texto_url in vistos:
                continue
            vistos.add(texto_url)
            urls_unicas.append(texto_url)

        resultado = None
        for url in urls_unicas:
            try:
                resposta = sessao_http.get(url, timeout=timeout_segundos, headers=headers)
            except Exception:
                continue

            if resposta.status_code != 200:
                continue

            corpo = (resposta.text or "").strip()
            if not corpo:
                continue

            try:
                payload = resposta.json()
            except Exception:
                continue

            if not isinstance(payload, dict) or payload.get("error"):
                continue

            resultado = montar_resultado_clima_normalizado(payload)
            if resultado:
                break

        if not resultado:
            if cache:
                return dict(cache)
            return fallback

        CLIMA_CACHE["testado_em"] = agora_ts
        CLIMA_CACHE["resultado"] = dict(resultado)
        return resultado

    except Exception as e:
        log_info("ERRO CLIMA:", e)
        if cache:
            return dict(cache)
        return fallback


def salvar_configuracao_clima_form(form):
    clima_ativo = 1 if form.get("clima_ativo") else 0
    clima_api_url = normalizar_texto_campo(form.get("clima_api_url")) or "https://api.open-meteo.com/v1/forecast"
    clima_local_label = normalizar_texto_campo(form.get("clima_local_label")) or "Cachoeirinha / RS"
    clima_timezone = normalizar_texto_campo(form.get("clima_timezone")) or "America/Sao_Paulo"
    clima_timeout_segundos = max(3, min(20, converter_inteiro(form.get("clima_timeout_segundos"), 8)))

    try:
        clima_latitude = float(str(form.get("clima_latitude") or "").replace(",", "."))
        clima_longitude = float(str(form.get("clima_longitude") or "").replace(",", "."))
    except Exception as e:
        raise ValueError("Informe latitude e longitude validas para o clima.") from e

    salvar_campos_configuracao_empresa({
        "clima_ativo": clima_ativo,
        "clima_api_url": clima_api_url,
        "clima_local_label": clima_local_label,
        "clima_latitude": clima_latitude,
        "clima_longitude": clima_longitude,
        "clima_timezone": clima_timezone,
        "clima_timeout_segundos": clima_timeout_segundos,
    })
    limpar_cache_clima()
    limpar_caches_interface()

    return {
        "clima_ativo": bool(clima_ativo),
        "clima_api_url": clima_api_url,
        "clima_local_label": clima_local_label,
        "clima_latitude": clima_latitude,
        "clima_longitude": clima_longitude,
        "clima_timezone": clima_timezone,
        "clima_timeout_segundos": clima_timeout_segundos,
    }


def salvar_configuracao_site_form(form, files):
    atual = obter_configuracao_empresa()
    remover_logo = bool_config_ativo(form.get("remover_logo"))
    remover_favicon = bool_config_ativo(form.get("remover_favicon"))
    logo_info = preparar_logo_site_upload(files.get("marca_logo"))
    favicon_info = preparar_favicon_site_upload(files.get("marca_favicon"))

    marca_nome = normalizar_texto_campo(form.get("marca_nome")) or "Wagen Estetica Automotiva"
    marca_subtitulo = normalizar_texto_campo(form.get("marca_subtitulo")) or "Gestao Estetica"
    site_titulo = normalizar_texto_campo(form.get("site_titulo")) or "Gestao Estetica"
    site_rodape_texto = (
        normalizar_texto_campo(form.get("site_rodape_texto"))
        or f"Desenvolvido por Luiz Henrique | Qualquer Erro Contate o Desenvolvedor | {marca_nome} | Direitos Reservados."
    )
    marca_logo_url = normalizar_texto_campo(form.get("marca_logo_url"))
    marca_favicon_url = normalizar_texto_campo(form.get("marca_favicon_url"))
    marca_cor_primaria = normalizar_cor_hex(form.get("marca_cor_primaria"), "#facc15")
    marca_cor_secundaria = normalizar_cor_hex(form.get("marca_cor_secundaria"), "#111827")
    marca_cor_fundo = normalizar_cor_hex(form.get("marca_cor_fundo"), "#0b0b0b")
    marca_cor_superficie = normalizar_cor_hex(form.get("marca_cor_superficie"), "#111827")
    marca_cor_texto = normalizar_cor_hex(form.get("marca_cor_texto"), "#f9fafb")
    login_titulo_publico = normalizar_texto_campo(form.get("login_titulo_publico")) or "Acesso ao sistema"
    login_subtitulo_publico = normalizar_texto_campo(form.get("login_subtitulo_publico")) or "Entre no sistema"
    login_botao_texto = normalizar_texto_campo(form.get("login_botao_texto")) or "Entrar"
    home_busca_placeholder = normalizar_texto_campo(form.get("home_busca_placeholder")) or "Digite a placa"
    home_busca_botao_texto = normalizar_texto_campo(form.get("home_busca_botao_texto")) or "Buscar"
    home_estado_inicial_titulo = normalizar_texto_campo(form.get("home_estado_inicial_titulo")) or "Digite uma placa para comecar"
    whitelabel_ativo = 1 if bool_config_ativo(form.get("whitelabel_ativo")) else 0

    logo_blob = atual.get("marca_logo_blob")
    logo_mime_type = atual.get("marca_logo_mime_type")
    logo_arquivo_nome = atual.get("marca_logo_arquivo_nome")
    favicon_blob = atual.get("marca_favicon_blob")
    favicon_mime_type = atual.get("marca_favicon_mime_type")
    favicon_arquivo_nome = atual.get("marca_favicon_arquivo_nome")

    if remover_logo:
        logo_blob = None
        logo_mime_type = ""
        logo_arquivo_nome = ""
        marca_logo_url = ""

    if remover_favicon:
        favicon_blob = None
        favicon_mime_type = ""
        favicon_arquivo_nome = ""
        marca_favicon_url = ""

    if logo_info:
        logo_blob = logo_info.get("arquivo_blob")
        logo_mime_type = logo_info.get("mime_type")
        logo_arquivo_nome = logo_info.get("arquivo_nome")

    if favicon_info:
        favicon_blob = favicon_info.get("arquivo_blob")
        favicon_mime_type = favicon_info.get("mime_type")
        favicon_arquivo_nome = favicon_info.get("arquivo_nome")

    salvar_campos_configuracao_empresa({
        "marca_nome": marca_nome,
        "marca_subtitulo": marca_subtitulo,
        "marca_logo_url": marca_logo_url,
        "marca_logo_blob": logo_blob,
        "marca_logo_mime_type": logo_mime_type,
        "marca_logo_arquivo_nome": logo_arquivo_nome,
        "marca_favicon_url": marca_favicon_url,
        "marca_favicon_blob": favicon_blob,
        "marca_favicon_mime_type": favicon_mime_type,
        "marca_favicon_arquivo_nome": favicon_arquivo_nome,
        "marca_cor_primaria": marca_cor_primaria,
        "marca_cor_secundaria": marca_cor_secundaria,
        "marca_cor_fundo": marca_cor_fundo,
        "marca_cor_superficie": marca_cor_superficie,
        "marca_cor_texto": marca_cor_texto,
        "site_titulo": site_titulo,
        "site_rodape_texto": site_rodape_texto,
        "login_titulo_publico": login_titulo_publico,
        "login_subtitulo_publico": login_subtitulo_publico,
        "login_botao_texto": login_botao_texto,
        "home_busca_placeholder": home_busca_placeholder,
        "home_busca_botao_texto": home_busca_botao_texto,
        "home_estado_inicial_titulo": home_estado_inicial_titulo,
        "whitelabel_ativo": whitelabel_ativo,
    })
    limpar_caches_interface()

    return {
        "marca_nome": marca_nome,
        "marca_subtitulo": marca_subtitulo,
        "site_titulo": site_titulo,
        "site_rodape_texto": site_rodape_texto,
        "marca_logo_url": marca_logo_url,
        "marca_logo_arquivo_nome": logo_arquivo_nome,
        "marca_favicon_url": marca_favicon_url,
        "marca_favicon_arquivo_nome": favicon_arquivo_nome,
        "marca_cor_primaria": marca_cor_primaria,
        "marca_cor_secundaria": marca_cor_secundaria,
        "marca_cor_fundo": marca_cor_fundo,
        "marca_cor_superficie": marca_cor_superficie,
        "marca_cor_texto": marca_cor_texto,
        "login_titulo_publico": login_titulo_publico,
        "login_subtitulo_publico": login_subtitulo_publico,
        "login_botao_texto": login_botao_texto,
        "home_busca_placeholder": home_busca_placeholder,
        "home_busca_botao_texto": home_busca_botao_texto,
        "home_estado_inicial_titulo": home_estado_inicial_titulo,
        "whitelabel_ativo": bool(whitelabel_ativo),
        "tem_logo_blob": bool(logo_blob),
        "tem_favicon_blob": bool(favicon_blob),
    }

def salvar_configuracao_empresa_form(form):
    salvar_campos_configuracao_empresa({
        "razao_social": normalizar_texto_campo(form.get("razao_social")),
        "nome_fantasia": normalizar_texto_campo(form.get("nome_fantasia")),
        "cnpj": normalizar_documento_fiscal(form.get("cnpj")),
        "inscricao_municipal": normalizar_texto_campo(form.get("inscricao_municipal")),
        "inscricao_estadual": normalizar_texto_campo(form.get("inscricao_estadual")),
        "regime_tributario": normalizar_texto_campo(form.get("regime_tributario")),
        "email": normalizar_texto_campo(form.get("email")),
        "telefone": normalizar_texto_campo(form.get("telefone")),
        "endereco": normalizar_texto_campo(form.get("endereco")),
        "numero": normalizar_texto_campo(form.get("numero")),
        "complemento": normalizar_texto_campo(form.get("complemento")),
        "bairro": normalizar_texto_campo(form.get("bairro")),
        "cidade": normalizar_texto_campo(form.get("cidade")),
        "uf": normalizar_texto_campo(form.get("uf")).upper()[:2],
        "cep": re.sub(r"\D", "", str(form.get("cep") or "")),
        "codigo_servico_padrao": normalizar_texto_campo(form.get("codigo_servico_padrao")),
        "aliquota_padrao": converter_valor_numerico(form.get("aliquota_padrao")),
    })

def integracao_fiscal_padrao():
    return {
        "id": 1,
        "empresa_id": 1,
        "tipo_integracao": "manual",
        "provedor_nome": "",
        "ambiente": "homologacao",
        "municipio_codigo_ibge": "",
        "municipio_nome": "",
        "uf": "",
        "endpoint_emissao": "",
        "endpoint_consulta": "",
        "endpoint_cancelamento": "",
        "autenticacao_tipo": "nenhuma",
        "usuario_api": "",
        "senha_api": "",
        "client_id": "",
        "client_secret": "",
        "token_api": "",
        "token_url": "",
        "certificado_tipo": "nenhum",
        "certificado_arquivo": "",
        "certificado_senha": "",
        "serie_rps": "",
        "serie_nfe": "",
        "ativo": False,
        "ultimo_status": "",
        "ultima_mensagem": "",
        "atualizado_em": "",
    }

def obter_configuracao_integracao_fiscal():
    def carregar(conn):
        c = conn.cursor()
        return selecionar_registro_administrativo_empresa_cursor(
            c,
            "integracao_fiscal",
            empresa_atual_id(),
        )

    item = executar_leitura_resiliente(
        carregar,
        descricao="CONFIG INTEGRACAO FISCAL",
        padrao=None,
    )

    dados = integracao_fiscal_padrao()

    if item:
        dados.update(dict(item))

    dados["ativo"] = bool(dados.get("ativo"))
    dados["tipo_integracao_label"] = next(
        (item["label"] for item in TIPOS_INTEGRACAO_FISCAL if item["value"] == dados.get("tipo_integracao")),
        "Manual / sem integracao",
    )
    dados["autenticacao_tipo_label"] = next(
        (item["label"] for item in AUTENTICACAO_INTEGRACAO_FISCAL if item["value"] == dados.get("autenticacao_tipo")),
        "Sem autenticacao",
    )
    dados["certificado_tipo_label"] = next(
        (item["label"] for item in TIPOS_CERTIFICADO_FISCAL if item["value"] == dados.get("certificado_tipo")),
        "Sem certificado",
    )
    return dados

def salvar_configuracao_integracao_fiscal_form(form):
    salvar_campos_registro_administrativo_empresa(
        "integracao_fiscal",
        {
            "tipo_integracao": normalizar_texto_campo(form.get("tipo_integracao")) or "manual",
            "provedor_nome": normalizar_texto_campo(form.get("provedor_nome")),
            "ambiente": normalizar_texto_campo(form.get("ambiente")) or "homologacao",
            "municipio_codigo_ibge": re.sub(r"\D", "", str(form.get("municipio_codigo_ibge") or "")),
            "municipio_nome": normalizar_texto_campo(form.get("municipio_nome")),
            "uf": normalizar_texto_campo(form.get("uf")).upper()[:2],
            "endpoint_emissao": normalizar_texto_campo(form.get("endpoint_emissao")),
            "endpoint_consulta": normalizar_texto_campo(form.get("endpoint_consulta")),
            "endpoint_cancelamento": normalizar_texto_campo(form.get("endpoint_cancelamento")),
            "autenticacao_tipo": normalizar_texto_campo(form.get("autenticacao_tipo")) or "nenhuma",
            "usuario_api": normalizar_texto_campo(form.get("usuario_api")),
            "senha_api": normalizar_texto_campo(form.get("senha_api")),
            "client_id": normalizar_texto_campo(form.get("client_id")),
            "client_secret": normalizar_texto_campo(form.get("client_secret")),
            "token_api": normalizar_texto_campo(form.get("token_api")),
            "token_url": normalizar_texto_campo(form.get("token_url")),
            "certificado_tipo": normalizar_texto_campo(form.get("certificado_tipo")) or "nenhum",
            "certificado_arquivo": normalizar_texto_campo(form.get("certificado_arquivo")),
            "certificado_senha": normalizar_texto_campo(form.get("certificado_senha")),
            "serie_rps": normalizar_texto_campo(form.get("serie_rps")),
            "serie_nfe": normalizar_texto_campo(form.get("serie_nfe")),
            "ativo": 1 if form.get("ativo") else 0,
            "ultimo_status": normalizar_texto_campo(form.get("ultimo_status")),
            "ultima_mensagem": normalizar_texto_campo(form.get("ultima_mensagem")),
            "atualizado_em": agora_iso(),
        },
    )

def avaliar_prontidao_integracao_fiscal(empresa, integracao):
    empresa = empresa or empresa_snapshot_padrao()
    integracao = integracao or integracao_fiscal_padrao()
    faltantes = []
    avisos = []
    tipo = integracao.get("tipo_integracao") or "manual"
    autenticacao = integracao.get("autenticacao_tipo") or "nenhuma"
    certificado = integracao.get("certificado_tipo") or "nenhum"

    if not empresa_possui_dados_fiscais(empresa):
        faltantes.append("Razao social e CNPJ do emitente")

    if not empresa.get("codigo_servico_padrao"):
        avisos.append("Definir codigo de servico padrao ajuda no mapeamento fiscal.")

    if tipo == "manual":
        avisos.append("Modo manual ativo. A estrutura esta preparada, mas sem transmissao automatica.")
    else:
        if not integracao.get("ativo"):
            faltantes.append("Ativar a integracao fiscal")

        if not integracao.get("ambiente"):
            faltantes.append("Escolher ambiente de homologacao ou producao")

        if tipo in {"nfse_nacional", "prefeitura_api"}:
            if not integracao.get("municipio_codigo_ibge"):
                faltantes.append("Codigo IBGE do municipio emissor")
            if not integracao.get("uf"):
                faltantes.append("UF do municipio emissor")
            if not integracao.get("serie_rps"):
                avisos.append("Definir serie do RPS deixa a emissao municipal pronta para conversao.")

        if tipo == "prefeitura_api":
            if not integracao.get("provedor_nome"):
                faltantes.append("Nome do provedor ou prefeitura")
            if not integracao.get("endpoint_emissao"):
                faltantes.append("Endpoint de emissao da prefeitura")

        if tipo == "nfse_nacional":
            avisos.append(
                "No padrao nacional, valide as parametrizacoes do municipio "
                "e o fluxo de token/certificado antes de ativar."
            )

        if tipo == "nfe_sefaz":
            if not empresa.get("inscricao_estadual"):
                faltantes.append("Inscricao estadual do emitente")
            if not integracao.get("uf"):
                faltantes.append("UF vinculada a SEFAZ")
            if not integracao.get("serie_nfe"):
                faltantes.append("Serie da NF-e")
            avisos.append("NF-e normalmente exige credenciamento e certificado ICP-Brasil.")

        if autenticacao == "basic":
            if not integracao.get("usuario_api"):
                faltantes.append("Usuario da API")
            if not integracao.get("senha_api"):
                faltantes.append("Senha da API")
        elif autenticacao == "oauth2":
            if not integracao.get("token_url"):
                faltantes.append("URL de token OAuth2")
            if not integracao.get("client_id"):
                faltantes.append("Client ID")
            if not integracao.get("client_secret"):
                faltantes.append("Client Secret")
        elif autenticacao == "token":
            if not integracao.get("token_api"):
                faltantes.append("Token ou chave de API")

        if certificado == "a1":
            if not integracao.get("certificado_arquivo"):
                faltantes.append("Arquivo/referencia do certificado A1")
            if not integracao.get("certificado_senha"):
                faltantes.append("Senha do certificado A1")
        elif certificado == "a3":
            avisos.append("Certificado A3 exige middleware/dispositivo no ambiente onde a integracao rodar.")

    pronta = not faltantes and tipo != "manual"
    if pronta:
        status = "Pronta para homologacao"
    elif tipo == "manual":
        status = "Modo manual"
    else:
        status = "Configuracao incompleta"

    return {
        "status": status,
        "pronta": pronta,
        "faltantes": faltantes,
        "avisos": avisos,
    }

def montar_payload_exemplo_integracao(empresa, integracao, prefill=None):
    empresa = empresa or empresa_snapshot_padrao()
    integracao = integracao or integracao_fiscal_padrao()
    prefill = prefill or {}
    itens = enriquecer_itens_documento(prefill.get("itens") or [
        {"descricao": "Lavagem completa", "quantidade": 1, "valor_unitario": 60.0, "valor_total": 60.0},
    ])

    return {
        "tipo_integracao": integracao.get("tipo_integracao"),
        "ambiente": integracao.get("ambiente"),
        "emitente": {
            "razao_social": empresa.get("razao_social"),
            "cnpj": empresa.get("cnpj"),
            "inscricao_municipal": empresa.get("inscricao_municipal"),
            "inscricao_estadual": empresa.get("inscricao_estadual"),
            "municipio_codigo_ibge": integracao.get("municipio_codigo_ibge"),
            "uf": integracao.get("uf") or empresa.get("uf"),
        },
        "tomador": {
            "nome": prefill.get("cliente_nome", "Cliente exemplo"),
            "documento": normalizar_documento_fiscal(prefill.get("cliente_documento", "")),
            "email": prefill.get("email", ""),
            "telefone": prefill.get("telefone", ""),
        },
        "documento": {
            "codigo_servico": prefill.get("codigo_servico") or empresa.get("codigo_servico_padrao"),
            "serie_rps": integracao.get("serie_rps"),
            "serie_nfe": integracao.get("serie_nfe"),
            "aliquota_iss": converter_valor_numerico(prefill.get("aliquota_iss") or empresa.get("aliquota_padrao")),
        },
        "itens": [
            {
                "descricao": item.get("descricao"),
                "quantidade": item.get("quantidade"),
                "valor_unitario": item.get("valor_unitario"),
                "valor_total": item.get("valor_total"),
            }
            for item in itens
        ],
        "endpoints": {
            "emissao": integracao.get("endpoint_emissao"),
            "consulta": integracao.get("endpoint_consulta"),
            "cancelamento": integracao.get("endpoint_cancelamento"),
        },
        "autenticacao": {
            "tipo": integracao.get("autenticacao_tipo"),
            "token_url": integracao.get("token_url"),
            "possui_client_id": bool(integracao.get("client_id")),
            "possui_token_api": bool(integracao.get("token_api")),
            "certificado_tipo": integracao.get("certificado_tipo"),
        },
    }

def serializar_empresa_snapshot(empresa):
    return json.dumps(empresa or empresa_snapshot_padrao(), ensure_ascii=True)

def desserializar_empresa_snapshot(valor):
    dados = empresa_snapshot_padrao()

    if not valor:
        return dados

    try:
        carregado = json.loads(valor)
        if isinstance(carregado, dict):
            dados.update(carregado)
    except Exception:
        pass

    dados["cnpj_formatado"] = formatar_documento_fiscal(dados.get("cnpj"))
    dados["cep_formatado"] = formatar_cep(dados.get("cep"))
    dados["aliquota_padrao"] = converter_valor_numerico(dados.get("aliquota_padrao"))
    return dados

def montar_empresa_snapshot(empresa):
    base = empresa_snapshot_padrao()
    base.update(empresa or {})
    base["cnpj"] = normalizar_documento_fiscal(base.get("cnpj"))
    base["cep"] = re.sub(r"\D", "", str(base.get("cep") or ""))
    base["aliquota_padrao"] = converter_valor_numerico(base.get("aliquota_padrao"))
    return base

def empresa_possui_dados_fiscais(empresa):
    return bool(normalizar_texto_campo((empresa or {}).get("razao_social"))) and bool(
        normalizar_documento_fiscal((empresa or {}).get("cnpj"))
    )

def montar_endereco_empresa(empresa):
    empresa = empresa or {}
    primeira_linha = " ".join(
        parte for parte in [
            normalizar_texto_campo(empresa.get("endereco")),
            normalizar_texto_campo(empresa.get("numero")),
        ]
        if parte
    )

    if empresa.get("complemento"):
        primeira_linha = (
            f"{primeira_linha} - {empresa['complemento']}"
            if primeira_linha else empresa["complemento"]
        )

    segunda_linha = " - ".join(
        parte for parte in [
            normalizar_texto_campo(empresa.get("bairro")),
            " ".join(
                parte
                for parte in [
                    normalizar_texto_campo(empresa.get("cidade")),
                    normalizar_texto_campo(empresa.get("uf")).upper(),
                ]
                if parte
            ).strip(),
            formatar_cep(empresa.get("cep")),
        ]
        if parte
    )

    return [linha for linha in [primeira_linha, segunda_linha] if linha]

def proximo_numero_documento_sql(cursor, tabela, campo):
    cursor.execute(f"SELECT MAX({campo}) FROM {tabela}")
    atual = cursor.fetchone()[0]
    return int(atual or 0) + 1

def extrair_itens_formulario(form, prefixo="item"):
    descricoes = form.getlist(f"{prefixo}_descricao[]")
    quantidades = form.getlist(f"{prefixo}_quantidade[]")
    valores_unitarios = form.getlist(f"{prefixo}_valor_unitario[]")
    tamanho = max(len(descricoes), len(quantidades), len(valores_unitarios), 0)
    itens = []

    for indice in range(tamanho):
        descricao = normalizar_texto_campo(descricoes[indice] if indice < len(descricoes) else "")
        quantidade = converter_valor_numerico(quantidades[indice] if indice < len(quantidades) else 1)
        valor_unitario = converter_valor_numerico(
            valores_unitarios[indice] if indice < len(valores_unitarios) else 0
        )

        if not descricao and quantidade <= 0 and valor_unitario <= 0:
            continue

        quantidade = quantidade if quantidade > 0 else 1
        descricao = descricao or "Servico"
        valor_total = round(quantidade * valor_unitario, 2)
        itens.append({
            "ordem": indice + 1,
            "descricao": descricao,
            "quantidade": quantidade,
            "valor_unitario": valor_unitario,
            "valor_total": valor_total,
            "quantidade_exibicao": (
                str(int(quantidade)) if float(quantidade).is_integer() else str(quantidade).replace(".", ",")
            ),
            "valor_unitario_exibicao": formatar_valor_brl(valor_unitario),
            "valor_total_exibicao": formatar_valor_brl(valor_total),
        })

    return itens

def enriquecer_itens_documento(itens):
    resultado = []

    for indice, item in enumerate(itens or [], start=1):
        quantidade = converter_valor_numerico(item.get("quantidade"))
        valor_unitario = converter_valor_numerico(item.get("valor_unitario"))
        valor_total = converter_valor_numerico(item.get("valor_total") or (quantidade * valor_unitario))
        item_dict = dict(item)
        item_dict["ordem"] = item.get("ordem") or indice
        item_dict["descricao"] = normalizar_texto_campo(item.get("descricao")) or "Servico"
        item_dict["quantidade"] = quantidade if quantidade > 0 else 1
        item_dict["valor_unitario"] = valor_unitario
        item_dict["valor_total"] = valor_total
        item_dict["quantidade_exibicao"] = (
            str(int(item_dict["quantidade"]))
            if float(item_dict["quantidade"]).is_integer()
            else str(item_dict["quantidade"]).replace(".", ",")
        )
        item_dict["valor_unitario_exibicao"] = formatar_valor_brl(item_dict["valor_unitario"])
        item_dict["valor_total_exibicao"] = formatar_valor_brl(item_dict["valor_total"])
        resultado.append(item_dict)

    return resultado

def listar_orcamentos_recentes(limit=8):
    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT id, numero, cliente_nome, placa, modelo, total, status, criado_em
        FROM orcamentos
        WHERE empresa_id=?
        ORDER BY numero DESC
        LIMIT ?
    """, (empresa_id, limit))
    registros = [dict(item) for item in c.fetchall()]
    conn.close()

    for item in registros:
        item["numero_formatado"] = formatar_numero_documento(item.get("numero"))
        item["total_exibicao"] = formatar_valor_brl(item.get("total"))
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))

    return registros

def buscar_orcamento_completo(orcamento_id):
    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM orcamentos WHERE empresa_id=? AND id=?", (empresa_id, orcamento_id))
    orcamento = c.fetchone()

    if not orcamento:
        conn.close()
        return None

    c.execute("""
        SELECT descricao, quantidade, valor_unitario, valor_total, ordem
        FROM orcamento_itens
        WHERE orcamento_id=?
        ORDER BY ordem ASC, id ASC
    """, (orcamento_id,))
    itens = [dict(item) for item in c.fetchall()]
    conn.close()

    dados = dict(orcamento)
    dados["itens"] = enriquecer_itens_documento(itens)
    dados["empresa"] = desserializar_empresa_snapshot(dados.get("empresa_snapshot"))
    dados["numero_formatado"] = formatar_numero_documento(dados.get("numero"))
    dados["cliente_documento_formatado"] = formatar_documento_fiscal(dados.get("cliente_documento"))
    dados["subtotal_exibicao"] = formatar_valor_brl(dados.get("subtotal"))
    dados["desconto_exibicao"] = formatar_valor_brl(dados.get("desconto"))
    dados["total_exibicao"] = formatar_valor_brl(dados.get("total"))
    dados["criado_em_fmt"] = formatar_datahora(dados.get("criado_em"))
    return dados

def salvar_orcamento(dados, itens, empresa):
    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    numero = proximo_numero_documento_sql(c, "orcamentos", "numero")
    criado_em = agora_iso()
    totais = calcular_totais_orcamento(itens, converter_valor_numerico(dados.get("desconto")))
    subtotal = totais["subtotal"]
    desconto = totais["desconto"]
    total = totais["total"]
    empresa_snapshot = serializar_empresa_snapshot(montar_empresa_snapshot(empresa))

    c.execute("""
        INSERT INTO orcamentos (
            empresa_id, numero, cliente_nome, cliente_documento, email, telefone, placa, modelo,
            validade_dias, forma_pagamento, observacoes, subtotal, desconto, total,
            status, empresa_snapshot, criado_em, atualizado_em, usuario
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'GERADO', ?, ?, ?, ?)
    """, (
        empresa_id,
        numero,
        normalizar_texto_campo(dados.get("cliente_nome")) or "Cliente sem nome",
        normalizar_documento_fiscal(dados.get("cliente_documento")),
        normalizar_texto_campo(dados.get("email")),
        normalizar_texto_campo(dados.get("telefone")),
        normalizar_texto_campo(dados.get("placa")).upper(),
        normalizar_texto_campo(dados.get("modelo")),
        max(1, converter_inteiro(dados.get("validade_dias"), 7)),
        normalizar_texto_campo(dados.get("forma_pagamento")),
        normalizar_texto_campo(dados.get("observacoes")),
        subtotal,
        desconto,
        total,
        empresa_snapshot,
        criado_em,
        criado_em,
        session.get("usuario_nome") or session.get("usuario") or "",
    ))
    orcamento_id = c.lastrowid

    for item in itens:
        c.execute("""
            INSERT INTO orcamento_itens (
                orcamento_id, descricao, quantidade, valor_unitario, valor_total, ordem
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            orcamento_id,
            item["descricao"],
            item["quantidade"],
            item["valor_unitario"],
            item["valor_total"],
            item["ordem"],
        ))

    conn.commit()
    conn.close()
    return buscar_orcamento_completo(orcamento_id)

def listar_notas_fiscais_recentes(limit=8, somente_emitidas=False, somente_pendentes=False):
    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    query = """
        SELECT id, rps_numero, numero_nota, serie, ambiente, status, cliente_nome, valor_total, criado_em
        FROM notas_fiscais
        WHERE empresa_id=?
    """
    params = [empresa_id]

    if somente_emitidas:
        query += " AND COALESCE(NULLIF(numero_nota, ''), '') <> ''"
    elif somente_pendentes:
        query += " AND COALESCE(NULLIF(numero_nota, ''), '') = ''"

    query += """
        ORDER BY rps_numero DESC
        LIMIT ?
    """
    params.append(limit)
    c.execute(query, tuple(params))
    registros = [dict(item) for item in c.fetchall()]
    conn.close()

    for item in registros:
        item["rps_formatado"] = formatar_numero_documento(item.get("rps_numero"))
        item["valor_total_exibicao"] = formatar_valor_brl(item.get("valor_total"))
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))

    return registros

def buscar_nota_fiscal_completa(nota_id):
    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM notas_fiscais WHERE empresa_id=? AND id=?", (empresa_id, nota_id))
    nota = c.fetchone()

    if not nota:
        conn.close()
        return None

    c.execute("""
        SELECT descricao, quantidade, valor_unitario, valor_total, ordem
        FROM nota_fiscal_itens
        WHERE nota_fiscal_id=?
        ORDER BY ordem ASC, id ASC
    """, (nota_id,))
    itens = [dict(item) for item in c.fetchall()]
    conn.close()

    dados = dict(nota)
    dados["itens"] = enriquecer_itens_documento(itens)
    dados["empresa"] = desserializar_empresa_snapshot(dados.get("empresa_snapshot"))
    dados["rps_formatado"] = formatar_numero_documento(dados.get("rps_numero"))
    dados["cliente_documento_formatado"] = formatar_documento_fiscal(dados.get("cliente_documento"))
    dados["valor_servicos_exibicao"] = formatar_valor_brl(dados.get("valor_servicos"))
    dados["desconto_exibicao"] = formatar_valor_brl(dados.get("desconto"))
    dados["valor_iss_exibicao"] = formatar_valor_brl(dados.get("valor_iss"))
    dados["valor_total_exibicao"] = formatar_valor_brl(dados.get("valor_total"))
    dados["criado_em_fmt"] = formatar_datahora(dados.get("criado_em"))
    return dados

def salvar_nota_fiscal(dados, itens, empresa):
    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    rps_numero = proximo_numero_documento_sql(c, "notas_fiscais", "rps_numero")
    criado_em = agora_iso()
    totais = calcular_totais_nota_fiscal(
        itens,
        converter_valor_numerico(dados.get("desconto")),
        converter_valor_numerico(dados.get("aliquota_iss")),
    )
    valor_servicos = totais["valor_servicos"]
    desconto = totais["desconto"]
    aliquota_iss = totais["aliquota_iss"]
    valor_iss = totais["valor_iss"]
    valor_total = totais["valor_total"]
    numero_nota = normalizar_texto_campo(dados.get("numero_nota"))
    empresa_snapshot = serializar_empresa_snapshot(montar_empresa_snapshot(empresa))

    c.execute("""
        INSERT INTO notas_fiscais (
            empresa_id, rps_numero, numero_nota, serie, ambiente, tipo_documento, status,
            cliente_nome, cliente_documento, email, telefone, placa, modelo,
            endereco, numero_endereco, complemento, bairro, cidade, uf, cep,
            codigo_servico, discriminacao, observacoes, aliquota_iss, valor_servicos,
            desconto, valor_iss, valor_total, empresa_snapshot, criado_em, atualizado_em,
            usuario, origem_orcamento_id
        )
        VALUES (?, ?, ?, ?, ?, 'NFS-e', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        empresa_id,
        rps_numero,
        numero_nota,
        normalizar_texto_campo(dados.get("serie")),
        normalizar_texto_campo(dados.get("ambiente")) or "Emissao manual",
        "EMITIDA MANUALMENTE" if numero_nota else "RASCUNHO",
        normalizar_texto_campo(dados.get("cliente_nome")) or "Cliente sem nome",
        normalizar_documento_fiscal(dados.get("cliente_documento")),
        normalizar_texto_campo(dados.get("email")),
        normalizar_texto_campo(dados.get("telefone")),
        normalizar_texto_campo(dados.get("placa")).upper(),
        normalizar_texto_campo(dados.get("modelo")),
        normalizar_texto_campo(dados.get("endereco")),
        normalizar_texto_campo(dados.get("numero_endereco")),
        normalizar_texto_campo(dados.get("complemento")),
        normalizar_texto_campo(dados.get("bairro")),
        normalizar_texto_campo(dados.get("cidade")),
        normalizar_texto_campo(dados.get("uf")).upper()[:2],
        re.sub(r"\D", "", str(dados.get("cep") or "")),
        normalizar_texto_campo(dados.get("codigo_servico")),
        normalizar_texto_campo(dados.get("discriminacao")),
        normalizar_texto_campo(dados.get("observacoes")),
        aliquota_iss,
        valor_servicos,
        desconto,
        valor_iss,
        valor_total,
        empresa_snapshot,
        criado_em,
        criado_em,
        session.get("usuario_nome") or session.get("usuario") or "",
        converter_inteiro(dados.get("origem_orcamento_id"), None),
    ))
    nota_id = c.lastrowid

    for item in itens:
        c.execute("""
            INSERT INTO nota_fiscal_itens (
                nota_fiscal_id, descricao, quantidade, valor_unitario, valor_total, ordem
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            nota_id,
            item["descricao"],
            item["quantidade"],
            item["valor_unitario"],
            item["valor_total"],
            item["ordem"],
        ))

    conn.commit()
    conn.close()
    return buscar_nota_fiscal_completa(nota_id)

def montar_prefill_nota_por_orcamento(orcamento):
    return montar_prefill_nota_por_orcamento_domain(orcamento, obter_configuracao_empresa())

def carregar_branding_documento(empresa=None):
    empresa = dict(empresa or {})
    empresa_id = normalize_empresa_id(empresa.get("empresa_id") or empresa_atual_id())
    dados = carregar_dados_contexto_produto(empresa_id=empresa_id, incluir_blobs=True)
    config = dados.get("config") or {}
    empresa_base = dados.get("empresa") or empresa
    contexto = build_brand_context(config, empresa_base)
    return contexto, config, empresa_base


def montar_logo_pdf_branding(config):
    if config and config.get("marca_logo_blob"):
        try:
            return ImagemRedonda(BytesIO(bytes(config["marca_logo_blob"])), size=72)
        except Exception:
            pass

    try:
        return ImagemRedonda("static/logo.jpg", size=72)
    except Exception:
        return None


def montar_tabela_itens_pdf(itens, cor_cabecalho="#111827"):
    from reportlab.platypus import Table, TableStyle
    from reportlab.lib import colors

    dados_tabela = [["Descricao", "Qtd", "Unitario", "Total"]]

    for item in itens:
        dados_tabela.append([
            item["descricao"],
            item["quantidade_exibicao"],
            f"R$ {item['valor_unitario_exibicao']}",
            f"R$ {item['valor_total_exibicao']}",
        ])

    tabela = Table(dados_tabela, colWidths=[250, 60, 90, 90])
    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(cor_cabecalho or "#111827")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.7, colors.HexColor("#d1d5db")),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f9fafb")]),
        ("ALIGN", (1, 1), (-1, -1), "CENTER"),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 10),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 8),
    ]))
    return tabela

def gerar_pdf_orcamento_buffer(orcamento):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    empresa = orcamento.get("empresa") or empresa_snapshot_padrao()
    branding, config_brand, empresa_brand = carregar_branding_documento(empresa)
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=28, bottomMargin=28)
    styles = getSampleStyleSheet()
    cor_titulo = colors.HexColor(branding.get("brand_surface_color") or "#111827")
    cor_subtitulo = colors.HexColor(branding.get("brand_primary_color") or "#facc15")
    cor_cabecalho_tabela = branding.get("brand_surface_color") or "#111827"
    cor_destaque_total = colors.HexColor(branding.get("brand_primary_color") or "#facc15")
    titulo_style = ParagraphStyle(
        "DocTitulo",
        parent=styles["Heading1"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=18,
        textColor=cor_titulo,
        spaceAfter=6,
    )
    subtitulo_style = ParagraphStyle(
        "DocSubtitulo",
        parent=styles["BodyText"],
        alignment=TA_CENTER,
        textColor=cor_subtitulo,
        spaceAfter=12,
    )
    normal = styles["BodyText"]
    elementos = []

    try:
        logo = montar_logo_pdf_branding(config_brand)
        if logo:
            tabela_logo = Table([[logo]], colWidths=[523])
            tabela_logo.setStyle([("ALIGN", (0, 0), (-1, -1), "CENTER")])
            elementos.append(tabela_logo)
            elementos.append(Spacer(1, 10))
    except Exception:
        pass

    nome_empresa = (
        empresa_brand.get("nome_fantasia")
        or empresa_brand.get("razao_social")
        or branding.get("brand_name")
        or "Wagen Estetica Automotiva"
    )
    elementos.append(Paragraph(xml_escape(nome_empresa), titulo_style))

    linhas_empresa = []
    if branding.get("brand_subtitle"):
        linhas_empresa.append(str(branding["brand_subtitle"]))
    if empresa_brand.get("razao_social"):
        linhas_empresa.append(f"Razao social: {empresa_brand['razao_social']}")
    if empresa_brand.get("cnpj"):
        linhas_empresa.append(f"CNPJ: {formatar_documento_fiscal(empresa_brand['cnpj'])}")
    linhas_empresa.extend(montar_endereco_empresa(empresa_brand))
    if empresa_brand.get("telefone") or empresa_brand.get("email"):
        linhas_empresa.append(
            " | ".join(
                parte for parte in [
                    normalizar_texto_campo(empresa_brand.get("telefone")),
                    normalizar_texto_campo(empresa_brand.get("email")),
                ]
                if parte
            )
        )

    if linhas_empresa:
        elementos.append(Paragraph(xml_escape(" | ".join(linhas_empresa)), subtitulo_style))

    elementos.append(Paragraph(
        xml_escape(
            f"Orcamento #{orcamento['numero_formatado']} - Emitido em {orcamento['criado_em_fmt']}"
        ),
        subtitulo_style
    ))

    cliente_tabela = Table([
        ["Cliente", orcamento.get("cliente_nome") or "-"],
        ["Documento", orcamento.get("cliente_documento_formatado") or "-"],
        ["Telefone", orcamento.get("telefone") or "-"],
        ["Email", orcamento.get("email") or "-"],
        ["Veiculo", " / ".join(parte for parte in [orcamento.get("modelo"), orcamento.get("placa")] if parte) or "-"],
        ["Validade", f"{max(1, converter_inteiro(orcamento.get('validade_dias'), 7))} dia(s)"],
        ["Pagamento", orcamento.get("forma_pagamento") or "-"],
    ], colWidths=[120, 403])
    cliente_tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3f4f6")),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#d1d5db")),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    elementos.append(cliente_tabela)
    elementos.append(Spacer(1, 14))
    elementos.append(montar_tabela_itens_pdf(orcamento.get("itens", []), cor_cabecalho=cor_cabecalho_tabela))
    elementos.append(Spacer(1, 14))

    totais = Table([
        ["Subtotal", f"R$ {orcamento['subtotal_exibicao']}"],
        ["Desconto", f"R$ {orcamento['desconto_exibicao']}"],
        ["Total", f"R$ {orcamento['total_exibicao']}"],
    ], colWidths=[120, 130], hAlign="RIGHT")
    totais.setStyle(TableStyle([
        ("BACKGROUND", (0, 2), (-1, 2), cor_destaque_total),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#d1d5db")),
        ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
    ]))
    elementos.append(totais)

    if orcamento.get("observacoes"):
        elementos.append(Spacer(1, 16))
        elementos.append(Paragraph("<b>Observacoes</b>", normal))
        elementos.append(Paragraph(xml_escape(orcamento["observacoes"]).replace("\n", "<br/>"), normal))

    doc.build(elementos)
    buffer.seek(0)
    return buffer

def gerar_pdf_nota_fiscal_buffer(nota):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    empresa = nota.get("empresa") or empresa_snapshot_padrao()
    branding, config_brand, empresa_brand = carregar_branding_documento(empresa)
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=28, bottomMargin=28)
    styles = getSampleStyleSheet()
    cor_titulo = colors.HexColor(branding.get("brand_surface_color") or "#111827")
    cor_subtitulo = colors.HexColor(branding.get("brand_primary_color") or "#facc15")
    cor_cabecalho_tabela = branding.get("brand_surface_color") or "#111827"
    cor_destaque_total = colors.HexColor(branding.get("brand_primary_color") or "#facc15")
    titulo_style = ParagraphStyle(
        "FiscalTitulo",
        parent=styles["Heading1"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=18,
        textColor=cor_titulo,
        spaceAfter=6,
    )
    subtitulo_style = ParagraphStyle(
        "FiscalSubtitulo",
        parent=styles["BodyText"],
        alignment=TA_CENTER,
        textColor=cor_subtitulo,
        spaceAfter=10,
    )
    normal = styles["BodyText"]
    elementos = []

    try:
        logo = montar_logo_pdf_branding(config_brand)
        if logo:
            tabela_logo = Table([[logo]], colWidths=[523])
            tabela_logo.setStyle([("ALIGN", (0, 0), (-1, -1), "CENTER")])
            elementos.append(tabela_logo)
            elementos.append(Spacer(1, 10))
    except Exception:
        pass

    elementos.append(Paragraph("Espelho Fiscal de Servico", titulo_style))
    elementos.append(Paragraph(
        xml_escape(
            f"RPS #{nota['rps_formatado']} - Status: {nota.get('status') or 'RASCUNHO'}"
        ),
        subtitulo_style
    ))

    if nota.get("numero_nota"):
        resumo_nota = (
            f"Numero da nota: {nota['numero_nota']} | "
            f"Serie: {nota.get('serie') or '-'} | "
            f"Ambiente: {nota.get('ambiente') or '-'}"
        )
        elementos.append(Paragraph(
            xml_escape(resumo_nota),
            subtitulo_style
        ))

    emitente_tabela = Table([
        ["Emitente", empresa_brand.get("razao_social") or empresa_brand.get("nome_fantasia") or branding.get("brand_name") or "-"],
        ["CNPJ", formatar_documento_fiscal(empresa_brand.get("cnpj")) or "-"],
        ["IM / IE", " / ".join(
            parte for parte in [
                normalizar_texto_campo(empresa_brand.get("inscricao_municipal")),
                normalizar_texto_campo(empresa_brand.get("inscricao_estadual")),
            ] if parte
        ) or "-"],
        ["Endereco", " | ".join(montar_endereco_empresa(empresa_brand)) or "-"],
        ["Contato", " | ".join(
            parte for parte in [
                normalizar_texto_campo(empresa_brand.get("telefone")),
                normalizar_texto_campo(empresa_brand.get("email")),
            ] if parte
        ) or "-"],
    ], colWidths=[120, 403])
    emitente_tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f3f4f6")),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#d1d5db")),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    elementos.append(emitente_tabela)
    elementos.append(Spacer(1, 12))

    tomador_tabela = Table([
        ["Tomador", nota.get("cliente_nome") or "-"],
        ["Documento", nota.get("cliente_documento_formatado") or "-"],
        ["Contato", " | ".join(
            parte for parte in [nota.get("telefone"), nota.get("email")] if parte
        ) or "-"],
        ["Veiculo", " / ".join(parte for parte in [nota.get("modelo"), nota.get("placa")] if parte) or "-"],
        ["Endereco", " - ".join(
            parte for parte in [
                " ".join(
                    parte for parte in [nota.get("endereco"), nota.get("numero_endereco")] if parte
                ).strip(),
                normalizar_texto_campo(nota.get("complemento")),
                " / ".join(
                    parte
                    for parte in [
                        nota.get("bairro"),
                        " ".join(
                            parte for parte in [nota.get("cidade"), nota.get("uf")] if parte
                        ).strip(),
                    ]
                    if parte
                ),
                formatar_cep(nota.get("cep")),
            ]
            if parte
        ) or "-"],
    ], colWidths=[120, 403])
    tomador_tabela.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, -1), colors.HexColor("#f9fafb")),
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#d1d5db")),
        ("TOPPADDING", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
    ]))
    elementos.append(tomador_tabela)
    elementos.append(Spacer(1, 12))
    elementos.append(montar_tabela_itens_pdf(nota.get("itens", []), cor_cabecalho=cor_cabecalho_tabela))
    elementos.append(Spacer(1, 14))

    fiscal_tabela = Table([
        ["Codigo servico", nota.get("codigo_servico") or "-"],
        ["Aliquota ISS", f"{converter_valor_numerico(nota.get('aliquota_iss')):.2f}%"],
        ["Valor servicos", f"R$ {nota['valor_servicos_exibicao']}"],
        ["Desconto", f"R$ {nota['desconto_exibicao']}"],
        ["ISS estimado", f"R$ {nota['valor_iss_exibicao']}"],
        ["Valor total", f"R$ {nota['valor_total_exibicao']}"],
    ], colWidths=[150, 120], hAlign="RIGHT")
    fiscal_tabela.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.6, colors.HexColor("#d1d5db")),
        ("BACKGROUND", (0, 5), (-1, 5), cor_destaque_total),
        ("FONTNAME", (0, 0), (-1, -1), "Helvetica-Bold"),
    ]))
    elementos.append(fiscal_tabela)

    if nota.get("discriminacao"):
        elementos.append(Spacer(1, 16))
        elementos.append(Paragraph("<b>Discriminacao do servico</b>", normal))
        elementos.append(Paragraph(xml_escape(nota["discriminacao"]).replace("\n", "<br/>"), normal))

    if nota.get("observacoes"):
        elementos.append(Spacer(1, 12))
        elementos.append(Paragraph("<b>Observacoes fiscais</b>", normal))
        elementos.append(Paragraph(xml_escape(nota["observacoes"]).replace("\n", "<br/>"), normal))

    elementos.append(Spacer(1, 12))
    elementos.append(Paragraph(
        (
            "Este PDF e um espelho de apoio para emissao e conferencia. "
            "A emissao oficial da NFS-e/NF-e depende do portal ou integracao "
            "fiscal habilitada para o seu CNPJ."
        ),
        normal,
    ))

    doc.build(elementos)
    buffer.seek(0)
    return buffer

def criar_itens_checklist_padrao():
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM checklist_itens")
    total = c.fetchone()[0]

    if total == 0:
        for ordem, nome in enumerate(ITENS_CHECKLIST_PADRAO, start=1):
            c.execute(
                "INSERT INTO checklist_itens (nome, ativo, ordem) VALUES (?, 1, ?)",
                (nome, ordem)
            )
        conn.commit()

    conn.close()

def listar_itens_checklist(apenas_ativos=False):
    conn = conectar()
    c = conn.cursor()

    query = "SELECT * FROM checklist_itens"
    params = ()

    if apenas_ativos:
        query += " WHERE ativo=1"

    query += " ORDER BY ordem ASC, id ASC"
    c.execute(query, params)
    itens = [dict(item) for item in c.fetchall()]
    conn.close()
    return itens

def normalizar_texto_campo(valor):
    return str(valor or "").strip()

def normalizar_redirect_interno(destino, fallback="/"):
    texto = normalizar_texto_campo(destino)
    if not texto:
        return fallback

    partes = urlparse(texto)
    if partes.scheme or partes.netloc:
        return fallback
    if not texto.startswith("/") or texto.startswith("//"):
        return fallback
    return texto

def normalizar_flag_sim_nao(valor):
    return "Sim" if normalizar_texto_comparacao(valor) == "sim" else "Nao"

def resumo_usuario_logado():
    if not has_request_context():
        return usuario_sistema_interno()

    return {
        "id": session.get("usuario_id"),
        "usuario": session.get("usuario") or "",
        "nome": session.get("usuario_nome") or session.get("usuario") or "",
    }

def formatar_usuario_exibicao(nome=None, usuario=None, fallback="Nao informado"):
    texto = normalizar_texto_campo(nome) or normalizar_texto_campo(usuario)
    return texto or fallback

def registrar_auditoria(acao, entidade, entidade_id=None, placa=None, detalhes=None, usuario=None):
    usuario_info = usuario or resumo_usuario_logado()
    detalhes_json = json.dumps(detalhes or {}, ensure_ascii=False, default=sanitizar_para_json)

    conn = conectar()
    c = conn.cursor()
    c.execute("""
        INSERT INTO auditoria (
            usuario_id, usuario, usuario_nome, acao, entidade, entidade_id, placa, detalhes_json, criado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        usuario_info.get("id"),
        normalizar_texto_campo(usuario_info.get("usuario")),
        normalizar_texto_campo(usuario_info.get("nome")),
        normalizar_texto_campo(acao),
        normalizar_texto_campo(entidade),
        entidade_id,
        normalizar_texto_campo(placa).upper(),
        detalhes_json,
        agora_iso(),
    ))
    conn.commit()
    conn.close()
    AUDITORIA_CONTEXT_CACHE["testado_em"] = 0.0
    AUDITORIA_CONTEXT_CACHE["chave"] = ""
    AUDITORIA_CONTEXT_CACHE["resultado"] = None


def registrar_auditoria_assincrona(acao, entidade, entidade_id=None, placa=None, detalhes=None):
    usuario_info = resumo_usuario_logado()
    detalhes_payload = deepcopy(detalhes or {})

    def executar():
        try:
            registrar_auditoria(
                acao,
                entidade,
                entidade_id=entidade_id,
                placa=placa,
                detalhes=detalhes_payload,
                usuario=usuario_info,
            )
        except Exception as erro:
            log_info("AVISO AUDITORIA ASSINCRONA:", erro)

    Thread(target=executar, daemon=True).start()

def obter_resample_lanczos():
    if not PILLOW_DISPONIVEL:
        return None

    if hasattr(Image, "Resampling"):
        return Image.Resampling.LANCZOS

    return getattr(Image, "LANCZOS", Image.BICUBIC)

def salvar_foto_perfil_usuario(foto, identificador="usuario"):
    if not foto or not str(getattr(foto, "filename", "") or "").strip():
        raise ValueError("Selecione uma foto para o perfil.")

    if not arquivo_permitido(foto.filename):
        raise ValueError("Envie uma imagem em JPG, PNG, WEBP, HEIC ou HEIF.")

    pasta_destino = caminho_uploads_perfis_absoluto()
    nome_seguro = secure_filename(foto.filename or "") or "perfil"
    nome_base, ext_original = os.path.splitext(nome_seguro)
    nome_base = re.sub(r"[^A-Za-z0-9_-]+", "_", nome_base or "perfil").strip("_") or "perfil"
    prefixo = re.sub(r"[^A-Za-z0-9_-]+", "_", str(identificador or "usuario")).strip("_") or "usuario"
    ext_original = (ext_original or ".jpg").lower()

    if PILLOW_DISPONIVEL:
        try:
            foto.stream.seek(0)
            imagem = Image.open(foto.stream)
            imagem = ImageOps.exif_transpose(imagem)

            largura_original, altura_original = imagem.size
            if max(largura_original, altura_original) > FOTO_PERFIL_MAX_DIMENSAO:
                imagem.thumbnail(
                    (FOTO_PERFIL_MAX_DIMENSAO, FOTO_PERFIL_MAX_DIMENSAO),
                    obter_resample_lanczos(),
                )

            if imagem.mode != "RGB":
                imagem = imagem.convert("RGB")

            destino = os.path.join(
                pasta_destino,
                f"{time.time_ns()}_{prefixo}_{nome_base}.jpg",
            )
            imagem.save(
                destino,
                format="JPEG",
                quality=FOTO_PERFIL_QUALIDADE_JPEG,
                optimize=True,
                progressive=True,
            )
            return {
                "caminho": caminho_relativo_usuario_foto(destino),
                "arquivo_blob": ler_bytes_arquivo(destino),
                "mime_type": "image/jpeg",
                "arquivo_nome": os.path.basename(destino),
            }
        except (UnidentifiedImageError, OSError, ValueError):
            raise ValueError("Nao consegui processar a foto enviada. Tente outra imagem.")
        except Exception:
            raise ValueError("Nao consegui salvar a foto do perfil agora.")

    ext_fallback = ext_original if ext_original in {".png", ".jpg", ".jpeg", ".webp", ".heic", ".heif"} else ".jpg"
    destino = os.path.join(
        pasta_destino,
        f"{time.time_ns()}_{prefixo}_{nome_base}{ext_fallback}",
    )
    foto.stream.seek(0)
    foto.save(destino)
    return {
        "caminho": caminho_relativo_usuario_foto(destino),
        "arquivo_blob": ler_bytes_arquivo(destino),
        "mime_type": detectar_mime_type_arquivo(destino),
        "arquivo_nome": os.path.basename(destino),
    }


def preparar_imagem_site_upload(
    foto,
    nome_padrao="logo",
    max_dimensao=640,
    formato_saida="JPEG",
    qualidade=92,
    preservar_alpha=False,
    fundo_rgb=(11, 11, 11),
):
    if not foto or not str(getattr(foto, "filename", "") or "").strip():
        return None

    if not arquivo_permitido(foto.filename):
        raise ValueError("Envie a imagem em JPG, PNG, WEBP, HEIC ou HEIF.")

    nome_seguro = secure_filename(foto.filename or "") or nome_padrao
    nome_base, ext_original = os.path.splitext(nome_seguro)
    nome_base = re.sub(r"[^A-Za-z0-9_-]+", "_", nome_base or nome_padrao).strip("_") or nome_padrao
    ext_original = (ext_original or ".jpg").lower()

    if PILLOW_DISPONIVEL:
        try:
            foto.stream.seek(0)
            imagem = Image.open(foto.stream)
            imagem = ImageOps.exif_transpose(imagem)
            if max(imagem.size) > max_dimensao:
                imagem.thumbnail((max_dimensao, max_dimensao), obter_resample_lanczos())
            buffer = BytesIO()

            if preservar_alpha:
                imagem = imagem.convert("RGBA")
                imagem.save(
                    buffer,
                    format=formato_saida,
                    optimize=True,
                )
            else:
                if imagem.mode != "RGBA":
                    imagem = imagem.convert("RGBA")
                fundo = Image.new("RGB", imagem.size, fundo_rgb)
                fundo.paste(imagem, mask=imagem.split()[-1] if "A" in imagem.getbands() else None)
                fundo.save(
                    buffer,
                    format=formato_saida,
                    quality=qualidade,
                    optimize=True,
                    progressive=True,
                )
            conteudo = buffer.getvalue()
            ext_saida = ".png" if formato_saida.upper() == "PNG" else ".jpg"
            mime_type = "image/png" if ext_saida == ".png" else "image/jpeg"
            return {
                "arquivo_blob": conteudo,
                "mime_type": mime_type,
                "arquivo_nome": f"{nome_base}{ext_saida}",
            }
        except (UnidentifiedImageError, OSError, ValueError):
            raise ValueError("Nao consegui processar a imagem enviada. Tente outra imagem.")
        except Exception:
            raise ValueError("Nao consegui salvar a imagem do site agora.")

    foto.stream.seek(0)
    conteudo = foto.stream.read()
    return {
        "arquivo_blob": conteudo,
        "mime_type": detectar_mime_type_arquivo(f"{nome_padrao}{ext_original}"),
        "arquivo_nome": f"{nome_base}{ext_original}",
    }


def preparar_logo_site_upload(foto):
    try:
        return preparar_imagem_site_upload(
            foto,
            nome_padrao="logo",
            max_dimensao=640,
            formato_saida="JPEG",
            qualidade=92,
            preservar_alpha=False,
            fundo_rgb=(11, 11, 11),
        )
    except ValueError as erro:
        texto = str(erro)
        if "imagem" in texto.lower():
            raise ValueError(texto.replace("imagem", "logo", 1))
        raise


def preparar_favicon_site_upload(foto):
    try:
        return preparar_imagem_site_upload(
            foto,
            nome_padrao="favicon",
            max_dimensao=256,
            formato_saida="PNG",
            preservar_alpha=True,
        )
    except ValueError as erro:
        texto = str(erro)
        if "imagem" in texto.lower():
            raise ValueError(texto.replace("imagem", "favicon", 1))
        raise


def detectar_mime_type_arquivo(caminho):
    mime_type = mimetypes.guess_type(str(caminho or ""))[0] or ""
    return mime_type or "application/octet-stream"


def ler_bytes_arquivo(caminho):
    with open(caminho, "rb") as arquivo:
        return arquivo.read()


def salvar_arquivo_imagem_otimizado(foto):
    pasta_destino = caminho_uploads_servicos_diretorio()
    nome_seguro = secure_filename(foto.filename or "") or "foto"
    nome_base, ext_original = os.path.splitext(nome_seguro)
    nome_base = re.sub(r"[^A-Za-z0-9_-]+", "_", nome_base or "foto").strip("_") or "foto"
    ext_original = (ext_original or ".jpg").lower()

    if PILLOW_DISPONIVEL:
        try:
            foto.stream.seek(0)
            imagem = Image.open(foto.stream)
            imagem = ImageOps.exif_transpose(imagem)

            largura_original, altura_original = imagem.size
            if max(largura_original, altura_original) > FOTO_MAX_DIMENSAO:
                imagem.thumbnail((FOTO_MAX_DIMENSAO, FOTO_MAX_DIMENSAO), obter_resample_lanczos())

            if imagem.mode != "RGB":
                imagem = imagem.convert("RGB")

            destino = os.path.join(
                pasta_destino,
                f"{time.time_ns()}_{nome_base}.jpg",
            )
            imagem.save(
                destino,
                format="JPEG",
                quality=FOTO_QUALIDADE_JPEG,
                optimize=True,
                progressive=True,
            )
            tamanho_bytes = os.path.getsize(destino)
            largura, altura = imagem.size
            return {
                "caminho": destino,
                "tamanho_bytes": tamanho_bytes,
                "largura": largura,
                "altura": altura,
                "arquivo_blob": ler_bytes_arquivo(destino),
                "mime_type": "image/jpeg",
                "arquivo_nome": os.path.basename(destino),
                "compactada": True,
            }
        except (UnidentifiedImageError, OSError, ValueError):
            pass
        except Exception:
            pass

    ext_fallback = ext_original if ext_original in {".png", ".jpg", ".jpeg", ".webp", ".heic", ".heif"} else ".jpg"
    destino = os.path.join(
        pasta_destino,
        f"{time.time_ns()}_{nome_base}{ext_fallback}",
    )
    foto.stream.seek(0)
    foto.save(destino)

    largura = None
    altura = None
    if PILLOW_DISPONIVEL:
        try:
            with Image.open(destino) as imagem_salva:
                largura, altura = imagem_salva.size
        except Exception:
            pass

    return {
        "caminho": destino,
        "tamanho_bytes": os.path.getsize(destino),
        "largura": largura,
        "altura": altura,
        "arquivo_blob": ler_bytes_arquivo(destino),
        "mime_type": detectar_mime_type_arquivo(destino),
        "arquivo_nome": os.path.basename(destino),
        "compactada": False,
    }

def resumir_uploaders_fotos(galeria_fotos):
    resumo = {}

    for tipo, fotos in (galeria_fotos or {}).items():
        nomes = []
        vistos = set()

        for foto in fotos:
            nome = formatar_usuario_exibicao(
                foto.get("usuario_nome"),
                foto.get("usuario"),
                fallback="Nao identificado",
            )
            chave = normalizar_texto_comparacao(nome)
            if chave in vistos:
                continue
            vistos.add(chave)
            nomes.append(nome)

        resumo[tipo] = ", ".join(nomes) if nomes else "Nao identificado"

    return resumo

def enriquecer_responsaveis_servico(servico):
    servico["criado_por_nome_exibicao"] = formatar_usuario_exibicao(
        servico.get("criado_por_nome"),
        servico.get("criado_por_usuario"),
        fallback="Nao identificado",
    )
    servico["operacional_por_nome_exibicao"] = formatar_usuario_exibicao(
        servico.get("operacional_por_nome"),
        servico.get("operacional_por_usuario"),
        fallback="Nao registrado",
    )
    servico["finalizado_por_nome_exibicao"] = formatar_usuario_exibicao(
        servico.get("finalizado_por_nome"),
        servico.get("finalizado_por_usuario"),
        fallback="Nao finalizado",
    )
    servico["resumo_uploaders_fotos"] = resumir_uploaders_fotos(servico.get("galeria_fotos") or {})
    return servico

def enriquecer_entrega_servico(servico, referencia=None):
    referencia = referencia or agora()
    valor_adicional = converter_valor_numerico(servico.get("valor_adicional"))
    entrega_prevista = interpretar_datahora_sistema(servico.get("entrega_prevista"))

    servico["valor_adicional_num"] = valor_adicional
    servico["valor_adicional_exibicao"] = formatar_valor_monetario(valor_adicional)
    servico["tem_valor_adicional"] = valor_adicional > 0

    if entrega_prevista:
        if entrega_prevista.tzinfo is None:
            entrega_prevista = entrega_prevista.replace(
                tzinfo=ZoneInfo("America/Sao_Paulo")
            )

        servico["entrega_prevista_dt"] = entrega_prevista
        servico["entrega_prevista_iso"] = entrega_prevista.isoformat(timespec="seconds")
        servico["entrega_prevista_exibicao"] = entrega_prevista.strftime("%d/%m/%Y %H:%M")
        servico["tempo_entrega"] = formatar_contagem_regressiva(servico["entrega_prevista_iso"], referencia)
        diferenca = int((entrega_prevista - referencia).total_seconds())
        servico["entrega_prevista_em_minutos"] = max(0, diferenca // 60)
        servico["entrega_prevista_vencida"] = diferenca <= 0
    else:
        servico["entrega_prevista_dt"] = None
        servico["entrega_prevista_iso"] = ""
        servico["entrega_prevista_exibicao"] = ""
        servico["tempo_entrega"] = "Sem horario combinado"
        servico["entrega_prevista_em_minutos"] = None
        servico["entrega_prevista_vencida"] = False

    return servico

def resumir_entregas_em_andamento(servicos, referencia=None):
    referencia = referencia or agora()
    total = 0
    com_horario = 0
    sem_horario = 0
    vencidas = 0
    proxima = None

    for servico in servicos or []:
        total += 1
        entrega_prevista = interpretar_datahora_sistema(servico.get("entrega_prevista"))

        if not entrega_prevista:
            sem_horario += 1
            continue

        if entrega_prevista.tzinfo is None:
            entrega_prevista = entrega_prevista.replace(
                tzinfo=ZoneInfo("America/Sao_Paulo")
            )

        com_horario += 1
        diferenca = int((entrega_prevista - referencia).total_seconds())

        if diferenca <= 0:
            vencidas += 1
            continue

        if not proxima or diferenca < proxima["segundos"]:
            proxima = {
                "segundos": diferenca,
                "minutos": max(1, math.ceil(diferenca / 60)),
                "placa": normalizar_texto_campo(servico.get("placa")).upper(),
                "modelo": normalizar_texto_campo(servico.get("modelo")),
                "cliente_nome": normalizar_texto_campo(servico.get("cliente_nome")),
                "tipo_nome": normalizar_texto_campo(servico.get("tipo_nome")) or "Servico",
                "entrega_prevista": entrega_prevista,
            }

    return {
        "total": total,
        "com_horario": com_horario,
        "sem_horario": sem_horario,
        "vencidas": vencidas,
        "proxima": proxima,
    }

def salvar_fotos_servico(cursor, servico_id, fotos, tipo):
    total_salvas = 0
    usuario_info = resumo_usuario_logado()
    empresa_id = empresa_atual_id()

    for foto in fotos or []:
        if not foto or not foto.filename or not arquivo_permitido(foto.filename):
            continue

        arquivo_salvo = salvar_arquivo_imagem_otimizado(foto)
        caminho = arquivo_salvo["caminho"]

        cursor.execute(
            """
            INSERT INTO fotos (
                empresa_id, servico_id, tipo, caminho, usuario, usuario_nome, tamanho_bytes, largura, altura,
                arquivo_blob, mime_type, arquivo_nome
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                empresa_id,
                servico_id,
                tipo,
                caminho,
                normalizar_texto_campo(usuario_info.get("usuario")),
                normalizar_texto_campo(usuario_info.get("nome")),
                arquivo_salvo.get("tamanho_bytes"),
                arquivo_salvo.get("largura"),
                arquivo_salvo.get("altura"),
                arquivo_salvo.get("arquivo_blob"),
                arquivo_salvo.get("mime_type"),
                arquivo_salvo.get("arquivo_nome"),
            )
        )
        total_salvas += 1

    return total_salvas

def caminho_foto_para_url(caminho):
    texto = str(caminho or "").strip().replace("\\", "/")

    if not texto:
        return ""

    if texto.startswith("http://") or texto.startswith("https://"):
        return texto

    if texto.startswith("/static/"):
        return texto

    if texto.startswith("static/"):
        return "/" + quote(texto)

    rel_static = caminho_relativo_static(texto)
    if rel_static:
        return "/" + quote(rel_static)

    return "/static/uploads/" + quote(os.path.basename(texto))


def caminho_absoluto_foto_servico(caminho):
    texto = str(caminho or "").strip()
    if not texto:
        return ""

    texto_normalizado = texto.replace("\\", "/")
    if texto_normalizado.startswith("/static/"):
        return os.path.abspath(texto_normalizado.lstrip("/"))
    if texto_normalizado.startswith("static/"):
        return os.path.abspath(texto_normalizado)

    rel_static = caminho_relativo_static(texto_normalizado)
    if rel_static:
        return os.path.abspath(rel_static)

    return normalizar_caminho_arquivo(texto)


def foto_local_disponivel(caminho):
    texto = str(caminho or "").strip()
    if not texto:
        return False

    texto_normalizado = texto.replace("\\", "/").lower()
    if texto_normalizado.startswith("http://") or texto_normalizado.startswith("https://"):
        return True

    caminho_abs = caminho_absoluto_foto_servico(texto)
    return bool(caminho_abs and os.path.isfile(caminho_abs))


def remover_foto_servico_local(caminho):
    caminho_abs = caminho_absoluto_foto_servico(caminho)
    if not caminho_abs or not os.path.isfile(caminho_abs):
        return

    pasta_uploads = caminho_uploads_servicos_diretorio()
    if not arquivo_dentro_da_pasta(caminho_abs, pasta_uploads):
        return

    remover_arquivo_se_existir(caminho_abs)


@app.route("/fotos/<int:foto_id>/arquivo")
def servir_foto_banco(foto_id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()
    empresa_id = empresa_atual_id()
    c.execute(
        """
        SELECT id, caminho, arquivo_blob, mime_type, arquivo_nome
        FROM fotos
        WHERE empresa_id=? AND id=?
        """,
        (empresa_id, foto_id),
    )
    foto = c.fetchone()
    conn.close()

    if not foto:
        return ("Foto nao encontrada.", 404)

    blob = foto["arquivo_blob"]
    if blob:
        nome_arquivo = str(foto["arquivo_nome"] or "").strip() or os.path.basename(str(foto["caminho"] or "").replace("\\", "/")) or f"foto_{foto_id}.jpg"
        mime_type = str(foto["mime_type"] or "").strip() or detectar_mime_type_arquivo(nome_arquivo)
        return send_file(
            BytesIO(bytes(blob)),
            mimetype=mime_type,
            download_name=nome_arquivo,
            max_age=86400,
        )

    caminho = foto["caminho"]
    if foto_local_disponivel(caminho):
        return redirect(caminho_foto_para_url(caminho))

    return ("Foto nao encontrada.", 404)


@app.route("/usuarios/<int:usuario_id>/foto")
def servir_foto_perfil_usuario_banco(usuario_id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, foto_perfil, foto_perfil_blob, foto_perfil_mime_type, foto_perfil_arquivo_nome
        FROM usuarios
        WHERE id=?
        """,
        (usuario_id,),
    )
    usuario = c.fetchone()
    conn.close()

    if not usuario:
        return ("Foto de perfil nao encontrada.", 404)

    blob = usuario["foto_perfil_blob"]
    if blob:
        nome_arquivo = (
            str(usuario["foto_perfil_arquivo_nome"] or "").strip()
            or os.path.basename(str(usuario["foto_perfil"] or "").replace("\\", "/"))
            or f"perfil_{usuario_id}.jpg"
        )
        mime_type = (
            str(usuario["foto_perfil_mime_type"] or "").strip()
            or detectar_mime_type_arquivo(nome_arquivo)
        )
        return send_file(
            BytesIO(bytes(blob)),
            mimetype=mime_type,
            download_name=nome_arquivo,
            max_age=86400,
        )

    caminho = str(usuario["foto_perfil"] or "").strip()
    if caminho:
        caminho_abs = caminho_absoluto_usuario_foto(caminho)
        if caminho_abs and os.path.isfile(caminho_abs):
            return redirect(caminho_foto_para_url(caminho))

    return ("Foto de perfil nao encontrada.", 404)


@app.route("/branding/logo")
def servir_logo_site():
    if not INIT_DB_EXECUTADO:
        return redirect("/static/logo.jpg")

    dados = (carregar_dados_contexto_produto(incluir_blobs=True).get("config") or {})

    blob = dados.get("marca_logo_blob")
    if blob:
        nome_arquivo = str(dados.get("marca_logo_arquivo_nome") or "").strip() or "logo-site.jpg"
        mime_type = str(dados.get("marca_logo_mime_type") or "").strip() or detectar_mime_type_arquivo(nome_arquivo)
        return send_file(
            BytesIO(bytes(blob)),
            mimetype=mime_type,
            download_name=nome_arquivo,
            max_age=86400,
        )

    logo_url = normalizar_texto_campo(dados.get("marca_logo_url"))
    if logo_url:
        return redirect(logo_url)

    return redirect("/static/logo.jpg")


@app.route("/branding/favicon")
def servir_favicon_site():
    if not INIT_DB_EXECUTADO:
        return redirect("/static/favicon.jpg")

    dados = (carregar_dados_contexto_produto(incluir_blobs=True).get("config") or {})

    blob = dados.get("marca_favicon_blob")
    if blob:
        nome_arquivo = str(dados.get("marca_favicon_arquivo_nome") or "").strip() or "favicon-site.png"
        mime_type = str(dados.get("marca_favicon_mime_type") or "").strip() or detectar_mime_type_arquivo(nome_arquivo)
        return send_file(
            BytesIO(bytes(blob)),
            mimetype=mime_type,
            download_name=nome_arquivo,
            max_age=86400,
        )

    favicon_url = normalizar_texto_campo(dados.get("marca_favicon_url"))
    if favicon_url:
        return redirect(favicon_url)

    logo_blob = dados.get("marca_logo_blob")
    if logo_blob:
        return redirect("/branding/logo")

    logo_url = normalizar_texto_campo(dados.get("marca_logo_url"))
    if logo_url:
        return redirect(logo_url)

    return redirect("/static/favicon.jpg")


@app.route("/site.webmanifest")
def servir_manifesto_site():
    produto = carregar_contexto_produto()
    manifest = montar_manifesto_pwa(produto)
    response = app.response_class(
        json.dumps(manifest, ensure_ascii=False),
        mimetype="application/manifest+json",
    )
    response.headers["Cache-Control"] = "no-cache"
    return response


@app.route("/sw.js")
def servir_service_worker_raiz():
    response = send_from_directory(
        os.path.join(app.root_path, "static"),
        "sw.js",
        mimetype="application/javascript",
        max_age=0,
    )
    response.headers["Service-Worker-Allowed"] = "/"
    response.headers["Cache-Control"] = "no-cache"
    return response


@app.route("/api/pwa/status")
def api_pwa_status():
    return jsonify(montar_status_pwa(request_is_secure=request.is_secure, host=request.host))


def listar_fotos_servicos(ids_servicos, conn=None, cursor=None):
    ids = [int(item) for item in (ids_servicos or []) if item]

    if not ids:
        return {}

    conn_local = None
    c = cursor
    if c is None:
        conn_local = conn or conectar()
        c = conn_local.cursor()
    empresa_id = empresa_atual_id()
    placeholders = ",".join(["?"] * len(ids))
    c.execute(f"""
        SELECT id, servico_id, tipo, caminho, criado_em, usuario, usuario_nome, tamanho_bytes, largura, altura,
               mime_type, arquivo_nome,
               CASE WHEN arquivo_blob IS NOT NULL THEN 1 ELSE 0 END AS possui_blob
        FROM fotos
        WHERE empresa_id=? AND servico_id IN ({placeholders})
        ORDER BY
            CASE tipo
                WHEN 'entrada' THEN 0
                WHEN 'detalhe' THEN 1
                WHEN 'saida' THEN 2
                ELSE 3
            END,
            id DESC
    """, (empresa_id, *ids))

    fotos_por_servico = {}
    labels = {
        "entrada": "Entrada",
        "detalhe": "Detalhe",
        "saida": "Finalizacao",
    }

    for row in c.fetchall():
        foto = dict(row)
        possui_blob = bool(int(foto.get("possui_blob") or 0))
        foto["url"] = f"/fotos/{foto['id']}/arquivo" if possui_blob else caminho_foto_para_url(foto.get("caminho"))
        foto["arquivo_nome"] = (
            str(foto.get("arquivo_nome") or "").strip()
            or os.path.basename(str(foto.get("caminho") or "").replace("\\", "/"))
        )
        foto["tipo_label"] = labels.get(foto.get("tipo"), "Foto")
        foto["criado_em_fmt"] = formatar_datahora(foto.get("criado_em"))
        foto["usuario_nome_exibicao"] = formatar_usuario_exibicao(
            foto.get("usuario_nome"),
            foto.get("usuario"),
            fallback="Nao identificado",
        )
        foto["tamanho_fmt"] = formatar_tamanho_arquivo(foto.get("tamanho_bytes"))
        foto["fonte_armazenamento"] = "Banco de dados" if possui_blob else "Arquivo local"

        if not foto["url"]:
            continue

        grupos = fotos_por_servico.setdefault(foto["servico_id"], {})
        grupos.setdefault(foto["tipo"], []).append(foto)

    if conn_local:
        conn_local.close()
    return fotos_por_servico

def listar_resumo_fotos_servicos(ids_servicos, conn=None, cursor=None):
    ids = [int(item) for item in (ids_servicos or []) if item]

    if not ids:
        return {}

    conn_local = None
    c = cursor
    if c is None:
        conn_local = conn or conectar()
        c = conn_local.cursor()

    placeholders = ",".join(["?"] * len(ids))
    c.execute(f"""
        SELECT servico_id, tipo, COUNT(*) AS total
        FROM fotos
        WHERE empresa_id=? AND servico_id IN ({placeholders})
        GROUP BY servico_id, tipo
    """, (empresa_atual_id(), *ids))

    resumo = {}
    for row in c.fetchall():
        item = dict(row)
        servico_id = item.get("servico_id")
        tipo = normalizar_texto_campo(item.get("tipo")).lower()
        if not servico_id or tipo not in {"entrada", "detalhe", "saida"}:
            continue
        grupo = resumo.setdefault(servico_id, {"entrada": 0, "detalhe": 0, "saida": 0})
        grupo[tipo] = converter_inteiro(item.get("total"), 0)

    if conn_local:
        conn_local.close()

    return resumo

def contar_fotos_validas(fotos):
    return sum(
        1
        for foto in (fotos or [])
        if foto and foto.filename and arquivo_permitido(foto.filename)
    )

def listar_cobrancas_extras_servicos(ids_servicos, conn=None, cursor=None):
    ids = [int(item) for item in (ids_servicos or []) if item]

    if not ids:
        return {}

    conn_local = None
    c = cursor
    if c is None:
        conn_local = conn or conectar()
        c = conn_local.cursor()
    placeholders = ",".join(["?"] * len(ids))
    c.execute(f"""
        SELECT
            id,
            servico_id,
            descricao,
            valor,
            criado_em,
            criado_por_usuario,
            criado_por_nome
        FROM servico_cobrancas_extras
        WHERE servico_id IN ({placeholders})
        ORDER BY id ASC
    """, ids)

    extras_por_servico = {}

    for row in c.fetchall():
        extra = dict(row)
        extra["valor_exibicao"] = formatar_valor_monetario(extra.get("valor"))
        extra["criado_em_exibicao"] = formatar_datahora(extra.get("criado_em"))
        extra["criado_por_nome_exibicao"] = formatar_usuario_exibicao(
            extra.get("criado_por_nome"),
            extra.get("criado_por_usuario"),
            fallback="Nao identificado",
        )

        grupo = extras_por_servico.setdefault(extra["servico_id"], {
            "itens": [],
            "total": 0.0,
        })
        grupo["itens"].append(extra)
        grupo["total"] += converter_valor_numerico(extra.get("valor"))

    if conn_local:
        conn_local.close()

    for grupo in extras_por_servico.values():
        grupo["total_exibicao"] = formatar_valor_monetario(grupo["total"])

    return extras_por_servico

def listar_resumo_cobrancas_extras_servicos(ids_servicos, conn=None, cursor=None):
    ids = [int(item) for item in (ids_servicos or []) if item]

    if not ids:
        return {}

    conn_local = None
    c = cursor
    if c is None:
        conn_local = conn or conectar()
        c = conn_local.cursor()

    placeholders = ",".join(["?"] * len(ids))
    c.execute(f"""
        SELECT servico_id, COUNT(*) AS quantidade, COALESCE(SUM(valor), 0) AS total
        FROM servico_cobrancas_extras
        WHERE servico_id IN ({placeholders})
        GROUP BY servico_id
    """, ids)

    resumo = {}
    for row in c.fetchall():
        item = dict(row)
        total = converter_valor_numerico(item.get("total"))
        resumo[item["servico_id"]] = {
            "itens": [],
            "quantidade": converter_inteiro(item.get("quantidade"), 0),
            "total": total,
            "total_exibicao": formatar_valor_monetario(total),
        }

    if conn_local:
        conn_local.close()

    return resumo

def listar_cobrancas_extras_servico(servico_id):
    extras = listar_cobrancas_extras_servicos([servico_id])
    return extras.get(servico_id, {"itens": [], "total": 0.0, "total_exibicao": "0.00"})

def atualizar_campos_operacionais_servico(cursor, servico_id, form, usuario_info=None):
    usuario_info = usuario_info or resumo_usuario_logado()
    empresa_id = empresa_atual_id()
    cursor.execute("""
        UPDATE servicos
        SET origem=?, guarita=?, observacoes=?, pneu=?, cera=?, hidro_lataria=?, hidro_vidros=?,
            operacional_por_usuario=?, operacional_por_nome=?
        WHERE empresa_id=? AND id=?
    """, (
        normalizar_texto_campo(form.get("origem")),
        normalizar_texto_campo(form.get("guarita")),
        normalizar_texto_campo(form.get("observacoes")),
        normalizar_texto_campo(form.get("pneu")),
        normalizar_flag_sim_nao(form.get("cera")),
        normalizar_flag_sim_nao(form.get("hidro_lataria")),
        normalizar_flag_sim_nao(form.get("hidro_vidros")),
        normalizar_texto_campo(usuario_info.get("usuario")),
        normalizar_texto_campo(usuario_info.get("nome")),
        empresa_id,
        servico_id,
    ))

def listar_checklist_servico(servico_id):
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT item_id, item_nome, marcado
        FROM servico_checklist
        WHERE servico_id=?
        ORDER BY id ASC
    """, (servico_id,))
    itens = [dict(item) for item in c.fetchall()]
    conn.close()
    return itens

def buscar_servico_operacional(servico_id):
    conn = conectar()
    c = conn.cursor()
    servico = consultar_servico_operacional_domain(c, empresa_atual_id(), servico_id)
    conn.close()
    return dict(servico) if servico else None

def recalcular_resumo_veiculo_por_servicos(c, veiculo_id):
    if not veiculo_id:
        return

    c.execute(
        """
        SELECT id, placa, cliente_id
        FROM veiculos
        WHERE id=?
        """,
        (veiculo_id,),
    )
    veiculo = c.fetchone()

    if not veiculo:
        return

    c.execute(
        """
        SELECT status, entrada, entrega
        FROM servicos
        WHERE veiculo_id=?
        ORDER BY id DESC
        LIMIT 1
        """,
        (veiculo_id,),
    )
    ultimo_servico = c.fetchone()

    if ultimo_servico:
        sincronizar_resumo_veiculo_cliente(
            c,
            veiculo_id,
            placa=veiculo["placa"],
            cliente_id=veiculo["cliente_id"],
            status_atendimento=ultimo_servico["status"],
            entrada=ultimo_servico["entrada"],
            entrega=ultimo_servico["entrega"],
        )
        return

    sincronizar_resumo_veiculo_cliente(
        c,
        veiculo_id,
        placa=veiculo["placa"],
        cliente_id=veiculo["cliente_id"],
        status_atendimento="SEM_ATENDIMENTO",
        entrada=None,
        entrega=None,
    )
    c.execute(
        """
        UPDATE veiculos
        SET atendimento_ativo=0, ultima_entrada=NULL, ultima_entrega=NULL
        WHERE id=?
        """,
        (veiculo_id,),
    )

def formatar_item_historico_servico(servico, checklist_itens=None, fotos_por_servico=None, extras_por_servico=None):
    s_dict = dict(servico)
    entrada = interpretar_datahora_sistema(s_dict.get("entrada"))
    entrega = interpretar_datahora_sistema(s_dict.get("entrega"))

    try:
        if entrada and entrega:
            tempo_str = str(entrega - entrada)
        elif normalizar_texto_campo(s_dict.get("status")).upper() == "EM ANDAMENTO":
            tempo_str = "Em andamento"
        else:
            tempo_str = "Sem registro de entrega"
    except Exception:
        tempo_str = "N/A"

    s_dict["entrada_exibicao"] = (
        entrada.strftime("%d/%m/%Y %H:%M")
        if entrada else (s_dict.get("entrada") or "-")
    )
    s_dict["entrega_exibicao"] = (
        entrega.strftime("%d/%m/%Y %H:%M")
        if entrega else ""
    )
    s_dict["valor_exibicao"] = formatar_valor_monetario(s_dict.get("valor"))
    enriquecer_entrega_servico(s_dict)
    s_dict["checklist_itens"] = list(checklist_itens or [])
    s_dict["checklist_resumo"] = ", ".join(s_dict["checklist_itens"])
    s_dict["galeria_fotos"] = (fotos_por_servico or {}).get(s_dict["id"], {})
    s_dict["fotos_entrada"] = len(s_dict["galeria_fotos"].get("entrada", []))
    s_dict["fotos_detalhe"] = len(s_dict["galeria_fotos"].get("detalhe", []))
    s_dict["fotos_saida"] = len(s_dict["galeria_fotos"].get("saida", []))
    s_dict["total_fotos"] = s_dict["fotos_entrada"] + s_dict["fotos_detalhe"] + s_dict["fotos_saida"]
    s_dict["tem_fotos"] = bool(s_dict["total_fotos"])
    s_dict["cobrancas_extras_info"] = (extras_por_servico or {}).get(
        s_dict["id"],
        {
            "itens": [],
            "total": 0.0,
            "total_exibicao": "0.00",
        },
    )
    s_dict["tem_cobrancas_extras"] = bool(s_dict["cobrancas_extras_info"]["itens"])
    enriquecer_responsaveis_servico(s_dict)
    enriquecer_etapas_operacionais_servico(s_dict)
    return {
        "servico": s_dict,
        "tempo_str": tempo_str,
    }

def listar_historico_servicos(placa=None, busca="", limite=None, conn=None):
    conn_local = None
    try:
        conn_local = conn or conectar()
        c = conn_local.cursor()
        servicos_db = consultar_historico_servicos_domain(
            c,
            empresa_atual_id(),
            placa=placa,
            busca=busca,
            limite=limite,
        )

        ids_servicos = [row["id"] for row in servicos_db]
        checklist_por_servico = listar_nomes_checklist_por_servicos_domain(c, ids_servicos)
        fotos_por_servico = listar_fotos_servicos(ids_servicos, cursor=c)
        extras_por_servico = listar_cobrancas_extras_servicos(ids_servicos, cursor=c)
        return [
            formatar_item_historico_servico(
                row,
                checklist_itens=checklist_por_servico.get(row["id"], []),
                fotos_por_servico=fotos_por_servico,
                extras_por_servico=extras_por_servico,
            )
            for row in servicos_db
        ]
    finally:
        if conn is None and conn_local:
            conn_local.close()

def detectar_novas_colunas(colunas_atuais, colunas_antigas_str):
    if not colunas_antigas_str:
        return []

    colunas_antigas = colunas_antigas_str.split(",")

    novas = [c for c in colunas_atuais if c not in colunas_antigas]
    return novas

def normalizar_texto_comparacao(valor):
    valor = str(valor or "").strip().lower()
    valor = unicodedata.normalize("NFKD", valor)
    return "".join(ch for ch in valor if not unicodedata.combining(ch))

def extrair_gid_url(url):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    fragment = parse_qs(parsed.fragment)
    return query.get("gid", fragment.get("gid", ["0"]))[0]

def montar_url_google_sheets_csv(sheet_id, gid="0"):
    sheet_id = (sheet_id or "").strip()
    gid = str(gid or "0").strip() or "0"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&gid={gid}"

def extrair_sheet_id_google(url):
    url = (url or "").strip()

    if "/d/" in url:
        try:
            return url.split("/d/")[1].split("/")[0]
        except Exception:
            return ""

    parsed = urlparse(url)
    partes = [parte for parte in parsed.path.split("/") if parte]

    for parte in reversed(partes):
        if len(parte) >= 20 and all(ch.isalnum() or ch in "-_" for ch in parte):
            return parte

    return ""

def normalizar_link_planilha(url):
    url = (url or "").strip()

    if "docs.google.com/spreadsheets" in url:
        sheet_id = extrair_sheet_id_google(url)
        if not sheet_id:
            return url

        gid = extrair_gid_url(url)
        return montar_url_google_sheets_csv(sheet_id, gid)

    if "googleusercontent.com" in url:
        sheet_id = extrair_sheet_id_google(url)
        if not sheet_id:
            return url

        gid = extrair_gid_url(url)
        return montar_url_google_sheets_csv(sheet_id, gid)

    if "export?format=" in url:
        return url

    return url

def corrigir_link_google_sheets(url):
    try:
        if "googleusercontent.com" in url:
            sheet_id = extrair_sheet_id_google(url)
            if not sheet_id:
                raise Exception("Link invalido. Use o link original do Google Sheets (docs.google.com).")

            return montar_url_google_sheets_csv(sheet_id, extrair_gid_url(url))

        if "docs.google.com" in url and "/spreadsheets/d/" in url:
            sheet_id = extrair_sheet_id_google(url)
            if not sheet_id:
                raise Exception("NÃ£o foi possÃ­vel identificar o ID da planilha.")

            return montar_url_google_sheets_csv(sheet_id, extrair_gid_url(url))

        raise Exception("Link nao suportado. Use uma planilha do Google Sheets.")

    except Exception as e:
        raise Exception(f"Erro ao processar link: {e}")

def adicionar_parametros_url(url, **novos_parametros):
    parsed = urlparse(url)
    query = parse_qs(parsed.query, keep_blank_values=True)

    for chave, valor in novos_parametros.items():
        query[chave] = [str(valor)]

    return urlunparse(parsed._replace(query=urlencode(query, doseq=True)))

def gerar_urls_candidatas_planilha(url):
    url = corrigir_link_google_sheets(url)
    base = normalizar_link_planilha(url)
    candidatas = [base]
    parsed = urlparse(base)
    host = parsed.netloc.lower()

    if any(dominio in host for dominio in ["1drv.ms", "onedrive.live.com", "sharepoint.com"]):
        candidatas.append(adicionar_parametros_url(base, download=1))
        candidatas.append(adicionar_parametros_url(base, download=1, web=0))

        if "sharepoint.com" in host:
            candidatas.append(
                f"{parsed.scheme}://{parsed.netloc}/_layouts/15/download.aspx?SourceUrl={quote(base, safe='')}"
            )

    urls_unicas = []

    for candidata in candidatas:
        if candidata and candidata not in urls_unicas:
            urls_unicas.append(candidata)

    return urls_unicas

def normalizar_intervalo_sincronizacao(valor):
    try:
        valor = int(valor)
    except Exception:
        valor = 60

    intervalos_validos = {item["value"] for item in INTERVALOS_SINCRONIZACAO}
    return valor if valor in intervalos_validos else 60

def parece_html(texto):
    texto = (texto or "").lstrip().lower()
    return texto.startswith("<!doctype html") or texto.startswith("<html") or "<body" in texto[:500]

def ler_csv_flexivel(conteudo_bytes):
    amostra = conteudo_bytes[:4096].decode("utf-8-sig", errors="ignore")

    if parece_html(amostra):
        raise ValueError(
            "O link retornou uma pÃ¡gina HTML em vez da planilha. "
            "Verifique se a planilha do Google estÃ¡ acessÃ­vel pelo link."
        )

    delimitadores = [";", ",", "\t", "|"]
    contagens = {
        delimitador: amostra.count(delimitador)
        for delimitador in delimitadores
    }
    delimitadores.sort(key=lambda item: contagens[item], reverse=True)

    tentativas = []

    try:
        dialect = csv.Sniffer().sniff(amostra, delimiters=";,\t|")
        if dialect.delimiter in delimitadores:
            delimitadores.remove(dialect.delimiter)
        delimitadores.insert(0, dialect.delimiter)
    except Exception:
        pass

    for delimitador in delimitadores:
        tentativas.extend([
            {
                "sep": delimitador,
                "engine": "python",
                "encoding": "utf-8-sig",
            },
            {
                "sep": delimitador,
                "engine": "python",
                "encoding": "utf-8-sig",
                "quotechar": '"',
                "doublequote": True,
                "skipinitialspace": True,
            },
            {
                "sep": delimitador,
                "engine": "python",
                "encoding": "utf-8-sig",
                "quoting": csv.QUOTE_NONE,
                "on_bad_lines": "skip",
            },
        ])

    melhor_df = None
    melhor_total_colunas = 0
    ultimo_erro = None

    for tentativa in tentativas:
        try:
            df = pd.read_csv(BytesIO(conteudo_bytes), **tentativa)

            if len(df.columns) > melhor_total_colunas:
                melhor_df = df
                melhor_total_colunas = len(df.columns)

            if len(df.columns) > 1:
                return df
        except Exception as e:
            ultimo_erro = e

    if melhor_df is not None:
        return melhor_df

    raise ultimo_erro or ValueError("NÃ£o foi possÃ­vel interpretar o CSV.")

def sugerir_mapeamento_colunas(colunas):
    sugestao = {}
    colunas_norm = {str(coluna): normalizar_texto_comparacao(coluna) for coluna in colunas}

    for chave, aliases in ALIASES_CAMPOS_SYNC.items():
        sugestao[chave] = ""

        for coluna_original, coluna_norm in colunas_norm.items():
            if coluna_norm == chave or coluna_norm in aliases:
                sugestao[chave] = coluna_original
                break

        if sugestao[chave]:
            continue

        for coluna_original, coluna_norm in colunas_norm.items():
            if any(alias in coluna_norm for alias in aliases):
                sugestao[chave] = coluna_original
                break

    return sugestao

def ler_dataframe_link_planilha(url, intervalo_minutos=None):

    # ðŸ”¥ DETECTAR SHEETY
    if "sheety.co" in url:
        df = ler_planilha_sheety(url, intervalo_minutos=intervalo_minutos)
        return df, url
    if not url:

        raise ValueError("Informe um link de planilha vÃ¡lido.")

    ultimo_erro = None

    for url_candidata in gerar_urls_candidatas_planilha(url):
        try:
            resposta = requests.get(url_candidata, timeout=20)
            resposta.raise_for_status()

            conteudo = BytesIO(resposta.content)
            content_type = (resposta.headers.get("content-type") or "").lower()
            url_final = resposta.url or url_candidata
            url_lower = url_final.lower()
            amostra_texto = resposta.content[:4096].decode("utf-8-sig", errors="ignore")

            if "text/html" in content_type or parece_html(amostra_texto):
                ultimo_erro = ValueError(
                    "O link abriu uma pÃ¡gina de visualizaÃ§Ã£o em vez do arquivo da planilha. "
                    "No OneDrive/SharePoint, confirme que o link permite download em modo leitura."
                )
                continue

            try:
                if "csv" in content_type or url_lower.endswith(".csv") or "format=csv" in url_lower:
                    df = ler_csv_flexivel(resposta.content)
                else:
                    df = pd.read_excel(conteudo)
            except Exception:
                if "csv" in content_type or url_lower.endswith(".csv") or "format=csv" in url_lower:
                    raise

                conteudo.seek(0)
                df = ler_csv_flexivel(resposta.content)

            df = df.fillna("")
            df.columns = [str(coluna).strip().lower() for coluna in df.columns]

            return df, url_final
        except Exception as e:
            ultimo_erro = e

    raise ultimo_erro or ValueError(
        "NÃ£o consegui acessar um arquivo de planilha vÃ¡lido por esse link."
    )

def limpar_valor_planilha(valor):
    if valor is None:
        return ""

    texto = str(valor).strip()
    return "" if texto.lower() == "nan" else texto

MAPA_MESES_PT = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}

def interpretar_data_planilha(valor):
    texto_original = limpar_valor_planilha(valor)

    if not texto_original:
        return None

    texto = normalizar_texto_comparacao(texto_original).replace(".", "").strip()

    for formato in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%Y-%m-%d"):
        try:
            return datetime.strptime(texto, formato)
        except Exception:
            continue

    match = re.search(r"(\d{1,2})\s*[-/ ]\s*([a-z]{3,9})(?:\s*[-/ ]\s*(\d{2,4}))?", texto)
    if not match:
        return None

    dia = int(match.group(1))
    mes_texto = match.group(2)[:3]
    mes = MAPA_MESES_PT.get(mes_texto)

    if not mes:
        return None

    ano_texto = match.group(3)
    hoje = agora().date()

    if ano_texto:
        ano = int(ano_texto)
        if ano < 100:
            ano += 2000
    else:
        ano = hoje.year

    try:
        data = datetime(ano, mes, dia)
    except Exception:
        return None

    if data.date() > hoje + timedelta(days=7):
        try:
            data = datetime(ano - 1, mes, dia)
        except Exception:
            return None

    return data

def obter_colunas_preview_sync(mapeamento):
    ordem = ["nome", "modelo", "cor", "placa", "servico"]
    colunas = []

    for chave in ordem:
        coluna = (mapeamento or {}).get(chave)
        if coluna and coluna not in colunas:
            colunas.append(coluna)

    return colunas

def montar_registros_historico_lavagens(df, mapeamento):
    registros = []
    estatisticas = {
        "historico_linhas": 0,
        "historico_com_data": 0,
    }

    for _, row in df.iterrows():
        placa = limpar_valor_planilha(row.get(mapeamento.get("placa", ""), "")).upper()

        if not placa:
            continue

        cliente = (
            limpar_valor_planilha(row.get(mapeamento.get("nome", ""), ""))
            if mapeamento.get("nome") else ""
        )
        carro = (
            limpar_valor_planilha(row.get(mapeamento.get("modelo", ""), ""))
            if mapeamento.get("modelo") else ""
        )
        cor = (
            limpar_valor_planilha(row.get(mapeamento.get("cor", ""), ""))
            if mapeamento.get("cor") else ""
        )
        servico = (
            limpar_valor_planilha(row.get(mapeamento.get("servico", ""), ""))
            if mapeamento.get("servico") else ""
        )
        data_original = (
            limpar_valor_planilha(row.get(mapeamento.get("data", ""), ""))
            if mapeamento.get("data") else ""
        )
        data_lavagem = interpretar_data_planilha(data_original)

        if not any([cliente, carro, cor, servico]):
            continue

        registros.append({
            "placa": placa,
            "cliente": cliente,
            "carro": carro,
            "cor": cor,
            "servico": servico,
            "data_original": data_original,
            "data_lavagem": data_lavagem.date().isoformat() if data_lavagem else None,
        })
        estatisticas["historico_linhas"] += 1

        if data_lavagem:
            estatisticas["historico_com_data"] += 1

    return registros, estatisticas

def salvar_historico_lavagens_sync(sync_id, registros, empresa_id=None):
    conn = conectar()
    c = conn.cursor()
    agora_atual = agora_iso()
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())
    salvar_historico_lavagens_sync_domain(c, sync_id, registros, agora_atual, empresa_id)

    conn.commit()
    conn.close()

def buscar_ultima_lavagem_sync_placa(placa):
    conn = conectar()
    c = conn.cursor()
    row = consultar_historico_sync_por_placa_domain(c, empresa_atual_id(), placa)
    conn.close()
    return dict(row) if row else None

def buscar_ultima_lavagem_local_placa(placa):
    conn = conectar()
    c = conn.cursor()
    row = consultar_ultima_lavagem_local_por_placa_domain(c, empresa_atual_id(), placa)
    conn.close()
    return dict(row) if row else None

def gerar_recomendacoes_servicos_lavagem(dias_desde=None, servico_anterior=""):
    servico_norm = normalizar_texto_comparacao(servico_anterior)
    recomendacoes = []

    if dias_desde is None:
        recomendacoes.append("Sem data valida no historico. Vale confirmar a ultima lavagem com o cliente.")
    elif dias_desde >= 30:
        recomendacoes.append("Sugerir lavagem completa com acabamento de pneus e revisao dos vidros.")
    elif dias_desde >= 15:
        recomendacoes.append("Boa hora para oferecer lavagem completa e reforco no acabamento externo.")
    elif dias_desde >= 7:
        recomendacoes.append("Sugerir lavagem de manutencao ou expressa para manter o carro em dia.")
    else:
        recomendacoes.append("Lavagem recente. Oferecer manutencao leve apenas se o carro ja estiver com poeira.")

    if "expressa" in servico_norm:
        recomendacoes.append("Como a ultima foi expressa, vale apresentar a completa na proxima visita.")

    if "completa" in servico_norm and (dias_desde or 0) >= 15:
        recomendacoes.append("Oferecer cera para prolongar brilho e protecao da pintura.")

    if "cera" not in servico_norm and (dias_desde or 0) >= 20:
        recomendacoes.append("Apresentar opcao de cera e hidro vidros para elevar o ticket.")

    unicas = []
    for item in recomendacoes:
        if item not in unicas:
            unicas.append(item)

    return unicas[:3]

def montar_contexto_lavagem_placa(placa):
    historico_sync = buscar_ultima_lavagem_sync_placa(placa)
    origem = "historico sincronizado"
    data_ref = None
    servico_anterior = ""

    if historico_sync:
        data_ref = interpretar_datahora_sistema(historico_sync.get("data_lavagem"))
        servico_anterior = historico_sync.get("servico") or "Servico nao informado"
        carro = historico_sync.get("carro") or ""
        cor = historico_sync.get("cor") or ""
    else:
        historico_local = buscar_ultima_lavagem_local_placa(placa)
        if not historico_local:
            return None
        origem = "historico interno"
        data_ref = interpretar_datahora_sistema(historico_local.get("entrega"))
        servico_anterior = historico_local.get("servico") or "Servico nao informado"
        carro = ""
        cor = ""

    dias_desde = None
    if data_ref:
        dias_desde = max(0, (agora().date() - data_ref.date()).days)

    recomendacoes = gerar_recomendacoes_servicos_lavagem(dias_desde, servico_anterior)

    return {
        "origem": origem,
        "servico_anterior": servico_anterior,
        "data_exibicao": data_ref.strftime("%d/%m/%Y") if data_ref else "Data nao encontrada",
        "dias_desde": dias_desde,
        "recomendacoes": recomendacoes,
        "carro": carro,
        "cor": cor,
        "tipo_alerta": "aviso" if dias_desde is not None and dias_desde >= 15 else "sucesso",
    }

def faixa_retorno_por_dias(dias_desde):
    if dias_desde is None:
        return None

    if dias_desde >= 45:
        return 45

    if dias_desde >= 30:
        return 30

    if dias_desde >= 15:
        return 15

    return None

def descrever_faixa_retorno(faixa):
    if faixa == 45:
        return "45+ dias"
    if faixa == 30:
        return "30+ dias"
    if faixa == 15:
        return "15+ dias"
    return "Sem faixa"

def descrever_prioridade_retorno(faixa):
    if faixa == 45:
        return "Urgente"
    if faixa == 30:
        return "Contato recomendado"
    if faixa == 15:
        return "Lembrete"
    return "Monitorar"

def montar_sugestao_contato_retorno(item):
    cliente = (item.get("cliente") or "").strip()
    primeiro_nome = cliente.split()[0] if cliente else "cliente"
    placa = item.get("placa") or "veiculo"
    carro = (item.get("carro") or "").strip()
    referencia = carro or f"veiculo placa {placa}"
    dias_desde = item.get("dias_desde")
    recomendacao = (item.get("recomendacoes") or ["Podemos sugerir um novo cuidado para o carro."])[0]
    saudacao = (
        f"Oi {primeiro_nome}, tudo bem?"
        if cliente else
        "Ola, tudo bem?"
    )
    dias_texto = f"{dias_desde} dias" if dias_desde is not None else "alguns dias"
    recomendacao = str(recomendacao or "").rstrip(".")

    return (
        f"{saudacao} Seu {referencia} ja esta ha {dias_texto} sem retornar na Wagen. "
        f"{recomendacao}. Se quiser, podemos deixar seu atendimento agendado."
    )

def listar_ultimas_lavagens_sync():
    conn = conectar()
    c = conn.cursor()
    rows = consultar_ultimas_lavagens_sync_domain(c, empresa_atual_id())
    conn.close()

    ultimos = {}

    for row in rows:
        item = dict(row)
        placa = (item.get("placa") or "").strip().upper()

        if not placa or placa in ultimos:
            continue

        item["placa"] = placa
        item["origem"] = "historico sincronizado"
        item["data_ref"] = interpretar_datahora_sistema(item.get("data_lavagem"))
        ultimos[placa] = item

    return ultimos

def listar_ultimas_lavagens_locais():
    conn = conectar()
    c = conn.cursor()
    rows = consultar_ultimas_lavagens_locais_domain(c, empresa_atual_id())
    conn.close()

    ultimos = {}

    for row in rows:
        item = dict(row)
        placa = (item.get("placa") or "").strip().upper()

        if not placa or placa in ultimos:
            continue

        item["placa"] = placa
        item["origem"] = "historico interno"
        item["data_ref"] = interpretar_datahora_sistema(item.get("entrega"))
        ultimos[placa] = item

    return ultimos

def escolher_referencia_retorno(sync_item=None, local_item=None):
    sync_data = sync_item.get("data_ref") if sync_item else None
    local_data = local_item.get("data_ref") if local_item else None

    if sync_item and local_item:
        if sync_data and local_data:
            principal = sync_item if sync_data >= local_data else local_item
            apoio = local_item if principal is sync_item else sync_item
            return principal, apoio
        if sync_data:
            return sync_item, local_item
        if local_data:
            return local_item, sync_item
        return sync_item, local_item

    if sync_item:
        return sync_item, None

    if local_item:
        return local_item, None

    return None, None

def mesclar_dados_retorno(principal, apoio=None):
    dados = {}

    for chave in ("placa", "cliente", "carro", "cor", "servico", "origem"):
        dados[chave] = (
            (principal or {}).get(chave) or
            (apoio or {}).get(chave) or
            ""
        )

    dados["data_ref"] = (principal or {}).get("data_ref") or (apoio or {}).get("data_ref")
    return dados

def normalizar_status_retorno(valor):
    chave = normalizar_texto_comparacao(valor).replace(" ", "_")
    return chave if chave in STATUS_RETORNO_LABELS else STATUS_RETORNO_PADRAO

def normalizar_filtro_retorno(valor):
    chave = normalizar_texto_comparacao(valor).replace(" ", "_")
    validos = {item["value"] for item in STATUS_RETORNO_OPCOES}
    return chave if chave in validos else "acao_agora"

def buscar_contatos_clientes_por_placa(placas):
    placas = [normalizar_texto_campo(item).upper() for item in placas if normalizar_texto_campo(item)]

    if not placas:
        return {}

    conn = conectar()
    c = conn.cursor()
    rows = consultar_contatos_clientes_por_placas_domain(c, empresa_atual_id(), placas)
    conn.close()

    return {
        item["placa"]: item
        for item in rows
        if item.get("placa")
    }

def carregar_estados_retornos(placas=None):
    conn = conectar()
    c = conn.cursor()
    rows = consultar_estados_retorno_domain(c, empresa_atual_id(), placas=placas)
    conn.close()

    for item in rows:
        item["placa"] = normalizar_texto_campo(item.get("placa")).upper()
        item["status"] = normalizar_status_retorno(item.get("status"))

    return {
        item["placa"]: item
        for item in rows
        if item.get("placa")
    }

def construir_link_whatsapp_retorno(telefone, mensagem=""):
    telefone_limpo = re.sub(r"\D", "", str(telefone or ""))

    if not telefone_limpo:
        return ""

    if len(telefone_limpo) in {10, 11} and not telefone_limpo.startswith("55"):
        telefone_limpo = "55" + telefone_limpo

    if not telefone_limpo:
        return ""

    mensagem = normalizar_texto_campo(mensagem)
    if mensagem:
        return f"https://wa.me/{telefone_limpo}?text={quote(mensagem)}"

    return f"https://wa.me/{telefone_limpo}"

def retorno_exige_acao_agora(status, proximo_contato_dt=None):
    status = normalizar_status_retorno(status)

    if status == "sem_interesse":
        return False

    if status == "contatado":
        return False

    if status == "reagendado" and proximo_contato_dt and proximo_contato_dt > agora():
        return False

    return True

def descrever_status_retorno_item(item):
    status = normalizar_status_retorno(item.get("status_retorno"))
    proximo_fmt = item.get("proximo_contato_em_fmt")

    if status == "reagendado":
        if item.get("reagendamento_vencido"):
            return f"Reagendamento vencido desde {proximo_fmt}"
        if item.get("proximo_contato_em"):
            return f"Reagendado para {proximo_fmt}"
        return "Reagendado sem data definida"

    if status == "contatado":
        if item.get("ultimo_contato_em_fmt"):
            return f"Cliente contatado em {item['ultimo_contato_em_fmt']}"
        return "Cliente contatado"

    if status == "sem_interesse":
        return "Cliente sem interesse no momento"

    return "Acao comercial pendente"

def enriquecer_item_retorno_comercial(item, estado=None, contato=None):
    item = dict(item or {})
    estado = estado or {}
    contato = contato or {}

    if contato.get("cliente_nome") and not item.get("cliente"):
        item["cliente"] = contato.get("cliente_nome")

    if contato.get("carro") and not item.get("carro"):
        item["carro"] = contato.get("carro")

    if contato.get("cor") and not item.get("cor"):
        item["cor"] = contato.get("cor")

    item["telefone"] = normalizar_texto_campo(contato.get("telefone"))
    item["cliente"] = normalizar_texto_campo(item.get("cliente"))
    item["carro"] = normalizar_texto_campo(item.get("carro"))
    item["cor"] = normalizar_texto_campo(item.get("cor"))
    item["servico"] = normalizar_texto_campo(item.get("servico"))
    item["origem"] = normalizar_texto_campo(item.get("origem")) or "historico"

    status_retorno = normalizar_status_retorno(estado.get("status"))
    proximo_contato_em = normalizar_texto_campo(estado.get("proximo_contato_em"))
    ultimo_contato_em = normalizar_texto_campo(estado.get("ultimo_contato_em"))
    proximo_contato_dt = interpretar_datahora_sistema(proximo_contato_em)
    ultimo_contato_dt = interpretar_datahora_sistema(ultimo_contato_em)

    item["status_retorno"] = status_retorno
    item["status_retorno_label"] = STATUS_RETORNO_LABELS.get(status_retorno, "Pendente")
    item["observacao"] = normalizar_texto_campo(estado.get("observacao"))
    item["proximo_contato_em"] = proximo_contato_em
    item["proximo_contato_em_fmt"] = formatar_datahora(proximo_contato_em) if proximo_contato_em else ""
    item["proximo_contato_restante"] = formatar_tempo_restante(proximo_contato_em) if proximo_contato_em else ""
    item["ultimo_contato_em"] = ultimo_contato_em
    item["ultimo_contato_em_fmt"] = formatar_datahora(ultimo_contato_em) if ultimo_contato_em else ""
    item["usuario_responsavel"] = formatar_usuario_exibicao(
        estado.get("usuario_nome"),
        estado.get("usuario"),
        fallback="Nao definido",
    )
    item["ultima_acao_retorno"] = normalizar_texto_campo(estado.get("ultima_acao"))
    item["reagendado_dias"] = int(estado.get("reagendado_dias") or 0)
    item["reagendamento_vencido"] = (
        status_retorno == "reagendado" and (
            not proximo_contato_dt or proximo_contato_dt <= agora()
        )
    )
    item["mostrar_na_agenda"] = retorno_exige_acao_agora(status_retorno, proximo_contato_dt)
    item["status_resumo"] = descrever_status_retorno_item(item)
    item["whatsapp_url"] = construir_link_whatsapp_retorno(
        item.get("telefone"),
        item.get("sugestao_contato"),
    )
    item["busca_texto"] = normalizar_texto_comparacao(
        " ".join(
            str(parte or "")
            for parte in (
                item.get("placa"),
                item.get("cliente"),
                item.get("carro"),
                item.get("cor"),
                item.get("telefone"),
                item.get("servico"),
                item.get("observacao"),
                item.get("status_retorno_label"),
            )
        )
    )
    return item

def montar_itens_retornos_comerciais():
    historico_sync = listar_ultimas_lavagens_sync()
    historico_local = listar_ultimas_lavagens_locais()
    placas = sorted(set(historico_sync.keys()) | set(historico_local.keys()))

    contatos = buscar_contatos_clientes_por_placa(placas)
    estados = carregar_estados_retornos(placas)
    itens = []
    hoje = agora().date()

    for placa in placas:
        principal, apoio = escolher_referencia_retorno(
            historico_sync.get(placa),
            historico_local.get(placa),
        )

        if not principal:
            continue

        item = mesclar_dados_retorno(principal, apoio)
        data_ref = item.get("data_ref")

        if not data_ref:
            continue

        dias_desde = max(0, (hoje - data_ref.date()).days)
        faixa = faixa_retorno_por_dias(dias_desde)

        if not faixa:
            continue

        recomendacoes = gerar_recomendacoes_servicos_lavagem(
            dias_desde,
            item.get("servico") or "",
        )

        item["dias_desde"] = dias_desde
        item["faixa_alerta"] = faixa
        item["faixa_label"] = descrever_faixa_retorno(faixa)
        item["prioridade_label"] = descrever_prioridade_retorno(faixa)
        item["ultima_lavagem"] = data_ref.strftime("%d/%m/%Y")
        item["recomendacoes"] = recomendacoes
        item["sugestao_contato"] = montar_sugestao_contato_retorno(item)
        itens.append(
            enriquecer_item_retorno_comercial(
                item,
                estado=estados.get(placa),
                contato=contatos.get(placa),
            )
        )

    prioridade_faixa = {45: 0, 30: 1, 15: 2}
    prioridade_status = {"pendente": 0, "reagendado": 1, "contatado": 2, "sem_interesse": 3}
    itens.sort(
        key=lambda item: (
            0 if item.get("mostrar_na_agenda") else 1,
            prioridade_status.get(item.get("status_retorno"), 9),
            prioridade_faixa.get(item.get("faixa_alerta"), 9),
            -(item.get("dias_desde") or 0),
            item.get("placa") or "",
        )
    )
    return itens

def carregar_contexto_retornos(args):
    filtro_status = normalizar_filtro_retorno(args.get("status"))
    busca = normalizar_texto_campo(args.get("busca"))
    busca_norm = normalizar_texto_comparacao(busca)
    itens = montar_itens_retornos_comerciais()

    resumo = {
        "total": len(itens),
        "acao_agora": sum(1 for item in itens if item.get("mostrar_na_agenda")),
        "reagendados": sum(1 for item in itens if item.get("status_retorno") == "reagendado"),
        "contatados": sum(1 for item in itens if item.get("status_retorno") == "contatado"),
        "sem_interesse": sum(1 for item in itens if item.get("status_retorno") == "sem_interesse"),
    }

    if busca_norm:
        itens = [
            item for item in itens
            if busca_norm in item.get("busca_texto", "")
        ]

    if filtro_status == "acao_agora":
        itens = [item for item in itens if item.get("mostrar_na_agenda")]
    elif filtro_status != "todos":
        itens = [item for item in itens if item.get("status_retorno") == filtro_status]

    return {
        "itens": itens,
        "resumo": resumo,
        "filtros": {
            "status": filtro_status,
            "busca": busca,
        },
        "status_opcoes": STATUS_RETORNO_OPCOES,
    }

def listar_agenda_retorno_lavagens(limite=AGENDA_RETORNO_LIMITE_ITENS):
    itens = [item for item in montar_itens_retornos_comerciais() if item.get("mostrar_na_agenda")]
    contadores = {"15": 0, "30": 0, "45": 0}

    for item in itens:
        faixa = str(item.get("faixa_alerta") or "")
        if faixa in contadores:
            contadores[faixa] += 1

    total = len(itens)
    limite = max(1, int(limite or AGENDA_RETORNO_LIMITE_ITENS))
    return {
        "total": total,
        "itens": itens[:limite],
        "contadores": contadores,
        "ultima_atualizacao": agora_iso(),
        "restantes": max(0, total - min(total, limite)),
    }

def proximo_contato_retorno_por_dias(dias):
    referencia = agora() + timedelta(days=max(1, int(dias or 1)))
    referencia = referencia.replace(hour=9, minute=0, second=0, microsecond=0)
    return referencia.isoformat(timespec="seconds")

def upsert_retorno_cliente(
    placa,
    status,
    observacao="",
    proximo_contato_em="",
    ultimo_contato_em="",
    ultima_acao="",
    reagendado_dias=0,
    usuario=None,
):
    usuario_info = usuario or resumo_usuario_logado()
    conn = conectar()
    try:
        c = conn.cursor()
        empresa_id = empresa_atual_id()
        placa_norm = normalizar_texto_campo(placa).upper()
        agora_atual = agora_iso()
        valores = (
            normalizar_status_retorno(status),
            normalizar_texto_campo(observacao),
            normalizar_texto_campo(proximo_contato_em),
            normalizar_texto_campo(ultimo_contato_em),
            normalizar_texto_campo(ultima_acao),
            int(reagendado_dias or 0),
            normalizar_texto_campo(usuario_info.get("usuario")),
            normalizar_texto_campo(usuario_info.get("nome")),
            agora_atual,
            empresa_id,
            placa_norm,
        )
        c.execute(
            """
            UPDATE retornos_clientes
            SET status=?,
                observacao=?,
                proximo_contato_em=?,
                ultimo_contato_em=?,
                ultima_acao=?,
                reagendado_dias=?,
                usuario=?,
                usuario_nome=?,
                atualizado_em=?
            WHERE empresa_id=? AND UPPER(TRIM(placa))=?
            """,
            valores,
        )
        if int(c.rowcount or 0) <= 0:
            c.execute(
                """
                INSERT INTO retornos_clientes (
                    empresa_id, placa, status, observacao, proximo_contato_em, ultimo_contato_em,
                    ultima_acao, reagendado_dias, usuario, usuario_nome, criado_em, atualizado_em
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    empresa_id,
                    placa_norm,
                    valores[0],
                    valores[1],
                    valores[2],
                    valores[3],
                    valores[4],
                    valores[5],
                    valores[6],
                    valores[7],
                    agora_atual,
                    agora_atual,
                ),
            )
        conn.commit()
    finally:
        conn.close()

def obter_mapeamento_sync_por_form(form):
    return {
        campo["key"]: (form.get(f"campo_{campo['key']}") or "").strip()
        for campo in CAMPOS_SINCRONIZACAO_CLIENTES
    }

def descrever_campos_sincronizados(sync):
    campos = []

    for campo in CAMPOS_SINCRONIZACAO_CLIENTES:
        coluna = sync.get(f"campo_{campo['key']}")

        if coluna:
            campos.append({
                "label": campo["label"],
                "coluna": coluna
            })

    return campos

def importar_clientes_dataframe(df, mapeamento, empresa_id=None):
    conn = conectar()
    c = conn.cursor()
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())

    estatisticas = {
        "linhas_lidas": len(df.index),
        "linhas_processadas": 0,
        "linhas_ignoradas": 0,
        "clientes_novos": 0,
        "clientes_atualizados": 0,
        "veiculos_novos": 0,
        "veiculos_atualizados": 0,
    }

    for _, row in df.iterrows():
        placa = limpar_valor_planilha(row.get(mapeamento.get("placa", ""), "")).upper()

        if not placa:
            estatisticas["linhas_ignoradas"] += 1
            continue

        nome = (
            limpar_valor_planilha(row.get(mapeamento.get("nome", ""), ""))
            if mapeamento.get("nome") else ""
        )
        telefone = (
            limpar_valor_planilha(row.get(mapeamento.get("telefone", ""), ""))
            if mapeamento.get("telefone") else ""
        )
        modelo = (
            limpar_valor_planilha(row.get(mapeamento.get("modelo", ""), ""))
            if mapeamento.get("modelo") else ""
        )
        cor = (
            limpar_valor_planilha(row.get(mapeamento.get("cor", ""), ""))
            if mapeamento.get("cor") else ""
        )

        resultado = salvar_cliente_veiculo_cursor_domain(
            c,
            empresa_id,
            placa,
            nome=nome,
            telefone=telefone,
            modelo=modelo,
            cor=cor,
            placa_referencia=placa,
            sincronizar_placa_principal_fn=sincronizar_placa_principal_cliente,
        )
        cliente_id = resultado.get("cliente_id")

        if resultado.get("acao") == "novo":
            estatisticas["veiculos_novos"] += 1
            if resultado.get("cliente_acao") == "novo":
                estatisticas["clientes_novos"] += 1
        else:
            estatisticas["veiculos_atualizados"] += 1
            if resultado.get("cliente_acao") == "novo":
                estatisticas["clientes_novos"] += 1
            elif resultado.get("cliente_acao") == "atualizado":
                estatisticas["clientes_atualizados"] += 1

        estatisticas["linhas_processadas"] += 1

    conn.commit()
    conn.close()

    return estatisticas

def resumir_importacao_clientes(estatisticas):
    resumo = (
        f"{estatisticas['linhas_processadas']} linha(s) processada(s), "
            f"{estatisticas['veiculos_novos']} veiculo(s) novo(s), "
            f"{estatisticas['veiculos_atualizados']} veiculo(s) atualizado(s), "
        f"{estatisticas['clientes_novos']} cliente(s) novo(s), "
        f"{estatisticas['clientes_atualizados']} cliente(s) atualizado(s)"
    )

    if "historico_linhas" in estatisticas:
        resumo += (
            f", {estatisticas['historico_linhas']} lavagem(ns) no historico"
        )

    return resumo

def buscar_sincronizacao_cliente(sync_id, empresa_id=None):
    conn = conectar()
    c = conn.cursor()
    sync = consultar_sincronizacao_cliente_domain(c, sync_id, empresa_id=empresa_id)
    conn.close()
    return sync

def executar_sincronizacao_cliente(sync_id, empresa_id=None):
    sync = buscar_sincronizacao_cliente(sync_id, empresa_id=empresa_id)
    debug_sync = bool_config_ativo(os.environ.get("DEBUG_SYNC_PLANILHA", ""))

    if not sync:
        return False, "Sincronizacao nao encontrada."

    empresa_id_sync = normalize_empresa_id(empresa_id or sync.get("empresa_id") or 1)

    try:
        url_base = sync["url"]

        # ðŸ”¥ SHEETY (FLUXO DIRETO)
        if "sheety.co" in url_base:
            df = ler_planilha_sheety(url_base, intervalo_minutos=sync["intervalo_minutos"])

            log_info("ðŸ”¥ DADOS SHEETY:")
            log_info(df.tail(5))

            hash_atual = hashlib.md5(
                df.to_csv(index=False).encode("utf-8")
            ).hexdigest()
            url_usada = url_base

        # ðŸ”½ PLANILHA NORMAL (GOOGLE, CSV, EXCEL)
        else:
            url_base = corrigir_link_google_sheets(url_base)

            url_base = url_base + "?_=" + str(time.time())
            resposta = requests.get(url_base, timeout=20)
            resposta.raise_for_status()

            log_info("URL FINAL:", url_base)
            log_info("STATUS:", resposta.status_code)
            log_info("CONTENT TYPE:", resposta.headers.get("content-type"))

            hash_atual = hashlib.md5(resposta.content).hexdigest()

            conteudo = BytesIO(resposta.content)

            try:
                if "csv" in (resposta.headers.get("content-type") or "").lower():
                    df = ler_csv_flexivel(resposta.content)
                else:
                    df = pd.read_excel(conteudo)
            except:
                conteudo.seek(0)
                df = ler_csv_flexivel(resposta.content)

            df = df.fillna("")
            df.columns = [str(col).strip().lower() for col in df.columns]

            log_info("ðŸ”¥ DADOS PLANILHA:")
            log_info(df.tail(5))

            url_usada = url_base

        if sync["ultimo_hash"] == hash_atual:
            agora_atual = agora_iso()
            mensagem = "Sem alteraÃ§Ãµes na planilha."
            conn = conectar()
            c = conn.cursor()
            atualizar_status_sincronizacao_cliente_domain(
                c,
                sync_id,
                empresa_id_sync,
                ultimo_sync_em=agora_atual,
                proximo_sync_em=somar_minutos_iso(sync["intervalo_minutos"]),
                ultimo_status="OK",
                ultima_mensagem=mensagem,
                atualizado_em=agora_atual,
            )
            conn.commit()
            conn.close()
            return True, mensagem

        # ðŸ”½ MAPEAMENTO
        mapeamento = {
            campo["key"]: sync[f"campo_{campo['key']}"] or ""
            for campo in CAMPOS_SINCRONIZACAO_CLIENTES
        }
        mapeamento["telefone"] = sync["campo_telefone"] or ""
        mapeamento["data"] = sync["campo_data"] or ""

        # ðŸ”½ IMPORTAÃ‡ÃƒO
        estatisticas = importar_clientes_dataframe(df, mapeamento, empresa_id=empresa_id_sync)
        registros_historico, estatisticas_historico = montar_registros_historico_lavagens(df, mapeamento)
        estatisticas.update(estatisticas_historico)
        salvar_historico_lavagens_sync(sync_id, registros_historico, empresa_id=empresa_id_sync)
        mensagem = resumir_importacao_clientes(estatisticas)

        agora_atual = agora_iso()

        # ðŸ”½ SALVAR RESULTADO
        conn = conectar()
        c = conn.cursor()

        atualizar_status_sincronizacao_cliente_domain(
            c,
            sync_id,
            empresa_id_sync,
            url=url_usada,
            ultimo_sync_em=agora_atual,
            proximo_sync_em=somar_minutos_iso(sync["intervalo_minutos"]),
            ultimo_status="OK",
            ultima_mensagem=mensagem,
            ultimo_hash=hash_atual,
            atualizado_em=agora_atual,
        )

        conn.commit()
        conn.close()

        salvar_notificacao(mensagem, "sucesso")

        return True, mensagem

    except Exception as e:
        fatal = getattr(e, "fatal", False)
        mensagem_base = str(e)
        mensagem = (
            f"Sincronizacao pausada automaticamente: {mensagem_base}"
            if fatal else
            f"Erro ao sincronizar: {mensagem_base}"
        )

        conn = conectar()
        c = conn.cursor()

        atualizar_status_sincronizacao_cliente_domain(
            c,
            sync_id,
            empresa_id_sync,
            ultimo_status="PAUSADA" if fatal else "ERRO",
            ultima_mensagem=mensagem,
            proximo_sync_em=None if fatal else somar_minutos_iso(sync["intervalo_minutos"]),
            atualizado_em=agora_iso(),
        )
        if fatal:
            c.execute(
                "UPDATE sincronizacoes_clientes SET ativo=? WHERE empresa_id=? AND id=?",
                (0, empresa_id_sync, sync_id),
            )

        conn.commit()
        conn.close()

        salvar_notificacao(mensagem, "erro")

        return False, mensagem

def importar_planilha_local():
    try:
        caminho = os.path.join("static", "CONTROLE LAVAGENS.xlsx")

        if not os.path.exists(caminho):
            return False, "Arquivo CONTROLE LAVAGENS.xlsx nao encontrado."

        df = pd.read_excel(caminho)

        df = df.fillna("")
        df.columns = [str(col).strip().lower() for col in df.columns]

        # ðŸ”½ MAPEAMENTO SIMPLES
        mapeamento = {
            "placa": "placa",
            "nome": "nome",
            "telefone": "telefone",
            "modelo": "modelo",
            "cor": "cor"
        }

        empresa_id = empresa_atual_id()
        estatisticas = importar_clientes_dataframe(df, mapeamento, empresa_id=empresa_id)
        mensagem = resumir_importacao_clientes(estatisticas)

        salvar_notificacao(mensagem, "sucesso")

        return True, mensagem

    except Exception as e:
        mensagem = f"Erro ao importar planilha local: {e}"
        salvar_notificacao(mensagem, "erro")
        return False, mensagem

def _formatar_registros_clientes(registros_db):
    registros = []
    for item in registros_db:
        item["nome"] = item.get("nome") or ""
        item["telefone"] = item.get("telefone") or ""
        item["data_nascimento"] = formatar_data_nascimento_input(item.get("data_nascimento"))
        item["data_nascimento_exibicao"] = formatar_data_nascimento_exibicao(item.get("data_nascimento"))
        item["modelo"] = item.get("modelo") or ""
        item["cor"] = item.get("cor") or ""
        item["placa_original"] = item.get("placa") or ""
        item["placa_principal"] = item.get("placa_principal") or item["placa_original"]
        registros.append(item)
    return registros


def listar_registros_clientes(busca="", conn=None, limite=None):
    def executar_listagem(conn_atual):
        c = conn_atual.cursor()
        registros_db = consultar_registros_clientes_domain(c, empresa_atual_id(), busca=busca, limite=limite)
        return _formatar_registros_clientes(registros_db)

    if conn is not None:
        return executar_listagem(conn)

    conn_local = None
    try:
        conn_local = conectar_somente_leitura()
        return executar_listagem(conn_local)
    except Exception as e:
        log_info("AVISO CLIENTES LISTA:", e)
        try:
            garantir_schema_sqlite_local_minima(force=True)
            conn_local = conectar_banco_local_forcado()
            return executar_listagem(conn_local)
        except Exception:
            raise
    finally:
        try:
            if conn_local:
                conn_local.close()
        except Exception:
            pass

def montar_resumo_base_dados(registros):
    clientes_unicos = set()

    for item in registros:
        if item.get("cliente_id"):
            clientes_unicos.add(f"id:{item['cliente_id']}")
        elif item.get("nome") or item.get("telefone"):
            clientes_unicos.add(f"manual:{item.get('nome','')}|{item.get('telefone','')}")

    return {
        "total_registros": len(registros),
        "total_clientes": len(clientes_unicos),
        "com_telefone": sum(1 for item in registros if item.get("telefone")),
        "com_modelo": sum(1 for item in registros if item.get("modelo")),
    }


def obter_payload_base_dados(force=False):
    chave_cache = f"{empresa_atual_id()}"
    agora_cache_ts = time.time()
    if (
        not force
        and BASE_DADOS_CACHE.get("resultado") is not None
        and BASE_DADOS_CACHE.get("chave") == chave_cache
        and agora_cache_ts - float(BASE_DADOS_CACHE.get("testado_em") or 0.0) < BASE_DADOS_CACHE_TTL
    ):
        return deepcopy(BASE_DADOS_CACHE["resultado"])

    registros = listar_registros_clientes()
    payload = {
        "dados": registros,
        "resumo": montar_resumo_base_dados(registros),
    }
    BASE_DADOS_CACHE["testado_em"] = agora_cache_ts
    BASE_DADOS_CACHE["chave"] = chave_cache
    BASE_DADOS_CACHE["resultado"] = deepcopy(payload)
    return payload

def ler_dataframe_arquivo_planilha(arquivo):
    filename = secure_filename(arquivo.filename or "")

    if not filename:
        raise ValueError("Selecione um arquivo para importar.")

    if not arquivo_planilha_permitido(filename):
        raise ValueError("Use um arquivo CSV, TSV, XLS ou XLSX.")

    conteudo = arquivo.read()

    if not conteudo:
        raise ValueError("O arquivo enviado esta vazio.")

    nome_lower = filename.lower()
    buffer = BytesIO(conteudo)

    try:
        if nome_lower.endswith(".csv") or nome_lower.endswith(".tsv"):
            df = ler_csv_flexivel(conteudo)
        else:
            df = pd.read_excel(buffer)
    except Exception:
        buffer.seek(0)
        df = ler_csv_flexivel(conteudo)

    df = df.fillna("")
    df.columns = [str(coluna).strip().lower() for coluna in df.columns]
    return df, filename

def salvar_cliente_veiculo(placa, nome="", telefone="", data_nascimento=None, modelo="", cor="", placa_original=None):
    placa_nova = limpar_valor_planilha(placa).upper()
    placa_referencia = limpar_valor_planilha(placa_original).upper() or placa_nova
    nome = limpar_valor_planilha(nome)
    telefone = limpar_valor_planilha(telefone)
    data_nascimento = normalizar_data_nascimento(data_nascimento)
    modelo = limpar_valor_planilha(modelo)
    cor = limpar_valor_planilha(cor)

    if not placa_nova:
        raise ValueError("Informe a placa do veiculo.")

    conn = conectar()
    c = conn.cursor()
    empresa_id = empresa_atual_id()

    try:
        resultado = salvar_cliente_veiculo_cursor_domain(
            c,
            empresa_id,
            placa_nova,
            nome=nome,
            telefone=telefone,
            data_nascimento=data_nascimento,
            modelo=modelo,
            cor=cor,
            placa_referencia=placa_referencia,
            sincronizar_placa_principal_fn=sincronizar_placa_principal_cliente,
        )

        conn.commit()

        espelho_planilha = {"sucesso": [], "falhas": [], "ignoradas": []}
        try:
            espelho_planilha = espelhar_cadastro_site_em_sincronizacoes_clientes(
                placa_nova,
                nome=nome,
                telefone=telefone,
                modelo=modelo,
                cor=cor,
            )
        except Exception as erro_espelho:
            espelho_planilha = {
                "sucesso": [],
                "falhas": [{"nome": "Sincronizacao", "erro": str(erro_espelho)}],
                "ignoradas": [],
            }

        return {
            "placa": resultado["placa"],
            "acao": resultado["acao"],
            "cliente_acao": resultado.get("cliente_acao"),
            "cliente_id": resultado.get("cliente_id"),
            "espelho_planilha": espelho_planilha,
        }
    finally:
        conn.close()

def sincronizar_resumo_veiculo_cliente(c, veiculo_id, placa=None, cliente_id=None, status_atendimento=None, entrada=None, entrega=None):
    campos = []
    valores = []

    placa = limpar_valor_planilha(placa).upper()

    if cliente_id is not None:
        campos.append("cliente_id=?")
        valores.append(cliente_id)

    if placa:
        campos.append("placa=?")
        valores.append(placa)

    if status_atendimento is not None:
        status_atendimento = normalizar_texto_campo(status_atendimento).upper() or "SEM_ATENDIMENTO"
        campos.append("status_atendimento=?")
        valores.append(status_atendimento)
        campos.append("atendimento_ativo=?")
        valores.append(1 if status_atendimento == "EM ANDAMENTO" else 0)

    if entrada is not None:
        campos.append("ultima_entrada=?")
        valores.append(entrada)

    if entrega is not None:
        campos.append("ultima_entrega=?")
        valores.append(entrega)

    if campos:
        valores.append(veiculo_id)
        c.execute(
            f"UPDATE veiculos SET {', '.join(campos)} WHERE id=?",
            valores,
        )

    if cliente_id is not None and placa:
        c.execute(
            "UPDATE clientes SET placa_principal=? WHERE id=?",
            (placa, cliente_id),
        )


def sincronizar_placa_principal_cliente(c, cliente_id, placa):
    placa = limpar_valor_planilha(placa).upper()
    if not cliente_id or not placa:
        return

    c.execute(
        "UPDATE clientes SET placa_principal=? WHERE id=?",
        (placa, cliente_id),
    )

def numero_para_coluna_planilha(numero):
    numero = int(numero or 0)
    if numero <= 0:
        return "A"

    letras = ""
    while numero:
        numero, resto = divmod(numero - 1, 26)
        letras = chr(65 + resto) + letras
    return letras

def obter_servico_google_sheets():
    credenciais_info, erro = carregar_credenciais_google_drive()
    if erro:
        raise RuntimeError(erro)
    if not credenciais_info:
        raise RuntimeError("Credenciais do Google nao configuradas.")
    if not google_build or not service_account:
        raise RuntimeError("Dependencias do Google Sheets nao estao disponiveis.")

    credenciais = service_account.Credentials.from_service_account_info(
        credenciais_info,
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/spreadsheets",
        ],
    )
    return google_build("sheets", "v4", credentials=credenciais, cache_discovery=False)

def obter_titulo_aba_planilha_google(spreadsheet_id, gid=None):
    service = obter_servico_google_sheets()
    resposta = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title,index))",
    ).execute()
    folhas = resposta.get("sheets", [])
    if not folhas:
        return ""

    if gid not in {None, "", "0"}:
        gid_texto = str(gid)
        for folha in folhas:
            propriedades = folha.get("properties", {})
            if str(propriedades.get("sheetId")) == gid_texto:
                return propriedades.get("title") or ""

    primeira = folhas[0].get("properties", {})
    return primeira.get("title") or ""

def obter_colunas_sincronizacao_cliente(sync):
    colunas_texto = normalizar_texto_campo(sync.get("colunas_ultima_sync"))
    if colunas_texto:
        colunas = [coluna.strip() for coluna in colunas_texto.split(",") if coluna.strip()]
        if colunas:
            return colunas

    colunas = []
    for chave in ("placa", "nome", "telefone", "modelo", "cor", "servico", "data"):
        coluna = normalizar_texto_campo(sync.get(f"campo_{chave}"))
        if coluna and coluna not in colunas:
            colunas.append(coluna)
    return colunas

def montar_registro_site_para_sincronizacao(placa, nome="", telefone="", modelo="", cor=""):
    agora_local = agora()
    return {
        "placa": limpar_valor_planilha(placa).upper(),
        "nome": limpar_valor_planilha(nome),
        "telefone": limpar_valor_planilha(telefone),
        "modelo": limpar_valor_planilha(modelo),
        "cor": limpar_valor_planilha(cor),
        "servico": "CADASTRO NO SITE",
        "data": agora_local.strftime("%d/%m/%Y"),
    }

def escrever_registro_em_sheety(sync, registro_base):
    url = normalizar_texto_campo(sync.get("url"))
    if not url:
        raise RuntimeError("Sincronizacao sem URL.")

    partes_url = urlparse(url)
    resource = partes_url.path.rstrip("/").split("/")[-1] or "geral"
    payload = {
        resource: {
            chave: valor
            for chave, valor in registro_base.items()
            if valor not in {None, ""}
        }
    }

    resposta = requests.post(url, json=payload, timeout=20)
    resposta.raise_for_status()
    return resposta

def escrever_registro_em_google_sheets(sync, registro_base):
    url = normalizar_texto_campo(sync.get("url"))
    sheet_id = extrair_sheet_id_google(url)
    if not sheet_id:
        raise RuntimeError("Nao foi possivel identificar o ID da planilha.")

    gid = extrair_gid_url(url)
    titulo_aba = obter_titulo_aba_planilha_google(sheet_id, gid)
    if not titulo_aba:
        raise RuntimeError("Nao foi possivel identificar a aba da planilha.")

    service = obter_servico_google_sheets()
    colunas = obter_colunas_sincronizacao_cliente(sync)
    if not colunas:
        raise RuntimeError("Nao foi possivel identificar as colunas da planilha.")

    mapa_campos = {}
    for chave in ("placa", "nome", "telefone", "modelo", "cor", "servico", "data"):
        coluna_destino = normalizar_texto_campo(sync.get(f"campo_{chave}"))
        if coluna_destino:
            mapa_campos[normalizar_texto_comparacao(coluna_destino)] = registro_base.get(chave, "")

    linha = [mapa_campos.get(normalizar_texto_comparacao(coluna), "") for coluna in colunas]
    intervalo_leitura = f"'{titulo_aba}'!A1:{numero_para_coluna_planilha(len(colunas))}"

    try:
        cabecalho_resp = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=intervalo_leitura,
            majorDimension="ROWS",
        ).execute()
        linhas_existentes = cabecalho_resp.get("values", [])
    except Exception:
        linhas_existentes = []

    if linhas_existentes and len(linhas_existentes) > 1:
        cabecalho = [str(coluna).strip() for coluna in linhas_existentes[0]]
        placa_coluna = normalizar_texto_campo(sync.get("campo_placa"))
        indice_placa = -1
        for indice, coluna in enumerate(cabecalho):
            if normalizar_texto_comparacao(coluna) == normalizar_texto_comparacao(placa_coluna):
                indice_placa = indice
                break

        if indice_placa >= 0:
            placa_procura = normalizar_texto_comparacao(registro_base.get("placa"))
            for numero_linha, valores in enumerate(linhas_existentes[1:], start=2):
                valor_celula = valores[indice_placa] if indice_placa < len(valores) else ""
                if normalizar_texto_comparacao(valor_celula) == placa_procura:
                    intervalo_update = f"'{titulo_aba}'!A{numero_linha}:{numero_para_coluna_planilha(len(colunas))}{numero_linha}"
                    service.spreadsheets().values().update(
                        spreadsheetId=sheet_id,
                        range=intervalo_update,
                        valueInputOption="USER_ENTERED",
                        body={"values": [linha]},
                    ).execute()
                    return {"acao": "atualizado", "aba": titulo_aba, "planilha_id": sheet_id}

    service.spreadsheets().values().append(
        spreadsheetId=sheet_id,
        range=f"'{titulo_aba}'!A:{numero_para_coluna_planilha(len(colunas))}",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": [linha]},
    ).execute()
    return {"acao": "adicionado", "aba": titulo_aba, "planilha_id": sheet_id}

def espelhar_cadastro_site_em_sincronizacoes_clientes(placa, nome="", telefone="", modelo="", cor="", empresa_id=None):
    conn = conectar()
    c = conn.cursor()
    empresa_id = normalize_empresa_id(empresa_id or empresa_atual_id())
    c.execute("""
        SELECT id, nome, url, campo_placa, campo_nome, campo_telefone, campo_modelo, campo_cor, campo_servico, campo_data, colunas_ultima_sync
        FROM sincronizacoes_clientes
        WHERE empresa_id=? AND ativo=1
          AND COALESCE(excluido_em, '')=''
        ORDER BY id ASC
    """, (empresa_id,))
    sincronizacoes = [dict(row) for row in c.fetchall()]
    conn.close()

    registro_base = montar_registro_site_para_sincronizacao(placa, nome, telefone, modelo, cor)
    resultado = {
        "sucesso": [],
        "falhas": [],
        "ignoradas": [],
    }

    for sync in sincronizacoes:
        url = normalizar_texto_campo(sync.get("url"))
        if not url:
            resultado["ignoradas"].append("Sincronizacao sem URL configurada")
            continue

        try:
            if "sheety.co" in url:
                escrever_registro_em_sheety(sync, registro_base)
            else:
                escrever_registro_em_google_sheets(sync, registro_base)

            resultado["sucesso"].append(sync.get("nome") or f"Sincronizacao {sync.get('id')}")
        except Exception as e:
            resultado["falhas"].append({
                "nome": sync.get("nome") or f"Sincronizacao {sync.get('id')}",
                "erro": str(e),
            })

    return resultado

def salvar_linhas_base_dados(linhas):
    estatisticas = {
        "linhas_recebidas": len(linhas),
        "linhas_salvas": 0,
        "linhas_novas": 0,
        "linhas_atualizadas": 0,
        "linhas_ignoradas": 0,
    }

    for indice, linha in enumerate(linhas, start=1):
        placa = limpar_valor_planilha(linha.get("placa", ""))
        nome = limpar_valor_planilha(linha.get("nome", ""))
        telefone = limpar_valor_planilha(linha.get("telefone", ""))
        modelo = limpar_valor_planilha(linha.get("modelo", ""))
        cor = limpar_valor_planilha(linha.get("cor", ""))
        placa_original = (
            limpar_valor_planilha(linha.get("placa_original", "")) or
            limpar_valor_planilha(linha.get("_original_placa", ""))
        )

        if not any([placa, nome, telefone, modelo, cor]):
            estatisticas["linhas_ignoradas"] += 1
            continue

        if not placa:
            raise ValueError(f"Linha {indice}: informe a placa antes de salvar.")

        resultado = salvar_cliente_veiculo(
            placa=placa,
            nome=nome,
            telefone=telefone,
            data_nascimento=linha.get("data_nascimento", ""),
            modelo=modelo,
            cor=cor,
            placa_original=placa_original,
        )

        estatisticas["linhas_salvas"] += 1

        if resultado["acao"] == "novo":
            estatisticas["linhas_novas"] += 1
        else:
            estatisticas["linhas_atualizadas"] += 1

    return estatisticas

def resumir_salvamento_base_dados(estatisticas):
    return (
        f"{estatisticas['linhas_salvas']} linha(s) salva(s), "
        f"{estatisticas['linhas_novas']} nova(s), "
        f"{estatisticas['linhas_atualizadas']} atualizada(s), "
        f"{estatisticas['linhas_ignoradas']} ignorada(s)"
    )

def sincronizar_fontes_pendentes():
    if not sync_lock.acquire(blocking=False):
        return

    try:
        while True:
            try:
                conn = conectar()
                c = conn.cursor()
                c.execute("""
                    SELECT id
                    FROM sincronizacoes_clientes
                    WHERE ativo=1
                      AND COALESCE(excluido_em, '')=''
                      AND (proximo_sync_em IS NULL OR proximo_sync_em<=?)
                    ORDER BY id
                    LIMIT 1
                """, (agora_iso(),))
                pendente = c.fetchone()
                conn.close()
            except Exception as e:
                texto_erro = str(e)
                if banco_online_ativo() and "sincronizacoes_clientes" in texto_erro and "does not exist" in texto_erro:
                    if garantir_schema_banco_online(force=True):
                        continue
                raise

            if not pendente:
                break

            executar_sincronizacao_cliente(pendente["id"])
    finally:
        sync_lock.release()

def loop_worker_sincronizacao():
    primeira_execucao = True
    while True:
        if primeira_execucao:
            primeira_execucao = False
            time.sleep(WORKER_SYNC_DELAY_INICIAL)
        try:
            sincronizar_fontes_pendentes()
        except Exception as e:
            log_info("ERRO WORKER SYNC:", e)

        time.sleep(120)

def iniciar_worker_sincronizacao():
    global sync_worker_iniciado

    if sync_worker_iniciado:
        return

    sync_worker_iniciado = True
    Thread(target=loop_worker_sincronizacao, daemon=True).start()


def loop_worker_sincronizacao_bancos():
    primeira_execucao = True
    while True:
        if primeira_execucao:
            primeira_execucao = False
            time.sleep(WORKER_SYNC_BANCOS_DELAY_INICIAL)
        try:
            resultado = sincronizar_bancos_incremental()
            if resultado.get("conectado"):
                log_info("SYNC BANCOS:", resultado.get("mensagem"))
        except Exception as e:
            log_info("ERRO WORKER SYNC BANCOS:", e)

        time.sleep(180)


def iniciar_worker_sincronizacao_bancos():
    global sync_bancos_worker_iniciado

    if sync_bancos_worker_iniciado:
        return

    sync_bancos_worker_iniciado = True
    Thread(target=loop_worker_sincronizacao_bancos, daemon=True).start()

def loop_importacao():
    while True:
        importar_planilha_local()
        time.sleep(3600)  # atualiza a cada 1 minuto


def carregar_contexto_clientes(busca="", limpar=False, detalhar_sincronizacoes=False):
    busca_aplicada = "" if limpar else busca
    empresa_id = empresa_atual_id()
    limite_inicial = None if busca_aplicada else CLIENTES_LISTA_INICIAL_LIMITE
    usuario_cache = f"{session.get('usuario') or ''}|{empresa_id}"
    chave_cache = f"{usuario_cache}|{empresa_id}|{busca_aplicada}|sync:{int(bool(detalhar_sincronizacoes))}|lim:{limite_inicial or 'todos'}"
    contexto_cache = obter_cache_consulta(
        CLIENTES_CONTEXT_CACHE,
        chave_cache,
        CLIENTES_CONTEXT_CACHE_TTL,
    )
    if contexto_cache is not None:
        registrar_metrica_consulta_sql("/clientes", "snapshot_clientes", 0, origem="cache", cache_hit=True)
        return (
            contexto_cache.get("clientes", []),
            contexto_cache.get("sincronizacoes", []),
        )

    contexto_lido = executar_leitura_resiliente(
        lambda conn: {
            "clientes": medir_consulta_sql(
                "/clientes",
                "lista_base_clientes",
                lambda: listar_registros_clientes(busca_aplicada, conn=conn, limite=limite_inicial),
                detalhes="veiculos + clientes",
            ),
            "sincronizacoes_raw": medir_consulta_sql(
                "/clientes",
                "sincronizacoes_clientes",
                lambda: consultar_sincronizacoes_clientes_domain(conn.cursor(), empresa_id),
                detalhes="sincronizacoes_clientes",
            )
            if detalhar_sincronizacoes
            else [],
        },
        descricao="CONTEXTO CLIENTES",
        padrao={"clientes": [], "sincronizacoes_raw": []},
    ) or {"clientes": [], "sincronizacoes_raw": []}
    clientes = contexto_lido.get("clientes", [])
    sincronizacoes_raw = contexto_lido.get("sincronizacoes_raw", []) or []

    sincronizacoes = []

    for sync in sincronizacoes_raw:
        item = dict(sync)
        item["ativo"] = bool(item["ativo"])
        item["intervalo_label"] = obter_label_intervalo_sincronizacao(item["intervalo_minutos"])
        item["ultimo_sync_em_fmt"] = formatar_datahora(item["ultimo_sync_em"])
        item["proximo_sync_em_fmt"] = formatar_datahora(item["proximo_sync_em"])
        item["tempo_restante"] = formatar_tempo_restante(item["proximo_sync_em"])
        item["campos"] = descrever_campos_sincronizados(item)
        sincronizacoes.append(item)

    salvar_cache_consulta(
        CLIENTES_CONTEXT_CACHE,
        chave_cache,
        {"clientes": clientes, "sincronizacoes": sincronizacoes},
    )
    return clientes, sincronizacoes


def preparar_rotinas_interface_logada():
    if not session.get("usuario"):
        return

    global ULTIMA_PREPARACAO_INTERFACE_TS
    global ULTIMO_SYNC_FONTES_SOB_DEMANDA_TS

    agora_ts = time.time()

    if agora_ts - ULTIMA_PREPARACAO_INTERFACE_TS >= PREPARACAO_INTERFACE_INTERVALO:
        ULTIMA_PREPARACAO_INTERFACE_TS = agora_ts
        iniciar_bootstrap_init_db()
        iniciar_worker_backup_banco()
        iniciar_worker_manutencao_arquivos()
        iniciar_worker_sincronizacao()
        iniciar_worker_sincronizacao_bancos()
        iniciar_worker_auto_teste()
        if modo_banco_preferido() == "postgres" and not SCHEMA_BANCO_ONLINE_GARANTIDO:
            iniciar_bootstrap_schema_online()

    if agora_ts - ULTIMO_SYNC_FONTES_SOB_DEMANDA_TS >= SYNC_FONTES_SOB_DEMANDA_INTERVALO:
        ULTIMO_SYNC_FONTES_SOB_DEMANDA_TS = agora_ts
        Thread(target=sincronizar_fontes_pendentes, daemon=True).start()


@app.route("/healthz")
def healthz():
    return {
        "ok": True,
        "init_db_executado": bool(INIT_DB_EXECUTADO),
        "modo_banco": modo_banco_preferido(),
        "versao": obter_versao_sistema(permitir_sem_sessao=True),
    }


@app.route("/offline")
def offline():
    return """
<!doctype html>
<html lang="pt-BR">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Sem conexao - Wagen Estetica</title>
    <link rel="stylesheet" href="/static/responsive.css">
</head>
<body>
    <main class="page-shell">
        <section class="page-card">
            <h1 class="section-title">Sem conexao</h1>
            <p class="muted-text">O app esta aberto, mas nao conseguiu falar com o servidor agora.</p>
            <div class="actions-row actions-row--left">
                <a href="/" class="button-link"><button type="button">Tentar novamente</button></a>
            </div>
        </section>
    </main>
</body>
</html>
""", 200


@app.errorhandler(Exception)
def tratar_erro_inesperado_producao(erro):
    if isinstance(erro, HTTPException):
        return erro

    registrar_ultimo_erro_producao(erro, descricao="erro_global")
    if excecao_relacionada_banco(erro):
        registrar_log_banco_online(f"Falha de banco capturada na request {request.path}: {erro}", intervalo_segundos=30)
        return tratar_banco_online_obrigatorio(
            BancoOnlineObrigatorioErro(
                "Servico indisponivel agora. Tentar novamente. Se persistir, valide o banco online no diagnostico."
            )
        )

    if request.path.startswith("/api/") or request.path.endswith(".json"):
        return jsonify(
            {
                "ok": False,
                "erro": "erro_interno",
                "mensagem": "Servico indisponivel agora. Tentar novamente.",
                "quando": ULTIMO_ERRO_PRODUCAO.get("quando"),
            }
        ), 500

    try:
        return render_template(
            "erro_sistema.html",
            ultimo_erro=dict(ULTIMO_ERRO_PRODUCAO),
        ), 500
    except Exception:
        return (
            "<h1>Sistema temporariamente indisponivel</h1>"
            "<p>Nao foi possivel concluir a operacao agora. Tente novamente em instantes.</p>"
        ), 500

@app.after_request
def aplicar_headers_resposta(response):
    return append_security_headers(response)


@app.before_request
def iniciar_medicao_tempo_resposta():
    g.inicio_tempo_resposta = time.perf_counter()


@app.after_request
def registrar_tempo_resposta(response):
    try:
        caminho = request.path or ""
        if caminho in ROTAS_MONITORADAS_RESPOSTA:
            inicio = float(getattr(g, "inicio_tempo_resposta", 0.0) or 0.0)
            if inicio:
                tempo_ms = int((time.perf_counter() - inicio) * 1000)
                atual = dict(METRICAS_TEMPO_RESPOSTA.get(caminho) or {"rota": caminho})
                amostras = int(atual.get("amostras") or 0) + 1
                media_anterior = int(atual.get("media_ms") or 0)
                ultimo_anterior = int(atual.get("ultimo_ms") or 0)
                media = int(((media_anterior * (amostras - 1)) + tempo_ms) / amostras)
                tendencia = classificar_tendencia_resposta_ms(ultimo_anterior, tempo_ms)
                pioras_consecutivas = int(atual.get("pioras_consecutivas") or 0)
                if tendencia == "piorou":
                    pioras_consecutivas += 1
                elif tendencia == "melhorou":
                    pioras_consecutivas = 0
                atual.update(
                    {
                        "rota": caminho,
                        "ultimo_ms": tempo_ms,
                        "anterior_ms": ultimo_anterior,
                        "media_ms": media,
                        "max_ms": max(int(atual.get("max_ms") or 0), tempo_ms),
                        "amostras": amostras,
                        "status": int(response.status_code),
                        "ultima_medicao": agora_iso(),
                        "classe": classificar_latencia_ms(tempo_ms),
                        "label": rotulo_latencia_ms(tempo_ms),
                        "tendencia": tendencia,
                        "tendencia_label": rotulo_tendencia_resposta(tendencia),
                        "alerta_2s": tempo_ms > 2000,
                        "pioras_consecutivas": pioras_consecutivas,
                    }
                )
                METRICAS_TEMPO_RESPOSTA[caminho] = atual
                avaliar_alerta_estabilidade_resposta(caminho, atual)
    except Exception as erro:
        log_info("ERRO METRICA RESPOSTA:", erro)
    return response


@app.errorhandler(BancoOnlineObrigatorioErro)
def tratar_banco_online_obrigatorio(erro):
    mensagem = xml_escape(str(erro or "Banco online indisponivel."))
    if (request.endpoint or "").startswith("api_"):
        return jsonify({"erro": "banco_online_indisponivel", "mensagem": mensagem}), 503
    html = f"""
    <!doctype html>
    <html lang="pt-br">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Banco indisponivel</title>
        <style>
            body {{ margin: 0; min-height: 100vh; display: grid; place-items: center; background: #0b0f19; color: #f9fafb; font-family: Arial, sans-serif; }}
            main {{ width: min(92vw, 680px); border: 1px solid #334155; border-radius: 8px; padding: 28px; background: #111827; }}
            h1 {{ margin: 0 0 12px; font-size: 1.6rem; }}
            p {{ color: #cbd5e1; line-height: 1.5; }}
            a {{ color: #facc15; font-weight: 700; }}
        </style>
    </head>
    <body>
        <main>
            <h1>Banco online indisponivel</h1>
            <p>O sistema nao conseguiu conectar no banco configurado. A pagina foi interrompida para evitar dados incompletos.</p>
            <p>{mensagem}</p>
            <p>Confira a conexao do Supabase, o limite de conexoes e a variavel DATABASE_URL. Depois recarregue a pagina.</p>
            <p><a href="/configuracoes">Abrir configuracoes</a></p>
        </main>
    </body>
    </html>
    """
    return html, 503


@app.before_request
def validar_csrf_basico():
    if request.endpoint == "static":
        return

    if not should_enforce_csrf(request, csrf_protection_ativa()):
        return

    if request.endpoint in {"healthz"}:
        return

    token = extract_csrf_token(request)
    if validate_csrf_token(session, token):
        return

    if (request.endpoint or "").startswith("api_"):
        return jsonify({"erro": "csrf_invalido"}), 400

    return "Token de seguranca invalido ou expirado.", 400

@app.before_request
def preparar_sincronizacoes():
    if request.endpoint == "static":
        return

    endpoint = request.endpoint or ""
    sessao_ativa = bool(session.get("usuario"))

    if not INIT_DB_EXECUTADO:
        iniciar_bootstrap_init_db()

        if request.method in {"GET", "HEAD", "OPTIONS"} or endpoint == "login":
            return

        if ambiente_hospedado_gerenciado():
            return

        garantir_init_db()

@app.before_request
def exigir_troca_senha_obrigatoria():
    endpoint = request.endpoint or ""

    if endpoint == "static" or not session.get("usuario"):
        return

    sincronizar_sessao_usuario_seguro(contexto="CONFIGURACOES")

    if not session.get("senha_alteracao_obrigatoria"):
        return

    if endpoint in {
        "configuracoes",
        "atualizar_minha_senha",
        "logout",
        "salvar_configuracao_banco",
        "testar_configuracao_banco",
        "migrar_banco_para_supabase",
    }:
        return

    definir_feedback_configuracoes(
        "erro",
        "Por seguranca, troque sua senha antes de continuar usando o sistema."
    )
    return redirect(destino_configuracoes("banco"))


@app.before_request
def bloquear_paginas_menu_desabilitadas():
    endpoint = request.endpoint or ""
    if endpoint == "static" or not session.get("usuario") or usuario_desenvolvedor():
        return

    pagina_id = ENDPOINTS_PAGINAS_MENU.get(endpoint)
    if not pagina_id or pagina_menu_habilitada(pagina_id):
        return

    definir_feedback_configuracoes(
        "erro",
        "Esta pagina foi desabilitada pelo desenvolvedor do sistema.",
    )
    if (endpoint or "").startswith("api_"):
        return jsonify({"erro": "pagina_desabilitada"}), 403
    return redirect(destino_configuracoes("banco"))


@app.before_request
def exigir_licenca_operacional():
    endpoint = request.endpoint or ""
    if endpoint_liberado_com_licenca_bloqueada(endpoint):
        return
    if not session.get("usuario"):
        return

    licenca = obter_contexto_licenca_empresa_cached()
    if not licenca.get("bloqueada"):
        return

    if request.method in {"GET", "HEAD", "OPTIONS"}:
        definir_feedback_configuracoes(
            "erro",
            "Licenca bloqueada ou vencida. Regularize a empresa antes de continuar usando os recursos operacionais.",
        )
        return redirect("/empresas")

    if (endpoint or "").startswith("api_"):
        return jsonify({"erro": "licenca_bloqueada"}), 403

    definir_feedback_configuracoes(
        "erro",
        "Licenca bloqueada ou vencida. Regularize a empresa antes de continuar usando os recursos operacionais.",
    )
    return redirect("/empresas")

@app.route("/api/cliente/<placa>")
def buscar_cliente_api(placa):
    if not session.get("usuario"):
        return {"erro": "nao autorizado"}

    conn = conectar()
    c = conn.cursor()
    empresa_id = empresa_atual_id()

    c.execute("""
        SELECT 
            veiculos.placa,
            veiculos.modelo,
            clientes.nome,
            clientes.telefone
        FROM veiculos
        LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
        WHERE veiculos.placa=?
          AND COALESCE(veiculos.empresa_id, 1)=?
    """, (placa.upper(), empresa_id))

    dados = c.fetchone()
    conn.close()

    if not dados:
        return {"encontrado": False}

    return {
        "encontrado": True,
        "nome": dados["nome"],
        "telefone": dados["telefone"],
        "modelo": dados["modelo"],
        "placa": dados["placa"]
    }


def buscar_global_sistema(termo, limite=8):
    termo = normalizar_texto_campo(termo)
    if len(termo) < 2:
        return []

    termo_like = f"%{termo}%"
    empresa_id = empresa_atual_id()
    conn = None
    try:
        conn = conectar()
        c = conn.cursor()
        resultados = []
        vistos = set()

        def adicionar_resultado(tipo, titulo, subtitulo="", placa="", url="/"):
            chave = f"{tipo}|{placa}|{titulo}|{subtitulo}"
            if chave in vistos or len(resultados) >= int(limite):
                return
            vistos.add(chave)
            resultados.append({
                "tipo": tipo,
                "titulo": titulo,
                "subtitulo": subtitulo,
                "placa": placa,
                "cliente": titulo if tipo == "veiculo" else "",
                "telefone": "",
                "modelo": "",
                "url": url,
            })

        veiculos = medir_consulta_sql(
            "/api/busca-global",
            "busca_veiculos_clientes",
            lambda: (
                c.execute(
                    """
                    SELECT
                        veiculos.placa,
                        veiculos.modelo,
                        clientes.nome AS cliente_nome,
                        clientes.telefone AS cliente_telefone
                    FROM veiculos
                    LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
                    WHERE COALESCE(veiculos.empresa_id, 1)=?
                      AND (
                          UPPER(COALESCE(veiculos.placa, '')) LIKE UPPER(?)
                          OR UPPER(COALESCE(veiculos.modelo, '')) LIKE UPPER(?)
                          OR UPPER(COALESCE(clientes.nome, '')) LIKE UPPER(?)
                          OR UPPER(COALESCE(clientes.telefone, '')) LIKE UPPER(?)
                      )
                    ORDER BY
                        CASE WHEN UPPER(COALESCE(veiculos.placa, '')) = UPPER(?) THEN 0 ELSE 1 END,
                        clientes.nome ASC,
                        veiculos.placa ASC
                    LIMIT ?
                    """,
                    (empresa_id, termo_like, termo_like, termo_like, termo_like, termo, int(limite)),
                ),
                [row_para_dict(row) for row in c.fetchall()],
            )[1],
        )
        for item in veiculos:
            placa = normalizar_texto_campo(item.get("placa")).upper()
            cliente = normalizar_texto_campo(item.get("cliente_nome")) or "Cliente sem nome"
            telefone = normalizar_texto_campo(item.get("cliente_telefone"))
            modelo = normalizar_texto_campo(item.get("modelo"))
            partes = [valor for valor in [placa, modelo, telefone] if valor]
            adicionar_resultado("veiculo", cliente, " | ".join(partes), placa, f"/?placa={quote(placa)}" if placa else "/")

        if len(resultados) < int(limite):
            historico = medir_consulta_sql(
                "/api/busca-global",
                "busca_historico_servicos",
                lambda: (
                    c.execute(
                        """
                        SELECT servicos.id, servicos.status, servicos.entrega, veiculos.placa, clientes.nome AS cliente_nome, tipos_servico.nome AS tipo_nome
                        FROM servicos
                        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
                        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
                        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
                        WHERE COALESCE(servicos.empresa_id, 1)=?
                          AND (
                              UPPER(COALESCE(veiculos.placa, '')) LIKE UPPER(?)
                              OR UPPER(COALESCE(clientes.nome, '')) LIKE UPPER(?)
                              OR UPPER(COALESCE(tipos_servico.nome, '')) LIKE UPPER(?)
                          )
                        ORDER BY servicos.id DESC
                        LIMIT ?
                        """,
                        (empresa_id, termo_like, termo_like, termo_like, int(limite)),
                    ),
                    [row_para_dict(row) for row in c.fetchall()],
                )[1],
            )
            for item in historico:
                placa = normalizar_texto_campo(item.get("placa")).upper()
                titulo = f"Servico: {item.get('tipo_nome') or 'Atendimento'}"
                subtitulo = " | ".join([valor for valor in [placa, item.get("cliente_nome"), item.get("status")] if valor])
                adicionar_resultado("servico", titulo, subtitulo, placa, f"/historico/servico/{item.get('id')}/editar?redirect_to=/historico")

        if len(resultados) < int(limite):
            try:
                retornos = medir_consulta_sql(
                    "/api/busca-global",
                    "busca_retornos",
                    lambda: (
                        c.execute(
                            """
                            SELECT retornos_clientes.id, retornos_clientes.placa, retornos_clientes.status, retornos_clientes.proximo_contato_em, clientes.nome AS cliente_nome
                            FROM retornos_clientes
                            LEFT JOIN veiculos ON veiculos.placa = retornos_clientes.placa AND COALESCE(veiculos.empresa_id, 1)=?
                            LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
                            WHERE (
                                UPPER(COALESCE(retornos_clientes.placa, '')) LIKE UPPER(?)
                                OR UPPER(COALESCE(clientes.nome, '')) LIKE UPPER(?)
                            )
                            ORDER BY retornos_clientes.id DESC
                            LIMIT ?
                            """,
                            (empresa_id, termo_like, termo_like, int(limite)),
                        ),
                        [row_para_dict(row) for row in c.fetchall()],
                    )[1],
                )
                for item in retornos:
                    placa = normalizar_texto_campo(item.get("placa")).upper()
                    subtitulo = " | ".join([valor for valor in [placa, item.get("cliente_nome"), item.get("status")] if valor])
                    adicionar_resultado("retorno", "Retorno de cliente", subtitulo, placa, "/retornos")
            except Exception:
                pass

        if len(resultados) < int(limite):
            try:
                orcamentos = medir_consulta_sql(
                    "/api/busca-global",
                    "busca_orcamentos",
                    lambda: (
                        c.execute(
                            """
                            SELECT id, numero, cliente_nome, placa, modelo, total, status
                            FROM orcamentos
                            WHERE COALESCE(empresa_id, 1)=?
                              AND (
                                  UPPER(COALESCE(cliente_nome, '')) LIKE UPPER(?)
                                  OR UPPER(COALESCE(placa, '')) LIKE UPPER(?)
                                  OR UPPER(COALESCE(modelo, '')) LIKE UPPER(?)
                              )
                            ORDER BY numero DESC
                            LIMIT ?
                            """,
                            (empresa_id, termo_like, termo_like, termo_like, int(limite)),
                        ),
                        [row_para_dict(row) for row in c.fetchall()],
                    )[1],
                )
                for item in orcamentos:
                    placa = normalizar_texto_campo(item.get("placa")).upper()
                    numero = formatar_numero_documento(item.get("numero"))
                    subtitulo = " | ".join([valor for valor in [placa, item.get("cliente_nome"), item.get("status")] if valor])
                    adicionar_resultado("orcamento", f"Orcamento {numero}", subtitulo, placa, "/orcamento")
            except Exception:
                pass
        return resultados
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="busca_global_sistema")
        return []
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


@app.route("/api/busca-global")
def api_busca_global():
    if not session.get("usuario"):
        return jsonify({"erro": "nao autorizado"}), 401
    termo = (request.args.get("q") or "").strip()
    return jsonify({
        "ok": True,
        "termo": termo,
        "resultados": buscar_global_sistema(termo),
    })

@app.route("/orcamento")
def pagina_orcamento():
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    c.execute("SELECT nome, valor FROM tipos_servico ORDER BY nome ASC")
    servicos = [dict(item) for item in c.fetchall()]

    conn.close()

    return render_template(
        "orcamento.html",
        servicos=servicos,
        empresa=obter_configuracao_empresa(),
        orcamentos=listar_orcamentos_recentes(),
        feedback=session.pop("orcamento_feedback", None),
    )

@app.route("/gerar_orcamento", methods=["POST"])
def gerar_orcamento():
    if not session.get("usuario"):
        return redirect("/login")

    itens = extrair_itens_formulario(request.form)

    if not itens:
        definir_feedback_orcamento("erro", "Adicione pelo menos um item ao orcamento antes de gerar o PDF.")
        return redirect("/orcamento")

    dados = {
        "cliente_nome": request.form.get("nome"),
        "cliente_documento": request.form.get("documento"),
        "email": request.form.get("email"),
        "telefone": request.form.get("telefone"),
        "placa": request.form.get("placa"),
        "modelo": request.form.get("modelo"),
        "validade_dias": request.form.get("validade_dias"),
        "forma_pagamento": request.form.get("forma_pagamento"),
        "observacoes": request.form.get("observacoes"),
        "desconto": request.form.get("desconto"),
    }
    orcamento = salvar_orcamento(dados, itens, obter_configuracao_empresa())
    buffer = gerar_pdf_orcamento_buffer(orcamento)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"orcamento_{orcamento['numero_formatado']}.pdf",
        mimetype="application/pdf",
    )

@app.route("/orcamento/<int:id>/pdf")
def baixar_orcamento_pdf(id):
    if not session.get("usuario"):
        return redirect("/login")

    orcamento = buscar_orcamento_completo(id)

    if not orcamento:
        definir_feedback_orcamento("erro", "Orcamento nao encontrado.")
        return redirect("/orcamento")

    buffer = gerar_pdf_orcamento_buffer(orcamento)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"orcamento_{orcamento['numero_formatado']}.pdf",
        mimetype="application/pdf",
    )

@app.route("/nota_fiscal")
def pagina_nota_fiscal():
    if not session.get("usuario"):
        return redirect("/login")
    bloqueio = bloquear_recurso_plano(
        "notas",
        "Nota fiscal exige plano Pro ou Business.",
        feedback_func=definir_feedback_nota_fiscal,
        destino="/configuracoes",
    )
    if bloqueio:
        return bloqueio

    preparar_rotinas_interface_logada()
    empresa = obter_configuracao_empresa()
    integracao = obter_configuracao_integracao_fiscal()
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT nome, valor FROM tipos_servico ORDER BY nome ASC")
    servicos = [dict(item) for item in c.fetchall()]
    conn.close()

    orcamento_origem = None
    prefill = None
    orcamento_id = request.args.get("orcamento_id", type=int)

    if orcamento_id:
        orcamento_origem = buscar_orcamento_completo(orcamento_id)
        if orcamento_origem:
            prefill = montar_prefill_nota_por_orcamento(orcamento_origem)
        else:
            definir_feedback_nota_fiscal("erro", "Nao encontrei o orcamento informado para montar a nota.")

    prontidao_integracao = avaliar_prontidao_integracao_fiscal(empresa, integracao)
    payload_integracao_json = json.dumps(
        montar_payload_exemplo_integracao(empresa, integracao, prefill=prefill),
        indent=2,
        ensure_ascii=False,
    )

    return render_template(
        "nota_fiscal.html",
        empresa=empresa,
        integracao=integracao,
        prontidao_integracao=prontidao_integracao,
        payload_integracao_json=payload_integracao_json,
        tipos_integracao_fiscal=TIPOS_INTEGRACAO_FISCAL,
        ambientes_integracao_fiscal=AMBIENTES_INTEGRACAO_FISCAL,
        autenticacoes_integracao_fiscal=AUTENTICACAO_INTEGRACAO_FISCAL,
        tipos_certificado_fiscal=TIPOS_CERTIFICADO_FISCAL,
        notas_pendentes=listar_notas_fiscais_recentes(limit=8, somente_pendentes=True),
        notas_emitidas=listar_notas_fiscais_recentes(limit=20, somente_emitidas=True),
        feedback=session.pop("nota_fiscal_feedback", None),
        servicos=servicos,
        prefill=prefill,
        orcamento_origem=orcamento_origem,
    )

@app.route("/nota_fiscal/empresa", methods=["POST"])
def salvar_emitente_nota_fiscal():
    if not session.get("usuario"):
        return redirect("/login")
    bloqueio = bloquear_recurso_plano(
        "notas",
        "Nota fiscal exige plano Pro ou Business.",
        feedback_func=definir_feedback_nota_fiscal,
        destino="/configuracoes",
    )
    if bloqueio:
        return bloqueio

    salvar_configuracao_empresa_form(request.form)
    registrar_auditoria(
        "atualizou_emitente_fiscal",
        "nota_fiscal",
        detalhes={
            "cnpj": normalizar_documento_fiscal(request.form.get("cnpj")),
            "razao_social": normalizar_texto_campo(request.form.get("razao_social")),
        },
    )
    definir_feedback_nota_fiscal("sucesso", "Dados do emitente fiscal salvos com sucesso.")

    origem_id = converter_inteiro(request.form.get("origem_orcamento_id"), 0)
    if origem_id:
        return redirect(f"/nota_fiscal?orcamento_id={origem_id}")

    return redirect("/nota_fiscal")

@app.route("/nota_fiscal/integracao", methods=["POST"])
def salvar_integracao_nota_fiscal():
    if not session.get("usuario"):
        return redirect("/login")
    bloqueio = bloquear_recurso_plano(
        "notas",
        "Nota fiscal exige plano Pro ou Business.",
        feedback_func=definir_feedback_nota_fiscal,
        destino="/configuracoes",
    )
    if bloqueio:
        return bloqueio

    salvar_configuracao_integracao_fiscal_form(request.form)
    integracao = obter_configuracao_integracao_fiscal()
    registrar_auditoria(
        "atualizou_integracao_fiscal",
        "nota_fiscal",
        detalhes={
            "tipo_integracao": integracao.get("tipo_integracao"),
            "ambiente": integracao.get("ambiente"),
            "ativo": bool(integracao.get("ativo")),
        },
    )
    prontidao = avaliar_prontidao_integracao_fiscal(obter_configuracao_empresa(), integracao)

    if prontidao["pronta"]:
        definir_feedback_nota_fiscal(
            "sucesso",
            (
                "Configuracao de integracao fiscal salva. O sistema ja esta "
                "preparado para avancar para homologacao futura."
            ),
        )
    elif integracao.get("tipo_integracao") == "manual":
        definir_feedback_nota_fiscal(
            "sucesso",
            (
                "Estrutura fiscal salva. O sistema segue em modo manual, "
                "mas a aba de nota fiscal ficou preparada para integracao futura."
            ),
        )
    else:
        definir_feedback_nota_fiscal(
            "erro",
            "Configuracao salva, mas ainda faltam alguns dados para deixar a integracao pronta.",
        )

    origem_id = converter_inteiro(request.form.get("origem_orcamento_id"), 0)
    if origem_id:
        return redirect(f"/nota_fiscal?orcamento_id={origem_id}")

    return redirect("/nota_fiscal")

@app.route("/nota_fiscal/gerar", methods=["POST"])
def gerar_nota_fiscal():
    if not session.get("usuario"):
        return redirect("/login")
    bloqueio = bloquear_recurso_plano(
        "notas",
        "Nota fiscal exige plano Pro ou Business.",
        feedback_func=definir_feedback_nota_fiscal,
        destino="/configuracoes",
    )
    if bloqueio:
        return bloqueio

    empresa = obter_configuracao_empresa()

    if not empresa_possui_dados_fiscais(empresa):
        definir_feedback_nota_fiscal(
            "erro",
            "Preencha pelo menos a razao social e o CNPJ do emitente antes de gerar a nota fiscal.",
        )
        return redirect("/nota_fiscal")

    itens = extrair_itens_formulario(request.form)

    if not itens:
        definir_feedback_nota_fiscal("erro", "Adicione pelo menos um item de servico para gerar o espelho fiscal.")
        return redirect("/nota_fiscal")

    dados = {
        "origem_orcamento_id": request.form.get("origem_orcamento_id"),
        "numero_nota": request.form.get("numero_nota"),
        "serie": request.form.get("serie"),
        "ambiente": request.form.get("ambiente"),
        "cliente_nome": request.form.get("cliente_nome"),
        "cliente_documento": request.form.get("cliente_documento"),
        "email": request.form.get("email"),
        "telefone": request.form.get("telefone"),
        "placa": request.form.get("placa"),
        "modelo": request.form.get("modelo"),
        "endereco": request.form.get("endereco"),
        "numero_endereco": request.form.get("numero_endereco"),
        "complemento": request.form.get("complemento"),
        "bairro": request.form.get("bairro"),
        "cidade": request.form.get("cidade"),
        "uf": request.form.get("uf"),
        "cep": request.form.get("cep"),
        "codigo_servico": request.form.get("codigo_servico"),
        "discriminacao": request.form.get("discriminacao"),
        "observacoes": request.form.get("observacoes"),
        "aliquota_iss": request.form.get("aliquota_iss"),
        "desconto": request.form.get("desconto"),
    }
    nota = salvar_nota_fiscal(dados, itens, empresa)
    registrar_auditoria(
        "gerou_nota_fiscal",
        "nota_fiscal",
        entidade_id=nota["id"],
        placa=nota.get("placa"),
        detalhes={
            "cliente": nota.get("cliente_nome"),
            "valor_total": nota.get("valor_total"),
            "status": nota.get("status"),
        },
    )
    buffer = gerar_pdf_nota_fiscal_buffer(nota)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"nota_fiscal_rps_{nota['rps_formatado']}.pdf",
        mimetype="application/pdf",
    )

@app.route("/nota_fiscal/<int:id>/pdf")
def baixar_nota_fiscal_pdf(id):
    if not session.get("usuario"):
        return redirect("/login")
    bloqueio = bloquear_recurso_plano(
        "notas",
        "Nota fiscal exige plano Pro ou Business.",
        feedback_func=definir_feedback_nota_fiscal,
        destino="/configuracoes",
    )
    if bloqueio:
        return bloqueio

    nota = buscar_nota_fiscal_completa(id)

    if not nota:
        definir_feedback_nota_fiscal("erro", "Nota fiscal nao encontrada.")
        return redirect("/nota_fiscal")

    buffer = gerar_pdf_nota_fiscal_buffer(nota)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"nota_fiscal_rps_{nota['rps_formatado']}.pdf",
        mimetype="application/pdf",
    )

@app.route("/nota_fiscal/<int:id>/registrar", methods=["POST"])
def registrar_emissao_nota_fiscal(id):
    if not session.get("usuario"):
        return redirect("/login")

    numero_nota = normalizar_texto_campo(request.form.get("numero_nota"))

    if not numero_nota:
        definir_feedback_nota_fiscal("erro", "Informe o numero oficial da nota para registrar a emissao.")
        return redirect("/nota_fiscal")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id FROM notas_fiscais WHERE id=?", (id,))
    nota = c.fetchone()

    if not nota:
        conn.close()
        definir_feedback_nota_fiscal("erro", "Nota fiscal nao encontrada.")
        return redirect("/nota_fiscal")

    c.execute("""
        UPDATE notas_fiscais
        SET numero_nota=?, serie=?, ambiente=?, status='EMITIDA MANUALMENTE', atualizado_em=?
        WHERE id=?
    """, (
        numero_nota,
        normalizar_texto_campo(request.form.get("serie")),
        normalizar_texto_campo(request.form.get("ambiente")) or "Emissao manual",
        agora_iso(),
        id,
    ))
    conn.commit()
    conn.close()

    registrar_auditoria(
        "registrou_emissao_nota_fiscal",
        "nota_fiscal",
        entidade_id=id,
        detalhes={
            "numero_nota": numero_nota,
            "serie": normalizar_texto_campo(request.form.get("serie")),
        },
    )
    definir_feedback_nota_fiscal("sucesso", "Emissao manual registrada com sucesso.")
    return redirect("/nota_fiscal")

@app.route("/api/clima")
def api_clima():
    return obter_resultado_clima_api()

@app.route("/clientes/importar-local")
def importar_local():
    sucesso, mensagem = importar_planilha_local()
    definir_feedback_clientes("sucesso" if sucesso else "erro", mensagem)
    return redirect("/clientes")


@app.route("/api/notificacoes")
def api_notificacoes():
    if not session.get("usuario"):
        return jsonify([])

    empresa_id = empresa_atual_id()
    garantir_notificacoes_aniversario(empresa_id=empresa_id)
    usuario_cache = str(session.get("usuario") or "")
    agora_cache_ts = time.time()
    if (
        NOTIFICACOES_CACHE.get("resultado") is not None
        and NOTIFICACOES_CACHE.get("usuario") == usuario_cache
        and agora_cache_ts - float(NOTIFICACOES_CACHE.get("testado_em") or 0.0) < NOTIFICACOES_CACHE_TTL
    ):
        return jsonify(NOTIFICACOES_CACHE["resultado"])

    def carregar_notificacoes(conn):
        c = conn.cursor()
        c.execute(
            """
            SELECT *
            FROM notificacoes
            WHERE empresa_id=?
            ORDER BY id DESC
            LIMIT 20
            """,
            (empresa_id,),
        )
        return c.fetchall()

    dados = executar_leitura_resiliente(
        carregar_notificacoes,
        descricao="API NOTIFICACOES",
        padrao=[],
    ) or []
    resultado = [dict(row) for row in dados]
    NOTIFICACOES_CACHE["testado_em"] = agora_cache_ts
    NOTIFICACOES_CACHE["usuario"] = usuario_cache
    NOTIFICACOES_CACHE["resultado"] = list(resultado)
    return jsonify(resultado)

@app.route("/api/notificacoes/lida/<int:id>", methods=["POST"])
def marcar_notificacao_lida(id):
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()

    c.execute("UPDATE notificacoes SET lida=1 WHERE empresa_id=? AND id=?", (empresa_id, id))

    conn.commit()
    conn.close()
    NOTIFICACOES_CACHE["testado_em"] = 0.0
    NOTIFICACOES_CACHE["resultado"] = None
    HUD_CACHE["testado_em"] = 0.0
    HUD_CACHE["resultado"] = None

    return jsonify({"status": "ok"})

@app.route("/api/notificacoes/limpar", methods=["POST"])
def limpar_notificacoes():
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM notificacoes WHERE empresa_id=?", (empresa_id,))
    total = c.fetchone()[0]
    c.execute("DELETE FROM notificacoes WHERE empresa_id=?", (empresa_id,))
    conn.commit()
    conn.close()
    NOTIFICACOES_CACHE["testado_em"] = 0.0
    NOTIFICACOES_CACHE["resultado"] = None
    HUD_CACHE["testado_em"] = 0.0
    HUD_CACHE["resultado"] = None

    return jsonify({"status": "ok", "removidas": total})

@app.route("/api/agenda-retornos")
def api_agenda_retorno():
    if not session.get("usuario"):
        return jsonify({"erro": "nao autorizado"}), 401

    try:
        usuario_cache = str(session.get("usuario") or "")
        agora_cache_ts = time.time()
        if (
            AGENDA_RETORNO_CACHE.get("resultado") is not None
            and AGENDA_RETORNO_CACHE.get("usuario") == usuario_cache
            and agora_cache_ts - float(AGENDA_RETORNO_CACHE.get("testado_em") or 0.0) < AGENDA_RETORNO_CACHE_TTL
        ):
            return jsonify(AGENDA_RETORNO_CACHE["resultado"])

        dados = listar_agenda_retorno_lavagens()
        AGENDA_RETORNO_CACHE["testado_em"] = agora_cache_ts
        AGENDA_RETORNO_CACHE["usuario"] = usuario_cache
        AGENDA_RETORNO_CACHE["resultado"] = dict(dados)
        return jsonify(dados)
    except Exception as e:
        log_info("ERRO AGENDA RETORNOS:", e)
        return jsonify({
            "erro": "nao foi possivel carregar a agenda agora",
            "detalhe": str(e),
        }), 500


def montar_resultado_hud_basico(sync_token="init-pendente"):
    status_banco = obter_status_banco_online()
    banco_online_ativo = bool(status_banco.get("conectado"))
    banco_online_backend = status_banco.get("backend_label") or "Supabase / PostgreSQL"
    banco_online_resumo = (
        "Banco online ativo"
        if banco_online_ativo
        else "Banco online indisponivel"
    )
    banco_online_mensagem = (
        f"Banco online ativo e gravando em tempo real ({banco_online_backend})"
        if banco_online_ativo
        else (status_banco.get("mensagem") or "Banco online indisponivel")
    )
    return {
        "total": 0.0,
        "andamento": 0,
        "atrasados": 0,
        "ticket": 0.0,
        "entregas_ativas": 0,
        "entregas_com_horario": 0,
        "entregas_sem_horario": 0,
        "entregas_vencidas": 0,
        "entrega_proxima_em_minutos": None,
        "entrega_proxima_placa": "",
        "entrega_proxima_hora": "",
        "entrega_mensagem": "Painel carregando dados...",
        "retornos_acao_agora": 0,
        "retornos_reagendados_vencidos": 0,
        "retornos_contatados_hoje": 0,
        "retornos_mensagem": "Painel retornos em dia",
        "banco_online_ativo": banco_online_ativo,
        "banco_online_resumo": banco_online_resumo,
        "banco_online_mensagem": banco_online_mensagem,
        "banco_online_backend_label": banco_online_backend,
        "versao": obter_versao_sistema(),
        "usuario": session.get("usuario") or "",
        "usuario_nome": session.get("usuario_nome") or session.get("usuario") or "",
        "usuario_iniciais": session.get("usuario_iniciais") or obter_iniciais_usuario(
            session.get("usuario_nome"),
            session.get("usuario"),
        ),
        "usuario_foto_url": session.get("usuario_foto_url") or "",
        "sync_token": sync_token,
    }


def resumo_retornos_hud_vazio():
    return {
        "acao_agora": 0,
        "reagendados_vencidos": 0,
        "contatados_hoje": 0,
    }


def obter_resumo_retornos_hud(usuario_cache, agora_cache_ts, referencia=None, somente_cache=False):
    if (
        RETORNOS_HUD_CACHE.get("resultado") is not None
        and RETORNOS_HUD_CACHE.get("usuario") == usuario_cache
        and agora_cache_ts - float(RETORNOS_HUD_CACHE.get("testado_em") or 0.0) < RETORNOS_HUD_CACHE_TTL
    ):
        return dict(RETORNOS_HUD_CACHE["resultado"])

    if somente_cache:
        return resumo_retornos_hud_vazio()

    referencia = referencia or agora()
    resultado = resumo_retornos_hud_vazio()

    try:
        itens_retornos = montar_itens_retornos_comerciais()
        hoje_data = referencia.date()
        resultado = {
            "acao_agora": sum(1 for item in itens_retornos if item.get("mostrar_na_agenda")),
            "reagendados_vencidos": sum(1 for item in itens_retornos if item.get("reagendamento_vencido")),
            "contatados_hoje": sum(
                1
                for item in itens_retornos
                if (
                    item.get("status_retorno") == "contatado"
                    and interpretar_datahora_sistema(item.get("ultimo_contato_em"))
                    and interpretar_datahora_sistema(item.get("ultimo_contato_em")).date() == hoje_data
                )
            ),
        }
    except Exception as erro:
        log_info("ERRO HUD RETORNOS:", erro)

    RETORNOS_HUD_CACHE["testado_em"] = agora_cache_ts
    RETORNOS_HUD_CACHE["usuario"] = usuario_cache
    RETORNOS_HUD_CACHE["resultado"] = dict(resultado)
    return resultado


def montar_resultado_hud_dinamico(usuario_cache, agora_cache_ts):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
    hoje = agora.strftime("%d/%m/%Y")
    empresa_id = empresa_atual_id()

    def carregar_hud(conn):
        c = conn.cursor()
        bloco = consultar_resumo_hud_domain(c, empresa_id, hoje + "%")
        resumo_financeiro = bloco.get("resumo_financeiro") or {"total": 0, "quantidade": 0}
        servicos_andamento = bloco.get("servicos_andamento") or []
        totais = {
            str(item["tabela"]): item
            for item in (bloco.get("totais") or [])
        }
        total = resumo_financeiro.get("total") or 0
        quantidade = resumo_financeiro.get("quantidade") or 0
        ticket = total / quantidade if quantidade > 0 else 0
        andamento = len(servicos_andamento)
        atrasados = 0
        for s in servicos_andamento:
            try:
                entrada = interpretar_datahora_sistema(s["entrada"])
                diff = (agora - entrada).total_seconds() if entrada else 0
                if diff > 7200:
                    atrasados += 1
            except Exception:
                pass

        return {
            "total": total,
            "andamento": andamento,
            "quantidade": quantidade,
            "ticket": ticket,
            "atrasados": atrasados,
            "resumo_entregas": resumir_entregas_em_andamento(servicos_andamento, referencia=agora),
            "servicos_total": totais.get("servicos", {}).get("total", 0),
            "servicos_ultimo_id": totais.get("servicos", {}).get("ultimo_id", 0),
            "veiculos_total": totais.get("veiculos", {}).get("total", 0),
            "veiculos_ultimo_id": totais.get("veiculos", {}).get("ultimo_id", 0),
            "clientes_total": totais.get("clientes", {}).get("total", 0),
            "clientes_ultimo_id": totais.get("clientes", {}).get("ultimo_id", 0),
            "notificacoes_total": totais.get("notificacoes", {}).get("total", 0),
            "notificacoes_ultimo_id": totais.get("notificacoes", {}).get("ultimo_id", 0),
            "auditoria_total": totais.get("auditoria", {}).get("total", 0),
            "auditoria_ultimo_id": totais.get("auditoria", {}).get("ultimo_id", 0),
            "usuarios_total": totais.get("usuarios", {}).get("total", 0),
            "usuarios_ultimo_id": totais.get("usuarios", {}).get("ultimo_id", 0),
        }

    leitura_hud = executar_leitura_resiliente(
        carregar_hud,
        descricao="HUD",
        padrao=None,
    )
    if not leitura_hud:
        resultado = dict(
            HUD_CACHE.get("resultado")
            or montar_resultado_hud_basico(sync_token=gerar_sync_token_leve())
        )
        HUD_CACHE["testado_em"] = agora_cache_ts
        HUD_CACHE["usuario"] = usuario_cache
        HUD_CACHE["resultado"] = dict(resultado)
        return resultado

    total = leitura_hud["total"]
    andamento = leitura_hud["andamento"]
    ticket = leitura_hud["ticket"]
    atrasados = leitura_hud["atrasados"]
    resumo_entregas = leitura_hud["resumo_entregas"]
    servicos_total = leitura_hud["servicos_total"]
    servicos_ultimo_id = leitura_hud["servicos_ultimo_id"]
    veiculos_total = leitura_hud["veiculos_total"]
    veiculos_ultimo_id = leitura_hud["veiculos_ultimo_id"]
    clientes_total = leitura_hud["clientes_total"]
    clientes_ultimo_id = leitura_hud["clientes_ultimo_id"]
    notificacoes_total = leitura_hud["notificacoes_total"]
    notificacoes_ultimo_id = leitura_hud["notificacoes_ultimo_id"]
    auditoria_total = leitura_hud["auditoria_total"]
    auditoria_ultimo_id = leitura_hud["auditoria_ultimo_id"]
    usuarios_total = leitura_hud["usuarios_total"]
    usuarios_ultimo_id = leitura_hud["usuarios_ultimo_id"]

    resumo_retornos = obter_resumo_retornos_hud(usuario_cache, agora_cache_ts, agora)
    retornos_acao_agora = resumo_retornos["acao_agora"]
    retornos_reagendados_vencidos = resumo_retornos["reagendados_vencidos"]
    retornos_contatados_hoje = resumo_retornos["contatados_hoje"]

    if retornos_acao_agora > 0:
        mensagem_retornos_hud = (
            f"Painel retornos requer atencao: {retornos_acao_agora} cliente(s)"
        )
        if retornos_reagendados_vencidos > 0:
            mensagem_retornos_hud += (
                f" | {retornos_reagendados_vencidos} reagendado(s) vencido(s)"
            )
    elif retornos_contatados_hoje > 0:
        mensagem_retornos_hud = (
            f"Painel retornos em dia | {retornos_contatados_hoje} contato(s) hoje"
        )
    else:
        mensagem_retornos_hud = "Painel retornos em dia"

    token_bruto = "|".join(
        str(v)
        for v in (
            servicos_total,
            servicos_ultimo_id,
            veiculos_total,
            veiculos_ultimo_id,
            clientes_total,
            clientes_ultimo_id,
            notificacoes_total,
            notificacoes_ultimo_id,
            auditoria_total,
            auditoria_ultimo_id,
            usuarios_total,
            usuarios_ultimo_id,
        )
    )
    sync_token = hashlib.sha1(token_bruto.encode("utf-8")).hexdigest()

    entrega_mensagem = "Entrega combinada em dia"
    if resumo_entregas["total"] > 0:
        if resumo_entregas["vencidas"] > 0:
            entrega_mensagem = (
                f"Entrega combinada: {resumo_entregas['vencidas']} vencida(s)"
            )
            if resumo_entregas["proxima"]:
                entrega_mensagem += (
                    f" | proxima em {formatar_duracao_segundos(resumo_entregas['proxima']['segundos'])}"
                )
        elif resumo_entregas["proxima"]:
            entrega_mensagem = (
                "Entrega combinada: proxima em "
                f"{formatar_duracao_segundos(resumo_entregas['proxima']['segundos'])}"
            )
            if resumo_entregas["proxima"]["placa"]:
                entrega_mensagem += f" ({resumo_entregas['proxima']['placa']})"
        elif resumo_entregas["sem_horario"] > 0:
            entrega_mensagem = (
                f"Entrega combinada: {resumo_entregas['sem_horario']} sem horario"
            )
        else:
            entrega_mensagem = (
                f"Entrega combinada: {resumo_entregas['com_horario']} agendada(s)"
            )

    status_banco = obter_status_banco_online()
    banco_online_ativo = bool(status_banco.get("conectado"))
    banco_online_backend = status_banco.get("backend_label") or "Supabase / PostgreSQL"
    banco_online_resumo = (
        "Banco online ativo"
        if banco_online_ativo
        else "Banco online indisponivel"
    )
    banco_online_mensagem = (
        f"Banco online ativo e gravando em tempo real ({banco_online_backend})"
        if banco_online_ativo
        else (status_banco.get("mensagem") or "Banco online indisponivel")
    )

    resultado = {
        "total": round(total, 2),
        "andamento": andamento,
        "atrasados": atrasados,
        "ticket": round(ticket, 2),
        "entregas_ativas": resumo_entregas["total"],
        "entregas_com_horario": resumo_entregas["com_horario"],
        "entregas_sem_horario": resumo_entregas["sem_horario"],
        "entregas_vencidas": resumo_entregas["vencidas"],
        "entrega_proxima_em_minutos": resumo_entregas["proxima"]["minutos"] if resumo_entregas["proxima"] else None,
        "entrega_proxima_placa": resumo_entregas["proxima"]["placa"] if resumo_entregas["proxima"] else "",
        "entrega_proxima_hora": resumo_entregas["proxima"]["entrega_prevista"].strftime("%H:%M") if resumo_entregas["proxima"] else "",
        "entrega_mensagem": entrega_mensagem,
        "retornos_acao_agora": retornos_acao_agora,
        "retornos_reagendados_vencidos": retornos_reagendados_vencidos,
        "retornos_contatados_hoje": retornos_contatados_hoje,
        "retornos_mensagem": mensagem_retornos_hud,
        "banco_online_ativo": banco_online_ativo,
        "banco_online_resumo": banco_online_resumo,
        "banco_online_mensagem": banco_online_mensagem,
        "banco_online_backend_label": banco_online_backend,
        "versao": obter_versao_sistema(),
        "usuario": session.get("usuario") or "",
        "usuario_nome": session.get("usuario_nome") or session.get("usuario") or "",
        "usuario_iniciais": session.get("usuario_iniciais") or obter_iniciais_usuario(
            session.get("usuario_nome"),
            session.get("usuario"),
        ),
        "usuario_foto_url": session.get("usuario_foto_url") or "",
        "sync_token": sync_token,
    }
    HUD_CACHE["testado_em"] = agora_cache_ts
    HUD_CACHE["usuario"] = usuario_cache
    HUD_CACHE["resultado"] = dict(resultado)
    return resultado


def _carregar_partes_sync_token(conn):
    c = conn.cursor()
    partes = []
    for tabela in ("servicos", "veiculos", "clientes", "notificacoes", "auditoria", "usuarios"):
        c.execute(
            f"""
            SELECT
                COALESCE(COUNT(*), 0) AS total,
                COALESCE(MAX(id), 0) AS ultimo_id
            FROM {tabela}
            """
        )
        total, ultimo_id = c.fetchone() or (0, 0)
        partes.extend([total, ultimo_id])
    return partes


def gerar_sync_token_leve():
    usuario_cache = str(session.get("usuario") or "")
    cache = HUD_CACHE.get("resultado") or {}
    cache_usuario = HUD_CACHE.get("usuario") or ""
    if cache and cache_usuario == usuario_cache and cache.get("sync_token"):
        return str(cache.get("sync_token"))

    cache_sync = SYNC_TOKEN_CACHE.get("resultado")
    if (
        cache_sync is not None
        and SYNC_TOKEN_CACHE.get("usuario") == usuario_cache
        and time.time() - float(SYNC_TOKEN_CACHE.get("testado_em") or 0.0) < SYNC_TOKEN_CACHE_TTL
    ):
        return str(cache_sync)

    if not INIT_DB_EXECUTADO or (
        modo_banco_preferido() == "postgres" and not SCHEMA_BANCO_ONLINE_GARANTIDO
    ):
        return "init-pendente"

    partes = executar_leitura_resiliente(
        lambda conn: _carregar_partes_sync_token(conn),
        descricao="SYNC TOKEN",
        padrao=None,
    )
    if not partes:
        return str(cache.get("sync_token") or "sync-indisponivel")

    token = hashlib.sha1("|".join(str(valor) for valor in partes).encode("utf-8")).hexdigest()
    SYNC_TOKEN_CACHE["testado_em"] = time.time()
    SYNC_TOKEN_CACHE["usuario"] = usuario_cache
    SYNC_TOKEN_CACHE["resultado"] = token
    return token


@app.route("/api/sync-token")
def api_sync_token():
    if not session.get("usuario"):
        return jsonify({"erro": "nao autorizado"}), 401
    return jsonify({"sync_token": gerar_sync_token_leve()})

def obter_payload_hud():
    if not session.get("usuario"):
        return {"erro": "nao autorizado"}

    usuario_cache = str(session.get("usuario") or "")
    agora_cache_ts = time.time()
    if (
        HUD_CACHE.get("resultado") is not None
        and HUD_CACHE.get("usuario") == usuario_cache
        and agora_cache_ts - float(HUD_CACHE.get("testado_em") or 0.0) < HUD_CACHE_TTL
    ):
        return dict(HUD_CACHE["resultado"])

    if not INIT_DB_EXECUTADO or (
        modo_banco_preferido() == "postgres" and not SCHEMA_BANCO_ONLINE_GARANTIDO
    ):
        resultado = montar_resultado_hud_basico(sync_token="init-pendente")
        HUD_CACHE["testado_em"] = agora_cache_ts
        HUD_CACHE["usuario"] = usuario_cache
        HUD_CACHE["resultado"] = dict(resultado)
        return resultado

    return montar_resultado_hud_dinamico(usuario_cache, agora_cache_ts)

@app.route("/api/hud")
def api_hud():
    return obter_payload_hud()

def obter_payload_status_sync():
    if not session.get("usuario"):
        return {"status": "erro", "mensagem": "", "id": 0}

    usuario_cache = str(session.get("usuario") or "")
    agora_cache_ts = time.time()
    if (
        STATUS_SYNC_CACHE.get("resultado") is not None
        and STATUS_SYNC_CACHE.get("usuario") == usuario_cache
        and agora_cache_ts - float(STATUS_SYNC_CACHE.get("testado_em") or 0.0) < STATUS_SYNC_CACHE_TTL
    ):
        return dict(STATUS_SYNC_CACHE["resultado"])

    def carregar(conn):
        c = conn.cursor()
        row = consultar_ultima_sincronizacao_cliente_domain(c, empresa_atual_id())
        if not row:
            return None
        return {
            "id": row.get("id"),
            "ultima_mensagem": row.get("ultima_mensagem"),
            "ultimo_status": row.get("ultimo_status"),
        }

    row = executar_leitura_resiliente(
        carregar,
        descricao="STATUS SYNC",
        padrao={
            "id": 0,
            "ultima_mensagem": "Sincronizacao em processamento.",
            "ultimo_status": "indisponivel",
        },
    )
    if not row:
        resultado = {"status": "vazio", "mensagem": "", "id": 0}
        STATUS_SYNC_CACHE["testado_em"] = agora_cache_ts
        STATUS_SYNC_CACHE["usuario"] = usuario_cache
        STATUS_SYNC_CACHE["resultado"] = dict(resultado)
        return resultado

    resultado = {
        "status": row.get("ultimo_status") or "vazio",
        "mensagem": row.get("ultima_mensagem") or "",
        "id": row["id"],
    }
    STATUS_SYNC_CACHE["testado_em"] = agora_cache_ts
    STATUS_SYNC_CACHE["usuario"] = usuario_cache
    STATUS_SYNC_CACHE["resultado"] = dict(resultado)
    return resultado

@app.route("/status_sync")
def status_sync():
    return jsonify(obter_payload_status_sync())


@app.route("/api/home-snapshot")
def api_home_snapshot():
    if not session.get("usuario"):
        return jsonify({"erro": "nao autorizado"}), 401
    usuario_cache = str(session.get("usuario") or "")
    agora_cache_ts = time.time()
    if (
        HOME_SNAPSHOT_CACHE.get("resultado") is not None
        and HOME_SNAPSHOT_CACHE.get("usuario") == usuario_cache
        and agora_cache_ts - float(HOME_SNAPSHOT_CACHE.get("testado_em") or 0.0) < HOME_SNAPSHOT_CACHE_TTL
    ):
        return jsonify(HOME_SNAPSHOT_CACHE["resultado"])

    resultado = {
        "hud": obter_payload_hud(),
        "clima": obter_resultado_clima_api(permitir_rede=False),
        "sync": obter_payload_status_sync(),
    }
    HOME_SNAPSHOT_CACHE["testado_em"] = agora_cache_ts
    HOME_SNAPSHOT_CACHE["usuario"] = usuario_cache
    HOME_SNAPSHOT_CACHE["resultado"] = dict(resultado)
    return jsonify(resultado)

@app.route("/editar_servico_inline/<int:id>", methods=["POST"])
def editar_servico_inline(id):
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    data = request.get_json()

    nome = data.get("nome")
    valor = converter_valor_numerico(data.get("valor"))

    conn = conectar()
    c = conn.cursor()

    c.execute("UPDATE tipos_servico SET nome=?, valor=? WHERE id=?", (nome, valor, id))
    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})

@app.route("/excluir_servico/<int:id>", methods=["POST"])
def excluir_servico(id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    c.execute("DELETE FROM tipos_servico WHERE id=?", (id,))
    conn.commit()
    conn.close()

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"status": "ok"})
    return redirect("/cadastrar_servico")


@app.route("/servico/<int:id>/status", methods=["POST"])
def atualizar_status_servico_legado(id):
    if not session.get("usuario"):
        return redirect("/login")

    redirect_to = normalizar_redirect_interno(request.form.get("redirect_to"), "/historico")
    status_destino = normalizar_texto_campo(request.form.get("status")).upper() or "EM ANDAMENTO"
    if status_destino not in {"EM ANDAMENTO", "FINALIZADO"}:
        definir_feedback_por_destino(redirect_to, "erro", "Status de atendimento invalido.")
        return redirect(redirect_to)

    usuario_info = resumo_usuario_logado()
    conn = conectar()
    c = conn.cursor()
    servico = consultar_servico_operacional_domain(c, empresa_atual_id(), id)
    if not servico:
        conn.close()
        definir_feedback_por_destino(redirect_to, "erro", "Atendimento nao encontrado.")
        return redirect(redirect_to)

    entrega_iso = servico.get("entrega")
    finalizado_por_usuario = servico.get("finalizado_por_usuario")
    finalizado_por_nome = servico.get("finalizado_por_nome")
    if status_destino == "FINALIZADO":
        entrega_iso = entrega_iso or agora_iso()
        finalizado_por_usuario = finalizado_por_usuario or normalizar_texto_campo(usuario_info.get("usuario"))
        finalizado_por_nome = finalizado_por_nome or normalizar_texto_campo(usuario_info.get("nome"))
    else:
        entrega_iso = None
        finalizado_por_usuario = None
        finalizado_por_nome = None

    servico_fluxo = aplicar_fluxo_etapa_atendimento_em_edicao(
        c,
        servico,
        status_destino,
        servico.get("etapa_atual") or "LAVAGEM",
    )
    fotos_saida = salvar_fotos_servico(c, id, request.files.getlist("fotos_depois"), "saida")
    atualizar_status_servico_domain(
        c,
        empresa_atual_id(),
        id,
        status_destino,
        entrega_iso,
        finalizado_por_usuario,
        finalizado_por_nome,
    )
    if servico_fluxo and servico_fluxo.get("veiculo_id"):
        recalcular_resumo_veiculo_por_servicos(c, servico_fluxo["veiculo_id"])
    conn.commit()
    conn.close()

    registrar_auditoria(
        "atualizou_status_atendimento",
        "servico",
        entidade_id=id,
        placa=servico.get("placa"),
        detalhes={"status": status_destino, "fotos_saida_adicionadas": fotos_saida},
        usuario=usuario_info,
    )
    mensagem = f"Status da placa {servico.get('placa') or '-'} atualizado para {status_destino}."
    if fotos_saida:
        mensagem += f" {fotos_saida} foto(s) de saida adicionada(s)."
    definir_feedback_por_destino(redirect_to, "sucesso", mensagem)
    return redirect(redirect_to)


@app.route("/editar_servico/<int:id>", methods=["GET", "POST"])
def editar_servico(id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    if request.method == "POST":
        nome = request.form['nome']
        valor = converter_valor_numerico(request.form['valor'])

        c.execute("UPDATE tipos_servico SET nome=?, valor=? WHERE id=?", (nome, valor, id))
        conn.commit()
        conn.close()

        return redirect("/cadastrar_servico")

    c.execute("SELECT * FROM tipos_servico WHERE id=?", (id,))
    servico = c.fetchone()

    conn.close()

    return render_template("editar_servico.html", servico=servico)

# ðŸ” CRIAR ADMIN PADRÃƒO
def criar_admin():
    conn = conectar()
    c = conn.cursor()

    c.execute("SELECT * FROM usuarios WHERE usuario=?", ("admin",))
    admin = c.fetchone()
    senha_temporaria = None
    aviso_troca = False

    if admin:
        c.execute("""
            UPDATE usuarios
            SET
                nome=COALESCE(NULLIF(nome, ''), 'Administrador'),
                perfil=COALESCE(NULLIF(perfil, ''), 'admin'),
                ativo=COALESCE(ativo, 1),
                criado_em=COALESCE(criado_em, ?),
                tentativas_login=COALESCE(tentativas_login, 0),
                senha_alteracao_obrigatoria=COALESCE(senha_alteracao_obrigatoria, 0),
                senha_atualizada_em=COALESCE(senha_atualizada_em, criado_em, ?)
            WHERE usuario='admin'
        """, (agora_iso(), agora_iso()))

        c.execute("SELECT * FROM usuarios WHERE usuario=?", ("admin",))
        admin_atualizado = c.fetchone()

        if not str(admin_atualizado["senha"] or "").strip():
            senha_temporaria = gerar_senha_temporaria_segura()
            c.execute("""
                UPDATE usuarios
                SET senha=?,
                    senha_alteracao_obrigatoria=1,
                    senha_atualizada_em=?,
                    tentativas_login=0,
                    bloqueado_ate=NULL
                WHERE usuario='admin'
            """, (
                senha_hash_bcrypt(senha_temporaria),
                agora_iso(),
            ))
        elif not senha_usa_bcrypt(admin_atualizado["senha"]):
            c.execute("""
                UPDATE usuarios
                SET senha_alteracao_obrigatoria=1
                WHERE usuario='admin'
            """)
        elif (
            not ambiente_hospedado_gerenciado()
            and not int(admin_atualizado["senha_alteracao_obrigatoria"] or 0)
            and senha_padrao_admin_ativa(admin_atualizado)
        ):
            c.execute("""
                UPDATE usuarios
                SET senha_alteracao_obrigatoria=1
                WHERE usuario='admin'
            """)
            aviso_troca = True
    else:
        senha_temporaria = gerar_senha_temporaria_segura()
        c.execute("""
            INSERT INTO usuarios (
                usuario, senha, nome, perfil, ativo, criado_em,
                tentativas_login, bloqueado_ate, ultimo_login_em,
                senha_alteracao_obrigatoria, senha_atualizada_em
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "admin",
            senha_hash_bcrypt(senha_temporaria),
            "Administrador",
            "admin",
            1,
            agora_iso(),
            0,
            None,
            None,
            1,
            agora_iso(),
        ))

    conn.commit()
    conn.close()

    if senha_temporaria:
        log_info(f"ADMIN criado/recuperado com senha temporaria segura: admin / {senha_temporaria}")
        log_info("ATENCAO: troque essa senha no primeiro login.")
    elif aviso_troca:
        log_info("ATENCAO: senha antiga/padrao do administrador detectada. Troca obrigatoria ativada.")

def carregar_usuarios_configuracao():
    def carregar(conn):
        c = conn.cursor()
        c.execute("""
            SELECT id, usuario, nome, perfil, ativo, criado_em,
                   tentativas_login, bloqueado_ate, ultimo_login_em,
                   senha_alteracao_obrigatoria, foto_perfil
            FROM usuarios
            WHERE empresa_id=?
            ORDER BY
                CASE
                    WHEN LOWER(COALESCE(perfil, ''))='desenvolvedor' THEN 0
                    WHEN LOWER(COALESCE(perfil, ''))='admin' THEN 1
                    ELSE 2
                END,
                LOWER(COALESCE(nome, usuario, '')),
                LOWER(COALESCE(usuario, nome, ''))
        """, (empresa_atual_id(),))
        return [dict(row) for row in c.fetchall()]

    usuarios = executar_leitura_resiliente(
        carregar,
        descricao="USUARIOS CONFIG",
        padrao=[],
    )

    for item in usuarios:
        item["nome"] = item.get("nome") or item.get("usuario")
        item["perfil"] = normalizar_perfil_usuario(item.get("perfil"))
        item["perfil_label"] = rotulo_perfil_usuario(item.get("perfil"))
        item["ativo"] = int(item.get("ativo") or 0)
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))
        item["ultimo_login_em_fmt"] = formatar_datahora(item.get("ultimo_login_em"))
        item["tentativas_login"] = int(item.get("tentativas_login") or 0)
        item["troca_senha_obrigatoria"] = bool(int(item.get("senha_alteracao_obrigatoria") or 0))
        item["iniciais"] = obter_iniciais_usuario(item.get("nome"), item.get("usuario"))
        item["foto_url"] = url_foto_usuario(item.get("foto_perfil"), item.get("id"))
        bloqueado_ate = usuario_bloqueado_ate(item)
        item["bloqueado"] = bool(bloqueado_ate)
        item["bloqueado_ate_fmt"] = formatar_datahora(item.get("bloqueado_ate"))
        item["bloqueado_restante"] = formatar_tempo_restante(item.get("bloqueado_ate")) if bloqueado_ate else ""

    return usuarios

@app.route("/login", methods=["GET", "POST"])
def login():

    versao_login = obter_versao_sistema(permitir_sem_sessao=True)

    if session.get("usuario"):
        sincronizar_sessao_usuario_seguro(contexto="LOGIN")
        if session.get("senha_alteracao_obrigatoria"):
            return redirect("/configuracoes")
        return redirect("/")

    if request.method == "POST":
        usuario = (request.form.get("usuario") or "").strip()
        senha = request.form.get("senha") or ""

        if not usuario or not senha:
            registrar_evento_telemetria_app(
                "login_campos_incompletos",
                categoria="auth",
                severidade="warning",
                payload={"usuario": usuario},
            )
            return render_template("login.html", erro="Informe usuario e senha.", app_version=versao_login)

        conn = None
        try:
            conn = conectar()
            c = conn.cursor()

            c.execute("SELECT * FROM usuarios WHERE usuario=?", (usuario,))
            user = c.fetchone()

            if not user:
                registrar_evento_telemetria_app(
                    "login_usuario_inexistente",
                    categoria="auth",
                    severidade="warning",
                    payload={"usuario": usuario},
                )
                conn.close()
                return render_template("login.html", erro="Usuario ou senha invalidos.", app_version=versao_login)

            if not int(user["ativo"] if user["ativo"] is not None else 1):
                registrar_evento_telemetria_app(
                    "login_usuario_inativo",
                    categoria="auth",
                    severidade="warning",
                    usuario_row=user,
                )
                conn.close()
                return render_template("login.html", erro="Este acesso esta desativado.", app_version=versao_login)

            bloqueado_ate = usuario_bloqueado_ate(user)
            if bloqueado_ate:
                registrar_evento_telemetria_app(
                    "login_bloqueado_temporariamente",
                    categoria="auth",
                    severidade="warning",
                    usuario_row=user,
                    payload={"bloqueado_ate": bloqueado_ate.isoformat(timespec="seconds")},
                )
                conn.close()
                return render_template(
                    "login.html",
                    erro=(
                        "Login bloqueado temporariamente. "
                        f"{formatar_tempo_restante(bloqueado_ate.isoformat(timespec='seconds'))} "
                        "para tentar de novo."
                    ),
                    app_version=versao_login,
                )

            if not verificar_senha_usuario(senha, user["senha"]):
                novo_bloqueio = registrar_falha_login(c, user)
                conn.commit()
                registrar_evento_telemetria_app(
                    "login_senha_invalida",
                    categoria="auth",
                    severidade="warning",
                    usuario_row=user,
                    payload={"novo_bloqueio": bool(novo_bloqueio)},
                )
                conn.close()
                if novo_bloqueio:
                    return render_template(
                        "login.html",
                        erro=f"Muitas tentativas invalidas. Login bloqueado por {MINUTOS_BLOQUEIO_LOGIN} minutos.",
                        app_version=versao_login,
                    )
                return render_template("login.html", erro="Usuario ou senha invalidos.", app_version=versao_login)

            if not senha_usa_bcrypt(user["senha"]):
                c.execute(
                    "UPDATE usuarios SET senha=?, senha_alteracao_obrigatoria=1, senha_atualizada_em=? WHERE id=?",
                    (senha_hash_bcrypt(senha), agora_iso(), user["id"])
                )
            elif senha_padrao_admin_ativa(user):
                c.execute(
                    "UPDATE usuarios SET senha_alteracao_obrigatoria=1 WHERE id=?",
                    (user["id"],)
                )

            limpar_status_login_usuario(c, user["id"], registrar_login=True)
            conn.commit()
            c.execute("SELECT * FROM usuarios WHERE id=?", (user["id"],))
            user = c.fetchone()
        except Exception as erro:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            log_info("ERRO LOGIN:", erro)
            registrar_evento_telemetria_app(
                "login_falha_interna",
                categoria="auth",
                severidade="error",
                payload={"usuario": usuario, "erro": str(erro)},
            )
            return render_template("login.html", erro=mensagem_erro_login_servidor(erro), app_version=versao_login)

        conn.close()
        preencher_sessao_usuario(user)
        registrar_evento_telemetria_app(
            "login_sucesso",
            categoria="auth",
            severidade="info",
            usuario_row=user,
        )
        if session.get("senha_alteracao_obrigatoria"):
            definir_feedback_configuracoes(
                "erro",
                (
                    "Por seguranca, troque sua senha antes de continuar. "
                    "Senhas temporarias, padrao ou antigas nao ficam "
                    "liberadas no sistema."
                )
            )
            return redirect("/configuracoes")
        return redirect("/")

    return render_template("login.html", app_version=versao_login)

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/empresas")
def pagina_empresas():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_empresas():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem gerenciar empresas e licencas.")
        return redirect(destino_configuracoes("banco"))

    conn = conectar()
    c = conn.cursor()
    empresas = listar_empresas_domain(c)
    for empresa in empresas:
        licenca = obter_licenca_domain(c, empresa["id"])
        uso = obter_uso_licenca_domain(c, empresa["id"], agora().strftime("%Y-%m"))
        empresa["licenca"] = montar_contexto_licenca_domain(licenca, uso, hoje=agora().date())
        empresa["licenca_validacao"] = validar_licenca_assinada_domain(licenca, segredo_assinatura_licenca())
    conn.close()

    return render_template(
        "empresas.html",
        feedback=session.pop("empresas_feedback", None) or session.pop("configuracoes_feedback", None),
        empresas=empresas,
        empresa_atual_id=empresa_atual_id(),
        planos_licenca=PLANOS_LICENCA,
        status_licenca=STATUS_LICENCA,
    )


@app.route("/empresas/salvar", methods=["POST"])
def salvar_empresa_admin():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_empresas():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem salvar empresas.")
        return redirect("/configuracoes")

    empresa_id = converter_inteiro(request.form.get("empresa_id"), 0)
    nome_fantasia = normalizar_texto_campo(request.form.get("nome_fantasia"))
    razao_social = normalizar_texto_campo(request.form.get("razao_social"))
    if not nome_fantasia and not razao_social:
        session["empresas_feedback"] = {"tipo": "erro", "mensagem": "Informe ao menos o nome fantasia ou razao social."}
        return redirect("/empresas")

    plano_codigo = normalizar_plano_licenca_domain(request.form.get("plano_codigo"))
    status = normalizar_status_licenca_domain(request.form.get("licenca_status"))
    slug = normalizar_slug_empresa(request.form.get("slug") or nome_fantasia or razao_social, fallback=f"empresa-{empresa_id or 'nova'}")
    ativa = 1 if request.form.get("ativa") else 0

    dados_empresa = {
        "slug": slug,
        "razao_social": razao_social,
        "nome_fantasia": nome_fantasia,
        "documento": normalizar_texto_campo(request.form.get("documento")),
        "email": normalizar_texto_campo(request.form.get("email")),
        "telefone": normalizar_texto_campo(request.form.get("telefone")),
        "ativa": ativa,
        "storage_provider": normalizar_texto_campo(request.form.get("storage_provider")) or "database",
        "dominio_personalizado": normalizar_texto_campo(request.form.get("dominio_personalizado")),
        "plano_codigo": plano_codigo,
        "licenca_status": status,
    }
    dados_licenca = montar_dados_licenca_form(request.form)

    try:
        conn = conectar()
        c = conn.cursor()
        empresa_salva_id = salvar_empresa_domain(c, dados_empresa, agora_iso(), empresa_id=empresa_id or None)
        if not empresa_salva_id:
            empresa_salva_id = empresa_id
        salvar_licenca_domain(c, empresa_salva_id, dados_licenca, agora_iso())
        if not selecionar_configuracao_empresa_cursor(c, empresa_salva_id):
            c.execute(
                """
                INSERT INTO configuracao_empresa (
                    empresa_id, marca_nome, marca_subtitulo,
                    licenca_plano, licenca_status, atualizado_em
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    empresa_salva_id,
                    nome_fantasia or razao_social or f"Empresa {empresa_salva_id}",
                    "Gestao Estetica",
                    plano_codigo,
                    status,
                    agora_iso(),
                ),
            )
        conn.commit()
        conn.close()
        limpar_caches_interface()
        registrar_auditoria(
            "salvou_empresa_licenca",
            "empresa",
            entidade_id=empresa_salva_id,
            detalhes={
                "nome_fantasia": nome_fantasia,
                "razao_social": razao_social,
                "plano_codigo": plano_codigo,
                "licenca_status": status,
                "ativa": bool(ativa),
            },
        )
        session["empresas_feedback"] = {"tipo": "sucesso", "mensagem": "Empresa e licenca salvas com sucesso."}
    except Exception as erro:
        session["empresas_feedback"] = {"tipo": "erro", "mensagem": f"Nao foi possivel salvar a empresa: {erro}"}

    return redirect("/empresas")


def salvar_licenca_assinada_empresa(empresa_id, dados_licenca, renovar=False):
    segredo = segredo_assinatura_licenca()
    agora_atual = agora_iso()
    conn = conectar()
    c = conn.cursor()
    existente = obter_licenca_domain(c, empresa_id)
    codigo_atual = (existente or {}).get("codigo_licenca") if renovar else None
    assinatura = gerar_licenca_assinada_domain(
        empresa_id,
        dados_licenca,
        segredo,
        agora_atual,
        renovar=renovar,
        codigo_licenca=codigo_atual,
    )
    payload = dict(dados_licenca)
    payload.update(assinatura)
    salvar_licenca_domain(c, empresa_id, payload, agora_atual)
    conn.commit()
    conn.close()
    limpar_caches_interface()
    return assinatura


@app.route("/empresas/<int:empresa_id>/licenca/gerar", methods=["POST"])
def gerar_licenca_empresa_admin(empresa_id):
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_empresas():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem gerar licencas.")
        return redirect("/configuracoes")

    try:
        assinatura = salvar_licenca_assinada_empresa(empresa_id, montar_dados_licenca_form(request.form), renovar=False)
        registrar_auditoria(
            "gerou_licenca_assinada",
            "licencas",
            entidade_id=empresa_id,
            detalhes={"codigo_licenca": assinatura.get("codigo_licenca")},
        )
        session["empresas_feedback"] = {"tipo": "sucesso", "mensagem": "Licenca assinada gerada com HMAC-SHA256."}
    except Exception as erro:
        session["empresas_feedback"] = {"tipo": "erro", "mensagem": f"Nao foi possivel gerar a licenca: {erro}"}
    return redirect("/empresas")


@app.route("/empresas/<int:empresa_id>/licenca/renovar", methods=["POST"])
def renovar_licenca_empresa_admin(empresa_id):
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_empresas():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem renovar licencas.")
        return redirect("/configuracoes")

    try:
        dados = montar_dados_licenca_form(request.form)
        if not dados.get("validade_em"):
            dados["validade_em"] = (agora().date() + timedelta(days=30)).isoformat()
        assinatura = salvar_licenca_assinada_empresa(empresa_id, dados, renovar=True)
        registrar_auditoria(
            "renovou_licenca_assinada",
            "licencas",
            entidade_id=empresa_id,
            detalhes={"codigo_licenca": assinatura.get("codigo_licenca"), "validade_em": dados.get("validade_em")},
        )
        session["empresas_feedback"] = {"tipo": "sucesso", "mensagem": "Licenca renovada e assinada novamente."}
    except Exception as erro:
        session["empresas_feedback"] = {"tipo": "erro", "mensagem": f"Nao foi possivel renovar a licenca: {erro}"}
    return redirect("/empresas")


@app.route("/empresas/trocar", methods=["POST"])
def trocar_empresa_ativa():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_empresas():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem trocar a empresa ativa.")
        return redirect("/configuracoes")

    empresa_id = converter_inteiro(request.form.get("empresa_id"), 1)
    conn = conectar()
    c = conn.cursor()
    empresa = obter_empresa_domain(c, empresa_id)
    conn.close()
    if not empresa:
        session["empresas_feedback"] = {"tipo": "erro", "mensagem": "Empresa selecionada nao foi encontrada."}
        return redirect("/empresas")
    if normalize_empresa_id(empresa_id) != empresa_atual_id() and not recurso_liberado_por_plano("multiempresa"):
        session["empresas_feedback"] = {
            "tipo": "erro",
            "mensagem": "Troca entre empresas exige plano Business com multiempresa liberado.",
        }
        return redirect("/empresas")

    session["empresa_id"] = normalize_empresa_id(empresa_id)
    session["empresas_feedback"] = {
        "tipo": "sucesso",
        "mensagem": f"Empresa ativa alterada para {empresa.get('nome_fantasia') or empresa.get('razao_social') or empresa_id}.",
    }
    registrar_auditoria(
        "trocou_empresa_ativa",
        "empresa",
        entidade_id=empresa_id,
        detalhes={
            "nome_fantasia": empresa.get("nome_fantasia"),
            "razao_social": empresa.get("razao_social"),
        },
    )
    return redirect("/empresas")


def montar_checklist_producao():
    banco_status = executar_com_fallback_producao(
        obter_status_banco_online,
        {"conectado": False, "mensagem": "Banco online indisponivel para validacao."},
        "status_banco_online",
    )
    backup_status = executar_com_fallback_producao(
        obter_status_backup_banco,
        status_backup_banco_padrao(),
        "status_backup",
    )
    credenciais_drive, erro_drive = executar_com_fallback_producao(
        carregar_credenciais_google_drive,
        (None, "Google Drive indisponivel para validacao."),
        "credenciais_google_drive",
    )
    itens = [
        {
            "nome": "Chave secreta configurada",
            "ok": bool(FLASK_SECRET_KEY_RAW),
            "detalhe": "FLASK_SECRET_KEY definida no ambiente." if FLASK_SECRET_KEY_RAW else "Defina FLASK_SECRET_KEY com valor unico antes de vender.",
            "acao": "Manter segredo fora do Git." if FLASK_SECRET_KEY_RAW else "Configurar FLASK_SECRET_KEY no deploy.",
        },
        {
            "nome": "CSRF ativo",
            "ok": csrf_protection_ativa(),
            "detalhe": "CSRF_PROTECTION=1" if csrf_protection_ativa() else "Defina CSRF_PROTECTION=1 antes de vender.",
            "acao": "Manter ativo no deploy." if csrf_protection_ativa() else "Ativar CSRF_PROTECTION=1 no ambiente.",
        },
        {
            "nome": "Cookie seguro",
            "ok": SESSION_COOKIE_SECURE_RAW in {"1", "true", "yes", "on"},
            "detalhe": "SESSION_COOKIE_SECURE=1 em HTTPS." if SESSION_COOKIE_SECURE_RAW in {"1", "true", "yes", "on"} else "Ative SESSION_COOKIE_SECURE=1 no deploy HTTPS.",
            "acao": "Confirmar HTTPS do dominio." if SESSION_COOKIE_SECURE_RAW in {"1", "true", "yes", "on"} else "Ativar SESSION_COOKIE_SECURE=1 no deploy HTTPS.",
        },
        {
            "nome": "Banco online",
            "ok": bool(banco_status.get("conectado")),
            "detalhe": banco_status.get("mensagem") or "Banco online nao validado.",
            "acao": "Rodar migrations no Supabase." if banco_status.get("conectado") else "Validar DATABASE_URL/SUPABASE_DATABASE_URL.",
        },
        {
            "nome": "Backup ativo",
            "ok": bool(backup_status.get("quantidade", 0) >= 0),
            "detalhe": f"Modo {backup_status.get('tipo_backup_label', '-')}, frequencia {backup_status.get('frequencia_label', '-')}.",
            "acao": "Gerar backup antes de suporte remoto.",
        },
        {
            "nome": "Google Drive",
            "ok": bool(credenciais_drive and google_drive_disponivel_para_backup()),
            "detalhe": "Credencial pronta." if credenciais_drive else (erro_drive or "Credencial nao configurada."),
            "acao": "Testar copia externa." if credenciais_drive else "Configurar service account e pasta do Drive.",
        },
    ]
    return itens


def executar_com_fallback_producao(funcao, padrao, descricao):
    try:
        return funcao()
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao=descricao)
        return padrao


def executar_check_auto_suporte(funcao, padrao, descricao, descricoes_resolvidas=None):
    try:
        resultado = funcao()
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao=descricao)
        return padrao
    if descricoes_resolvidas:
        marcar_erros_auto_suporte_resolvidos(descricoes_resolvidas)
    return resultado


def caminho_erros_producao():
    pasta = os.path.dirname(os.path.abspath(ERROS_PRODUCAO_ARQUIVO))
    os.makedirs(pasta, exist_ok=True)
    return os.path.abspath(ERROS_PRODUCAO_ARQUIVO)


def carregar_erros_producao():
    caminho = caminho_erros_producao()
    if not os.path.isfile(caminho):
        return []
    try:
        with open(caminho, "r", encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
        if isinstance(dados, list):
            return [item for item in dados if isinstance(item, dict)]
    except Exception:
        return []
    return []


def salvar_erros_producao(erros):
    caminho = caminho_erros_producao()
    dados = list(erros or [])[:ERROS_PRODUCAO_LIMITE]
    caminho_temp = f"{caminho}.tmp"
    with open(caminho_temp, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=2, default=sanitizar_para_json)
    os.replace(caminho_temp, caminho)


def resumir_stack_erro(erro, limite_linhas=18):
    linhas = traceback.format_exception(type(erro), erro, erro.__traceback__)
    stack = "".join(linhas).strip().splitlines()
    return "\n".join(stack[-limite_linhas:])


def montar_registro_erro_producao(erro, descricao=""):
    endpoint = (request.endpoint if has_request_context() else "") or descricao or ""
    path = (request.path if has_request_context() else "") or ""
    usuario = ""
    if has_request_context():
        usuario = normalizar_texto_campo(session.get("usuario") or session.get("usuario_nome"))
    quando = agora_iso()
    mensagem = str(erro)
    origem = "teste" if (
        path.startswith("/rota-com-falha")
        or "falha teste" in mensagem.lower()
        or normalizar_texto_campo(descricao).endswith("_teste")
    ) else "producao"
    return {
        "id": f"{int(time.time() * 1000)}-{secrets.token_hex(3)}",
        "quando": quando,
        "endpoint": endpoint,
        "path": path,
        "usuario": usuario,
        "tipo": erro.__class__.__name__,
        "mensagem": mensagem,
        "stack": resumir_stack_erro(erro),
        "origem": origem,
        "resolvido": False,
        "resolvido_em": "",
        "resolvido_por": "",
        "descricao": normalizar_texto_campo(descricao),
    }


def normalizar_filtro_erros_producao(filtro):
    filtro = normalizar_texto_campo(filtro or "abertos").lower()
    if filtro in {"resolvidos", "todos"}:
        return filtro
    return "abertos"


def listar_erros_producao(apenas_abertos=False, limite=20, filtro=None):
    erros = carregar_erros_producao()
    filtro_normalizado = normalizar_filtro_erros_producao(filtro) if filtro else ""
    if filtro_normalizado == "abertos" or apenas_abertos:
        erros = [item for item in erros if not item.get("resolvido")]
    elif filtro_normalizado == "resolvidos":
        erros = [item for item in erros if item.get("resolvido")]
    return erros[: int(limite or 20)]


def contar_erros_producao_por_status():
    erros = carregar_erros_producao()
    resolvidos = sum(1 for item in erros if item.get("resolvido"))
    abertos = len(erros) - resolvidos
    testes = sum(1 for item in erros if item.get("origem") == "teste")
    reais = len(erros) - testes
    reais_abertos = sum(1 for item in erros if not item.get("resolvido") and item.get("origem") != "teste")
    testes_abertos = sum(1 for item in erros if not item.get("resolvido") and item.get("origem") == "teste")
    return {
        "abertos": abertos,
        "resolvidos": resolvidos,
        "todos": len(erros),
        "teste": testes,
        "reais": reais,
        "reais_abertos": reais_abertos,
        "testes_abertos": testes_abertos,
    }


def salvar_registro_erro_producao(registro):
    with ERROS_PRODUCAO_LOCK:
        erros = carregar_erros_producao()
        erros.insert(0, dict(registro))
        salvar_erros_producao(erros)


def marcar_erro_producao_resolvido(erro_id, usuario=""):
    erro_id = normalizar_texto_campo(erro_id)
    if not erro_id:
        return False
    with ERROS_PRODUCAO_LOCK:
        erros = carregar_erros_producao()
        alterado = False
        for item in erros:
            if normalizar_texto_campo(item.get("id")) == erro_id:
                item["resolvido"] = True
                item["resolvido_em"] = agora_iso()
                item["resolvido_por"] = normalizar_texto_campo(usuario)
                alterado = True
                break
        if alterado:
            salvar_erros_producao(erros)
        return alterado


def marcar_erros_auto_suporte_resolvidos(descricoes, usuario="auto_suporte"):
    descricoes_normalizadas = {
        normalizar_texto_campo(descricao)
        for descricao in (descricoes or [])
        if normalizar_texto_campo(descricao)
    }
    if not descricoes_normalizadas:
        return 0

    with ERROS_PRODUCAO_LOCK:
        erros = carregar_erros_producao()
        resolvidos = 0
        for item in erros:
            if item.get("resolvido"):
                continue
            descricao = normalizar_texto_campo(item.get("descricao"))
            if descricao not in descricoes_normalizadas:
                continue
            item["resolvido"] = True
            item["resolvido_em"] = agora_iso()
            item["resolvido_por"] = normalizar_texto_campo(usuario)
            item["resolucao_auto_suporte"] = "Check voltou a executar sem erro."
            resolvidos += 1
        if resolvidos:
            salvar_erros_producao(erros)
        return resolvidos


def limpar_erros_producao_resolvidos():
    with ERROS_PRODUCAO_LOCK:
        erros = carregar_erros_producao()
        mantidos = [item for item in erros if not item.get("resolvido")]
        removidos = len(erros) - len(mantidos)
        if removidos:
            salvar_erros_producao(mantidos)
        return removidos


def limpar_todos_erros_producao(usuario=""):
    with ERROS_PRODUCAO_LOCK:
        erros = carregar_erros_producao()
        removidos = len(erros)
        if removidos:
            salvar_erros_producao([])
    if removidos:
        registrar_historico_auto_suporte(
            "limpeza_erros",
            "Erros limpos",
            f"{removidos} erro(s) removido(s) da Central de erros.",
            severidade="info",
            detalhes={
                "removidos": removidos,
                "usuario": normalizar_texto_campo(usuario),
            },
        )
    return removidos


def registrar_ultimo_erro_producao(erro, descricao=""):
    registro = montar_registro_erro_producao(erro, descricao=descricao)
    ULTIMO_ERRO_PRODUCAO.update(registro)
    try:
        salvar_registro_erro_producao(registro)
    except Exception as erro_log:
        log_info("ERRO AO GRAVAR CENTRAL DE ERROS:", erro_log)
    log_info("ERRO PRODUCAO:", descricao or ULTIMO_ERRO_PRODUCAO["endpoint"], erro)
    log_info(registro.get("stack") or "".join(traceback.format_exception(type(erro), erro, erro.__traceback__)))
    if descricao == "erro_global":
        texto = montar_alerta_erro_500_telegram(registro)
        chave = f"erro500:{registro.get('path')}:{registro.get('tipo')}:{str(registro.get('mensagem') or '')[:80]}"
        enviar_alerta_estabilidade_assincrono(texto, chave=chave)


def registrar_falha_banco_online_request(erro, descricao="banco_online"):
    agora_ts = time.time()
    caminho = (request.path if has_request_context() else "") or descricao
    mensagem = str(erro or "")[:160]
    chave = f"{caminho}|{erro.__class__.__name__}|{mensagem}"
    if agora_ts - float(BANCO_ONLINE_FALHAS_ROTAS.get(chave) or 0.0) < 60:
        return
    BANCO_ONLINE_FALHAS_ROTAS[chave] = agora_ts
    try:
        registrar_ultimo_erro_producao(erro, descricao=descricao)
    except Exception as erro_log:
        log_info("ERRO AO REGISTRAR FALHA BANCO ONLINE:", erro_log)


def enviar_alerta_estabilidade_assincrono(texto, chave, intervalo=None):
    chave = normalizar_texto_campo(chave)
    if not chave:
        return False
    agora_ts = time.time()
    intervalo = int(intervalo or ESTABILIDADE_ALERTA_INTERVALO)
    if agora_ts - float(ESTABILIDADE_ALERTAS_CACHE.get(chave) or 0.0) < intervalo:
        return False
    ESTABILIDADE_ALERTAS_CACHE[chave] = agora_ts

    def executar():
        try:
            enviar_alerta_telegram_auto_suporte(texto)
        except Exception as erro:
            log_info("ERRO ALERTA ESTABILIDADE TELEGRAM:", erro)

    Thread(target=executar, daemon=True).start()
    return True


def montar_alerta_erro_500_telegram(registro):
    return (
        "Alerta de estabilidade Wagen Estetica\n"
        "Tipo: erro 500\n"
        f"Rota: {registro.get('path') or registro.get('endpoint') or '-'}\n"
        f"Usuario: {registro.get('usuario') or '-'}\n"
        f"Quando: {registro.get('quando') or '-'}\n"
        f"Erro: {registro.get('tipo')}: {registro.get('mensagem')}\n"
        "Acao: abrir Configuracoes > Desenvolvedor > Central de erros."
    )


def avaliar_alerta_estabilidade_resposta(caminho, metrica):
    tempo_ms = int(metrica.get("ultimo_ms") or 0)
    status = int(metrica.get("status") or 0)
    pioras = int(metrica.get("pioras_consecutivas") or 0)
    alertas = []
    if tempo_ms > 2000:
        alertas.append(
            (
                f"rota_lenta:{caminho}",
                "Alerta de estabilidade Wagen Estetica\n"
                "Tipo: rota acima de 2s\n"
                f"Rota: {caminho}\n"
                f"Tempo: {tempo_ms} ms\n"
                f"Media: {metrica.get('media_ms')} ms\n"
                "Acao: abrir Configuracoes > Desenvolvedor > Central Tecnica."
            )
        )
    if pioras >= 3:
        alertas.append(
            (
                f"rota_piorando:{caminho}",
                "Alerta de estabilidade Wagen Estetica\n"
                "Tipo: rota piorou 3 vezes seguidas\n"
                f"Rota: {caminho}\n"
                f"Ultimo tempo: {tempo_ms} ms\n"
                f"Pioras consecutivas: {pioras}\n"
                "Acao: revisar a rota na Central Tecnica."
            )
        )
    if status >= 500:
        alertas.append(
            (
                f"rota_500:{caminho}:{status}",
                "Alerta de estabilidade Wagen Estetica\n"
                "Tipo: resposta 500\n"
                f"Rota: {caminho}\n"
                f"Status: {status}\n"
                f"Tempo: {tempo_ms} ms\n"
                "Acao: verificar Central de erros."
            )
        )
    for chave, texto in alertas:
        enviar_alerta_estabilidade_assincrono(texto, chave=chave)


def request_https_ativo():
    if not has_request_context():
        return False
    proto = normalizar_texto_campo(request.headers.get("X-Forwarded-Proto")).lower()
    return request.is_secure or proto == "https"


def testar_rota_interna_status(nome, caminho, status_esperado=200):
    inicio = time.perf_counter()
    try:
        with app.test_client() as client:
            resposta = client.get(caminho)
        elapsed_ms = int((time.perf_counter() - inicio) * 1000)
        return {
            "nome": nome,
            "ok": int(resposta.status_code) == int(status_esperado),
            "detalhe": f"HTTP {resposta.status_code} em {elapsed_ms} ms",
            "acao": "OK" if int(resposta.status_code) == int(status_esperado) else f"Esperado HTTP {status_esperado}.",
        }
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao=f"predeploy_{nome}")
        return {
            "nome": nome,
            "ok": False,
            "detalhe": str(erro),
            "acao": "Revisar rota ou dependencia antes do deploy.",
        }


def montar_pre_deploy_checklist():
    checklist = montar_checklist_producao()
    banco_status = executar_com_fallback_producao(
        obter_status_banco_online,
        {"conectado": False, "backend": "", "mensagem": "Banco online indisponivel."},
        "predeploy_banco",
    )
    backup_status = executar_com_fallback_producao(
        obter_status_backup_banco,
        status_backup_banco_padrao(),
        "predeploy_backup",
    )

    checklist.extend(
        [
            {
                "nome": "HTTPS ativo",
                "ok": request_https_ativo(),
                "detalhe": "Requisicao atual em HTTPS." if request_https_ativo() else "A requisicao atual nao parece HTTPS.",
                "acao": "Usar dominio HTTPS com proxy configurado.",
            },
            {
                "nome": "Banco online em producao",
                "ok": banco_status.get("backend") == "postgres" and bool(banco_status.get("conectado")),
                "detalhe": banco_status.get("mensagem") or "Banco nao validado.",
                "acao": "Confirmar DATABASE_BACKEND=postgres e connection string online.",
            },
            {
                "nome": "Backup com arquivo recente",
                "ok": bool(backup_status.get("ultimo_backup")),
                "detalhe": backup_status.get("ultimo_backup_em_fmt") or "Nenhum backup localizado.",
                "acao": "Gerar backup antes do deploy.",
            },
            testar_rota_interna_status("Manifest PWA", "/site.webmanifest", 200),
            testar_rota_interna_status("Service worker", "/sw.js", 200),
            testar_rota_interna_status("PWA status", "/api/pwa/status", 200),
        ]
    )
    return checklist


def montar_diagnostico_seguro():
    banco_status = executar_com_fallback_producao(
        obter_status_banco_online,
        {"conectado": False, "configurado": False, "modo_label": "Banco indisponivel", "mensagem": "Nao foi possivel validar o banco agora."},
        "diagnostico_banco",
    )
    backup_status = executar_com_fallback_producao(obter_status_backup_banco, status_backup_banco_padrao(), "diagnostico_backup")
    arquivos_status = executar_com_fallback_producao(obter_status_arquivos, status_arquivos_padrao(), "diagnostico_arquivos")
    licenca = executar_com_fallback_producao(carregar_contexto_licenca_empresa_seguro, {}, "diagnostico_licenca")
    return {
        "banco_status": banco_status,
        "banco_online_tabelas": executar_com_fallback_producao(
            lambda: listar_tabelas_banco_online(banco_status),
            {"quantidade": 0, "mensagem": "Tabelas nao carregadas."},
            "diagnostico_tabelas",
        ),
        "backup_status": backup_status,
        "arquivos_status": arquivos_status,
        "licenca": licenca,
        "google_drive_pronto": executar_com_fallback_producao(google_drive_pronto_para_backup, False, "diagnostico_drive_pronto"),
        "google_drive_disponivel": executar_com_fallback_producao(google_drive_disponivel_para_backup, False, "diagnostico_drive_disponivel"),
        "csrf_ativo": csrf_protection_ativa(),
        "session_cookie_secure": SESSION_COOKIE_SECURE_RAW in {"1", "true", "yes", "on"},
        "modo_banco": modo_banco_preferido(),
        "versao": obter_versao_sistema(),
        "storage_provider": carregar_contexto_produto().get("storage_provider"),
        "checklist": montar_checklist_producao(),
        "pre_deploy": montar_pre_deploy_checklist(),
        "ultima_validacao": session.get("ultima_validacao_diagnostico") if has_request_context() else "",
        "ultimo_erro": dict(ULTIMO_ERRO_PRODUCAO),
    }


def montar_status_sistema_dono():
    banco_status = executar_com_fallback_producao(
        obter_status_banco_online,
        {"conectado": False, "mensagem": "Banco indisponivel.", "backend_label": "-"},
        "status_dono_banco",
    )
    backup_status = executar_com_fallback_producao(obter_status_backup_banco, status_backup_banco_padrao(), "status_dono_backup")
    licenca = executar_com_fallback_producao(carregar_contexto_licenca_empresa_seguro, {}, "status_dono_licenca")
    usuarios_ativos = executar_com_fallback_producao(contar_usuarios_ativos_empresa, 0, "status_dono_usuarios")
    configuracao = executar_com_fallback_producao(lambda: obter_configuracao_empresa(force=True), empresa_snapshot_padrao(), "status_dono_config")
    pwa_status = executar_com_fallback_producao(
        lambda: {
            "https": request_https_ativo(),
            "manifest": testar_rota_interna_status("Manifest PWA", "/site.webmanifest", 200).get("ok"),
            "service_worker": testar_rota_interna_status("Service worker", "/sw.js", 200).get("ok"),
        },
        {"https": False, "manifest": False, "service_worker": False},
        "status_dono_pwa",
    )

    itens = [
        {
            "nome": "Banco online",
            "ok": bool(banco_status.get("conectado")),
            "valor": banco_status.get("backend_label") or banco_status.get("modo_label") or "-",
            "detalhe": banco_status.get("mensagem") or "-",
            "acao": "OK" if banco_status.get("conectado") else "Validar DATABASE_URL e migrations do Supabase.",
        },
        {
            "nome": "Backup",
            "ok": bool(backup_status.get("ultimo_backup")),
            "valor": backup_status.get("ultimo_backup_em_fmt") or "Sem backup",
            "detalhe": f"{backup_status.get('tipo_backup_label', '-')} | {backup_status.get('frequencia_label', '-')}",
            "acao": "OK" if backup_status.get("ultimo_backup") else "Gerar backup e confirmar rotina automatica.",
        },
        {
            "nome": "Licenca",
            "ok": not bool(licenca.get("bloqueada")),
            "valor": f"{licenca.get('plano_label', '-') } / {licenca.get('status_label', '-')}",
            "detalhe": licenca.get("validade_em") or "Sem validade definida",
            "acao": "OK" if not bool(licenca.get("bloqueada")) else "Renovar ou desbloquear a licenca.",
        },
        {
            "nome": "Usuarios ativos",
            "ok": True,
            "valor": str(usuarios_ativos),
            "detalhe": "Acessos ativos na empresa atual.",
            "acao": "Revisar acessos inativos periodicamente.",
        },
        {
            "nome": "Bot Telegram",
            "ok": bool(configuracao.get("auto_teste_ativo") and configuracao.get("auto_teste_telegram_bot_token") and configuracao.get("auto_teste_telegram_chat_id")),
            "valor": "Ativo" if configuracao.get("auto_teste_ativo") else "Desativado",
            "detalhe": (
                f"Ultimo teste: {configuracao.get('auto_teste_ultimo_teste_em_fmt')}. Status: {configuracao.get('auto_teste_ultimo_status') or 'nao testado'}."
            ),
            "acao": "OK" if configuracao.get("auto_teste_ativo") and configuracao.get("auto_teste_telegram_chat_id") else "Configurar token/chat e enviar teste.",
        },
        {
            "nome": "PWA instalado",
            "ok": bool(pwa_status.get("https") and pwa_status.get("manifest") and pwa_status.get("service_worker")),
            "valor": "Pronto" if pwa_status.get("https") else "Verificar HTTPS",
            "detalhe": "HTTPS, manifest e service worker validados." if pwa_status.get("https") else "Abra pelo dominio HTTPS.",
            "acao": "OK" if pwa_status.get("https") else "Usar o dominio HTTPS antes de instalar.",
        },
        {
            "nome": "Versao",
            "ok": True,
            "valor": obter_versao_sistema(),
            "detalhe": "Versao atual exibida no sistema.",
            "acao": "Atualizar changelog ao publicar novas entregas.",
        },
        {
            "nome": "Onboarding",
            "ok": bool(int(configuracao.get("onboarding_concluido") or 0)),
            "valor": "Concluido" if int(configuracao.get("onboarding_concluido") or 0) else "Pendente",
            "detalhe": "Empresa, plano, usuarios, servicos, banco, backup e PWA devem estar configurados.",
            "acao": "Criar fluxo guiado para instalacao em cliente novo." if not int(configuracao.get("onboarding_concluido") or 0) else "OK",
        },
    ]

    return {
        "gerado_em": agora_iso(),
        "itens": itens,
        "ultimo_erro": dict(ULTIMO_ERRO_PRODUCAO),
        "resumo": {
            "ok": all(item["ok"] for item in itens),
            "falhas": [item["nome"] for item in itens if not item["ok"]],
        },
    }


def contar_usuarios_ativos_empresa():
    conn = conectar()
    try:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM usuarios WHERE empresa_id=? AND COALESCE(ativo, 1)=1", (empresa_atual_id(),))
        return int(c.fetchone()[0] or 0)
    finally:
        conn.close()


def scanner_rotas_central_tecnica():
    resultados = []
    with app.test_client() as client:
        for nome, caminho in ROTAS_CENTRAL_TECNICA:
            inicio = time.perf_counter()
            try:
                resposta = client.get(caminho)
                tempo_ms = int((time.perf_counter() - inicio) * 1000)
                tamanho = len(resposta.get_data() or b"")
                status = int(resposta.status_code)
                destino = resposta.headers.get("Location") or ""
                resultados.append({
                    "nome": nome,
                    "caminho": caminho,
                    "status": status,
                    "ok": status < 500,
                    "tempo_ms": tempo_ms,
                    "tempo_label": rotulo_latencia_ms(tempo_ms),
                    "tempo_classe": classificar_latencia_ms(tempo_ms),
                    "alerta_2s": tempo_ms > 2000,
                    "tamanho_kb": round(tamanho / 1024, 1),
                    "destino": destino,
                    "mensagem": f"HTTP {status}" + (f" -> {destino}" if destino else ""),
                    "causa_provavel": causa_provavel_lentidao_rota(
                        caminho,
                        {"status": status, "tempo_ms": tempo_ms},
                    ),
                })
            except Exception as erro:
                tempo_ms = int((time.perf_counter() - inicio) * 1000)
                registrar_ultimo_erro_producao(erro, descricao=f"central_tecnica_rota_{caminho}")
                resultados.append({
                    "nome": nome,
                    "caminho": caminho,
                    "status": "",
                    "ok": False,
                    "tempo_ms": tempo_ms,
                    "tempo_label": "Falha",
                    "tempo_classe": "lento",
                    "alerta_2s": tempo_ms > 2000,
                    "tamanho_kb": 0,
                    "destino": "",
                    "mensagem": str(erro),
                    "causa_provavel": "Excecao durante revalidacao interna; revisar stack registrado.",
                })
    return resultados


def metricas_tempo_resposta_central_tecnica():
    return enriquecer_metricas_tempo_resposta(
        METRICAS_TEMPO_RESPOSTA,
        ROTAS_MONITORADAS_RESPOSTA,
    )


def contar_registros_tabela_central(cursor, tabela):
    cursor.execute(f"SELECT COUNT(*) AS total FROM {tabela}")
    linha = cursor.fetchone()
    if linha is None:
        return 0
    try:
        return int(linha["total"] or 0)
    except Exception:
        return int(linha[0] or 0)


def estado_banco_central_tecnica():
    inicio = time.perf_counter()
    banco_status = executar_com_fallback_producao(
        obter_status_banco_online,
        {"conectado": False, "backend_label": "Indisponivel", "mensagem": "Banco nao validado."},
        "central_tecnica_banco_status",
    )
    tabelas = []
    conn = None
    try:
        conn = conectar()
        cursor = conn.cursor()
        for tabela in TABELAS_CENTRAL_TECNICA:
            try:
                total = contar_registros_tabela_central(cursor, tabela)
                tabelas.append({
                    "nome": tabela,
                    "ok": True,
                    "total": total,
                    "mensagem": f"{total} registro(s)",
                })
            except Exception as erro_tabela:
                tabelas.append({
                    "nome": tabela,
                    "ok": False,
                    "total": "",
                    "mensagem": str(erro_tabela),
                })
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="central_tecnica_banco")
        tabelas.append({
            "nome": "conexao",
            "ok": False,
            "total": "",
            "mensagem": str(erro),
        })
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

    return {
        "backend": banco_status.get("backend_label") or banco_status.get("modo_label") or "-",
        "conectado": bool(banco_status.get("conectado")),
        "mensagem": banco_status.get("mensagem") or "-",
        "tempo_ms": int((time.perf_counter() - inicio) * 1000),
        "tabelas": tabelas,
        "tabelas_ok": sum(1 for item in tabelas if item.get("ok")),
        "tabelas_falha": [item["nome"] for item in tabelas if not item.get("ok")],
    }


def idade_cache_segundos(cache):
    testado = float((cache or {}).get("testado_em") or 0.0)
    if not testado:
        return None
    return max(0, int(time.time() - testado))


def resumo_cache_central(nome, cache, ttl, chave_campo="chave"):
    idade = idade_cache_segundos(cache)
    ativo = idade is not None and idade < int(ttl or 0)
    chave = cache.get(chave_campo)
    if isinstance(chave, set):
        chave = ",".join(sorted(chave))
    return {
        "nome": nome,
        "ativo": ativo,
        "idade": idade,
        "idade_label": f"{idade}s" if idade is not None else "Vazio",
        "ttl": int(ttl or 0),
        "chave": str(chave or ""),
        "tem_resultado": cache.get("resultado") is not None,
    }


def caches_central_tecnica():
    return [
        resumo_cache_central("HUD", HUD_CACHE, HUD_CACHE_TTL, "usuario"),
        resumo_cache_central("Home snapshot", HOME_SNAPSHOT_CACHE, HOME_SNAPSHOT_CACHE_TTL, "usuario"),
        resumo_cache_central("Notificacoes", NOTIFICACOES_CACHE, NOTIFICACOES_CACHE_TTL, "usuario"),
        resumo_cache_central("Status sync", STATUS_SYNC_CACHE, STATUS_SYNC_CACHE_TTL, "usuario"),
        resumo_cache_central("Clientes", CLIENTES_CONTEXT_CACHE, CLIENTES_CONTEXT_CACHE_TTL),
        resumo_cache_central("Painel", PAINEL_CONTEXT_CACHE, PAINEL_CONTEXT_CACHE_TTL),
        resumo_cache_central("Historico", HISTORICO_CONTEXT_CACHE, HISTORICO_CONTEXT_CACHE_TTL),
        resumo_cache_central("Relatorios", RELATORIOS_CONTEXT_CACHE, RELATORIOS_CONTEXT_CACHE_TTL),
        resumo_cache_central("Auditoria", AUDITORIA_CONTEXT_CACHE, AUDITORIA_CONTEXT_CACHE_TTL),
        resumo_cache_central("Banco online", BANCO_ONLINE_STATUS_CACHE, BANCO_ONLINE_STATUS_CACHE_TTL, "backend"),
        resumo_cache_central("Tabelas online", BANCO_ONLINE_TABELAS_CACHE, BANCO_ONLINE_TABELAS_CACHE_TTL, "dsn"),
        resumo_cache_central("Produto/template", TEMPLATE_PRODUTO_CACHE, TEMPLATE_PRODUTO_CACHE_TTL, "empresa_id"),
        resumo_cache_central("Licenca/template", TEMPLATE_LICENCA_CACHE, TEMPLATE_LICENCA_CACHE_TTL, "empresa_id"),
        resumo_cache_central("Configuracao empresa", CONFIG_EMPRESA_CACHE, CONFIG_EMPRESA_CACHE_TTL, "empresa_id"),
        resumo_cache_central("Versao", VERSAO_SISTEMA_CACHE, VERSAO_SISTEMA_CACHE_TTL, "empresa_id"),
        resumo_cache_central("Paginas menu", PAGINAS_MENU_CACHE, PAGINAS_MENU_CACHE_TTL, "empresa_id"),
    ]


def montar_central_tecnica_desenvolvedor(filtro_erros="abertos"):
    filtro_erros = normalizar_filtro_erros_producao(filtro_erros)
    status = medir_consulta_sql("/configuracoes/desenvolvedor", "status_sistema", montar_status_sistema_dono, origem="check")
    banco = medir_consulta_sql("/configuracoes/desenvolvedor", "estado_banco", estado_banco_central_tecnica, origem="check")
    rotas = medir_consulta_sql("/configuracoes/desenvolvedor", "scanner_rotas", scanner_rotas_central_tecnica, origem="check")
    caches = medir_consulta_sql("/configuracoes/desenvolvedor", "caches_memoria", caches_central_tecnica, origem="memoria")
    tempo_resposta = metricas_tempo_resposta_central_tecnica()
    contagem_erros = medir_consulta_sql("/configuracoes/desenvolvedor", "contagem_erros", contar_erros_producao_por_status, origem="arquivo")
    erros_abertos = medir_consulta_sql(
        "/configuracoes/desenvolvedor",
        "erros_abertos",
        lambda: listar_erros_producao(apenas_abertos=True, limite=12),
        origem="arquivo",
    )
    erros_recentes = medir_consulta_sql(
        "/configuracoes/desenvolvedor",
        "erros_recentes",
        lambda: listar_erros_producao(limite=12, filtro=filtro_erros),
        origem="arquivo",
    )
    metricas_sql = obter_metricas_consultas_sql(limite=60)
    falhas_rotas = [item for item in rotas if not item.get("ok")]
    rotas_lentas = [item for item in rotas if item.get("tempo_classe") == "lento"]
    rotas_alerta_2s = [item for item in rotas if item.get("alerta_2s")]
    paginas_lentas = [item for item in tempo_resposta if item.get("classe") == "lento"]
    paginas_alerta_2s = [item for item in tempo_resposta if item.get("alerta_2s")]
    paginas_pioraram = [item for item in tempo_resposta if item.get("tendencia") == "piorou"]
    ranking_rotas_lentas = sorted(
        [
            {
                "tipo": "pagina",
                "rota": item.get("rota"),
                "nome": item.get("rota"),
                "tempo_ms": int(item.get("ultimo_ms") or 0),
                "media_ms": int(item.get("media_ms") or 0),
                "status": item.get("status") or "-",
                "tendencia": item.get("tendencia") or "estavel",
                "tendencia_label": item.get("tendencia_label") or "Estavel",
                "causa_provavel": item.get("causa_provavel") or "",
                "alerta_2s": bool(item.get("alerta_2s")),
            }
            for item in tempo_resposta
            if int(item.get("ultimo_ms") or 0) > 0
        ] + [
            {
                "tipo": "scanner",
                "rota": item.get("caminho"),
                "nome": item.get("nome"),
                "tempo_ms": int(item.get("tempo_ms") or 0),
                "media_ms": 0,
                "status": item.get("status") or "erro",
                "tendencia": "estavel",
                "tendencia_label": "Medicao atual",
                "causa_provavel": item.get("causa_provavel") or "",
                "alerta_2s": bool(item.get("alerta_2s")),
            }
            for item in rotas
        ],
        key=lambda item: int(item.get("tempo_ms") or 0),
        reverse=True,
    )[:10]
    return {
        "gerado_em": agora_iso(),
        "saude": status,
        "banco": banco,
        "rotas": rotas,
        "caches": caches,
        "tempo_resposta": tempo_resposta,
        "metricas_sql": metricas_sql,
        "ranking_rotas_lentas": ranking_rotas_lentas,
        "erros_abertos": erros_abertos,
        "erros_recentes": erros_recentes,
        "erros_filtro": filtro_erros,
        "erros_contagem": contagem_erros,
        "rotas_alerta_2s": rotas_alerta_2s,
        "paginas_alerta_2s": paginas_alerta_2s,
        "paginas_pioraram": paginas_pioraram,
        "resumo": {
            "ok": bool(status.get("resumo", {}).get("ok")) and not falhas_rotas and not banco.get("tabelas_falha") and not erros_abertos,
            "rotas_testadas": len(rotas),
            "rotas_falha": len(falhas_rotas),
            "rotas_lentas": len(rotas_lentas),
            "rotas_alerta_2s": len(rotas_alerta_2s),
            "paginas_lentas": len(paginas_lentas),
            "paginas_alerta_2s": len(paginas_alerta_2s),
            "paginas_pioraram": len(paginas_pioraram),
            "erros_abertos": len(erros_abertos),
            "erros_resolvidos": contagem_erros.get("resolvidos", 0),
            "erros_total": contagem_erros.get("todos", 0),
            "caches_ativos": sum(1 for item in caches if item.get("ativo")),
            "tabelas_ok": banco.get("tabelas_ok", 0),
        },
    }


ACOES_AUTO_SUPORTE = {
    "limpar_caches": "Limpar caches",
    "limpar_cache_rota_lenta": "Limpar cache da pagina lenta",
    "revalidar_rota_lenta": "Revalidar rota lenta",
    "validar_ambiente": "Reiniciar validacao",
    "testar_banco": "Testar banco",
    "testar_backup": "Testar backup",
    "testar_telegram": "Testar Telegram",
    "revalidar_pwa": "Revalidar PWA",
    "revalidar_estaticos": "Revalidar arquivos estaticos",
    "resolver_erros_com_checks_ok": "Resolver erros com checks OK",
    "gerar_backup_suporte": "Gerar backup de suporte",
    "desativar_planilhas_com_erro": "Pausar planilhas com erro",
    "corrigir_classificacao_clientes": "Corrigir classificacao novo/retorno",
    "limpar_erros_resolvidos": "Limpar erros resolvidos",
    "limpar_todos_erros": "Limpar todos os erros",
    "gerar_pacote_codex": "Gerar pacote Codex",
    "enviar_relatorio_telegram": "Enviar relatorio Telegram",
    "registrar_incidente": "Registrar incidente",
    "enviar_alerta_telegram": "Enviar alerta Telegram",
    "marcar_fluxo_suspeito": "Marcar fluxo suspeito",
}

AUTO_SUPORTE_ACOES_ADMIN = {
    "limpar_caches",
    "limpar_cache_rota_lenta",
    "revalidar_rota_lenta",
    "validar_ambiente",
    "testar_banco",
    "testar_backup",
    "revalidar_pwa",
    "revalidar_estaticos",
    "resolver_erros_com_checks_ok",
    "gerar_backup_suporte",
    "gerar_pacote_codex",
}

AUTO_SUPORTE_ACOES_AUTONOMAS = {
    "limpar_caches",
    "limpar_cache_rota_lenta",
    "revalidar_rota_lenta",
    "validar_ambiente",
    "testar_banco",
    "testar_backup",
    "revalidar_pwa",
    "revalidar_estaticos",
    "resolver_erros_com_checks_ok",
    "limpar_erros_resolvidos",
    "gerar_pacote_codex",
}
AUTO_SUPORTE_ACOES_EXIGEM_CONFIRMACAO = {
    "desativar_planilhas_com_erro": {
        "confirmacao": "PAUSAR PLANILHAS",
        "risco": "Pode interromper sincronizacoes de clientes ate alguem reativar.",
    },
    "corrigir_classificacao_clientes": {
        "confirmacao": "CORRIGIR CLASSIFICACAO",
        "risco": "Altera atendimentos marcados como NOVO para RETORNO quando ha historico.",
    },
    "limpar_erros_resolvidos": {
        "confirmacao": "LIMPAR ERROS RESOLVIDOS",
        "risco": "Remove evidencias ja resolvidas da Central de erros.",
    },
    "limpar_todos_erros": {
        "confirmacao": "LIMPAR TODOS OS ERROS",
        "risco": "Remove todas as evidencias da Central de erros, inclusive abertas.",
    },
    "enviar_relatorio_telegram": {
        "confirmacao": "ENVIAR RELATORIO",
        "risco": "Envia resumo tecnico para um destino externo no Telegram.",
    },
    "enviar_alerta_telegram": {
        "confirmacao": "ENVIAR ALERTA",
        "risco": "Envia mensagem externa para o Telegram configurado.",
    },
}
AUTO_SUPORTE_AUTONOMIA_COOLDOWN_SEGUNDOS = 15 * 60
AUTO_SUPORTE_AUTONOMIA_LIMITE_ACOES = 3
AUTO_SUPORTE_MODO_AUTONOMIA_PADRAO = "seguro"
AUTO_SUPORTE_MODOS_AUTONOMIA = {
    "manual": {
        "label": "Manual",
        "descricao": "Nada roda sozinho. O bot apenas registra o que encontrou.",
        "executa": False,
        "simula": True,
    },
    "observador": {
        "label": "Observador",
        "descricao": "Diagnostica, sugere e simula sem executar reparos.",
        "executa": False,
        "simula": True,
    },
    "seguro": {
        "label": "Seguro",
        "descricao": "Executa somente reparos reversiveis e de baixo risco.",
        "executa": True,
        "simula": True,
    },
    "assistido": {
        "label": "Assistido",
        "descricao": "Executa reparos seguros e prepara acoes sensiveis para confirmacao.",
        "executa": True,
        "simula": True,
    },
}


def usuario_pode_usar_auto_suporte():
    return bool(session.get("usuario") and perfil_auto_suporte())


def validar_permissao_acao_auto_suporte(acao):
    if not has_request_context():
        return True
    if usuario_auto_suporte_tecnico():
        return True
    if perfil_auto_suporte() == "administrador" and acao in AUTO_SUPORTE_ACOES_ADMIN:
        return True
    raise ValueError("Acao disponivel somente para o desenvolvedor.")


def caminho_auto_suporte_json(caminho_relativo):
    caminho = os.path.abspath(caminho_relativo)
    os.makedirs(os.path.dirname(caminho), exist_ok=True)
    return caminho


def carregar_json_auto_suporte(caminho_relativo, padrao):
    caminho = caminho_auto_suporte_json(caminho_relativo)
    if not os.path.isfile(caminho):
        return deepcopy(padrao)
    try:
        with open(caminho, "r", encoding="utf-8") as arquivo:
            dados = json.load(arquivo)
        return dados if isinstance(dados, type(padrao)) else deepcopy(padrao)
    except Exception:
        return deepcopy(padrao)


def salvar_json_auto_suporte(caminho_relativo, dados):
    caminho = caminho_auto_suporte_json(caminho_relativo)
    caminho_temp = f"{caminho}.tmp"
    with open(caminho_temp, "w", encoding="utf-8") as arquivo:
        json.dump(dados, arquivo, ensure_ascii=False, indent=2, default=sanitizar_para_json)
    os.replace(caminho_temp, caminho)


def listar_historico_auto_suporte(limite=12):
    historico = carregar_json_auto_suporte(AUTO_SUPORTE_HISTORICO_ARQUIVO, [])
    return [item for item in historico if isinstance(item, dict)][: int(limite or 12)]


def registrar_historico_auto_suporte(evento, titulo, mensagem, severidade="info", detalhes=None):
    registro = {
        "id": f"{int(time.time() * 1000)}-{secrets.token_hex(3)}",
        "quando": agora_iso(),
        "evento": normalizar_texto_campo(evento),
        "titulo": normalizar_texto_campo(titulo),
        "mensagem": normalizar_texto_campo(mensagem),
        "severidade": normalizar_texto_campo(severidade) or "info",
        "usuario": normalizar_texto_campo(session.get("usuario")) if has_request_context() else "",
        "detalhes": dict(detalhes or {}),
    }
    with AUTO_SUPORTE_LOCK:
        historico = carregar_json_auto_suporte(AUTO_SUPORTE_HISTORICO_ARQUIVO, [])
        historico.insert(0, registro)
        salvar_json_auto_suporte(AUTO_SUPORTE_HISTORICO_ARQUIVO, historico[:AUTO_SUPORTE_HISTORICO_LIMITE])
    return registro


def carregar_estado_auto_suporte():
    estado = carregar_json_auto_suporte(AUTO_SUPORTE_ESTADO_ARQUIVO, {})
    return estado if isinstance(estado, dict) else {}


def salvar_estado_auto_suporte(estado):
    with AUTO_SUPORTE_LOCK:
        salvar_json_auto_suporte(AUTO_SUPORTE_ESTADO_ARQUIVO, dict(estado or {}))


def normalizar_modo_autonomia_auto_suporte(modo):
    modo = normalizar_texto_campo(modo).lower()
    return modo if modo in AUTO_SUPORTE_MODOS_AUTONOMIA else AUTO_SUPORTE_MODO_AUTONOMIA_PADRAO


def obter_modo_autonomia_auto_suporte(estado=None):
    estado = estado or carregar_estado_auto_suporte()
    autonomia = estado.get("autonomia") if isinstance(estado, dict) else {}
    autonomia = autonomia if isinstance(autonomia, dict) else {}
    return normalizar_modo_autonomia_auto_suporte(autonomia.get("modo") or AUTO_SUPORTE_MODO_AUTONOMIA_PADRAO)


def salvar_modo_autonomia_auto_suporte(modo):
    estado = carregar_estado_auto_suporte()
    autonomia = estado.get("autonomia") if isinstance(estado.get("autonomia"), dict) else {}
    autonomia["modo"] = normalizar_modo_autonomia_auto_suporte(modo)
    autonomia["modo_atualizado_em"] = agora_iso()
    autonomia["modo_atualizado_por"] = normalizar_texto_campo(session.get("usuario")) if has_request_context() else ""
    estado["autonomia"] = autonomia
    salvar_estado_auto_suporte(estado)
    return autonomia["modo"]


def registrar_incidente_auto_suporte(titulo, mensagem, detalhes=None, severidade="warning"):
    detalhes = dict(detalhes or {})
    detalhes.update(
        {
            "titulo": normalizar_texto_campo(titulo),
            "mensagem": normalizar_texto_campo(mensagem),
            "severidade": normalizar_texto_campo(severidade) or "warning",
        }
    )
    try:
        registrar_auditoria(
            "auto_suporte_incidente",
            "auto_suporte",
            detalhes=detalhes,
        )
    except Exception as erro:
        log_info("ERRO AUDITORIA AUTO SUPORTE:", erro)

    try:
        salvar_notificacao(
            f"AutoSuporte: {titulo} - {mensagem}",
            "erro" if severidade == "error" else "info",
            categoria="auto_suporte",
            referencia="incidente",
        )
    except Exception as erro:
        log_info("ERRO NOTIFICACAO AUTO SUPORTE:", erro)

    registrar_evento_telemetria_app(
        "auto_suporte_incidente",
        categoria="auto_suporte",
        severidade=severidade,
        payload=detalhes,
    )
    registrar_historico_auto_suporte(
        "incidente",
        titulo,
        mensagem,
        severidade=severidade,
        detalhes=detalhes,
    )


def detectar_fluxos_suspeitos_auto_suporte():
    conn = conectar()
    try:
        c = conn.cursor()
        agregador_ids = (
            "STRING_AGG(servicos.id::text, ',')"
            if getattr(c, "backend", "") == "postgres"
            else "GROUP_CONCAT(servicos.id)"
        )
        c.execute(
            f"""
            SELECT
                veiculos.placa,
                COUNT(*) AS total,
                {agregador_ids} AS servicos_ids
            FROM servicos
            LEFT JOIN veiculos
              ON servicos.veiculo_id = veiculos.id
             AND veiculos.empresa_id = ?
            WHERE servicos.empresa_id = ?
              AND COALESCE(TRIM(UPPER(servicos.status)), '')='EM ANDAMENTO'
            GROUP BY servicos.veiculo_id, veiculos.placa
            HAVING COUNT(*) > 1
            ORDER BY total DESC
            """,
            (empresa_atual_id(), empresa_atual_id()),
        )
        return [dict(row) for row in c.fetchall()]
    finally:
        conn.close()


def desativar_planilhas_com_erro_auto_suporte():
    conn = conectar()
    try:
        c = conn.cursor()
        agora_atual = agora_iso()
        padrao_erro = "%ERRO%"
        padrao_falha = "%FALHA%"
        c.execute(
            """
            UPDATE sincronizacoes_clientes
            SET ativo=0,
                ultimo_status='PAUSADA_AUTO_SUPORTE',
                ultima_mensagem='Pausada temporariamente pelo AutoSuporte por status de erro.',
                proximo_sync_em=NULL,
                atualizado_em=?
            WHERE empresa_id=?
              AND ativo=1
              AND COALESCE(excluido_em, '')=''
              AND (
                    UPPER(COALESCE(ultimo_status, '')) LIKE ?
                 OR UPPER(COALESCE(ultimo_status, '')) LIKE ?
                 OR UPPER(COALESCE(ultima_mensagem, '')) LIKE ?
                 OR UPPER(COALESCE(ultima_mensagem, '')) LIKE ?
              )
            """,
            (agora_atual, empresa_atual_id(), padrao_erro, padrao_falha, padrao_erro, padrao_falha),
        )
        total = int(c.rowcount or 0)
        conn.commit()
        return total
    finally:
        conn.close()


def listar_planilhas_com_erro_auto_suporte(limite=6):
    conn = conectar()
    try:
        c = conn.cursor()
        padrao_erro = "%ERRO%"
        padrao_falha = "%FALHA%"
        c.execute(
            """
            SELECT id, nome, ultimo_status, ultima_mensagem
            FROM sincronizacoes_clientes
            WHERE empresa_id=?
              AND ativo=1
              AND COALESCE(excluido_em, '')=''
              AND (
                    UPPER(COALESCE(ultimo_status, '')) LIKE ?
                 OR UPPER(COALESCE(ultimo_status, '')) LIKE ?
                 OR UPPER(COALESCE(ultima_mensagem, '')) LIKE ?
                 OR UPPER(COALESCE(ultima_mensagem, '')) LIKE ?
              )
            ORDER BY atualizado_em DESC
            LIMIT ?
            """,
            (empresa_atual_id(), padrao_erro, padrao_falha, padrao_erro, padrao_falha, int(limite or 6)),
        )
        return [dict(row) for row in c.fetchall()]
    finally:
        conn.close()


def contar_query_auto_suporte(cursor, sql, params=()):
    cursor.execute(sql, params)
    row = cursor.fetchone()
    if not row:
        return 0
    try:
        return int(row[0] or 0)
    except Exception:
        return 0


def detectar_inconsistencias_negocio_auto_suporte():
    empresa_id = empresa_atual_id()
    limite_sync = (agora() - timedelta(hours=1)).isoformat(timespec="seconds")
    checks = [
        {
            "id": "servico_sem_veiculo",
            "titulo": "Servico sem veiculo",
            "mensagem": "Atendimentos sem veiculo vinculado podem quebrar painel, fotos e historico.",
            "acao": "gerar_pacote_codex",
            "sql": """
                SELECT COUNT(*)
                FROM servicos
                LEFT JOIN veiculos ON veiculos.id=servicos.veiculo_id AND veiculos.empresa_id=servicos.empresa_id
                WHERE servicos.empresa_id=? AND servicos.veiculo_id IS NOT NULL AND veiculos.id IS NULL
            """,
            "params": (empresa_id,),
        },
        {
            "id": "veiculo_sem_cliente",
            "titulo": "Veiculo sem cliente",
            "mensagem": "Veiculos sem cliente dificultam identificar novo cadastro ou retorno.",
            "acao": "gerar_pacote_codex",
            "sql": """
                SELECT COUNT(*)
                FROM veiculos
                LEFT JOIN clientes ON clientes.id=veiculos.cliente_id AND clientes.empresa_id=veiculos.empresa_id
                WHERE veiculos.empresa_id=? AND veiculos.cliente_id IS NOT NULL AND clientes.id IS NULL
            """,
            "params": (empresa_id,),
        },
        {
            "id": "novo_com_historico",
            "titulo": "Cliente novo contado com historico",
            "mensagem": "Atendimento marcado como NOVO mesmo com servico anterior para o mesmo veiculo.",
            "acao": "corrigir_classificacao_clientes",
            "sql": """
                SELECT COUNT(*)
                FROM servicos atual
                WHERE atual.empresa_id=?
                  AND UPPER(COALESCE(atual.perfil_cliente_atendimento, ''))='NOVO'
                  AND atual.veiculo_id IS NOT NULL
                  AND EXISTS (
                      SELECT 1 FROM servicos anterior
                      WHERE anterior.empresa_id=atual.empresa_id
                        AND anterior.veiculo_id=atual.veiculo_id
                        AND anterior.id<>atual.id
                        AND COALESCE(anterior.entrada, '') < COALESCE(atual.entrada, '')
                  )
            """,
            "params": (empresa_id,),
        },
        {
            "id": "orcamento_aprovado_sem_servico",
            "titulo": "Orcamento aprovado sem atendimento",
            "mensagem": "Orcamentos aprovados sem atendimento para a mesma placa podem indicar fluxo comercial incompleto.",
            "acao": "gerar_pacote_codex",
            "sql": """
                SELECT COUNT(*)
                FROM orcamentos
                WHERE empresa_id=?
                  AND UPPER(COALESCE(status, '')) IN ('APROVADO', 'APROVADA')
                  AND COALESCE(TRIM(placa), '')<>''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM servicos
                      JOIN veiculos ON veiculos.id=servicos.veiculo_id AND veiculos.empresa_id=servicos.empresa_id
                      WHERE servicos.empresa_id=orcamentos.empresa_id
                        AND UPPER(TRIM(veiculos.placa))=UPPER(TRIM(orcamentos.placa))
                  )
            """,
            "params": (empresa_id,),
        },
        {
            "id": "nota_sem_atendimento",
            "titulo": "Nota fiscal sem atendimento",
            "mensagem": "Notas com placa sem atendimento vinculado devem ser revisadas antes do suporte fiscal.",
            "acao": "gerar_pacote_codex",
            "sql": """
                SELECT COUNT(*)
                FROM notas_fiscais
                WHERE empresa_id=?
                  AND COALESCE(TRIM(placa), '')<>''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM servicos
                      JOIN veiculos ON veiculos.id=servicos.veiculo_id AND veiculos.empresa_id=servicos.empresa_id
                      WHERE servicos.empresa_id=notas_fiscais.empresa_id
                        AND UPPER(TRIM(veiculos.placa))=UPPER(TRIM(notas_fiscais.placa))
                  )
            """,
            "params": (empresa_id,),
        },
        {
            "id": "planilha_sincronizando_ha_muito_tempo",
            "titulo": "Planilha sincronizando ha muito tempo",
            "mensagem": "Sincronizacao presa por mais de 1 hora pode deixar clientes desatualizados.",
            "acao": "desativar_planilhas_com_erro",
            "sql": """
                SELECT COUNT(*)
                FROM sincronizacoes_clientes
                WHERE empresa_id=?
                  AND ativo=1
                  AND COALESCE(excluido_em, '')=''
                  AND UPPER(COALESCE(ultimo_status, '')) IN ('SINCRONIZANDO', 'EM_ANDAMENTO', 'EM ANDAMENTO')
                  AND COALESCE(atualizado_em, criado_em, '') < ?
            """,
            "params": (empresa_id, limite_sync),
        },
    ]

    conn = conectar()
    try:
        c = conn.cursor()
        inconsistencias = []
        for check in checks:
            try:
                total = contar_query_auto_suporte(c, check["sql"], check["params"])
            except Exception as erro:
                registrar_ultimo_erro_producao(erro, descricao=f"auto_suporte_negocio_{check['id']}")
                total = 0
            if total > 0:
                inconsistencias.append({
                    "id": check["id"],
                    "titulo": check["titulo"],
                    "mensagem": check["mensagem"],
                    "acao": check["acao"],
                    "total": total,
                    "severidade": "alerta",
                })
        return inconsistencias
    finally:
        conn.close()


def corrigir_classificacao_clientes_auto_suporte():
    conn = conectar()
    try:
        c = conn.cursor()
        c.execute(
            """
            UPDATE servicos
            SET perfil_cliente_atendimento='RETORNO'
            WHERE empresa_id=?
              AND UPPER(COALESCE(perfil_cliente_atendimento, ''))='NOVO'
              AND veiculo_id IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM servicos anterior
                  WHERE anterior.empresa_id=servicos.empresa_id
                    AND anterior.veiculo_id=servicos.veiculo_id
                    AND anterior.id<>servicos.id
                    AND COALESCE(anterior.entrada, '') < COALESCE(servicos.entrada, '')
              )
            """,
            (empresa_atual_id(),),
        )
        corrigidos = int(c.rowcount or 0)
        conn.commit()
        limpar_caches_operacionais_leves()
        limpar_cache_clientes()
        return {
            "novos_corrigidos_para_retorno": corrigidos,
        }
    finally:
        conn.close()


def montar_item_diagnostico_auto_suporte(nivel, titulo, mensagem, acao="", detalhes=None):
    nivel = normalizar_texto_campo(nivel) or "info"
    ordem = {"critico": 4, "alerta": 3, "atencao": 2, "info": 1}
    labels = {
        "critico": "Erro critico",
        "alerta": "Alerta",
        "atencao": "Atencao",
        "info": "Informativo",
    }
    return {
        "nivel": nivel,
        "peso": ordem.get(nivel, 1),
        "label": labels.get(nivel, "Informativo"),
        "titulo": normalizar_texto_campo(titulo),
        "mensagem": normalizar_texto_campo(mensagem),
        "acao": normalizar_texto_campo(acao),
        "detalhes": dict(detalhes or {}),
    }


def montar_diagnostico_auto_suporte(status_sistema, fluxos, planilhas_erro, tempo_resposta, erros_abertos, inconsistencias_negocio=None):
    itens = []
    inconsistencias_negocio = list(inconsistencias_negocio or [])
    itens_status = status_sistema.get("itens") or []
    banco_item = next((item for item in itens_status if item.get("nome") == "Banco online"), {})
    backup_item = next((item for item in itens_status if item.get("nome") == "Backup"), {})
    telegram_item = next((item for item in itens_status if item.get("nome") == "Bot Telegram"), {})
    licenca_item = next((item for item in itens_status if item.get("nome") == "Licenca"), {})
    onboarding_item = next((item for item in itens_status if item.get("nome") == "Onboarding"), {})

    if not banco_item.get("ok"):
        itens.append(montar_item_diagnostico_auto_suporte(
            "critico",
            "Banco online indisponivel",
            banco_item.get("detalhe") or "A conexao online nao respondeu.",
            "testar_banco",
        ))
    if erros_abertos:
        itens.append(montar_item_diagnostico_auto_suporte(
            "critico",
            "Erro 500 aberto",
            f"{len(erros_abertos)} erro(s) aberto(s) aguardando revisao tecnica.",
            "gerar_pacote_codex",
        ))

    rotas_500 = [item for item in tempo_resposta if int(item.get("status") or 0) >= 500]
    if rotas_500:
        rota = rotas_500[0]
        itens.append(montar_item_diagnostico_auto_suporte(
            "critico",
            "Rota retornando 500",
            f"{rota.get('rota')} retornou HTTP {rota.get('status')}. Gere o pacote para correcao.",
            "gerar_pacote_codex",
        ))

    rotas_lentas = [item for item in tempo_resposta if item.get("classe") == "lento" or item.get("alerta_2s")]
    if rotas_lentas:
        rota = sorted(rotas_lentas, key=lambda item: int(item.get("ultimo_ms") or 0), reverse=True)[0]
        itens.append(montar_item_diagnostico_auto_suporte(
            "alerta",
            "Pagina lenta",
            f"{rota.get('rota')} respondeu em {rota.get('ultimo_ms')} ms. Recomendo limpar caches e revisar a Central Tecnica.",
            "limpar_caches",
        ))
    if not backup_item.get("ok"):
        itens.append(montar_item_diagnostico_auto_suporte(
            "alerta",
            "Backup precisa de atencao",
            backup_item.get("detalhe") or "O backup nao esta em estado ideal.",
            "testar_backup",
        ))
    if not telegram_item.get("ok"):
        itens.append(montar_item_diagnostico_auto_suporte(
            "alerta",
            "Telegram sem validacao completa",
            telegram_item.get("detalhe") or "Configure token, chat ID e envie um teste.",
            "testar_telegram",
        ))
    if planilhas_erro:
        itens.append(montar_item_diagnostico_auto_suporte(
            "alerta",
            "Planilha com erro",
            f"{len(planilhas_erro)} planilha(s) ativa(s) com erro podem travar sincronizacoes.",
            "desativar_planilhas_com_erro",
        ))
    if fluxos:
        itens.append(montar_item_diagnostico_auto_suporte(
            "alerta",
            "Atendimento duplicado suspeito",
            f"{len(fluxos)} placa(s) aparecem duplicadas em andamento.",
            "marcar_fluxo_suspeito",
        ))
    if inconsistencias_negocio:
        total = sum(int(item.get("total") or 0) for item in inconsistencias_negocio)
        itens.append(montar_item_diagnostico_auto_suporte(
            "alerta",
            "Inconsistencia de negocio",
            f"{total} ponto(s) de regra de negocio precisam de revisao.",
            inconsistencias_negocio[0].get("acao") or "gerar_pacote_codex",
            {"inconsistencias": inconsistencias_negocio[:6]},
        ))
    if licenca_item and not licenca_item.get("ok"):
        itens.append(montar_item_diagnostico_auto_suporte(
            "atencao",
            "Licenca exige revisao",
            licenca_item.get("detalhe") or "Revise plano, validade e status da licenca.",
            "",
        ))
    if onboarding_item and not onboarding_item.get("ok"):
        itens.append(montar_item_diagnostico_auto_suporte(
            "atencao",
            "Onboarding pendente",
            onboarding_item.get("detalhe") or "Concluir configuracao guiada do cliente.",
            "registrar_incidente",
        ))

    if not itens:
        itens.append(montar_item_diagnostico_auto_suporte(
            "info",
            "Tudo operacional",
            "Nenhum incidente critico no momento. Ultima validacao concluida sem bloqueios.",
            "",
        ))

    itens = sorted(itens, key=lambda item: item.get("peso", 0), reverse=True)
    principal = itens[0]
    return {
        "nivel": principal.get("nivel"),
        "label": principal.get("label"),
        "titulo": principal.get("titulo"),
        "frase": principal.get("mensagem"),
        "auto_abrir": principal.get("nivel") == "critico",
        "itens": itens[:10],
    }


def montar_sugestoes_auto_suporte(status_sistema, fluxos, planilhas_erro, tempo_resposta, erros_abertos, inconsistencias_negocio=None):
    sugestoes = []
    inconsistencias_negocio = list(inconsistencias_negocio or [])
    banco_item = next((item for item in status_sistema.get("itens", []) if item.get("nome") == "Banco online"), {})
    backup_item = next((item for item in status_sistema.get("itens", []) if item.get("nome") == "Backup"), {})
    pwa_item = next((item for item in status_sistema.get("itens", []) if item.get("nome") == "PWA instalado"), {})
    telegram_config = obter_configuracao_empresa(force=False)
    token_ok = bool(normalizar_texto_campo(telegram_config.get("auto_teste_telegram_bot_token")))
    chat_ok = bool(normalizar_texto_campo(telegram_config.get("auto_teste_telegram_chat_id")))

    if not banco_item.get("ok"):
        sugestoes.append({"titulo": "Banco indisponivel", "mensagem": "Validar a conexao do banco online agora.", "acao": "testar_banco"})
    if any(item.get("classe") == "lento" for item in tempo_resposta):
        lento = next(item for item in tempo_resposta if item.get("classe") == "lento")
        sugestoes.append({
            "titulo": "Pagina demorou",
            "mensagem": f"{lento.get('rota')} carregou em {lento.get('ultimo_ms')} ms. {lento.get('causa_provavel') or ''}".strip(),
            "acao": "limpar_cache_rota_lenta",
        })
        sugestoes.append({
            "titulo": "Revalidar rota",
            "mensagem": "Mede novamente a rota lenta e confirma se voltou ao normal.",
            "acao": "revalidar_rota_lenta",
        })
    if not backup_item.get("ok"):
        sugestoes.append({"titulo": "Backup falhou", "mensagem": "Gerar um backup de suporte antes de investigar.", "acao": "gerar_backup_suporte"})
    if pwa_item and not pwa_item.get("ok"):
        sugestoes.append({"titulo": "PWA precisa de revalidacao", "mensagem": pwa_item.get("detalhe") or "Manifest ou service worker precisam ser revisados.", "acao": "revalidar_pwa"})
    if any(item.get("status") and int(item.get("status") or 0) >= 500 for item in tempo_resposta):
        sugestoes.append({"titulo": "Rota falhando", "mensagem": "Revalidar arquivos estaticos e gerar pacote tecnico.", "acao": "revalidar_estaticos"})
    if not (token_ok and chat_ok):
        sugestoes.append({"titulo": "Telegram nao configurado", "mensagem": "Configure token e chat ID no Auto teste para receber alertas.", "acao": "registrar_incidente"})
    if planilhas_erro:
        sugestoes.append({"titulo": "Planilha com erro", "mensagem": f"{len(planilhas_erro)} planilha(s) ativa(s) com erro.", "acao": "desativar_planilhas_com_erro"})
    if fluxos:
        sugestoes.append({"titulo": "Atendimento duplicado suspeito", "mensagem": f"{len(fluxos)} placa(s) com duplicidade em andamento.", "acao": "marcar_fluxo_suspeito"})
    if erros_abertos:
        sugestoes.append({"titulo": "Erro 500 aberto", "mensagem": f"{len(erros_abertos)} erro(s) aguardando revisao. Gere o pacote e me envie no Codex.", "acao": "gerar_pacote_codex"})
    for item in inconsistencias_negocio[:3]:
        sugestoes.append({
            "titulo": item.get("titulo") or "Inconsistencia de negocio",
            "mensagem": f"{item.get('total')} ocorrencia(s). {item.get('mensagem')}",
            "acao": item.get("acao") or "gerar_pacote_codex",
        })
    return sugestoes[:8]


def montar_acoes_bloqueadas_auto_suporte(status_payload):
    bloqueadas = []
    vistos = set()
    status_payload = status_payload or {}
    origem = list(status_payload.get("sugestoes") or [])
    origem.extend((status_payload.get("diagnostico") or {}).get("itens") or [])

    for item in origem:
        acao = normalizar_texto_campo(item.get("acao"))
        if not acao or acao in vistos or acao in AUTO_SUPORTE_ACOES_AUTONOMAS:
            continue
        if acao not in ACOES_AUTO_SUPORTE:
            continue
        bloqueadas.append({
            "acao": acao,
            "label": ACOES_AUTO_SUPORTE.get(acao, acao),
            "motivo": normalizar_texto_campo(item.get("titulo") or item.get("mensagem") or "Exige confirmacao manual."),
            "seguranca": f"Requer confirmacao manual. {risco_acao_auto_suporte(acao).get('seguranca') or ''}".strip(),
            "confirmacao": risco_acao_auto_suporte(acao).get("confirmacao") or "",
        })
        vistos.add(acao)
    return bloqueadas[:6]


def risco_acao_auto_suporte(acao):
    if acao in AUTO_SUPORTE_ACOES_EXIGEM_CONFIRMACAO:
        dados = AUTO_SUPORTE_ACOES_EXIGEM_CONFIRMACAO[acao]
        return {
            "risco": "alto" if acao == "limpar_todos_erros" else "medio",
            "reversivel": False,
            "seguranca": dados["risco"],
            "confirmacao": dados["confirmacao"],
        }
    if acao in AUTO_SUPORTE_ACOES_AUTONOMAS:
        return {
            "risco": "baixo",
            "reversivel": True,
            "seguranca": "Reparo reversivel, local ou apenas diagnostico.",
        }
    return {
        "risco": "medio",
        "reversivel": False,
        "seguranca": "Exige confirmacao manual porque pode alterar dados, enviar mensagem externa ou ocultar evidencias.",
    }


def montar_acoes_auto_suporte_payload():
    return [
        {
            "id": chave,
            "label": label,
            **risco_acao_auto_suporte(chave),
            "confirmacao_obrigatoria": chave in AUTO_SUPORTE_ACOES_EXIGEM_CONFIRMACAO,
        }
        for chave, label in ACOES_AUTO_SUPORTE.items()
    ]


def validar_confirmacao_acao_auto_suporte(acao, confirmacao=""):
    requisitos = AUTO_SUPORTE_ACOES_EXIGEM_CONFIRMACAO.get(acao)
    if not requisitos:
        return True
    confirmacao = normalizar_texto_campo(confirmacao).upper()
    esperado = normalizar_texto_campo(requisitos.get("confirmacao")).upper()
    if confirmacao != esperado:
        raise ValueError(f"Confirmacao obrigatoria: digite {requisitos.get('confirmacao')} para executar {ACOES_AUTO_SUPORTE.get(acao, acao)}.")
    return True


def montar_acoes_simples_auto_suporte(status_payload=None):
    status_payload = status_payload or {}
    acoes = [
        {
            "acao": "limpar_caches",
            "label": "Melhorar carregamento",
            "descricao": "Limpa caches leves da interface e recarrega dados da tela.",
        },
        {
            "acao": "testar_banco",
            "label": "Testar banco",
            "descricao": "Confere se o banco online esta respondendo.",
        },
        {
            "acao": "testar_backup",
            "label": "Testar backup",
            "descricao": "Confere se existe backup recente configurado.",
        },
        {
            "acao": "revalidar_pwa",
            "label": "Revalidar app",
            "descricao": "Verifica manifest, service worker e instalacao PWA.",
        },
        {
            "acao": "gerar_pacote_codex",
            "label": "Enviar ao desenvolvedor",
            "descricao": "Gera um pacote tecnico para mandar no Codex.",
        },
    ]
    if any(item.get("classe") == "lento" or item.get("alerta_2s") for item in status_payload.get("tempo_resposta") or []):
        acoes.insert(1, {
            "acao": "limpar_cache_rota_lenta",
            "label": "Corrigir tela lenta",
            "descricao": "Limpa o cache especifico da rota que ficou lenta.",
        })
        acoes.insert(2, {
            "acao": "revalidar_rota_lenta",
            "label": "Revalidar rota",
            "descricao": "Mede novamente a rota lenta e confirma se normalizou.",
        })
    return acoes


def montar_dialogo_ia_auto_suporte(status_payload=None, saude_dono=None, historico=None):
    status_payload = status_payload or {}
    saude_dono = list(saude_dono or [])
    historico = list(historico or [])
    diagnostico = status_payload.get("diagnostico") or {}
    narrativa = status_payload.get("narrativa") or {}
    plano = status_payload.get("plano_acao") or {}
    autonomia = status_payload.get("autonomia") or {}
    perfil = normalizar_texto_campo(status_payload.get("perfil")) or (perfil_auto_suporte() if has_request_context() else "desenvolvedor")
    desenvolvedor = perfil == "desenvolvedor"
    catalogo = {
        item.get("id"): item
        for item in (status_payload.get("acoes") or montar_acoes_auto_suporte_payload())
        if isinstance(item, dict) and item.get("id")
    }
    acoes_permitidas = set(ACOES_AUTO_SUPORTE if desenvolvedor else AUTO_SUPORTE_ACOES_ADMIN)

    def acao_dialogo(acao_id, label=None, descricao=None):
        acao_id = normalizar_texto_campo(acao_id)
        if not acao_id or acao_id not in acoes_permitidas:
            return None
        labels_guiados = {
            "limpar_caches": "Melhorar carregamento",
            "limpar_cache_rota_lenta": "Limpar cache da rota lenta",
            "gerar_pacote_codex": "Preparar pacote Codex",
            "gerar_backup_suporte": "Criar backup de suporte",
            "validar_ambiente": "Revalidar ambiente",
        }
        dados = dict(catalogo.get(acao_id) or {"id": acao_id, "label": ACOES_AUTO_SUPORTE.get(acao_id, acao_id)})
        dados["label"] = normalizar_texto_campo(label or labels_guiados.get(acao_id) or dados.get("label") or ACOES_AUTO_SUPORTE.get(acao_id, acao_id))
        dados["descricao"] = normalizar_texto_campo(descricao or dados.get("seguranca") or "")
        dados["confirmacao"] = normalizar_texto_campo(dados.get("confirmacao"))
        dados["confirmacao_obrigatoria"] = bool(dados.get("confirmacao_obrigatoria") or dados["confirmacao"])
        return dados

    def filtrar_acoes(ids):
        return [item for item in (acao_dialogo(acao_id) for acao_id in ids) if item]

    titulo = normalizar_texto_campo(diagnostico.get("titulo")) or "Sistema em verificacao"
    frase = normalizar_texto_campo(diagnostico.get("frase")) or normalizar_texto_campo(status_payload.get("mensagem")) or "Vou ler os sinais do sistema e sugerir a proxima acao."
    linhas_narrativa = [
        normalizar_texto_campo(linha)
        for linha in (narrativa.get("linhas") or [])
        if normalizar_texto_campo(linha)
    ][:3]
    if not linhas_narrativa:
        linhas_narrativa = [frase]

    abertura = [
        f"Estou lendo o AutoSuporte como {('desenvolvedor' if desenvolvedor else 'administrador')}.",
        f"Diagnostico atual: {titulo}.",
        *linhas_narrativa,
    ]
    if plano.get("titulo"):
        abertura.append(f"Minha proxima recomendacao e: {normalizar_texto_campo(plano.get('titulo'))}.")

    opcoes = [
        {
            "id": "diagnostico",
            "label": "Diagnosticar",
            "titulo": "Vou conferir a saude principal.",
            "resposta": [
                "Primeiro eu valido banco, backup, app instalado e arquivos estaticos.",
                "Essas acoes nao alteram dados importantes; elas apenas medem e atualizam a leitura do suporte.",
            ],
            "acoes": filtrar_acoes(["validar_ambiente", "testar_banco", "testar_backup", "revalidar_pwa", "revalidar_estaticos"]),
        },
        {
            "id": "performance",
            "label": "Melhorar velocidade",
            "titulo": "Vou atacar carregamento lento.",
            "resposta": [
                "Posso limpar caches leves, limpar a rota lenta especifica e medir de novo a tela mais pesada.",
                "Se o banco online estiver lento, eu mostro isso como causa provavel em vez de esconder o problema.",
            ],
            "acoes": filtrar_acoes(["limpar_caches", "limpar_cache_rota_lenta", "revalidar_rota_lenta"]),
        },
        {
            "id": "relatorio",
            "label": "Preparar relatorio",
            "titulo": "Vou organizar evidencias para manutencao.",
            "resposta": [
                "Eu gero um pacote tecnico com o estado atual, erros recentes, rotas lentas e contexto para o Codex.",
                "Tambem posso criar um backup de suporte antes de qualquer investigacao mais sensivel.",
            ],
            "acoes": filtrar_acoes(["gerar_pacote_codex", "gerar_backup_suporte"]),
        },
    ]

    if desenvolvedor:
        opcoes.extend([
            {
                "id": "reparo",
                "label": "Reparo seguro",
                "titulo": "Vou executar somente correcoes reversiveis.",
                "resposta": [
                    f"O modo atual de auto-reparo e {normalizar_texto_campo(autonomia.get('modo_label')) or 'Seguro'}.",
                    "Eu priorizo checks, limpeza de cache e erros ja resolvidos antes de tocar em qualquer dado sensivel.",
                ],
                "acoes": filtrar_acoes(["resolver_erros_com_checks_ok", "limpar_erros_resolvidos", "validar_ambiente"]),
            },
            {
                "id": "sensivel",
                "label": "Acoes sensiveis",
                "titulo": "Vou separar tudo que exige confirmacao.",
                "resposta": [
                    "Essas acoes podem alterar dados, limpar evidencias ou enviar mensagem externa.",
                    "Eu so libero o botao depois de pedir a frase de confirmacao configurada no backend.",
                ],
                "acoes": filtrar_acoes([
                    "corrigir_classificacao_clientes",
                    "desativar_planilhas_com_erro",
                    "limpar_todos_erros",
                    "enviar_relatorio_telegram",
                    "enviar_alerta_telegram",
                    "registrar_incidente",
                ]),
            },
        ])

    opcoes = [opcao for opcao in opcoes if opcao.get("acoes")]
    return {
        "perfil": perfil,
        "status": "ok" if status_payload.get("ok") else "revisar",
        "nivel": normalizar_texto_campo(diagnostico.get("nivel")) or "info",
        "titulo": titulo,
        "frase": frase,
        "abertura": abertura,
        "opcoes": opcoes,
        "indicadores": [
            {"label": "Erros abertos", "valor": len(status_payload.get("erros_abertos") or [])},
            {"label": "Regras", "valor": len(status_payload.get("inconsistencias_negocio") or [])},
            {"label": "Historico", "valor": len(historico)},
            {"label": "Saude OK", "valor": len([item for item in saude_dono if item.get("ok")])},
        ],
    }


def montar_simulacao_autonomia_auto_suporte(status_payload, planejadas=None, bloqueadas=None, modo=None):
    status_payload = status_payload or {}
    planejadas = list(planejadas if planejadas is not None else planejar_acoes_autonomas_auto_suporte(status_payload))
    bloqueadas = list(bloqueadas if bloqueadas is not None else montar_acoes_bloqueadas_auto_suporte(status_payload))
    modo = normalizar_modo_autonomia_auto_suporte(modo or AUTO_SUPORTE_MODO_AUTONOMIA_PADRAO)
    detectado = []
    diagnostico = status_payload.get("diagnostico") or {}
    if diagnostico.get("titulo"):
        detectado.append(diagnostico.get("titulo"))
    detectado.extend(status_payload.get("falhas") or [])
    for item in status_payload.get("tempo_resposta") or []:
        if item.get("classe") == "lento" or item.get("alerta_2s"):
            detectado.append(f"{item.get('rota')} lenta ({item.get('ultimo_ms')} ms)")

    return {
        "modo": modo,
        "modo_label": AUTO_SUPORTE_MODOS_AUTONOMIA[modo]["label"],
        "detectado": list(dict.fromkeys([normalizar_texto_campo(item) for item in detectado if normalizar_texto_campo(item)]))[:8],
        "pretende_fazer": [
            {
                **item,
                **risco_acao_auto_suporte(item.get("acao")),
            }
            for item in planejadas
        ],
        "precisa_confirmacao": [
            {
                **item,
                **risco_acao_auto_suporte(item.get("acao")),
            }
            for item in bloqueadas
        ],
    }


def listar_log_autonomia_auto_suporte(limite=12):
    eventos = []
    for item in listar_historico_auto_suporte(limite=80):
        evento = normalizar_texto_campo(item.get("evento"))
        if evento not in {"autonomia", "autonomia_simulacao", "autonomia_bloqueada", "normalizacao", "diagnostico"}:
            continue
        categoria = "o bot fez isso" if evento == "autonomia" else "precisa de confirmacao" if evento == "autonomia_bloqueada" else "nao fez isso por seguranca" if evento == "autonomia_simulacao" else "diagnostico"
        eventos.append({
            "quando": item.get("quando"),
            "categoria": categoria,
            "titulo": item.get("titulo") or item.get("evento"),
            "mensagem": item.get("mensagem"),
            "usuario": item.get("usuario") or "auto_suporte",
            "severidade": item.get("severidade") or "info",
        })
        if len(eventos) >= int(limite or 12):
            break
    return eventos


def montar_autonomia_auto_suporte(estado=None, status_payload=None):
    estado = estado or carregar_estado_auto_suporte()
    autonomia = estado.get("autonomia") if isinstance(estado, dict) else {}
    autonomia = autonomia if isinstance(autonomia, dict) else {}
    status_payload = status_payload or {}
    modo = obter_modo_autonomia_auto_suporte(estado)
    bloqueadas = montar_acoes_bloqueadas_auto_suporte(status_payload)
    planejadas = planejar_acoes_autonomas_auto_suporte(status_payload, estado) if status_payload else []
    return {
        "habilitada": True,
        "modo": modo,
        "modo_label": AUTO_SUPORTE_MODOS_AUTONOMIA[modo]["label"],
        "modo_descricao": AUTO_SUPORTE_MODOS_AUTONOMIA[modo]["descricao"],
        "modos": [
            {"id": chave, "label": dados["label"], "descricao": dados["descricao"]}
            for chave, dados in AUTO_SUPORTE_MODOS_AUTONOMIA.items()
        ],
        "cooldown_segundos": AUTO_SUPORTE_AUTONOMIA_COOLDOWN_SEGUNDOS,
        "limite_por_ciclo": AUTO_SUPORTE_AUTONOMIA_LIMITE_ACOES,
        "acoes_permitidas": [
            {"id": acao, "label": ACOES_AUTO_SUPORTE.get(acao, acao)}
            for acao in ACOES_AUTO_SUPORTE
            if acao in AUTO_SUPORTE_ACOES_AUTONOMAS
        ],
        "ultimo_ciclo": autonomia.get("ultimo_ciclo"),
        "ultimo_resultado": autonomia.get("ultimo_resultado") or [],
        "acoes_bloqueadas": bloqueadas,
        "simulacao": montar_simulacao_autonomia_auto_suporte(status_payload, planejadas, bloqueadas, modo) if status_payload else {},
        "log": listar_log_autonomia_auto_suporte(limite=10),
    }


def montar_plano_acao_auto_suporte(status_payload):
    status_payload = status_payload or {}
    diagnostico = status_payload.get("diagnostico") or {}
    autonomia = status_payload.get("autonomia") or {}
    simulacao = autonomia.get("simulacao") or {}
    planejadas = list(simulacao.get("pretende_fazer") or [])
    bloqueadas = list(autonomia.get("acoes_bloqueadas") or simulacao.get("precisa_confirmacao") or [])
    sugestoes = list(status_payload.get("sugestoes") or [])
    erros_abertos = list(status_payload.get("erros_abertos") or [])
    falhas = list(status_payload.get("falhas") or [])
    nivel = normalizar_texto_campo(diagnostico.get("nivel") or "info")
    prioridade_por_nivel = {
        "critico": "alta",
        "alerta": "media",
        "atencao": "baixa",
        "info": "normal",
    }

    proxima = None
    origem = ""
    if planejadas:
        proxima = planejadas[0]
        origem = "auto_reparo"
    else:
        proxima = next((item for item in sugestoes if normalizar_texto_campo(item.get("acao"))), None)
        origem = "sugestao" if proxima else ""
    if not proxima and bloqueadas:
        proxima = bloqueadas[0]
        origem = "confirmacao"
    if not proxima and erros_abertos:
        proxima = {"acao": "gerar_pacote_codex", "titulo": "Erro aberto exige pacote tecnico"}
        origem = "codex"

    acao = normalizar_texto_campo((proxima or {}).get("acao"))
    risco = risco_acao_auto_suporte(acao) if acao else {}
    permitido = True
    if has_request_context() and perfil_auto_suporte() == "administrador":
        permitido = acao in AUTO_SUPORTE_ACOES_ADMIN
    executavel = bool(acao and acao in ACOES_AUTO_SUPORTE and permitido)
    confirmacao = normalizar_texto_campo(risco.get("confirmacao"))
    acao_label = ACOES_AUTO_SUPORTE.get(acao, acao) if acao else ""
    itens = []

    if planejadas:
        itens.append(f"{len(planejadas)} reparo(s) seguro(s) podem ser executados pelo bot.")
    if bloqueadas:
        itens.append(f"{len(bloqueadas)} acao(oes) sensivel(is) ficaram pendentes de confirmacao.")
    if erros_abertos:
        itens.append("Existe erro aberto: gere o pacote Codex antes de mexer em regra sensivel.")
    if falhas and not itens:
        itens.append("Revise o diagnostico e execute a primeira acao sugerida.")
    if not falhas and not planejadas and not bloqueadas and not erros_abertos:
        itens.append("Nenhuma acao urgente. Mantenha o monitoramento automatico ativo.")

    if origem == "auto_reparo":
        cta = "Rodar auto-reparo"
        mensagem = "O bot encontrou uma acao reversivel e de baixo risco para tentar primeiro."
    elif acao and not permitido:
        cta = "Acao restrita"
        mensagem = "A proxima acao e tecnica e deve ser enviada ao desenvolvedor pelo pacote Codex."
    elif confirmacao:
        cta = "Executar com confirmacao"
        mensagem = "A proxima acao altera dados ou evidencias e precisa de confirmacao manual."
    elif acao == "gerar_pacote_codex":
        cta = "Gerar pacote Codex"
        mensagem = "Prepare um relatorio tecnico para enviar ao desenvolvedor."
    elif acao:
        cta = "Executar proxima acao"
        mensagem = "Ha uma sugestao direta para reduzir o problema detectado."
    else:
        cta = "Sem acao pendente"
        mensagem = "O AutoSuporte nao encontrou reparo pendente nesta leitura."

    return {
        "prioridade": prioridade_por_nivel.get(nivel, "normal"),
        "nivel": nivel,
        "titulo": diagnostico.get("titulo") or "Plano de acao",
        "resumo": diagnostico.get("frase") or status_payload.get("mensagem") or mensagem,
        "mensagem": mensagem,
        "origem": origem or "monitoramento",
        "acao": acao,
        "acao_label": acao_label,
        "cta_label": cta,
        "executavel": executavel,
        "permitido": permitido,
        "bloqueio": "" if permitido else "Acao restrita ao desenvolvedor. Gere o pacote Codex.",
        "confirmacao": confirmacao,
        "risco": risco.get("risco") or ("baixo" if executavel else ""),
        "itens": itens[:4],
    }


def montar_narrativa_auto_suporte(status_payload):
    status_payload = status_payload or {}
    diagnostico = status_payload.get("diagnostico") or {}
    plano = status_payload.get("plano_acao") or {}
    autonomia = status_payload.get("autonomia") or {}
    simulacao = autonomia.get("simulacao") or {}
    tempo_resposta = list(status_payload.get("tempo_resposta") or [])
    erros_abertos = list(status_payload.get("erros_abertos") or [])
    falhas = list(status_payload.get("falhas") or [])
    sugestoes = list(status_payload.get("sugestoes") or [])
    planejadas = list(simulacao.get("pretende_fazer") or [])
    bloqueadas = list(autonomia.get("acoes_bloqueadas") or simulacao.get("precisa_confirmacao") or [])
    ultimo_resultado = list(autonomia.get("ultimo_resultado") or [])
    rotas_lentas = [
        item
        for item in tempo_resposta
        if item.get("alerta_2s") or item.get("classe") == "lento"
    ]

    linhas = []
    nivel = normalizar_texto_campo(diagnostico.get("nivel") or "info")
    titulo = diagnostico.get("titulo") or "Monitoramento ativo"
    frase = diagnostico.get("frase") or status_payload.get("mensagem") or "Sistema em leitura."

    if nivel in {"critico", "alerta"}:
        abertura = f"Encontrei um ponto de atencao: {titulo}. {frase}"
    else:
        abertura = f"Nesta leitura, o sistema esta operacional. {frase}"
    linhas.append(abertura)

    if rotas_lentas:
        pior = sorted(rotas_lentas, key=lambda item: int(item.get("ultimo_ms") or 0), reverse=True)[0]
        linhas.append(
            f"Estou acompanhando lentidao em {pior.get('rota')} ({pior.get('ultimo_ms')} ms) e posso revalidar a rota ou limpar o cache especifico."
        )
    elif tempo_resposta:
        linhas.append("As rotas monitoradas nao mostram lentidao critica nesta medicao.")

    if erros_abertos:
        linhas.append(f"Ha {len(erros_abertos)} erro(s) aberto(s); antes de reparo sensivel, o ideal e gerar o pacote Codex.")
    elif "Erro 500 aberto" not in falhas:
        linhas.append("Nao encontrei erro 500 aberto nesta leitura.")

    if planejadas:
        nomes = ", ".join((item.get("label") or item.get("acao") or "acao segura") for item in planejadas[:3])
        linhas.append(f"Posso executar reparo seguro agora: {nomes}.")
    elif ultimo_resultado:
        nomes = ", ".join((item.get("label") or item.get("acao") or "acao") for item in ultimo_resultado[:3])
        linhas.append(f"No ultimo ciclo, registrei resultado para: {nomes}.")
    elif sugestoes:
        nomes = ", ".join((item.get("titulo") or item.get("acao") or "revisao") for item in sugestoes[:3])
        linhas.append(f"Minha proxima recomendacao e revisar: {nomes}.")
    else:
        linhas.append("No momento estou apenas monitorando e mantendo os caches e checks sob observacao.")

    if bloqueadas:
        linhas.append(f"{len(bloqueadas)} acao(oes) ficaram bloqueadas por seguranca e precisam de confirmacao manual.")

    if plano.get("acao_label"):
        linhas.append(f"Proxima melhor acao: {plano.get('acao_label')} ({plano.get('prioridade') or 'normal'}).")

    return {
        "titulo": titulo,
        "resumo": linhas[0],
        "linhas": linhas[:6],
        "status": "atuando" if planejadas or ultimo_resultado else "monitorando",
        "prioridade": plano.get("prioridade") or ("alta" if nivel == "critico" else "media" if nivel == "alerta" else "normal"),
        "gerado_em": agora_iso(),
    }


def planejar_acoes_autonomas_auto_suporte(status_payload, estado=None):
    status_payload = status_payload or {}
    estado = estado or carregar_estado_auto_suporte()
    autonomia = estado.get("autonomia") if isinstance(estado, dict) else {}
    autonomia = autonomia if isinstance(autonomia, dict) else {}
    acoes_estado = autonomia.get("acoes") if isinstance(autonomia.get("acoes"), dict) else {}
    agora_ts = time.time()
    candidatos = []

    for sugestao in status_payload.get("sugestoes") or []:
        acao = normalizar_texto_campo(sugestao.get("acao"))
        if acao:
            candidatos.append((acao, sugestao.get("titulo") or ACOES_AUTO_SUPORTE.get(acao, acao)))

    diagnostico = status_payload.get("diagnostico") or {}
    for item in diagnostico.get("itens") or []:
        acao = normalizar_texto_campo(item.get("acao"))
        if acao:
            candidatos.append((acao, item.get("titulo") or ACOES_AUTO_SUPORTE.get(acao, acao)))

    if status_payload.get("erros_abertos"):
        candidatos.append(("gerar_pacote_codex", "Erro aberto exige pacote tecnico"))

    planejadas = []
    vistos = set()
    for acao, motivo in candidatos:
        if acao in vistos or acao not in AUTO_SUPORTE_ACOES_AUTONOMAS:
            continue
        visto_acao = acoes_estado.get(acao) if isinstance(acoes_estado, dict) else {}
        ultimo_ts = float((visto_acao or {}).get("ultimo_ts") or 0.0)
        if agora_ts - ultimo_ts < AUTO_SUPORTE_AUTONOMIA_COOLDOWN_SEGUNDOS:
            continue
        planejadas.append({"acao": acao, "label": ACOES_AUTO_SUPORTE.get(acao, acao), "motivo": normalizar_texto_campo(motivo)})
        vistos.add(acao)
        if len(planejadas) >= AUTO_SUPORTE_AUTONOMIA_LIMITE_ACOES:
            break
    return planejadas


def limpar_cache_rota_lenta_auto_suporte(tempo_resposta=None):
    tempo_resposta = list(tempo_resposta or metricas_tempo_resposta_central_tecnica())
    lentas = [item for item in tempo_resposta if item.get("classe") == "lento" or item.get("alerta_2s")]
    if not lentas:
        return {"rota": "", "caches_limpos": [], "mensagem": "Nenhuma rota lenta detectada agora."}
    rota = sorted(lentas, key=lambda item: int(item.get("ultimo_ms") or 0), reverse=True)[0].get("rota")
    caches = []
    if rota == "/":
        HOME_SNAPSHOT_CACHE["testado_em"] = 0.0
        HOME_SNAPSHOT_CACHE["resultado"] = None
        HUD_CACHE["testado_em"] = 0.0
        HUD_CACHE["resultado"] = None
        caches.extend(["home_snapshot", "hud"])
    elif rota == "/clientes":
        limpar_cache_clientes()
        caches.append("clientes")
    elif rota == "/painel":
        PAINEL_CONTEXT_CACHE["testado_em"] = 0.0
        PAINEL_CONTEXT_CACHE["resultado"] = None
        caches.append("painel")
    elif rota in {"/financeiro", "/relatorios"}:
        RELATORIOS_CONTEXT_CACHE["testado_em"] = 0.0
        RELATORIOS_CONTEXT_CACHE["resultado"] = None
        RELATORIOS_CONTEXT_CACHE["entradas"] = {}
        caches.append("relatorios")
    elif rota == "/configuracoes":
        limpar_cache_configuracao_empresa()
        caches.append("configuracoes")
    else:
        limpar_caches_operacionais_leves()
        caches.append("operacionais_leves")
    return {"rota": rota, "caches_limpos": caches, "mensagem": f"Cache especifico da rota {rota} foi limpo."}


def revalidar_rota_lenta_auto_suporte(rota=""):
    rota = normalizar_texto_campo(rota)
    rotas_validas = {item[1] for item in ROTAS_CENTRAL_TECNICA} | set(ROTAS_MONITORADAS_RESPOSTA)
    if not rota or rota not in rotas_validas:
        tempo_resposta = metricas_tempo_resposta_central_tecnica()
        lentas = [item for item in tempo_resposta if item.get("classe") == "lento" or item.get("alerta_2s")]
        rota = (
            sorted(lentas, key=lambda item: int(item.get("ultimo_ms") or 0), reverse=True)[0].get("rota")
            if lentas else
            "/"
        )

    inicio = time.perf_counter()
    try:
        with app.test_client() as client:
            resposta = client.get(rota)
        tempo_ms = int((time.perf_counter() - inicio) * 1000)
        status = int(resposta.status_code)
        ok = status < 500
        resolvidos = 0
        if ok and tempo_ms <= 1500:
            resolvidos = marcar_erros_auto_suporte_resolvidos(
                {f"central_tecnica_rota_{rota}", "erro_global"},
                usuario="auto_suporte_rota_ok",
            )
        return {
            "ok": ok,
            "rota": rota,
            "status": status,
            "tempo_ms": tempo_ms,
            "classe": classificar_latencia_ms(tempo_ms),
            "causa_provavel": causa_provavel_lentidao_rota(rota, {"status": status, "tempo_ms": tempo_ms}),
            "erros_resolvidos": resolvidos,
            "mensagem": f"Rota {rota} respondeu HTTP {status} em {tempo_ms} ms.",
        }
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao=f"central_tecnica_rota_{rota}")
        return {
            "ok": False,
            "rota": rota,
            "status": "",
            "tempo_ms": int((time.perf_counter() - inicio) * 1000),
            "classe": "lento",
            "causa_provavel": "Excecao durante revalidacao interna.",
            "erros_resolvidos": 0,
            "mensagem": str(erro),
        }


def revalidar_estaticos_auto_suporte():
    checks = [
        testar_rota_interna_status("CSS principal", "/static/responsive.css", 200),
        testar_rota_interna_status("CSS AutoSuporte", "/static/auto_suporte.css", 200),
        testar_rota_interna_status("JS AutoSuporte", "/static/auto_suporte.js", 200),
        testar_rota_interna_status("Service worker", "/sw.js", 200),
        testar_rota_interna_status("Manifest PWA", "/site.webmanifest", 200),
    ]
    falhas = [item.get("nome") for item in checks if not item.get("ok")]
    return {
        "ok": not falhas,
        "checks": checks,
        "falhas": falhas,
        "mensagem": "Arquivos estaticos e service worker revalidados." if not falhas else "Falhas em: " + ", ".join(falhas),
    }


def resolver_erros_com_checks_ok_auto_suporte():
    total = 0
    total += executar_check_auto_suporte(
        detectar_fluxos_suspeitos_auto_suporte,
        [],
        "auto_suporte_fluxos",
        {"auto_suporte_fluxos", "pacote_codex_fluxos"},
    ) == []
    total += executar_check_auto_suporte(
        listar_planilhas_com_erro_auto_suporte,
        [],
        "auto_suporte_planilhas_erro",
        {"auto_suporte_planilhas_erro", "pacote_codex_planilhas"},
    ) == []
    total += executar_check_auto_suporte(
        detectar_inconsistencias_negocio_auto_suporte,
        [],
        "auto_suporte_negocio",
        {"auto_suporte_negocio"},
    ) == []
    return {
        "checks_ok": int(total),
        "mensagem": "Erros ligados a checks saudaveis foram marcados como resolvidos quando aplicavel.",
    }


def executar_autonomia_auto_suporte(status_payload=None, modo=None, simular=False):
    status_payload = status_payload or status_auto_suporte()
    estado = carregar_estado_auto_suporte()
    modo = normalizar_modo_autonomia_auto_suporte(modo or obter_modo_autonomia_auto_suporte(estado))
    planejadas = planejar_acoes_autonomas_auto_suporte(status_payload, estado)
    bloqueadas = montar_acoes_bloqueadas_auto_suporte(status_payload)
    simulacao = montar_simulacao_autonomia_auto_suporte(status_payload, planejadas, bloqueadas, modo)
    resultados = []

    if simular or not AUTO_SUPORTE_MODOS_AUTONOMIA[modo]["executa"]:
        registrar_historico_auto_suporte(
            "autonomia_simulacao",
            "Simulacao de autonomia",
            f"Modo {AUTO_SUPORTE_MODOS_AUTONOMIA[modo]['label']}: nenhuma acao automatica executada.",
            severidade="info",
            detalhes={"modo": modo, "simulacao": simulacao},
        )
        status_atualizado = status_auto_suporte()
        status_atualizado["autonomia"] = montar_autonomia_auto_suporte(status_payload=status_atualizado)
        return {
            "ok": True,
            "modo": modo,
            "simulacao": simulacao,
            "executadas": [],
            "planejadas": planejadas,
            "pendentes_confirmacao": bloqueadas,
            "mensagem": "Simulacao concluida. Nenhum reparo foi executado.",
            "status": status_atualizado,
        }

    for item in planejadas:
        acao = item.get("acao")
        try:
            resultado = executar_acao_auto_suporte(
                acao,
                observacao=f"Executado automaticamente pelo AutoSuporte: {item.get('motivo') or ACOES_AUTO_SUPORTE.get(acao, acao)}",
            )
            resultados.append({
                "acao": acao,
                "label": item.get("label"),
                "ok": bool(resultado.get("ok")),
                "mensagem": resultado.get("mensagem") or "",
            })
        except Exception as erro:
            registrar_ultimo_erro_producao(erro, descricao=f"auto_suporte_autonomia_{acao}")
            resultados.append({
                "acao": acao,
                "label": item.get("label"),
                "ok": False,
                "mensagem": str(erro),
            })

    estado = carregar_estado_auto_suporte()
    autonomia = estado.get("autonomia") if isinstance(estado.get("autonomia"), dict) else {}
    acoes_estado = autonomia.get("acoes") if isinstance(autonomia.get("acoes"), dict) else {}
    agora_ts = time.time()
    for resultado in resultados:
        acoes_estado[resultado["acao"]] = {
            "ultimo_ts": agora_ts,
            "ultimo_em": agora_iso(),
            "ok": bool(resultado.get("ok")),
            "mensagem": resultado.get("mensagem") or "",
        }
    autonomia["acoes"] = acoes_estado
    autonomia["ultimo_ciclo"] = agora_iso()
    autonomia["ultimo_resultado"] = resultados
    estado["autonomia"] = autonomia
    salvar_estado_auto_suporte(estado)

    if resultados:
        registrar_historico_auto_suporte(
            "autonomia",
            "Auto-reparo seguro",
            f"{len(resultados)} acao(oes) autonoma(s) executada(s).",
            severidade="info" if all(item.get("ok") for item in resultados) else "warning",
            detalhes={"resultados": resultados},
        )
    if modo == "assistido" and bloqueadas:
        registrar_historico_auto_suporte(
            "autonomia_bloqueada",
            "Acoes exigem confirmacao",
            f"{len(bloqueadas)} acao(oes) sensivel(is) preparada(s), mas nao executada(s).",
            severidade="warning",
            detalhes={"acoes": bloqueadas},
        )

    status_atualizado = status_auto_suporte()
    status_atualizado["autonomia"] = montar_autonomia_auto_suporte(status_payload=status_atualizado)
    return {
        "ok": all(item.get("ok") for item in resultados) if resultados else True,
        "executadas": resultados,
        "planejadas": planejadas,
        "pendentes_confirmacao": bloqueadas,
        "simulacao": simulacao,
        "modo": modo,
        "mensagem": (
            f"AutoSuporte executou {len(resultados)} reparo(s) seguro(s)."
            if resultados else
            "Nenhuma acao autonoma pendente ou cooldown ainda ativo."
        ),
        "status": status_atualizado,
    }


def avaliar_alertas_auto_suporte(status_payload):
    diagnostico = status_payload.get("diagnostico") or {}
    nivel = diagnostico.get("nivel") or "info"
    falhas = list(status_payload.get("falhas") or [])
    chave_estado = "|".join([nivel, *falhas])[:240]
    agora_ts = time.time()
    estado = carregar_estado_auto_suporte()
    estado_anterior = normalizar_texto_campo(estado.get("chave_estado"))
    resumo_ts = float(estado.get("ultimo_resumo_ts") or 0.0)

    if chave_estado != estado_anterior:
        registrar_historico_auto_suporte(
            "diagnostico",
            diagnostico.get("titulo") or "Diagnostico atualizado",
            diagnostico.get("frase") or status_payload.get("mensagem") or "",
            severidade=nivel,
            detalhes={"falhas": falhas, "nivel": nivel},
        )
        if nivel in {"critico", "alerta"}:
            enviar_alerta_estabilidade_assincrono(
                "AutoSuporte Wagen Estetica\n"
                f"Nivel: {diagnostico.get('label')}\n"
                f"Resumo: {diagnostico.get('frase')}\n"
                "Acao: abra o AutoSuporte ou gere pacote Codex.",
                chave=f"auto_suporte_estado:{chave_estado}",
                intervalo=1800,
            )
        elif estado_anterior and nivel == "info":
            registrar_historico_auto_suporte(
                "normalizacao",
                "Status normalizado",
                "AutoSuporte confirmou que os checks criticos voltaram ao normal.",
                severidade="info",
                detalhes={"estado_anterior": estado_anterior},
            )
            enviar_alerta_estabilidade_assincrono(
                "AutoSuporte Wagen Estetica\nStatus normalizado.\nNenhum incidente critico no momento.",
                chave="auto_suporte_normalizou",
                intervalo=1800,
            )

    if agora_ts - resumo_ts >= 7200:
        enviar_alerta_estabilidade_assincrono(
            "Resumo AutoSuporte Wagen Estetica\n"
            f"Nivel: {diagnostico.get('label')}\n"
            f"Resumo: {diagnostico.get('frase')}\n"
            f"Erros abertos: {len(status_payload.get('erros_abertos') or [])}\n"
            f"Gerado em: {status_payload.get('gerado_em')}",
            chave=f"auto_suporte_resumo:{int(agora_ts // 7200)}",
            intervalo=3600,
        )
        estado["ultimo_resumo_ts"] = agora_ts

    estado["chave_estado"] = chave_estado
    estado["ultimo_nivel"] = nivel
    estado["atualizado_em"] = agora_iso()
    salvar_estado_auto_suporte(estado)


def montar_saude_sistema_dono_auto_suporte(status_sistema=None, status_auto=None):
    status_sistema = status_sistema or montar_status_sistema_dono()
    status_auto = status_auto or {}
    itens = {item.get("nome"): item for item in status_sistema.get("itens", [])}
    ultimo_erro = status_sistema.get("ultimo_erro") or {}
    return [
        {
            "nome": "Sistema online",
            "ok": bool(status_auto.get("ok", status_sistema.get("resumo", {}).get("ok"))),
            "valor": "Operando" if status_auto.get("ok", status_sistema.get("resumo", {}).get("ok")) else "Revisar",
            "detalhe": status_auto.get("mensagem") or "Acompanhamento ativo pelo AutoSuporte.",
        },
        {
            "nome": "Banco online",
            "ok": bool((itens.get("Banco online") or {}).get("ok")),
            "valor": (itens.get("Banco online") or {}).get("valor") or "-",
            "detalhe": (itens.get("Banco online") or {}).get("detalhe") or "-",
        },
        {
            "nome": "Backup",
            "ok": bool((itens.get("Backup") or {}).get("ok")),
            "valor": (itens.get("Backup") or {}).get("valor") or "-",
            "detalhe": (itens.get("Backup") or {}).get("detalhe") or "-",
        },
        {
            "nome": "App instalado",
            "ok": bool((itens.get("PWA instalado") or {}).get("ok")),
            "valor": (itens.get("PWA instalado") or {}).get("valor") or "-",
            "detalhe": (itens.get("PWA instalado") or {}).get("detalhe") or "-",
        },
        {
            "nome": "Licenca",
            "ok": bool((itens.get("Licenca") or {}).get("ok")),
            "valor": (itens.get("Licenca") or {}).get("valor") or "-",
            "detalhe": (itens.get("Licenca") or {}).get("detalhe") or "-",
        },
        {
            "nome": "Bot de alerta",
            "ok": bool((itens.get("Bot Telegram") or {}).get("ok")),
            "valor": (itens.get("Bot Telegram") or {}).get("valor") or "-",
            "detalhe": (itens.get("Bot Telegram") or {}).get("detalhe") or "-",
        },
        {
            "nome": "Ultimo problema",
            "ok": not bool(ultimo_erro.get("mensagem")),
            "valor": ultimo_erro.get("tipo") or "Nenhum erro recente",
            "detalhe": ultimo_erro.get("mensagem") or "Sem falhas recentes registradas.",
        },
        {
            "nome": "Ultima verificacao",
            "ok": True,
            "valor": status_auto.get("gerado_em") or status_sistema.get("gerado_em") or "-",
            "detalhe": "Horario da ultima leitura do painel.",
        },
    ]


def agrupar_historico_auto_suporte_por_dia(historico=None, limite_dias=7):
    historico = list(historico or listar_historico_auto_suporte(limite=80))
    contagem = {}
    for item in historico:
        dia = str(item.get("quando") or "")[:10] or "sem_data"
        contagem[dia] = contagem.get(dia, 0) + 1
    return [
        {"dia": dia, "total": total}
        for dia, total in sorted(contagem.items())[-int(limite_dias or 7):]
    ]


def executar_git_local_auto_suporte(*args):
    try:
        resultado = subprocess.run(
            ["git", *args],
            cwd=app.root_path,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=6,
            check=False,
        )
    except Exception:
        return ""
    if resultado.returncode != 0:
        return ""
    return (resultado.stdout or "").strip()


def montar_git_info_auto_suporte():
    return {
        "branch": executar_git_local_auto_suporte("branch", "--show-current"),
        "commit": executar_git_local_auto_suporte("rev-parse", "--short", "HEAD"),
        "commit_completo": executar_git_local_auto_suporte("rev-parse", "HEAD"),
        "status": executar_git_local_auto_suporte("status", "--short"),
        "ultimo_commit": executar_git_local_auto_suporte("log", "-1", "--pretty=format:%h %ad %s", "--date=iso"),
    }


def montar_pacote_codex_auto_suporte(force=False):
    chave_cache = chave_cache_auto_suporte()
    agora_cache_ts = time.time()
    if (
        not force
        and AUTO_SUPORTE_PACOTE_CACHE.get("resultado") is not None
        and AUTO_SUPORTE_PACOTE_CACHE.get("chave") == chave_cache
        and agora_cache_ts - float(AUTO_SUPORTE_PACOTE_CACHE.get("testado_em") or 0.0) < AUTO_SUPORTE_PACOTE_CACHE_TTL
    ):
        return deepcopy(AUTO_SUPORTE_PACOTE_CACHE["resultado"])

    status_sistema = executar_com_fallback_producao(
        montar_status_sistema_dono,
        {"resumo": {"ok": False, "falhas": ["status_indisponivel"]}, "itens": []},
        "pacote_codex_status",
    )
    banco = executar_com_fallback_producao(
        obter_status_banco_online,
        {"conectado": False, "mensagem": "Banco indisponivel.", "backend": "-"},
        "pacote_codex_banco",
    )
    backup = executar_com_fallback_producao(
        obter_status_backup_banco,
        status_backup_banco_padrao(),
        "pacote_codex_backup",
    )
    pacote = {
        "tipo": "pacote_tecnico_codex",
        "gerado_em": agora_iso(),
        "instrucao_para_codex": (
            "Analise este pacote tecnico, corrija o erro no projeto Flask, rode os testes, "
            "faca commit e push no GitHub se a validacao passar."
        ),
        "ambiente": {
            "versao_sistema": obter_versao_sistema(),
            "app_version": APP_VERSION,
            "host": request.host if has_request_context() else "",
            "url": request.url if has_request_context() else "",
            "usuario": session.get("usuario") if has_request_context() else "",
            "empresa_id": empresa_atual_id() if has_request_context() else "",
        },
        "git": montar_git_info_auto_suporte(),
        "ultimo_erro": dict(ULTIMO_ERRO_PRODUCAO),
        "tempo_resposta": metricas_tempo_resposta_central_tecnica(),
        "status_sistema": status_sistema,
        "banco": banco,
        "backup": backup,
        "plano_acao": montar_plano_acao_auto_suporte({
            "diagnostico": (status_sistema.get("diagnostico") or {}),
            "erros_abertos": listar_erros_producao(apenas_abertos=True, limite=8),
            "tempo_resposta": metricas_tempo_resposta_central_tecnica(),
        }),
        "inconsistencias_negocio": executar_check_auto_suporte(
            detectar_inconsistencias_negocio_auto_suporte,
            [],
            "pacote_codex_negocio",
            {"auto_suporte_negocio", "pacote_codex_negocio"},
        ),
        "fluxos_suspeitos": executar_check_auto_suporte(
            detectar_fluxos_suspeitos_auto_suporte,
            [],
            "pacote_codex_fluxos",
            {"pacote_codex_fluxos", "auto_suporte_fluxos"},
        ),
        "planilhas_erro": executar_check_auto_suporte(
            listar_planilhas_com_erro_auto_suporte,
            [],
            "pacote_codex_planilhas",
            {"pacote_codex_planilhas", "auto_suporte_planilhas_erro"},
        ),
    }
    pacote["erros_abertos"] = listar_erros_producao(apenas_abertos=True, limite=8)
    pacote["texto_para_codex"] = formatar_pacote_codex_texto(pacote)
    AUTO_SUPORTE_PACOTE_CACHE["testado_em"] = agora_cache_ts
    AUTO_SUPORTE_PACOTE_CACHE["chave"] = chave_cache
    AUTO_SUPORTE_PACOTE_CACHE["resultado"] = deepcopy(pacote)
    return pacote


def formatar_pacote_codex_texto(pacote):
    ultimo = pacote.get("ultimo_erro") or {}
    git_info = pacote.get("git") or {}
    linhas = [
        "PACOTE TECNICO AUTOSUPORTE -> CODEX",
        "",
        pacote.get("instrucao_para_codex") or "",
        "",
        f"Gerado em: {pacote.get('gerado_em')}",
        f"Versao: {pacote.get('ambiente', {}).get('versao_sistema')}",
        f"Git: {git_info.get('branch')} @ {git_info.get('commit')}",
        f"Host: {pacote.get('ambiente', {}).get('host')}",
        f"Usuario: {pacote.get('ambiente', {}).get('usuario')}",
        "",
        "ULTIMO ERRO",
        f"Rota: {ultimo.get('path') or ultimo.get('endpoint') or '-'}",
        f"Tipo: {ultimo.get('tipo') or '-'}",
        f"Mensagem: {ultimo.get('mensagem') or '-'}",
        f"Quando: {ultimo.get('quando') or '-'}",
        "",
        "STACK RESUMIDO",
        str(ultimo.get("stack") or "-")[-3500:],
        "",
        "ROTAS MONITORADAS",
    ]
    for item in pacote.get("tempo_resposta") or []:
        linhas.append(
            f"- {item.get('rota')}: {item.get('ultimo_ms')}ms, status {item.get('status') or '-'}, "
            f"{item.get('tendencia_label') or item.get('tendencia') or '-'}"
        )
    linhas.extend(
        [
            "",
            "STATUS SISTEMA",
            json.dumps(pacote.get("status_sistema") or {}, ensure_ascii=False, indent=2, default=sanitizar_para_json),
            "",
            "ERROS ABERTOS",
            json.dumps(pacote.get("erros_abertos") or [], ensure_ascii=False, indent=2, default=sanitizar_para_json),
            "",
            "INCONSISTENCIAS DE NEGOCIO",
            json.dumps(pacote.get("inconsistencias_negocio") or [], ensure_ascii=False, indent=2, default=sanitizar_para_json),
        ]
    )
    return "\n".join(linhas)


def enviar_alerta_telegram_auto_suporte(texto):
    config = obter_configuracao_empresa(force=True)
    token = normalizar_texto_campo(config.get("auto_teste_telegram_bot_token"))
    chat_id = normalizar_texto_campo(config.get("auto_teste_telegram_chat_id"))
    if not token:
        raise ValueError("Token do Telegram nao configurado no Auto teste.")
    chat_id = resolver_chat_id_telegram(token, chat_id)
    if not chat_id:
        raise ValueError("Chat ID nao configurado. Envie /start para o bot e teste novamente.")
    send_site_monitor_telegram_message(token, chat_id, texto, 15)
    return chat_id


def status_auto_suporte(force=False):
    chave_cache = chave_cache_auto_suporte()
    agora_cache_ts = time.time()
    if (
        not force
        and AUTO_SUPORTE_STATUS_CACHE.get("resultado") is not None
        and AUTO_SUPORTE_STATUS_CACHE.get("chave") == chave_cache
        and agora_cache_ts - float(AUTO_SUPORTE_STATUS_CACHE.get("testado_em") or 0.0) < AUTO_SUPORTE_STATUS_CACHE_TTL
    ):
        return deepcopy(AUTO_SUPORTE_STATUS_CACHE["resultado"])

    status = montar_status_sistema_dono()
    fluxos = executar_check_auto_suporte(
        detectar_fluxos_suspeitos_auto_suporte,
        [],
        "auto_suporte_fluxos",
        {"auto_suporte_fluxos", "pacote_codex_fluxos"},
    )
    planilhas_erro = executar_check_auto_suporte(
        listar_planilhas_com_erro_auto_suporte,
        [],
        "auto_suporte_planilhas_erro",
        {"auto_suporte_planilhas_erro", "pacote_codex_planilhas"},
    )
    inconsistencias_negocio = executar_check_auto_suporte(
        detectar_inconsistencias_negocio_auto_suporte,
        [],
        "auto_suporte_negocio",
        {"auto_suporte_negocio"},
    )
    tempo_resposta = metricas_tempo_resposta_central_tecnica()
    erros_abertos = listar_erros_producao(apenas_abertos=True, limite=8)
    falhas = list(status.get("resumo", {}).get("falhas") or [])
    if fluxos:
        falhas.append("Atendimento duplicado suspeito")
    if planilhas_erro:
        falhas.append("Planilha com erro")
    if any(item.get("classe") == "lento" for item in tempo_resposta):
        falhas.append("Pagina demorou")
    if erros_abertos:
        falhas.append("Erro 500 aberto")
    if inconsistencias_negocio:
        falhas.append("Inconsistencia de negocio")

    diagnostico = montar_diagnostico_auto_suporte(status, fluxos, planilhas_erro, tempo_resposta, erros_abertos, inconsistencias_negocio)
    if diagnostico.get("nivel") in {"critico", "alerta"} and diagnostico.get("titulo") not in falhas:
        falhas.insert(0, diagnostico.get("titulo"))
    ok_status = not falhas and diagnostico.get("nivel") not in {"critico", "alerta"}
    resposta = {
        "ok": ok_status,
        "gerado_em": agora_iso(),
        "falhas": falhas,
        "diagnostico": diagnostico,
        "ultimo_erro": dict(ULTIMO_ERRO_PRODUCAO),
        "erros_abertos": erros_abertos,
        "fluxos_suspeitos": fluxos,
        "planilhas_erro": planilhas_erro,
        "inconsistencias_negocio": inconsistencias_negocio,
        "tempo_resposta": tempo_resposta,
        "sugestoes": montar_sugestoes_auto_suporte(status, fluxos, planilhas_erro, tempo_resposta, erros_abertos, inconsistencias_negocio),
        "historico": listar_historico_auto_suporte(limite=10),
        "auto_abrir": bool(diagnostico.get("auto_abrir")),
        "acoes": montar_acoes_auto_suporte_payload(),
        "mensagem": diagnostico.get("frase") or ("Sistema sem incidentes criticos." if not falhas else "AutoSuporte encontrou pontos para revisar."),
    }
    resposta["autonomia"] = montar_autonomia_auto_suporte(status_payload=resposta)
    resposta["plano_acao"] = montar_plano_acao_auto_suporte(resposta)
    resposta["perfil"] = perfil_auto_suporte() if has_request_context() else "desenvolvedor"
    resposta["modo_interface"] = "tecnico" if resposta["perfil"] == "desenvolvedor" else "simples"
    resposta["acoes_simples"] = montar_acoes_simples_auto_suporte(resposta)
    avaliar_alertas_auto_suporte(resposta)
    resposta["historico"] = listar_historico_auto_suporte(limite=10)
    resposta["narrativa"] = montar_narrativa_auto_suporte(resposta)
    AUTO_SUPORTE_STATUS_CACHE["testado_em"] = agora_cache_ts
    AUTO_SUPORTE_STATUS_CACHE["chave"] = chave_cache
    AUTO_SUPORTE_STATUS_CACHE["resultado"] = deepcopy(resposta)
    return resposta


def executar_acao_auto_suporte(acao, observacao="", confirmacao=""):
    acao = normalizar_texto_campo(acao)
    observacao = normalizar_texto_campo(observacao)
    if acao not in ACOES_AUTO_SUPORTE:
        raise ValueError("Acao de AutoSuporte nao permitida.")
    validar_permissao_acao_auto_suporte(acao)
    validar_confirmacao_acao_auto_suporte(acao, confirmacao)
    limpar_cache_auto_suporte()

    detalhes = {"acao": acao, "observacao": observacao}
    mensagem = ""
    severidade = "info"

    if acao == "limpar_caches":
        limpar_caches_interface()
        mensagem = "Caches da interface, painel, clientes, relatorios, banco e configuracoes foram limpos."
    elif acao == "limpar_cache_rota_lenta":
        resultado = limpar_cache_rota_lenta_auto_suporte()
        detalhes["cache_rota_lenta"] = resultado
        mensagem = resultado.get("mensagem") or "Cache especifico da rota lenta foi limpo."
    elif acao == "revalidar_rota_lenta":
        resultado = revalidar_rota_lenta_auto_suporte(observacao)
        detalhes["rota_lenta"] = resultado
        mensagem = resultado.get("mensagem") or "Rota lenta revalidada."
        severidade = "info" if resultado.get("ok") else "warning"
    elif acao == "validar_ambiente":
        checklist = montar_pre_deploy_checklist()
        falhas = [item["nome"] for item in checklist if not item.get("ok")]
        detalhes["falhas"] = falhas
        mensagem = "Ambiente validado sem pendencias." if not falhas else "Pendencias: " + ", ".join(falhas)
        severidade = "warning" if falhas else "info"
    elif acao == "testar_banco":
        inicio = time.perf_counter()
        status = diagnosticar_banco_online(force=True)
        tempo_ms = int((time.perf_counter() - inicio) * 1000)
        status["tempo_ms"] = tempo_ms
        detalhes["banco"] = status
        if status.get("conectado"):
            mensagem = (
                f"Banco respondeu em {tempo_ms / 1000:.1f}s. "
                + ("Esta acima do ideal; recomendo revisar a conexao online." if tempo_ms > 2000 else "Conexao online validada.")
            )
        else:
            mensagem = status.get("mensagem") or "Banco indisponivel."
        severidade = "info" if status.get("conectado") else "warning"
    elif acao == "testar_backup":
        backup = obter_status_backup_banco()
        detalhes["backup"] = backup
        ultimo = backup.get("ultimo_backup") or backup.get("ultimo") or {}
        mensagem = (
            f"Backup validado. Ultimo arquivo: {ultimo.get('nome') or backup.get('ultimo_nome') or 'verificar configuracao'}."
            if backup.get("ok") or backup.get("existe") or ultimo
            else "Backup nao encontrado ou precisa de configuracao."
        )
        severidade = "info" if (backup.get("ok") or ultimo) else "warning"
    elif acao == "testar_telegram":
        chat_id = enviar_alerta_telegram_auto_suporte(
            "AutoSuporte Wagen Estetica\nTeste manual do bot concluido com sucesso."
        )
        detalhes["chat_id"] = chat_id
        detalhes["telegram_enviado"] = True
        mensagem = "Mensagem de teste enviada para o Telegram."
    elif acao == "revalidar_pwa":
        checks = [
            testar_rota_interna_status("Manifest PWA", "/site.webmanifest", 200),
            testar_rota_interna_status("Service worker", "/sw.js", 200),
            testar_rota_interna_status("Status PWA", "/api/pwa/status", 200),
        ]
        detalhes["pwa"] = checks
        falhas = [item.get("nome") for item in checks if not item.get("ok")]
        mensagem = "PWA, manifest e service worker revalidados." if not falhas else "Falhas PWA: " + ", ".join(falhas)
        severidade = "info" if not falhas else "warning"
    elif acao == "revalidar_estaticos":
        resultado = revalidar_estaticos_auto_suporte()
        detalhes["estaticos"] = resultado
        mensagem = resultado.get("mensagem") or "Arquivos estaticos revalidados."
        severidade = "info" if resultado.get("ok") else "warning"
    elif acao == "resolver_erros_com_checks_ok":
        resultado = resolver_erros_com_checks_ok_auto_suporte()
        detalhes["checks_resolvidos"] = resultado
        mensagem = resultado.get("mensagem") or "Checks saudaveis foram reavaliados."
    elif acao == "gerar_backup_suporte":
        sucesso, msg, destino = criar_backup_banco(force=True, tipo_backup="completo")
        detalhes.update({"sucesso": bool(sucesso), "destino": destino})
        mensagem = msg or ("Backup de suporte gerado." if sucesso else "Backup de suporte falhou.")
        severidade = "info" if sucesso else "error"
    elif acao == "desativar_planilhas_com_erro":
        total = desativar_planilhas_com_erro_auto_suporte()
        limpar_cache_clientes()
        detalhes["planilhas_pausadas"] = total
        mensagem = f"{total} planilha(s) com erro foram pausadas temporariamente."
    elif acao == "corrigir_classificacao_clientes":
        resultado = corrigir_classificacao_clientes_auto_suporte()
        detalhes["classificacao_clientes"] = resultado
        total = int(resultado.get("novos_corrigidos_para_retorno") or 0)
        mensagem = f"{total} atendimento(s) marcado(s) como NOVO com historico foram corrigidos para RETORNO."
        severidade = "info" if total >= 0 else "warning"
    elif acao == "limpar_erros_resolvidos":
        total = limpar_erros_producao_resolvidos()
        detalhes["erros_removidos"] = total
        mensagem = f"{total} erro(s) resolvido(s) foram removido(s) da Central de erros."
    elif acao == "limpar_todos_erros":
        total = limpar_todos_erros_producao(usuario=session.get("usuario") if has_request_context() else "")
        detalhes["erros_removidos"] = total
        mensagem = f"{total} erro(s) foram limpo(s) da Central de erros."
    elif acao == "gerar_pacote_codex":
        pacote = montar_pacote_codex_auto_suporte()
        detalhes["pacote_codex"] = pacote
        mensagem = "Pacote tecnico gerado. Copie ou baixe o relatorio e envie aqui no Codex."
    elif acao == "enviar_relatorio_telegram":
        pacote = montar_pacote_codex_auto_suporte()
        resumo = (
            "Relatorio AutoSuporte Wagen Estetica\n"
            f"Gerado em: {pacote.get('gerado_em')}\n"
            f"Erros abertos: {len(pacote.get('erros_abertos') or [])}\n"
            f"Ultimo erro: {(pacote.get('ultimo_erro') or {}).get('tipo') or '-'}\n"
            f"Versao: {pacote.get('ambiente', {}).get('versao_sistema') or '-'}"
        )
        chat_id = enviar_alerta_telegram_auto_suporte(resumo)
        detalhes["chat_id"] = chat_id
        detalhes["telegram_enviado"] = True
        detalhes["pacote_codex"] = pacote
        mensagem = "Relatorio tecnico enviado para o Telegram."
    elif acao == "registrar_incidente":
        mensagem = observacao or "Incidente registrado manualmente pelo AutoSuporte."
        severidade = "warning"
    elif acao == "enviar_alerta_telegram":
        texto = observacao or "AutoSuporte executado: revisar o status do sistema."
        chat_id = enviar_alerta_telegram_auto_suporte(f"AutoSuporte Wagen Estetica\n{texto}")
        detalhes["chat_id"] = chat_id
        detalhes["telegram_enviado"] = True
        mensagem = "Alerta enviado para o Telegram."
    elif acao == "marcar_fluxo_suspeito":
        fluxos = detectar_fluxos_suspeitos_auto_suporte()
        detalhes["fluxos_suspeitos"] = fluxos
        mensagem = (
            f"{len(fluxos)} fluxo(s) suspeito(s) encontrado(s)."
            if fluxos else
            "Nenhum fluxo duplicado em andamento foi encontrado."
        )
        severidade = "warning" if fluxos else "info"

    detalhes_incidente = dict(detalhes)
    if "pacote_codex" in detalhes_incidente:
        pacote = detalhes_incidente.pop("pacote_codex") or {}
        detalhes_incidente["pacote_codex_resumo"] = {
            "gerado_em": pacote.get("gerado_em"),
            "ultimo_erro": (pacote.get("ultimo_erro") or {}).get("id"),
            "erros_abertos": len(pacote.get("erros_abertos") or []),
        }
    detalhes_incidente["telegram_enviado"] = bool(detalhes.get("telegram_enviado"))
    registrar_incidente_auto_suporte(ACOES_AUTO_SUPORTE[acao], mensagem, detalhes=detalhes_incidente, severidade=severidade)
    return {
        "ok": severidade != "error",
        "acao": acao,
        "titulo": ACOES_AUTO_SUPORTE[acao],
        "mensagem": mensagem,
        "detalhes": detalhes,
        "status": status_auto_suporte(force=True),
    }


@app.route("/api/auto-suporte/status")
def api_auto_suporte_status():
    if not usuario_pode_usar_auto_suporte():
        return jsonify({"erro": "nao_autorizado"}), 403
    return jsonify(json.loads(json.dumps(status_auto_suporte(), ensure_ascii=False, default=str)))


@app.route("/auto-suporte")
def pagina_auto_suporte():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_pode_usar_auto_suporte():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem acessar o AutoSuporte.")
        return redirect("/")

    status_auto = status_auto_suporte()
    status_auto.setdefault("autonomia", montar_autonomia_auto_suporte())
    status_auto.setdefault("perfil", perfil_auto_suporte())
    status_auto.setdefault("modo_interface", "tecnico" if status_auto.get("perfil") == "desenvolvedor" else "simples")
    status_auto.setdefault("acoes", montar_acoes_auto_suporte_payload())
    status_auto.setdefault("acoes_simples", montar_acoes_simples_auto_suporte(status_auto))
    status_auto.setdefault("plano_acao", montar_plano_acao_auto_suporte(status_auto))
    status_auto.setdefault("narrativa", montar_narrativa_auto_suporte(status_auto))
    status_sistema = montar_status_sistema_dono()
    historico = listar_historico_auto_suporte(limite=80)
    saude_dono = montar_saude_sistema_dono_auto_suporte(status_sistema, status_auto)
    return render_template(
        "auto_suporte.html",
        status_auto=status_auto,
        status_sistema=status_sistema,
        modo_auto_suporte=perfil_auto_suporte(),
        saude_dono=saude_dono,
        historico_auto_suporte=historico,
        incidentes_por_dia=agrupar_historico_auto_suporte_por_dia(historico),
        dialogo_ia=montar_dialogo_ia_auto_suporte(status_auto, saude_dono, historico),
    )


@app.route("/auto-suporte.json")
def auto_suporte_json():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_pode_usar_auto_suporte():
        return jsonify({"erro": "nao_autorizado"}), 403
    status_auto = status_auto_suporte()
    status_auto.setdefault("autonomia", montar_autonomia_auto_suporte())
    status_auto.setdefault("perfil", perfil_auto_suporte())
    status_auto.setdefault("modo_interface", "tecnico" if status_auto.get("perfil") == "desenvolvedor" else "simples")
    status_auto.setdefault("acoes_simples", montar_acoes_simples_auto_suporte(status_auto))
    status_sistema = montar_status_sistema_dono()
    historico = listar_historico_auto_suporte(limite=80)
    payload = {
        "status_auto": status_auto,
        "status_sistema": status_sistema,
        "saude_dono": montar_saude_sistema_dono_auto_suporte(status_sistema, status_auto),
        "historico": historico,
        "incidentes_por_dia": agrupar_historico_auto_suporte_por_dia(historico),
    }
    return jsonify(json.loads(json.dumps(payload, ensure_ascii=False, default=sanitizar_para_json)))


@app.route("/auto-suporte/acao", methods=["POST"])
def pagina_auto_suporte_acao():
    if not session.get("usuario"):
        return redirect("/login")
    redirect_to = normalizar_redirect_interno(request.form.get("redirect_to"), "/auto-suporte")
    if not usuario_pode_usar_auto_suporte():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem executar o AutoSuporte.")
        return redirect("/")
    try:
        resultado = executar_acao_auto_suporte(
            request.form.get("acao"),
            observacao=request.form.get("observacao") or "",
            confirmacao=request.form.get("confirmacao") or "",
        )
        session["diagnostico_feedback"] = {
            "tipo": "sucesso" if resultado.get("ok") else "erro",
            "mensagem": resultado.get("mensagem") or "Acao executada pelo AutoSuporte.",
        }
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="auto_suporte_pagina_acao")
        session["diagnostico_feedback"] = {
            "tipo": "erro",
            "mensagem": f"Nao foi possivel executar a acao: {erro}",
        }
    return redirect(redirect_to)


@app.route("/api/auto-suporte/acao", methods=["POST"])
def api_auto_suporte_acao():
    if not usuario_pode_usar_auto_suporte():
        return jsonify({"erro": "nao_autorizado"}), 403

    payload = request.get_json(silent=True) or request.form or {}
    try:
        resultado = executar_acao_auto_suporte(
            payload.get("acao"),
            observacao=payload.get("observacao") or "",
            confirmacao=payload.get("confirmacao") or "",
        )
        return jsonify(json.loads(json.dumps(resultado, ensure_ascii=False, default=str)))
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="auto_suporte_acao")
        return jsonify({"ok": False, "erro": str(erro)}), 400


@app.route("/api/auto-suporte/autonomia", methods=["POST"])
def api_auto_suporte_autonomia():
    if not usuario_pode_usar_auto_suporte():
        return jsonify({"erro": "nao_autorizado"}), 403
    if not usuario_auto_suporte_tecnico():
        return jsonify({"erro": "disponivel_somente_para_desenvolvedor"}), 403

    payload = request.get_json(silent=True) or request.form or {}
    try:
        modo = payload.get("modo")
        if modo:
            modo = salvar_modo_autonomia_auto_suporte(modo)
        resultado = executar_autonomia_auto_suporte(
            modo=modo,
            simular=bool(payload.get("simular")),
        )
        return jsonify(json.loads(json.dumps(resultado, ensure_ascii=False, default=sanitizar_para_json)))
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="auto_suporte_autonomia")
        return jsonify({"ok": False, "erro": str(erro)}), 400


@app.route("/api/auto-suporte/pacote-codex")
def api_auto_suporte_pacote_codex():
    if not usuario_pode_usar_auto_suporte():
        return jsonify({"erro": "nao_autorizado"}), 403
    pacote = montar_pacote_codex_auto_suporte()
    return jsonify(json.loads(json.dumps({"ok": True, "pacote_codex": pacote}, ensure_ascii=False, default=sanitizar_para_json)))


@app.route("/configuracoes/desenvolvedor/erros/<erro_id>/resolver", methods=["POST"])
def resolver_erro_central_tecnica(erro_id):
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente o desenvolvedor pode resolver erros da Central Tecnica.")
        return redirect(destino_configuracoes("desenvolvedor"))

    resolvido = marcar_erro_producao_resolvido(erro_id, usuario=session.get("usuario"))
    if resolvido:
        registrar_auditoria(
            "resolveu_erro_producao",
            "central_tecnica",
            detalhes={"erro_id": erro_id},
            usuario=resumo_usuario_logado(),
        )
        definir_feedback_configuracoes("sucesso", "Erro marcado como resolvido.")
    else:
        definir_feedback_configuracoes("erro", "Erro nao encontrado na Central Tecnica.")
    return redirect(destino_configuracoes("desenvolvedor"))


@app.route("/configuracoes/desenvolvedor/erros/<erro_id>/telegram", methods=["POST"])
def enviar_erro_central_tecnica_telegram(erro_id):
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente o desenvolvedor pode enviar erros da Central Tecnica.")
        return redirect(destino_configuracoes("desenvolvedor"))

    erro_id = normalizar_texto_campo(erro_id)
    erro_item = next((item for item in carregar_erros_producao() if normalizar_texto_campo(item.get("id")) == erro_id), None)
    if not erro_item:
        definir_feedback_configuracoes("erro", "Erro nao encontrado para envio.")
        return redirect(destino_configuracoes("desenvolvedor"))

    texto = (
        "Erro de producao Wagen Estetica\n"
        f"Quando: {erro_item.get('quando')}\n"
        f"Rota: {erro_item.get('path') or erro_item.get('endpoint')}\n"
        f"Usuario: {erro_item.get('usuario') or '-'}\n"
        f"Tipo: {erro_item.get('tipo')}\n"
        f"Mensagem: {erro_item.get('mensagem')}\n"
        f"Stack:\n{str(erro_item.get('stack') or '')[-1200:]}"
    )
    try:
        enviar_alerta_telegram_auto_suporte(texto)
        definir_feedback_configuracoes("sucesso", "Erro enviado para o Telegram.")
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="central_erros_telegram")
        definir_feedback_configuracoes("erro", f"Nao foi possivel enviar para o Telegram: {erro}")
    return redirect(destino_configuracoes("desenvolvedor"))


@app.route("/configuracoes/desenvolvedor/erros/limpar-resolvidos", methods=["POST"])
def limpar_erros_resolvidos_central_tecnica():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente o desenvolvedor pode limpar erros resolvidos.")
        return redirect(destino_configuracoes("desenvolvedor"))

    removidos = limpar_erros_producao_resolvidos()
    if removidos:
        registrar_auditoria(
            "limpou_erros_resolvidos",
            "central_tecnica",
            detalhes={"removidos": removidos},
            usuario=resumo_usuario_logado(),
        )
        definir_feedback_configuracoes("sucesso", f"{removidos} erro(s) resolvido(s) removido(s) da lista.")
    else:
        definir_feedback_configuracoes("info", "Nao havia erros resolvidos para limpar.")
    return redirect(destino_configuracoes("desenvolvedor"))


@app.route("/configuracoes/desenvolvedor/relatorio.json")
def exportar_relatorio_tecnico_central():
    if not session.get("usuario"):
        return jsonify({"ok": False, "erro": "login_necessario"}), 401
    if not usuario_desenvolvedor():
        return jsonify({"ok": False, "erro": "acesso_negado"}), 403

    filtro_erros = request.args.get("erros") or "todos"
    relatorio = montar_central_tecnica_desenvolvedor(filtro_erros=filtro_erros)
    return jsonify({"ok": True, "relatorio": relatorio})


@app.route("/diagnostico")
def pagina_diagnostico():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem acessar o diagnostico.")
        return redirect("/configuracoes")

    diagnostico = montar_diagnostico_seguro()
    return render_template(
        "diagnostico.html",
        feedback=session.pop("diagnostico_feedback", None),
        diagnostico=diagnostico,
    )


@app.route("/diagnostico/validar", methods=["POST"])
def validar_diagnostico():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem validar o ambiente.")
        return redirect("/configuracoes")

    itens = montar_pre_deploy_checklist()
    session["ultima_validacao_diagnostico"] = agora_iso()
    falhas = [item["nome"] for item in itens if not item["ok"]]
    if falhas:
        session["diagnostico_feedback"] = {"tipo": "erro", "mensagem": "Pendencias de producao: " + ", ".join(falhas)}
    else:
        session["diagnostico_feedback"] = {"tipo": "sucesso", "mensagem": "Ambiente validado para producao."}
    return redirect("/diagnostico")


@app.route("/diagnostico/exportar.json")
def exportar_diagnostico_json():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_configuracao_sistema():
        return jsonify({"erro": "nao_autorizado"}), 403
    payload = {"gerado_em": agora_iso(), **montar_diagnostico_seguro()}
    return jsonify(json.loads(json.dumps(payload, ensure_ascii=False, default=str)))


@app.route("/pre-deploy.json")
def pre_deploy_json():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_configuracao_sistema():
        return jsonify({"erro": "nao_autorizado"}), 403
    checklist = montar_pre_deploy_checklist()
    payload = {
        "gerado_em": agora_iso(),
        "ok": all(item.get("ok") for item in checklist),
        "checklist": checklist,
        "ultimo_erro": dict(ULTIMO_ERRO_PRODUCAO),
    }
    return jsonify(json.loads(json.dumps(payload, ensure_ascii=False, default=str)))


@app.route("/status-sistema")
def pagina_status_sistema():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem acessar o status do sistema.")
        return redirect("/configuracoes")
    return render_template(
        "status_sistema.html",
        status_sistema=montar_status_sistema_dono(),
    )


@app.route("/status-sistema.json")
def status_sistema_json():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_configuracao_sistema():
        return jsonify({"erro": "nao_autorizado"}), 403
    return jsonify(json.loads(json.dumps(montar_status_sistema_dono(), ensure_ascii=False, default=str)))


@app.route("/diagnostico/backup-suporte", methods=["POST"])
def gerar_backup_suporte():
    if not session.get("usuario"):
        return redirect("/login")
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem gerar backup de suporte.")
        return redirect("/configuracoes")
    sucesso, mensagem, destino = criar_backup_banco(force=True, tipo_backup="completo")
    registrar_auditoria(
        "gerou_backup_suporte",
        "backup",
        detalhes={"sucesso": bool(sucesso), "destino": destino or "", "mensagem": mensagem},
    )
    session["diagnostico_feedback"] = {
        "tipo": "sucesso" if sucesso else "erro",
        "mensagem": mensagem or ("Backup de suporte gerado." if sucesso else "Nao foi possivel gerar backup de suporte."),
    }
    return redirect("/diagnostico")


CONFIGURACOES_SECOES = {
    "meu-acesso": "Meu acesso",
    "sistema": "Sistema",
    "banco": "Banco",
    "auto-teste": "Auto teste",
    "backup": "Backup",
    "usuarios": "Usuarios",
    "desenvolvedor": "Desenvolvedor",
}


def normalizar_secao_configuracoes(secao):
    secao = normalizar_texto_campo(secao).lower().replace("_", "-")
    return secao if secao in CONFIGURACOES_SECOES else "meu-acesso"


def destino_configuracoes(secao="meu-acesso", query=""):
    destino = f"/configuracoes/{normalizar_secao_configuracoes(secao)}"
    return f"{destino}?{query}" if query else destino


@app.route("/configuracoes")
@app.route("/configuracoes/<secao>")
def configuracoes(secao="meu-acesso"):
    if not session.get("usuario"):
        return redirect("/login")

    secao = normalizar_secao_configuracoes(secao)
    preparar_rotinas_interface_logada()
    sincronizar_sessao_usuario_seguro(contexto="VERSAO")
    senha_pendente = bool(session.get("senha_alteracao_obrigatoria"))
    perfil_logado = normalizar_perfil_usuario(
        session.get("usuario_perfil") or (
            "admin" if session.get("usuario") == "admin" else "funcionario"
        )
    )
    pode_gerenciar_usuarios = usuario_gerencia_acessos() and not senha_pendente
    pode_gerenciar_banco_online = usuario_gerencia_banco_online()
    pode_gerenciar_config_sistema = usuario_gerencia_configuracao_sistema() and not senha_pendente
    pode_gerenciar_base = usuario_desenvolvedor() and not senha_pendente
    pode_configurar_hud_usuario = usuario_gerencia_configuracao_sistema() and not senha_pendente
    usuarios = []
    configuracao_empresa = {}
    banco_status = {}
    banco_config = {}
    backup_status = {}
    backup_config = {}
    arquivos_status = {}
    backups_disponiveis = []
    pastas_sync_sugeridas = []
    banco_online_tabelas = {}
    central_tecnica = {}

    if pode_gerenciar_usuarios and secao == "usuarios" and request.args.get("detalhar_usuarios") == "1":
        try:
            usuarios = carregar_usuarios_configuracao()
        except Exception as erro:
            log_info("ERRO CONFIG USUARIOS:", erro)
            usuarios = []

    if pode_gerenciar_banco_online and secao == "banco":
        try:
            banco_status = obter_status_banco_online()
        except Exception as erro:
            log_info("ERRO CONFIG BANCO STATUS:", erro)
            banco_status = {}

        try:
            banco_config = obter_configuracao_banco_form(banco_status)
        except Exception as erro:
            log_info("ERRO CONFIG BANCO FORM:", erro)
            banco_config = {}

    if pode_gerenciar_config_sistema and secao in {"sistema", "desenvolvedor", "auto-teste"}:
        try:
            configuracao_empresa = obter_configuracao_empresa()
        except Exception as erro:
            log_info("ERRO CONFIG EMPRESA:", erro)
            configuracao_empresa = empresa_snapshot_padrao()

    paginas_desabilitadas_config = normalizar_paginas_menu_desabilitadas(
        configuracao_empresa.get("paginas_menu_desabilitadas_json")
    )
    if configuracao_empresa:
        PAGINAS_MENU_CACHE["testado_em"] = time.time()
        PAGINAS_MENU_CACHE["empresa_id"] = normalize_empresa_id(empresa_atual_id())
        PAGINAS_MENU_CACHE["resultado"] = set(paginas_desabilitadas_config)

    if pode_gerenciar_base and secao == "banco" and request.args.get("detalhar_banco") == "1":
        try:
            banco_online_tabelas = listar_tabelas_banco_online(banco_status)
        except Exception as erro:
            log_info("ERRO CONFIG TABELAS BANCO ONLINE:", erro)
            banco_online_tabelas = {
                "disponivel": False,
                "mensagem": f"Nao foi possivel carregar as tabelas do banco online: {erro}",
                "database": "",
                "usuario": "",
                "tabelas": [],
                "quantidade": 0,
            }

    if pode_gerenciar_base and secao == "backup":
        try:
            backup_config = obter_configuracao_backup()
        except Exception as erro:
            log_info("ERRO CONFIG BACKUP:", erro)
            backup_config = configuracao_backup_padrao()

        if request.args.get("detalhar_backup") == "1":
            try:
                backup_status = obter_status_backup_banco()
            except Exception as erro:
                log_info("ERRO CONFIG BACKUP STATUS:", erro)
                backup_status = status_backup_banco_padrao()

            try:
                arquivos_status = obter_status_arquivos()
            except Exception as erro:
                log_info("ERRO CONFIG ARQUIVOS:", erro)
                arquivos_status = status_arquivos_padrao()

            try:
                backups_disponiveis = listar_arquivos_backup_banco()
            except Exception as erro:
                log_info("ERRO CONFIG LISTA BACKUPS:", erro)
                backups_disponiveis = []
        else:
            backup_status = status_backup_banco_padrao()
            arquivos_status = status_arquivos_padrao()
            backups_disponiveis = []

        try:
            pastas_sync_sugeridas = listar_pastas_sincronizadas_sugeridas()
        except Exception as erro:
            log_info("ERRO CONFIG PASTAS SYNC:", erro)
            pastas_sync_sugeridas = []

    if pode_gerenciar_base and secao == "desenvolvedor":
        filtro_erros = request.args.get("erros") or "abertos"
        try:
            central_tecnica = montar_central_tecnica_desenvolvedor(filtro_erros=filtro_erros)
        except Exception as erro:
            registrar_ultimo_erro_producao(erro, descricao="central_tecnica")
            central_tecnica = {
                "gerado_em": agora_iso(),
                "saude": {"resumo": {"ok": False, "falhas": ["Central tecnica"]}, "itens": []},
                "banco": {"backend": "-", "conectado": False, "mensagem": str(erro), "tempo_ms": 0, "tabelas": [], "tabelas_ok": 0, "tabelas_falha": ["central_tecnica"]},
                "rotas": [],
                "caches": [],
                "tempo_resposta": metricas_tempo_resposta_central_tecnica(),
                "metricas_sql": obter_metricas_consultas_sql(limite=60),
                "ranking_rotas_lentas": [],
                "erros_abertos": listar_erros_producao(apenas_abertos=True, limite=12),
                "erros_recentes": listar_erros_producao(limite=12, filtro=filtro_erros),
                "erros_filtro": normalizar_filtro_erros_producao(filtro_erros),
                "erros_contagem": contar_erros_producao_por_status(),
                "rotas_alerta_2s": [],
                "paginas_alerta_2s": [],
                "paginas_pioraram": [],
                "resumo": {"ok": False, "rotas_testadas": 0, "rotas_falha": 0, "rotas_lentas": 0, "rotas_alerta_2s": 0, "paginas_lentas": 0, "paginas_alerta_2s": 0, "paginas_pioraram": 0, "erros_abertos": 0, "erros_resolvidos": 0, "erros_total": 0, "caches_ativos": 0, "tabelas_ok": 0},
            }

    return render_template(
        "configuracoes.html",
        feedback=session.pop("configuracoes_feedback", None),
        secao_configuracoes=secao,
        secoes_configuracoes=[
            {"id": chave, "label": label}
            for chave, label in CONFIGURACOES_SECOES.items()
            if (
                chave == "meu-acesso"
                or (chave in {"sistema", "banco", "auto-teste", "usuarios"} and usuario_gerencia_configuracao_sistema())
                or (chave in {"backup", "desenvolvedor"} and usuario_desenvolvedor())
            )
        ],
        usuario_logado={
            "id": session.get("usuario_id"),
            "usuario": session.get("usuario"),
            "nome": session.get("usuario_nome") or session.get("usuario"),
            "iniciais": session.get("usuario_iniciais") or obter_iniciais_usuario(
                session.get("usuario_nome"),
                session.get("usuario"),
            ),
            "foto_url": session.get("usuario_foto_url") or "",
            "perfil": perfil_logado,
            "perfil_label": rotulo_perfil_usuario(perfil_logado),
            "senha_alteracao_obrigatoria": senha_pendente,
        },
        usuarios=usuarios,
        gerencia_usuarios_logado=pode_gerenciar_usuarios,
        configuracao_sistema_logado=pode_gerenciar_config_sistema,
        desenvolvedor_logado=pode_gerenciar_base,
        banco_online_logado=pode_gerenciar_banco_online,
        hud_usuario_logado=pode_configurar_hud_usuario,
        hud_usuario_config=obter_configuracao_hud_usuario() if pode_configurar_hud_usuario and secao == "meu-acesso" else configuracao_hud_usuario_padrao(),
        hud_usuario_itens=montar_itens_hud_configuracao_usuario() if pode_configurar_hud_usuario and secao == "meu-acesso" else [],
        configuracao_empresa=configuracao_empresa,
        banco_status=banco_status,
        banco_config=banco_config,
        backup_status=backup_status,
        backup_config=backup_config,
        frequencias_backup=FREQUENCIAS_BACKUP,
        tipos_backup=TIPOS_BACKUP,
        backups_disponiveis=backups_disponiveis,
        checklist_recuperacao_backup=CHECKLIST_RECUPERACAO_BACKUP,
        arquivos_status=arquivos_status,
        pastas_sync_sugeridas=pastas_sync_sugeridas,
        banco_online_tabelas=banco_online_tabelas,
        central_tecnica=central_tecnica,
        paginas_menu_configuracao=montar_paginas_menu_configuracao(
            paginas_desabilitadas_config
        ) if pode_gerenciar_base else [],
    )

@app.route("/configuracoes/versao", methods=["POST"])
def salvar_configuracao_versao():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario_seguro(contexto="TROCA SENHA OBRIGATORIA")
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem alterar a versao do sistema.")
        return redirect("/configuracoes")

    try:
        versao = salvar_configuracao_versao_form(request.form)
    except Exception as e:
        definir_feedback_configuracoes("erro", f"Nao foi possivel salvar a versao do sistema: {e}")
        return redirect("/configuracoes")

    registrar_auditoria_assincrona(
        "atualizou_versao_sistema",
        "configuracao_empresa",
        detalhes={
            "versao_sistema": versao,
        },
    )
    definir_feedback_configuracoes(
        "sucesso",
        f"Versao do sistema atualizada para {formatar_versao_sistema(versao)}.",
    )
    return redirect(destino_configuracoes("sistema"))

@app.route("/configuracoes/clima", methods=["POST"])
def salvar_configuracao_clima():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem alterar o clima do sistema.")
        return redirect("/configuracoes")

    try:
        clima = salvar_configuracao_clima_form(request.form)
    except ValueError as e:
        definir_feedback_configuracoes("erro", str(e))
        return redirect("/configuracoes")
    except Exception as e:
        definir_feedback_configuracoes("erro", f"Nao foi possivel salvar a configuracao do clima: {e}")
        return redirect("/configuracoes")

    registrar_auditoria(
        "atualizou_configuracao_clima",
        "configuracao_empresa",
        detalhes={
            "clima_ativo": clima.get("clima_ativo"),
            "clima_api_url": clima.get("clima_api_url"),
            "clima_local_label": clima.get("clima_local_label"),
            "clima_latitude": clima.get("clima_latitude"),
            "clima_longitude": clima.get("clima_longitude"),
            "clima_timezone": clima.get("clima_timezone"),
            "clima_timeout_segundos": clima.get("clima_timeout_segundos"),
        },
    )
    definir_feedback_configuracoes(
        "sucesso",
        (
            f"Configuracao do clima salva para {clima.get('clima_local_label')}. "
            "O HUD atualiza automaticamente em ate 60 segundos."
        ),
    )
    return redirect(destino_configuracoes("sistema"))


@app.route("/configuracoes/auto-teste", methods=["POST"])
def salvar_configuracao_auto_teste():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem configurar o auto teste.")
        return redirect(destino_configuracoes("meu-acesso"))

    try:
        config = salvar_configuracao_auto_teste_form(request.form)
    except Exception as erro:
        definir_feedback_configuracoes("erro", f"Nao foi possivel salvar o auto teste: {erro}")
        return redirect(destino_configuracoes("auto-teste"))

    registrar_auditoria_assincrona(
        "configurou_auto_teste",
        "configuracao_empresa",
        detalhes={
            "ativo": bool(config.get("auto_teste_ativo")),
            "site_url": config.get("auto_teste_site_url"),
            "bot_nick": config.get("auto_teste_telegram_bot_nick"),
            "chat_id_configurado": bool(config.get("auto_teste_telegram_chat_id")),
            "token_configurado": bool(config.get("auto_teste_telegram_bot_token")),
        },
    )
    definir_feedback_configuracoes("sucesso", "Auto teste salvo. Use o botao de teste para confirmar o Telegram.")
    return redirect(destino_configuracoes("auto-teste"))


@app.route("/configuracoes/auto-teste/testar", methods=["POST"])
def testar_configuracao_auto_teste():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_configuracao_sistema():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem testar o monitor.")
        return redirect(destino_configuracoes("meu-acesso"))

    try:
        config = salvar_configuracao_auto_teste_form(request.form)
        resultado = executar_auto_teste_site(config, enviar_telegram=True)
    except Exception as erro:
        salvar_resultado_auto_teste_seguro("erro", f"Erro no teste manual: {erro}")
        definir_feedback_configuracoes("erro", f"Nao foi possivel enviar o teste: {erro}")
        return redirect(destino_configuracoes("auto-teste"))

    registrar_auditoria_assincrona(
        "testou_auto_teste",
        "configuracao_empresa",
        detalhes={
            "status": resultado.get("status"),
            "chat_id_configurado": bool(resultado.get("chat_id")),
        },
    )
    definir_feedback_configuracoes(
        "sucesso" if resultado.get("ok") else "erro",
        "Teste enviado ao Telegram. Resultado: " + ("OK." if resultado.get("ok") else "falha no site."),
    )
    return redirect(destino_configuracoes("auto-teste"))


@app.route("/configuracoes/paginas", methods=["POST"])
def salvar_configuracao_paginas():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem habilitar ou desabilitar paginas.")
        return redirect("/configuracoes")

    try:
        desabilitadas = salvar_paginas_menu_configuracao_form(request.form)
    except Exception as erro:
        definir_feedback_configuracoes("erro", f"Nao foi possivel salvar as paginas do menu: {erro}")
        return redirect("/configuracoes")

    registrar_auditoria(
        "atualizou_paginas_menu",
        "configuracao_empresa",
        detalhes={
            "paginas_desabilitadas": desabilitadas,
        },
    )
    definir_feedback_configuracoes(
        "sucesso",
        "Paginas do menu atualizadas. O que foi desabilitado some da sidebar e fica bloqueado para acessos comuns.",
    )
    return redirect(destino_configuracoes("desenvolvedor"))


@app.route("/configuracoes/hud", methods=["POST"])
def salvar_configuracao_hud_usuario():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_configuracao_sistema() or session.get("senha_alteracao_obrigatoria"):
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem alterar o HUD do proprio acesso.")
        return redirect("/configuracoes")

    try:
        config = salvar_configuracao_hud_usuario_form(request.form)
    except Exception as erro:
        definir_feedback_configuracoes("erro", f"Nao foi possivel salvar o HUD do usuario: {erro}")
        return redirect("/configuracoes")

    registrar_auditoria(
        "atualizou_hud_usuario",
        "usuario",
        entidade_id=session.get("usuario_id"),
        detalhes={
            "hud_ativo": config.get("hud_ativo"),
            "itens_habilitados": config.get("itens_habilitados"),
        },
    )
    definir_feedback_configuracoes(
        "sucesso",
        "Preferencias do HUD salvas para o seu usuario. Isso nao altera o HUD de outros acessos.",
    )
    return redirect(destino_configuracoes("meu-acesso"))


@app.route("/configuracoes/site", methods=["GET", "POST"])
def configuracoes_site():
    if not session.get("usuario"):
        return redirect("/login")

    if request.method == "GET":
        preparar_rotinas_interface_logada()
    sincronizar_sessao_usuario(force=request.method == "GET")
    if not usuario_gerencia_configuracao_sistema() or session.get("senha_alteracao_obrigatoria"):
        definir_feedback_configuracoes(
            "erro",
            "Somente administradores ou desenvolvedores com senha regularizada podem alterar a identidade visual do sistema.",
        )
        return redirect("/configuracoes")
    bloqueio = bloquear_recurso_plano(
        "whitelabel",
        "White-label e identidade visual personalizada exigem plano Pro ou Business.",
        destino="/configuracoes",
    )
    if bloqueio:
        return bloqueio

    if request.method == "POST":
        try:
            site = salvar_configuracao_site_form(request.form, request.files)
        except ValueError as erro:
            definir_feedback_configuracoes("erro", str(erro))
            return redirect("/configuracoes/site")
        except Exception as erro:
            definir_feedback_configuracoes("erro", f"Nao foi possivel salvar as configuracoes do site: {erro}")
            return redirect("/configuracoes/site")

        registrar_auditoria(
            "atualizou_configuracoes_site",
            "configuracao_empresa",
            detalhes={
                "marca_nome": site.get("marca_nome"),
                "marca_subtitulo": site.get("marca_subtitulo"),
                "site_titulo": site.get("site_titulo"),
                "login_titulo_publico": site.get("login_titulo_publico"),
                "home_estado_inicial_titulo": site.get("home_estado_inicial_titulo"),
                "whitelabel_ativo": site.get("whitelabel_ativo"),
                "tem_logo_blob": site.get("tem_logo_blob"),
                "tem_favicon_blob": site.get("tem_favicon_blob"),
            },
        )
        definir_feedback_configuracoes(
            "sucesso",
            "Configuracoes do site atualizadas. O branding novo ja passa a valer nas telas e abas do sistema.",
        )
        return redirect("/configuracoes/site")

    try:
        configuracao_empresa = obter_configuracao_empresa()
    except Exception as erro:
        log_info("ERRO CONFIG SITE:", erro)
        configuracao_empresa = empresa_snapshot_padrao()

    return render_template(
        "configuracoes_site.html",
        feedback=session.pop("configuracoes_feedback", None),
        configuracao_empresa=configuracao_empresa,
    )

@app.route("/configuracoes/banco", methods=["POST"])
def salvar_configuracao_banco():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_banco_online():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem alterar o banco online.")
        return redirect("/configuracoes")

    try:
        status = salvar_configuracao_banco_form(request.form)
    except ValueError as e:
        definir_feedback_configuracoes("erro", str(e))
        return redirect("/configuracoes")
    except Exception as e:
        definir_feedback_configuracoes(
            "erro",
            f"Nao foi possivel salvar a configuracao do banco online: {e}",
        )
        return redirect("/configuracoes")

    registrar_auditoria_assincrona(
        "atualizou_banco_online",
        "banco",
        detalhes={
            "modo": status.get("modo"),
            "conectado": bool(status.get("conectado")),
            "host": status.get("host"),
            "database": status.get("database"),
        },
    )

    if status.get("conectado"):
        mensagem = "Configuracao do banco salva e conexao com o Supabase validada com sucesso."
        if status.get("migracao_automatica"):
            mensagem += " Os dados locais foram migrados automaticamente."
        elif status.get("mensagem_migracao"):
            mensagem += status.get("mensagem_migracao")
        definir_feedback_configuracoes(
            "sucesso",
            mensagem,
        )
    else:
        definir_feedback_configuracoes(
            "erro",
            f"Configuracao salva, mas a conexao online ainda nao respondeu: {status.get('mensagem')}",
        )

    return redirect("/configuracoes")

@app.route("/configuracoes/banco/testar", methods=["POST"])
def testar_configuracao_banco():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_banco_online():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem testar o banco online.")
        return redirect("/configuracoes")

    status = diagnosticar_banco_online(force=True)
    registrar_auditoria(
        "testou_banco_online",
        "banco",
        detalhes={
            "conectado": bool(status.get("conectado")),
            "mensagem": status.get("mensagem"),
        },
    )

    if status.get("conectado"):
        definir_feedback_configuracoes("sucesso", "Conexao com o Supabase validada com sucesso.")
    else:
        definir_feedback_configuracoes("erro", status.get("mensagem") or "Nao foi possivel validar a conexao online.")

    return redirect("/configuracoes")

@app.route("/configuracoes/banco/migrar", methods=["POST"])
def migrar_banco_para_supabase():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_banco_online():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem migrar o banco para o Supabase.")
        return redirect("/configuracoes")

    status = diagnosticar_banco_online(force=True)
    if not status.get("conectado"):
        definir_feedback_configuracoes(
            "erro",
            "Antes de migrar, a conexao com o Supabase precisa estar ativa.",
        )
        return redirect("/configuracoes")

    try:
        garantir_schema_banco_online(force=True)
        importar_sqlite_para_banco_atual(caminho_banco_absoluto())
        criar_backup_banco(force=True, tipo_backup="banco")
        salvar_env_local({
            "DATABASE_ONLINE_MIGRADO": "1",
        })
        atualizar_configuracao_banco_runtime(migrado="1")
        limpar_cache_banco_online()
        limpar_caches_interface()
        registrar_auditoria(
            "migracao_banco_online",
            "banco",
            detalhes={
                "origem": caminho_banco_absoluto(),
                "destino": "supabase",
            },
        )
        definir_feedback_configuracoes(
            "sucesso",
            "Dados do SQLite local migrados para o Supabase com sucesso. O sistema ja pode operar online.",
        )
    except Exception as e:
        definir_feedback_configuracoes(
            "erro",
            f"Nao consegui migrar os dados para o Supabase: {e}",
        )

    return redirect("/configuracoes")

@app.route("/configuracoes/backup", methods=["POST"])
def salvar_configuracao_backup():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem alterar a rotina de backup.")
        return redirect("/configuracoes")

    destino_externo_ativo = 1 if bool_config_ativo(request.form.get("destino_externo_ativo")) else 0
    destino_externo_tipo = normalizar_tipo_destino_backup(request.form.get("destino_externo_tipo"))
    destino_externo_pasta = normalizar_caminho_destino_externo(
        request.form.get("destino_externo_pasta"),
    )
    destino_externo_drive_folder_id = normalizar_texto_campo(
        request.form.get("destino_externo_drive_folder_id")
    )

    if destino_externo_ativo:
        bloqueio = bloquear_recurso_plano(
            "backup_online",
            "Backup online exige plano Pro ou Business.",
            destino="/configuracoes",
        )
        if bloqueio:
            return bloqueio
        if destino_externo_tipo == "google_drive":
            if not destino_externo_drive_folder_id:
                definir_feedback_configuracoes(
                    "erro",
                    "Informe o ID da pasta do Google Drive antes de ativar a copia externa.",
                )
                return redirect("/configuracoes")
            credenciais_drive, erro_drive = carregar_credenciais_google_drive()
            if not credenciais_drive:
                definir_feedback_configuracoes(
                    "erro",
                    erro_drive or "Configure as credenciais do Google Drive antes de ativar a copia online.",
                )
                return redirect("/configuracoes")
        else:
            if not destino_externo_pasta:
                definir_feedback_configuracoes(
                    "erro",
                    "Informe a pasta sincronizada antes de ativar a copia externa.",
                )
                return redirect("/configuracoes")

            try:
                os.makedirs(destino_externo_pasta, exist_ok=True)
            except Exception as e:
                definir_feedback_configuracoes(
                    "erro",
                    f"Nao foi possivel acessar a pasta sincronizada informada: {e}",
                )
                return redirect("/configuracoes")

    configuracao = salvar_configuracao_backup_form(request.form)
    registrar_auditoria(
        "configurou_backup",
        "backup",
        detalhes={
            "frequencia": configuracao["frequencia"],
            "tipo_backup": configuracao["tipo_backup"],
            "retencao_arquivos": configuracao["retencao_arquivos"],
            "destino_externo_ativo": bool(int(configuracao["destino_externo_ativo"] or 0)),
            "destino_externo_tipo": configuracao["destino_externo_tipo"],
            "destino_externo_pasta": configuracao["destino_externo_pasta"],
            "destino_externo_drive_folder_id": configuracao["destino_externo_drive_folder_id"],
        },
    )
    definir_feedback_configuracoes(
        "sucesso",
        (
            f"Rotina de backup atualizada para "
            f"{configuracao['frequencia_label'].lower()} "
            f"em modo {configuracao['tipo_backup_label'].lower()} "
            f"com retencao de {configuracao['retencao_arquivos']} arquivo(s). "
            f"{'Copia externa ativada.' if int(configuracao['destino_externo_ativo'] or 0) else 'Copia externa desativada.'}"
        )
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/backup/agora", methods=["POST"])
def gerar_backup_agora():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem gerar backups manuais.")
        return redirect("/configuracoes")

    sucesso, mensagem, caminho = criar_backup_banco(force=True)
    if sucesso:
        nome = os.path.basename(caminho) if caminho else ""
        tipo_item = identificar_tipo_backup_por_nome(nome)
        registrar_auditoria(
            "gerou_backup_manual",
            "backup",
            placa="",
            detalhes={
                "arquivo": nome,
                "tipo_backup": tipo_item or obter_configuracao_backup()["tipo_backup"],
            },
        )
        definir_feedback_configuracoes("sucesso", f"{mensagem} {nome}".strip())
    else:
        definir_feedback_configuracoes("erro", mensagem)
    return redirect(destino_configuracoes("banco"))


@app.route("/configuracoes/backup/validar", methods=["POST"])
def validar_backup_configuracoes():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem validar backups.")
        return redirect("/configuracoes")

    nome_backup = normalizar_texto_campo(request.form.get("backup_nome"))
    backups_disponiveis = {item["nome"]: item for item in listar_arquivos_backup_banco()}
    selecionado = backups_disponiveis.get(nome_backup)
    if not selecionado:
        definir_feedback_configuracoes("erro", "Backup selecionado nao foi encontrado.")
        return redirect("/configuracoes")

    validacao = validar_backup_disponivel(selecionado)
    registrar_auditoria(
        "validou_backup",
        "backup",
        detalhes={
            "arquivo": nome_backup,
            "ok": bool(validacao.get("ok")),
            "mensagem": validacao.get("mensagem"),
        },
    )
    detalhes = validacao.get("detalhes") or []
    mensagem = validacao.get("mensagem") or "Validacao concluida."
    if detalhes:
        mensagem += " " + " ".join(detalhes[:3])
    definir_feedback_configuracoes("sucesso" if validacao.get("ok") else "erro", mensagem)
    return redirect(destino_configuracoes("banco"))

@app.route("/configuracoes/backup/restaurar", methods=["POST"])
def restaurar_backup_configuracoes():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem restaurar backups.")
        return redirect("/configuracoes")

    nome_backup = request.form.get("backup_nome") or ""
    sucesso, mensagem = restaurar_backup_banco(nome_backup)
    if sucesso:
        tipo_item = identificar_tipo_backup_por_nome(nome_backup)
        registrar_auditoria(
            "restaurou_backup",
            "backup",
            detalhes={
                "arquivo_restaurado": nome_backup,
                "tipo_backup": tipo_item or "",
            },
        )
    definir_feedback_configuracoes("sucesso" if sucesso else "erro", mensagem)
    return redirect("/configuracoes")

@app.route("/configuracoes/arquivos/manutencao", methods=["POST"])
def executar_manutencao_arquivos_configuracoes():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_desenvolvedor():
        definir_feedback_configuracoes("erro", "Somente desenvolvedores podem executar a manutencao de arquivos.")
        return redirect("/configuracoes")

    sucesso, mensagem, _ = executar_manutencao_arquivos(
        force=True,
        registrar_log=True,
        usuario=resumo_usuario_logado(),
    )
    if not sucesso and "ja esta em execucao" in mensagem.lower():
        time.sleep(1)
        sucesso, mensagem, _ = executar_manutencao_arquivos(
            force=True,
            registrar_log=True,
            usuario=resumo_usuario_logado(),
    )
    definir_feedback_configuracoes("sucesso" if sucesso else "erro", mensagem)
    return redirect("/configuracoes")

@app.route("/configuracoes/foto", methods=["POST"])
def atualizar_minha_foto():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario_seguro(contexto="FOTO USUARIO")
    foto = request.files.get("foto_perfil")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM usuarios WHERE id=?", (session.get("usuario_id"),))
    usuario = c.fetchone()

    if not usuario:
        conn.close()
        definir_feedback_configuracoes("erro", "Usuario nao encontrado.")
        return redirect("/configuracoes")

    antiga_foto = usuario["foto_perfil"]
    nova_foto = ""

    try:
        foto_info = salvar_foto_perfil_usuario(
            foto,
            identificador=f"{usuario['usuario']}_{usuario['id']}",
        )
        nova_foto = foto_info["caminho"]
        c.execute(
            """
            UPDATE usuarios
            SET foto_perfil=?,
                foto_perfil_blob=?,
                foto_perfil_mime_type=?,
                foto_perfil_arquivo_nome=?
            WHERE id=?
            """,
            (
                nova_foto,
                foto_info.get("arquivo_blob"),
                foto_info.get("mime_type"),
                foto_info.get("arquivo_nome"),
                usuario["id"],
            ),
        )
        conn.commit()
        c.execute("SELECT * FROM usuarios WHERE id=?", (usuario["id"],))
        usuario_atualizado = c.fetchone()
    except ValueError as erro:
        conn.rollback()
        conn.close()
        if nova_foto:
            remover_foto_perfil_antiga(nova_foto)
        definir_feedback_configuracoes("erro", str(erro))
        return redirect("/configuracoes")
    except Exception:
        conn.rollback()
        conn.close()
        if nova_foto:
            remover_foto_perfil_antiga(nova_foto)
        definir_feedback_configuracoes("erro", "Nao foi possivel atualizar a foto do perfil agora.")
        return redirect("/configuracoes")

    conn.close()
    remover_foto_perfil_antiga(antiga_foto)
    preencher_sessao_usuario(usuario_atualizado, limpar=False)
    registrar_auditoria(
        "atualizou_foto_perfil",
        "usuario",
        entidade_id=usuario["id"],
        detalhes={"usuario_alvo": usuario["usuario"]},
    )
    definir_feedback_configuracoes(
        "sucesso",
        "Foto do seu perfil atualizada com sucesso e sincronizada no banco ativo."
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/senha", methods=["POST"])
def atualizar_minha_senha():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario_seguro(contexto="SENHA")

    senha_atual = request.form.get("senha_atual") or ""
    nova_senha = request.form.get("nova_senha") or ""
    confirmar_senha = request.form.get("confirmar_senha") or ""
    destino = destino_configuracoes("meu-acesso")

    if not senha_atual or not nova_senha or not confirmar_senha:
        definir_feedback_configuracoes("erro", "Preencha todos os campos para alterar a senha.")
        return redirect(destino)

    if nova_senha != confirmar_senha:
        definir_feedback_configuracoes("erro", "A confirmacao da nova senha nao confere.")
        return redirect(destino)

    conn = None
    try:
        conn = conectar()
        c = conn.cursor()
        usuario = buscar_usuario_para_alteracao_senha(
            c,
            usuario_id=session.get("usuario_id"),
            usuario_login=session.get("usuario"),
        )

        if not usuario or not verificar_senha_usuario(senha_atual, usuario["senha"]):
            definir_feedback_configuracoes("erro", "A senha atual informada esta incorreta.")
            return redirect(destino)

        erro_forca = validar_forca_senha(nova_senha, usuario["usuario"])
        if erro_forca:
            definir_feedback_configuracoes("erro", erro_forca)
            return redirect(destino)

        if verificar_senha_usuario(nova_senha, usuario["senha"]):
            definir_feedback_configuracoes("erro", "Escolha uma senha diferente da atual.")
            return redirect(destino)

        atualizado_em = agora_iso()
        c.execute(
            """
            UPDATE usuarios
            SET senha=?, senha_alteracao_obrigatoria=0, senha_atualizada_em=?, tentativas_login=0, bloqueado_ate=NULL
            WHERE id=?
            """,
            (senha_hash_bcrypt(nova_senha), atualizado_em, usuario["id"])
        )
        conn.commit()
        c.execute("SELECT * FROM usuarios WHERE id=?", (usuario["id"],))
        usuario_atualizado = c.fetchone()
    except Exception as erro:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        log_info("ERRO ALTERAR SENHA:", erro)
        definir_feedback_configuracoes("erro", "Nao foi possivel alterar a senha agora. Tente novamente em instantes.")
        return redirect(destino)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    preencher_sessao_usuario(usuario_atualizado)
    try:
        registrar_auditoria_assincrona(
            "alterou_propria_senha",
            "usuario",
            entidade_id=usuario_atualizado["id"],
            detalhes={"usuario_alvo": usuario_atualizado["usuario"]},
        )
    except Exception as erro:
        log_info("AVISO AUDITORIA SENHA:", erro)
    definir_feedback_configuracoes(
        "sucesso",
        (
            "Senha atualizada com sucesso. Seu acesso agora esta protegido "
            "com hash bcrypt e politica forte, sincronizado no banco ativo."
        ),
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/usuarios", methods=["POST"])
def criar_usuario_funcionario():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario_seguro(contexto="CRIAR USUARIO")
    if not usuario_gerencia_acessos():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem criar novos acessos.")
        return redirect(destino_configuracoes("usuarios"))

    nome = (request.form.get("nome") or "").strip()
    usuario = (request.form.get("usuario") or "").strip().lower()
    senha = request.form.get("senha") or ""
    perfil = normalizar_perfil_usuario(request.form.get("perfil"))
    foto_perfil = request.files.get("foto_perfil")

    if not nome or not usuario or not senha:
        definir_feedback_configuracoes("erro", "Informe nome, login e senha para criar o usuario.")
        return redirect(destino_configuracoes("usuarios"))

    erro_forca = validar_forca_senha(senha, usuario)
    if erro_forca:
        definir_feedback_configuracoes("erro", erro_forca)
        return redirect(destino_configuracoes("usuarios"))

    if perfil == "desenvolvedor" and not usuario_desenvolvedor():
        definir_feedback_configuracoes(
            "erro",
            "Somente um desenvolvedor pode criar um acesso com perfil Desenvolvedor.",
        )
        return redirect(destino_configuracoes("usuarios"))

    if bloquear_criacao_usuario_por_licenca():
        licenca = carregar_contexto_licenca_empresa_seguro()
        definir_feedback_configuracoes(
            "erro",
            f"Limite de usuarios do plano atingido ({licenca['usuarios_ativos']}/{licenca['limite_usuarios']}).",
        )
        return redirect(destino_configuracoes("usuarios"))

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id FROM usuarios WHERE usuario=?", (usuario,))
    existente = c.fetchone()

    if existente:
        conn.close()
        definir_feedback_configuracoes("erro", "Ja existe um acesso com esse login.")
        return redirect(destino_configuracoes("usuarios"))

    nova_foto = ""

    try:
        c.execute("""
            INSERT INTO usuarios (
                empresa_id, usuario, senha, nome, perfil, ativo, criado_em,
                tentativas_login, bloqueado_ate, ultimo_login_em,
                senha_alteracao_obrigatoria, senha_atualizada_em
            )
            VALUES (?, ?, ?, ?, ?, 1, ?, 0, NULL, NULL, 1, ?)
        """, (
            empresa_atual_id(),
            usuario,
            senha_hash_bcrypt(senha),
            nome,
            perfil,
            agora_iso(),
            agora_iso(),
        ))
        usuario_id = c.lastrowid

        if foto_perfil and str(foto_perfil.filename or "").strip():
            foto_info = salvar_foto_perfil_usuario(
                foto_perfil,
                identificador=f"{usuario}_{usuario_id}",
            )
            nova_foto = foto_info["caminho"]
            c.execute(
                """
                UPDATE usuarios
                SET foto_perfil=?,
                    foto_perfil_blob=?,
                    foto_perfil_mime_type=?,
                    foto_perfil_arquivo_nome=?
                WHERE id=?
                """,
                (
                    nova_foto,
                    foto_info.get("arquivo_blob"),
                    foto_info.get("mime_type"),
                    foto_info.get("arquivo_nome"),
                    usuario_id,
                ),
            )

        conn.commit()
    except ValueError as erro:
        conn.rollback()
        conn.close()
        if nova_foto:
            remover_foto_perfil_antiga(nova_foto)
        definir_feedback_configuracoes("erro", str(erro))
        return redirect(destino_configuracoes("usuarios"))
    except Exception:
        conn.rollback()
        conn.close()
        if nova_foto:
            remover_foto_perfil_antiga(nova_foto)
        definir_feedback_configuracoes("erro", "Nao foi possivel criar o usuario agora.")
        return redirect(destino_configuracoes("usuarios"))

    conn.close()

    try:
        registrar_auditoria_assincrona(
            "criou_usuario",
            "usuario",
            detalhes={"usuario_alvo": usuario, "perfil": perfil, "com_foto": bool(nova_foto)},
        )
    except Exception as erro:
        log_info("AVISO AUDITORIA CRIAR USUARIO:", erro)
    definir_feedback_configuracoes(
        "sucesso",
        (
            f"Usuario {usuario} criado com sucesso e sincronizado no banco ativo. "
            "A troca de senha sera obrigatoria no primeiro login."
        ),
    )
    return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

@app.route("/configuracoes/usuarios/<int:usuario_id>/senha", methods=["POST"])
def redefinir_senha_usuario(usuario_id):
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario_seguro(contexto="REDEFINIR SENHA")
    if not usuario_gerencia_acessos():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem redefinir senhas.")
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    nova_senha = request.form.get("nova_senha") or ""

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id, usuario, perfil FROM usuarios WHERE empresa_id=? AND id=?", (empresa_atual_id(), usuario_id))
    alvo = c.fetchone()

    if not alvo:
        conn.close()
        definir_feedback_configuracoes("erro", "Usuario nao encontrado.")
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    if (
        normalizar_perfil_usuario(alvo["perfil"]) == "desenvolvedor" and
        not usuario_desenvolvedor()
    ):
        conn.close()
        definir_feedback_configuracoes(
            "erro",
            "Somente um desenvolvedor pode redefinir a senha de outro desenvolvedor.",
        )
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    erro_forca = validar_forca_senha(nova_senha, alvo["usuario"])
    if erro_forca:
        conn.close()
        definir_feedback_configuracoes("erro", erro_forca)
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    c.execute(
        """
        UPDATE usuarios
        SET senha=?, senha_alteracao_obrigatoria=1, senha_atualizada_em=?, tentativas_login=0, bloqueado_ate=NULL
        WHERE empresa_id=? AND id=?
        """,
        (senha_hash_bcrypt(nova_senha), agora_iso(), empresa_atual_id(), usuario_id)
    )
    conn.commit()
    conn.close()

    try:
        registrar_auditoria_assincrona(
            "redefiniu_senha_usuario",
            "usuario",
            entidade_id=usuario_id,
            detalhes={"usuario_alvo": alvo["usuario"]},
        )
    except Exception as erro:
        log_info("AVISO AUDITORIA REDEFINIR SENHA:", erro)
    definir_feedback_configuracoes(
        "sucesso",
        (
            f"Senha do usuario {alvo['usuario']} atualizada. "
            "Ele vai precisar trocar a senha no proximo login."
        ),
    )
    return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

@app.route("/configuracoes/usuarios/<int:usuario_id>/foto", methods=["POST"])
def atualizar_foto_usuario(usuario_id):
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario_seguro(contexto="FOTO USUARIO")
    if not usuario_gerencia_acessos():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem atualizar fotos de acessos.")
        return redirect("/configuracoes")

    foto = request.files.get("foto_perfil")
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM usuarios WHERE empresa_id=? AND id=?", (empresa_atual_id(), usuario_id))
    alvo = c.fetchone()

    if not alvo:
        conn.close()
        definir_feedback_configuracoes("erro", "Usuario nao encontrado.")
        return redirect("/configuracoes")

    if (
        normalizar_perfil_usuario(alvo["perfil"]) == "desenvolvedor" and
        not usuario_desenvolvedor()
    ):
        conn.close()
        definir_feedback_configuracoes(
            "erro",
            "Somente um desenvolvedor pode atualizar a foto de outro desenvolvedor.",
        )
        return redirect("/configuracoes")

    antiga_foto = alvo["foto_perfil"]
    nova_foto = ""

    try:
        foto_info = salvar_foto_perfil_usuario(
            foto,
            identificador=f"{alvo['usuario']}_{alvo['id']}",
        )
        nova_foto = foto_info["caminho"]
        c.execute(
            """
            UPDATE usuarios
            SET foto_perfil=?,
                foto_perfil_blob=?,
                foto_perfil_mime_type=?,
                foto_perfil_arquivo_nome=?
            WHERE empresa_id=? AND id=?
            """,
            (
                nova_foto,
                foto_info.get("arquivo_blob"),
                foto_info.get("mime_type"),
                foto_info.get("arquivo_nome"),
                empresa_atual_id(),
                usuario_id,
            ),
        )
        conn.commit()
        c.execute("SELECT * FROM usuarios WHERE empresa_id=? AND id=?", (empresa_atual_id(), usuario_id))
        alvo_atualizado = c.fetchone()
    except ValueError as erro:
        conn.rollback()
        conn.close()
        if nova_foto:
            remover_foto_perfil_antiga(nova_foto)
        definir_feedback_configuracoes("erro", str(erro))
        return redirect("/configuracoes")
    except Exception:
        conn.rollback()
        conn.close()
        if nova_foto:
            remover_foto_perfil_antiga(nova_foto)
        definir_feedback_configuracoes("erro", "Nao foi possivel atualizar a foto desse acesso agora.")
        return redirect("/configuracoes")

    conn.close()
    remover_foto_perfil_antiga(antiga_foto)

    if int(session.get("usuario_id") or 0) == int(usuario_id):
        preencher_sessao_usuario(alvo_atualizado, limpar=False)

    registrar_auditoria(
        "atualizou_foto_perfil",
        "usuario",
        entidade_id=usuario_id,
        detalhes={"usuario_alvo": alvo["usuario"]},
    )
    definir_feedback_configuracoes(
        "sucesso",
        f"Foto do usuario {alvo['usuario']} atualizada com sucesso e sincronizada no banco ativo."
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/usuarios/<int:usuario_id>/alternar", methods=["POST"])
def alternar_status_usuario(usuario_id):
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario_seguro(contexto="ALTERNAR USUARIO")
    if not usuario_gerencia_acessos():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem alterar o status de acessos.")
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id, usuario, perfil, ativo FROM usuarios WHERE empresa_id=? AND id=?", (empresa_atual_id(), usuario_id))
    alvo = c.fetchone()

    if not alvo:
        conn.close()
        definir_feedback_configuracoes("erro", "Usuario nao encontrado.")
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    if (
        normalizar_perfil_usuario(alvo["perfil"]) == "desenvolvedor" and
        not usuario_desenvolvedor()
    ):
        conn.close()
        definir_feedback_configuracoes(
            "erro",
            "Somente um desenvolvedor pode alterar o status de outro desenvolvedor.",
        )
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    if alvo["usuario"] == "admin" or normalizar_perfil_usuario(alvo["perfil"]) == "admin":
        conn.close()
        definir_feedback_configuracoes("erro", "O acesso administrador principal nao pode ser desativado.")
        return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

    novo_status = 0 if int(alvo["ativo"] or 0) else 1
    c.execute("UPDATE usuarios SET ativo=? WHERE empresa_id=? AND id=?", (novo_status, empresa_atual_id(), usuario_id))
    conn.commit()
    conn.close()

    definir_feedback_configuracoes(
        "sucesso",
        f"Usuario {alvo['usuario']} {'ativado' if novo_status else 'pausado'} com sucesso e sincronizado no banco ativo."
    )
    return redirect(destino_configuracoes("usuarios", "detalhar_usuarios=1"))

@app.route("/retornos")
def pagina_retornos():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    contexto = carregar_contexto_retornos(request.args)
    retorno_url_atual = request.path

    if request.query_string:
        retorno_url_atual = request.full_path.rstrip("?")

    return render_template(
        "retornos.html",
        retornos=contexto["itens"],
        resumo_retornos=contexto["resumo"],
        filtros=contexto["filtros"],
        status_retorno_opcoes=contexto["status_opcoes"],
        feedback=session.pop("retornos_feedback", None),
        retorno_url_atual=retorno_url_atual,
    )

@app.route("/retornos/atualizar", methods=["POST"])
def atualizar_retorno_cliente():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    placa = normalizar_texto_campo(request.form.get("placa")).upper()
    acao = normalizar_texto_campo(request.form.get("acao"))
    retorno_url = normalizar_texto_campo(request.form.get("retorno_url"))

    if not retorno_url.startswith("/retornos"):
        retorno_url = "/retornos"

    if not placa:
        definir_feedback_retornos("erro", "Informe a placa para atualizar o retorno.")
        return redirect(retorno_url)

    try:
        estado_atual = carregar_estados_retornos([placa]).get(placa, {})
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="retornos_carregar_estado")
        definir_feedback_retornos(
            "erro",
            "Nao foi possivel atualizar o retorno agora. O banco online ficou indisponivel por instantes; tente novamente.",
        )
        return redirect(retorno_url)

    status_atual = normalizar_status_retorno(estado_atual.get("status"))
    observacao_atual = normalizar_texto_campo(estado_atual.get("observacao"))
    observacao_form = normalizar_texto_campo(request.form.get("observacao"))
    observacao_final = observacao_form or observacao_atual
    proximo_contato_atual = normalizar_texto_campo(estado_atual.get("proximo_contato_em"))
    ultimo_contato_atual = normalizar_texto_campo(estado_atual.get("ultimo_contato_em"))
    reagendado_dias_atual = int(estado_atual.get("reagendado_dias") or 0)
    ultima_acao_atual = normalizar_texto_campo(estado_atual.get("ultima_acao"))

    payload = {
        "status": status_atual,
        "observacao": observacao_final,
        "proximo_contato_em": proximo_contato_atual,
        "ultimo_contato_em": ultimo_contato_atual,
        "ultima_acao": ultima_acao_atual,
        "reagendado_dias": reagendado_dias_atual,
    }
    acao_auditoria = ""
    mensagem_sucesso = ""

    if acao == "salvar_observacao":
        payload["ultima_acao"] = ultima_acao_atual or "observacao"
        acao_auditoria = "salvou_observacao_retorno"
        mensagem_sucesso = "Observacao do retorno salva com sucesso."
    elif acao == "contatado":
        payload.update({
            "status": "contatado",
            "ultimo_contato_em": agora_iso(),
            "proximo_contato_em": "",
            "ultima_acao": "contatado",
            "reagendado_dias": 0,
        })
        acao_auditoria = "marcou_retorno_como_contatado"
        mensagem_sucesso = "Retorno marcado como contatado."
    elif acao == "sem_interesse":
        payload.update({
            "status": "sem_interesse",
            "ultimo_contato_em": agora_iso(),
            "proximo_contato_em": "",
            "ultima_acao": "sem_interesse",
            "reagendado_dias": 0,
        })
        acao_auditoria = "marcou_retorno_sem_interesse"
        mensagem_sucesso = "Retorno marcado como sem interesse."
    elif acao == "reagendar_7":
        payload.update({
            "status": "reagendado",
            "ultimo_contato_em": agora_iso(),
            "proximo_contato_em": proximo_contato_retorno_por_dias(7),
            "ultima_acao": "reagendar_7",
            "reagendado_dias": 7,
        })
        acao_auditoria = "reagendou_retorno"
        mensagem_sucesso = "Retorno reagendado para 7 dias."
    elif acao == "reagendar_15":
        payload.update({
            "status": "reagendado",
            "ultimo_contato_em": agora_iso(),
            "proximo_contato_em": proximo_contato_retorno_por_dias(15),
            "ultima_acao": "reagendar_15",
            "reagendado_dias": 15,
        })
        acao_auditoria = "reagendou_retorno"
        mensagem_sucesso = "Retorno reagendado para 15 dias."
    elif acao == "reativar":
        payload.update({
            "status": "pendente",
            "proximo_contato_em": "",
            "ultima_acao": "reativado",
            "reagendado_dias": 0,
        })
        acao_auditoria = "reativou_retorno"
        mensagem_sucesso = "Retorno reativado e colocado novamente como pendente."
    else:
        definir_feedback_retornos("erro", "Acao de retorno nao reconhecida.")
        return redirect(retorno_url)

    try:
        upsert_retorno_cliente(
            placa,
            payload["status"],
            observacao=payload["observacao"],
            proximo_contato_em=payload["proximo_contato_em"],
            ultimo_contato_em=payload["ultimo_contato_em"],
            ultima_acao=payload["ultima_acao"],
            reagendado_dias=payload["reagendado_dias"],
            usuario=resumo_usuario_logado(),
        )
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="retornos_atualizar")
        definir_feedback_retornos(
            "erro",
            "Nao foi possivel salvar o retorno agora. O banco online ficou indisponivel por instantes; tente novamente.",
        )
        return redirect(retorno_url)

    try:
        registrar_auditoria(
            acao_auditoria,
            "retorno",
            placa=placa,
            detalhes={
                "status": payload["status"],
                "observacao": payload["observacao"],
                "proximo_contato_em": payload["proximo_contato_em"],
                "ultimo_contato_em": payload["ultimo_contato_em"],
                "reagendado_dias": payload["reagendado_dias"],
            },
        )
    except Exception as erro:
        registrar_ultimo_erro_producao(erro, descricao="retornos_auditoria")
    definir_feedback_retornos("sucesso", mensagem_sucesso)
    return redirect(retorno_url)

@app.route("/auditoria")
def pagina_auditoria():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_gerencia_acessos():
        definir_feedback_configuracoes("erro", "Somente administradores ou desenvolvedores podem acessar a auditoria.")
        return redirect("/configuracoes")

    contexto = carregar_contexto_auditoria(request.args)
    return render_template(
        "auditoria.html",
        registros=contexto["registros"],
        usuarios_auditoria=contexto["usuarios"],
        acoes_auditoria=contexto["acoes"],
        filtros=contexto["filtros"],
        periodos_auditoria=contexto["periodos"],
        resumo_auditoria=contexto["resumo"],
    )


@app.route("/changelog")
def pagina_changelog():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    contexto = carregar_contexto_changelog_domain(
        os.path.dirname(os.path.abspath(__file__)),
        versao_atual=obter_versao_sistema(),
    )
    return render_template(
        "changelog.html",
        changelog_resumo=contexto["resumo"],
        changelog_marcos=contexto["marcos"],
        changelog_grupos=contexto["grupos"],
        changelog_commits_recentes=contexto.get("commits_recentes", []),
        changelog_gerado_em=contexto["gerado_em"],
    )

@app.route("/clima")
def clima():
    if not session.get("usuario"):
        return redirect("/login")

    return render_template("clima.html")

@app.route("/relatorios/exportar.csv")
def exportar_relatorios_csv():
    if not session.get("usuario"):
        return redirect("/login")

    periodo_atual = normalizar_periodo_financeiro(request.args.get("periodo"))
    tipo = normalizar_texto_campo(request.args.get("tipo")).lower() if request.args.get("tipo") else "servicos"
    contexto = carregar_contexto_relatorios(periodo_atual)

    if tipo == "documentos":
        linhas = construir_linhas_csv_relatorio_documentos(
            contexto.get("orcamentos_periodo_raw"),
            contexto.get("notas_periodo_raw"),
        )
        return montar_csv_resposta(
            f"relatorios_documentos_{periodo_atual}.csv",
            linhas,
        )

    linhas = construir_linhas_csv_relatorio_servicos(contexto.get("finalizados_periodo_raw"))
    return montar_csv_resposta(
        f"relatorios_atendimentos_{periodo_atual}.csv",
        linhas,
    )


@app.route("/relatorios")
@app.route("/financeiro")
def financeiro():
    if not session.get("usuario"):
        return redirect("/login")
    contexto = medir_consulta_sql(
        "/financeiro",
        "montagem_contexto_financeiro",
        lambda: carregar_contexto_relatorios(
            request.args.get("periodo"),
            detalhado=bool(request.args.get("detalhar") or request.args.get("detalhes")),
        ),
        origem="memoria",
    )
    return medir_consulta_sql(
        "/financeiro",
        "render_template_financeiro",
        lambda: render_template("financeiro.html", **contexto),
        origem="template",
    )


@app.route("/", methods=["GET", "POST"])
def index():
    if not session.get("usuario"):
        return redirect("/login")

    if request.method == "POST":
        placa = request.form.get("placa", "").upper()
        return redirect(f"/?placa={placa}")

    preparar_rotinas_interface_logada()
    dados = None
    historico = []
    buscou = False
    lavagem_info = None
    servicos_lista = []
    produtos_pneu = []
    placa = request.args.get("placa", "").upper()

    if placa:
        buscou = True
        lavagem_info = montar_contexto_lavagem_placa(placa)
        conn = conectar()
        c = conn.cursor()
        empresa_id = empresa_atual_id()

        # ðŸ”¥ CLIENTE
        c.execute("""
        SELECT 
            veiculos.placa,
            veiculos.modelo,
            veiculos.cor,
            clientes.nome,
            clientes.telefone,
            clientes.data_nascimento
        FROM veiculos
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id AND clientes.empresa_id=?
        WHERE veiculos.empresa_id=? AND veiculos.placa=?
        """, (empresa_id, empresa_id, placa))

        dados = c.fetchone()

        if dados:
            dados = dict(dados)
            dados["data_nascimento"] = formatar_data_nascimento_input(dados.get("data_nascimento"))
            dados["data_nascimento_exibicao"] = formatar_data_nascimento_exibicao(dados.get("data_nascimento"))
            historico = listar_historico_servicos(placa=placa)

            c.execute("SELECT * FROM tipos_servico")
            servicos_lista = c.fetchall()

            c.execute("SELECT * FROM produtos_pneu")
            produtos_pneu = c.fetchall()

        conn.close()

    return render_template(
        "index.html",
        dados=dados,
        historico=historico,
        buscou=buscou,
        placa=placa,
        lavagem_info=lavagem_info,
        version=obter_versao_sistema(),
        feedback_index=session.pop("index_feedback", None),
        servicos_lista=servicos_lista,
        produtos_pneu=produtos_pneu
    )

def renderizar_pagina_clientes(busca="", limpar=False):
    detalhar_sincronizacoes = bool(request.args.get("detalhar_sincronizacoes") or request.args.get("sync"))
    cliente_em_edicao = normalizar_texto_campo(request.args.get("editar"))
    clientes_lista, sincronizacoes = carregar_contexto_clientes(
        busca=busca,
        limpar=limpar,
        detalhar_sincronizacoes=detalhar_sincronizacoes,
    )

    return render_template(
        "clientes.html",
        clientes=clientes_lista,
        sincronizacoes=sincronizacoes,
        detalhar_sincronizacoes=detalhar_sincronizacoes,
        feedback=session.pop("clientes_feedback", None),
        preview_sync=session.get("clientes_sync_preview"),
        campos_sync=CAMPOS_SINCRONIZACAO_CLIENTES,
        intervalos_sync=INTERVALOS_SINCRONIZACAO,
        busca=busca,
        limite_inicial_clientes=CLIENTES_LISTA_INICIAL_LIMITE,
        clientes_limitados=(not busca and len(clientes_lista) >= CLIENTES_LISTA_INICIAL_LIMITE),
        cliente_em_edicao=cliente_em_edicao,
    )

@app.route("/clientes/sincronizacao/preview", methods=["POST"])
def preview_sincronizacao_clientes():
    if not session.get("usuario"):
        return redirect("/login")

    preview_sync = session.get("clientes_sync_preview") or {}
    nome = (request.form.get("nome") or preview_sync.get("nome") or "").strip()
    url = (request.form.get("url") or preview_sync.get("url") or "").strip()
    intervalo_minutos = normalizar_intervalo_sincronizacao(
        request.form.get("intervalo_minutos") or preview_sync.get("intervalo_minutos")
    )

    if not url:
        definir_feedback_clientes("erro", "Informe um link de planilha para continuar.")
        return redirect("/clientes")

    try:
        df, url_normalizada = ler_dataframe_link_planilha(
            url,
            intervalo_minutos=intervalo_minutos,
        )
        colunas = list(df.columns)
        mapeamento_sugerido = sugerir_mapeamento_colunas(colunas)
        colunas_exibicao = obter_colunas_preview_sync(mapeamento_sugerido) or colunas[:5]
        proximo_sync_previsto = somar_minutos_iso(intervalo_minutos)

        if not mapeamento_sugerido.get("placa"):
            raise ValueError("Nao encontrei uma coluna de placa para configurar a sincronizacao.")

        if not colunas:
            raise ValueError("Nao encontrei colunas validas nessa planilha.")

        amostra_tratada = [
            {k: sanitizar_para_json(v) for k, v in linha.items()}
            for linha in df.head(8).to_dict(orient="records")
        ]

        session["clientes_sync_preview"] = {
            "nome": nome,
            "url": url_normalizada,
            "intervalo_minutos": intervalo_minutos,
            "colunas": colunas,
            "mapeamento_sugerido": mapeamento_sugerido,
            "mapeamento_automatico": mapeamento_sugerido,
            "colunas_exibicao": colunas_exibicao,
            "amostra": amostra_tratada,
            "total_linhas": len(df.index),
            "intervalo_label": obter_label_intervalo_sincronizacao(intervalo_minutos),
            "proximo_sync_previsto": proximo_sync_previsto,
            "proximo_sync_previsto_fmt": formatar_datahora(proximo_sync_previsto),
            "proximo_sync_previsto_relativo": formatar_tempo_restante(proximo_sync_previsto),
        }

        definir_feedback_clientes(
            "sucesso",
            f"Planilha carregada. {len(df.index)} linha(s) encontrada(s) para configurar a sincronizacao."
        )

    except Exception as e:
        limpar_preview_sincronizacao()
        definir_feedback_clientes("erro", f"Nao consegui ler a planilha: {e}")

    return redirect("/clientes")

@app.route("/clientes/sincronizacao/cancelar_preview", methods=["POST"])
def cancelar_preview_sincronizacao_clientes():
    if not session.get("usuario"):
        return redirect("/login")

    limpar_preview_sincronizacao()
    definir_feedback_clientes("sucesso", "Pre-visualizacao cancelada.")
    return redirect("/clientes")

@app.route("/clientes/sincronizacao/adicionar", methods=["POST"])
def adicionar_sincronizacao_clientes():
    if not session.get("usuario"):
        return redirect("/login")

    preview_sync = session.get("clientes_sync_preview") or {}
    nome = (request.form.get("nome") or preview_sync.get("nome") or "").strip()
    url = (request.form.get("url") or preview_sync.get("url") or "").strip()
    intervalo_minutos = normalizar_intervalo_sincronizacao(
        request.form.get("intervalo_minutos") or preview_sync.get("intervalo_minutos")
    )

    if not url:
        definir_feedback_clientes("erro", "Informe um link de planilha.")
        return redirect("/clientes")

    try:
        df, url_normalizada = ler_dataframe_link_planilha(
            url,
            intervalo_minutos=intervalo_minutos,
        )

        colunas = list(df.columns)
        mapeamento_sugerido = sugerir_mapeamento_colunas(colunas)
        mapeamento = {
            "nome": (
                request.form.get("campo_nome") or
                preview_sync.get("mapeamento_sugerido", {}).get("nome") or
                mapeamento_sugerido.get("nome") or
                ""
            ),
            "modelo": (
                request.form.get("campo_modelo") or
                preview_sync.get("mapeamento_sugerido", {}).get("modelo") or
                mapeamento_sugerido.get("modelo") or
                ""
            ),
            "cor": (
                request.form.get("campo_cor") or
                preview_sync.get("mapeamento_sugerido", {}).get("cor") or
                mapeamento_sugerido.get("cor") or
                ""
            ),
            "placa": (
                request.form.get("campo_placa") or
                preview_sync.get("mapeamento_sugerido", {}).get("placa") or
                mapeamento_sugerido.get("placa") or
                ""
            ),
            "servico": (
                request.form.get("campo_servico") or
                preview_sync.get("mapeamento_sugerido", {}).get("servico") or
                mapeamento_sugerido.get("servico") or
                ""
            ),
            "telefone": "",
            "data": (
                preview_sync.get("mapeamento_automatico", {}).get("data") or
                mapeamento_sugerido.get("data") or
                ""
            )
        }

        if not mapeamento.get("placa"):
            raise Exception("Nao consegui identificar a coluna de placa automaticamente.")

        empresa_id = empresa_atual_id()
        estatisticas = importar_clientes_dataframe(df, mapeamento, empresa_id=empresa_id)
        registros_historico, estatisticas_historico = montar_registros_historico_lavagens(df, mapeamento)
        estatisticas.update(estatisticas_historico)

        conn = conectar()
        c = conn.cursor()
        agora_atual = agora_iso()
        sync_id = criar_sincronizacao_cliente_domain(
            c,
            empresa_id,
            nome or "Planilha automatica",
            url_normalizada,
            intervalo_minutos,
            mapeamento,
            ativo=1,
            proximo_sync_em=somar_minutos_iso(intervalo_minutos),
            ultimo_status="OK",
            criado_em=agora_atual,
            atualizado_em=agora_atual,
        )
        conn.commit()
        conn.close()

        salvar_historico_lavagens_sync(sync_id, registros_historico, empresa_id=empresa_id)
        limpar_cache_clientes()

        definir_feedback_clientes(
            "sucesso",
            f"Importacao concluida: {resumir_importacao_clientes(estatisticas)}"
        )

    except Exception as e:
        definir_feedback_clientes("erro", f"Erro ao importar: {e}")

    return redirect("/clientes")
@app.route("/clientes/sincronizacao/salvar", methods=["POST"])
def salvar_sincronizacao_clientes():
    if not session.get("usuario"):
        return redirect("/login")

    preview_sync = session.get("clientes_sync_preview") or {}
    nome = (request.form.get("nome") or preview_sync.get("nome") or "").strip()
    url = (request.form.get("url") or preview_sync.get("url") or "").strip()
    intervalo_minutos = normalizar_intervalo_sincronizacao(
        request.form.get("intervalo_minutos") or preview_sync.get("intervalo_minutos")
    )

    if not url:
        definir_feedback_clientes("erro", "Informe um link de planilha.")
        return redirect("/clientes")

    try:
        empresa_id = empresa_atual_id()
        df, url_normalizada = ler_dataframe_link_planilha(
            url,
            intervalo_minutos=intervalo_minutos,
        )
        colunas = list(df.columns)
        mapeamento = obter_mapeamento_sync_por_form(request.form)

        if not any(mapeamento.values()):
            mapeamento = preview_sync.get("mapeamento_sugerido") or sugerir_mapeamento_colunas(colunas)

        mapeamento["telefone"] = ""
        mapeamento["data"] = (
            preview_sync.get("mapeamento_automatico", {}).get("data") or
            sugerir_mapeamento_colunas(colunas).get("data") or
            ""
        )

        if not mapeamento.get("placa"):
            raise ValueError("Nao consegui identificar a coluna de placa.")

        for campo in CAMPOS_SINCRONIZACAO_CLIENTES:
            coluna = mapeamento.get(campo["key"])
            if coluna and coluna not in colunas:
                raise ValueError(f"A coluna '{coluna}' nao foi encontrada na planilha.")

        estatisticas = importar_clientes_dataframe(df, mapeamento, empresa_id=empresa_id)
        registros_historico, estatisticas_historico = montar_registros_historico_lavagens(df, mapeamento)
        estatisticas.update(estatisticas_historico)
        mensagem_importacao = resumir_importacao_clientes(estatisticas)
        agora_atual = agora_iso()
        proximo_sync_em = somar_minutos_iso(intervalo_minutos)
        hash_atual = hashlib.md5(df.to_csv(index=False).encode("utf-8")).hexdigest()

        conn = conectar()
        c = conn.cursor()
        sync_id = criar_sincronizacao_cliente_domain(
            c,
            empresa_id,
            nome or "Planilha automatica",
            url_normalizada,
            intervalo_minutos,
            mapeamento,
            ativo=1,
            ultimo_sync_em=agora_atual,
            proximo_sync_em=proximo_sync_em,
            ultimo_status="OK",
            ultima_mensagem=mensagem_importacao,
            criado_em=agora_atual,
            atualizado_em=agora_atual,
            ultimo_hash=hash_atual,
            colunas_ultima_sync=",".join(colunas),
        )
        conn.commit()
        conn.close()

        salvar_historico_lavagens_sync(sync_id, registros_historico, empresa_id=empresa_id)
        limpar_cache_clientes()

        limpar_preview_sincronizacao()
        salvar_notificacao(mensagem_importacao, "sucesso")
        definir_feedback_clientes(
            "sucesso",
            (
                f"Sincronizacao salva. Link: {url_normalizada} | "
                f"Proxima execucao: {formatar_datahora(proximo_sync_em)} "
                f"({formatar_tempo_restante(proximo_sync_em)}) | "
                f"{mensagem_importacao}"
            )
        )
    except Exception as e:
        definir_feedback_clientes("erro", f"Erro ao salvar sincronizacao: {e}")

    return redirect("/clientes")
@app.route("/clientes/sincronizacao/<int:sync_id>/executar", methods=["POST"])
def executar_sync_clientes(sync_id):
    if not session.get("usuario"):
        return redirect("/login")

    sucesso, mensagem = executar_sincronizacao_cliente(sync_id, empresa_id=empresa_atual_id())
    limpar_cache_clientes()
    definir_feedback_clientes("sucesso" if sucesso else "erro", mensagem)
    return redirect("/clientes")

@app.route("/clientes/sincronizacao/<int:sync_id>/alternar", methods=["POST"])
def alternar_sync_clientes(sync_id):
    if not session.get("usuario"):
        return redirect("/login")

    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    sync = consultar_sincronizacao_cliente_domain(c, sync_id, empresa_id=empresa_id)

    if not sync:
        conn.close()
        definir_feedback_clientes("erro", "Sincronizacao nao encontrada.")
        return redirect("/clientes")

    novo_ativo = 0 if sync["ativo"] else 1
    novo_status = "AGENDADO" if novo_ativo else "PAUSADO"
    proximo_sync = agora_iso() if novo_ativo else None
    atualizado_em = agora_iso()

    alternar_sincronizacao_cliente_domain(
        c,
        sync_id,
        empresa_id,
        novo_ativo,
        novo_status,
        proximo_sync,
        atualizado_em,
    )
    conn.commit()
    conn.close()
    limpar_cache_clientes()

    definir_feedback_clientes(
        "sucesso",
        "Sincronizacao ativada." if novo_ativo else "Sincronizacao pausada."
    )
    return redirect("/clientes")

@app.route("/clientes/sincronizacao/<int:sync_id>/excluir", methods=["POST"])
def excluir_sync_clientes(sync_id):
    if not session.get("usuario"):
        return redirect("/login")

    empresa_id = empresa_atual_id()
    try:
        conn = conectar()
        c = conn.cursor()
        removidos = excluir_sincronizacao_cliente_domain(
            c,
            sync_id,
            empresa_id,
            agora_iso(),
            session.get("usuario") or "",
        )
        conn.commit()
        conn.close()

        if removidos:
            limpar_cache_clientes()
            registrar_auditoria(
                "excluiu_planilha_clientes",
                "sincronizacoes_clientes",
                entidade_id=sync_id,
                detalhes={"empresa_id": empresa_id},
            )
            definir_feedback_clientes("sucesso", "Sincronizacao removida.")
        else:
            definir_feedback_clientes("erro", "Sincronizacao nao encontrada.")
    except Exception as e:
        try:
            conn.close()
        except Exception:
            pass
        definir_feedback_clientes("erro", f"Nao foi possivel excluir a sincronizacao: {e}")
    return redirect("/clientes")

@app.route("/cadastrar", methods=["POST"])
def cadastrar():
    if not session.get("usuario"):
        return redirect("/login")

    placa = (request.form.get("placa") or "").strip().upper()

    try:
        resultado = salvar_cliente_veiculo(
            placa=placa,
            nome=request.form.get("nome", ""),
            telefone=request.form.get("telefone", ""),
            data_nascimento=request.form.get("data_nascimento", ""),
            modelo=request.form.get("modelo", ""),
            cor=request.form.get("cor", ""),
        )
        registrar_alertas_espelho_planilha_cadastro(resultado)
        mensagem = montar_mensagem_publica_cadastro_veiculo(resultado)
        if resultado.get("acao") == "novo":
            registrar_cadastro_novo_para_atendimento(resultado["placa"])
        else:
            remover_cadastro_novo_para_atendimento(resultado["placa"])
        definir_feedback_index("sucesso", mensagem)
        placa = resultado["placa"]
    except Exception as e:
        log_info("ERRO CADASTRO:", e)
        definir_feedback_index("erro", f"Erro ao salvar a placa {placa}: {e}")

    return redirect(f"/?placa={placa}")

@app.route("/editar_cliente", methods=["POST"])
def editar_cliente():
    if not session.get("usuario"):
        return redirect("/login")

    placa_original = (request.form.get("placa_original") or request.form.get("placa") or "").strip()
    placa = (request.form.get("placa") or placa_original).strip()
    redirect_to = normalizar_redirect_interno(
        request.form.get("redirect_to"),
        f"/?placa={placa.upper()}",
    )

    try:
        resultado = salvar_cliente_veiculo(
            placa=placa,
            nome=request.form.get("nome", ""),
            telefone=request.form.get("telefone", ""),
            data_nascimento=request.form.get("data_nascimento", ""),
            modelo=request.form.get("modelo", ""),
            cor=request.form.get("cor", ""),
            placa_original=placa_original,
        )
        registrar_alertas_espelho_planilha_cadastro(resultado)
        mensagem = montar_mensagem_publica_cadastro_veiculo(resultado)

        if redirect_to.startswith("/clientes"):
            definir_feedback_clientes("sucesso", mensagem)
        elif redirect_to.startswith("/base_dados"):
            definir_feedback_base_dados("sucesso", mensagem)
    except Exception as e:
        if redirect_to.startswith("/clientes"):
            definir_feedback_clientes("erro", f"Erro ao salvar cliente: {e}")
        elif redirect_to.startswith("/base_dados"):
            definir_feedback_base_dados("erro", f"Erro ao salvar cliente: {e}")

    return redirect(redirect_to)

@app.route("/servico", methods=["POST"])
def servico():
    if not session.get("usuario"):
        return redirect("/login")

    if bloquear_criacao_atendimento_por_licenca():
        licenca = carregar_contexto_licenca_empresa_seguro()
        definir_feedback_painel(
            "erro",
            f"Limite de atendimentos do plano atingido ({licenca['atendimentos_mes']}/{licenca['limite_atendimentos_mes']}).",
        )
        return redirect("/painel")

    data = request.form
    usuario_info = resumo_usuario_logado()

    from datetime import datetime
    agora = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat()
    empresa_id = empresa_atual_id()

    conn = conectar()
    c = conn.cursor()

    # ðŸ”¥ BUSCAR VEÃCULO PELA PLACA
    placa = data["placa"].upper()

    veiculo = consultar_veiculo_por_placa_domain(c, empresa_id, placa)

    if not veiculo:
        conn.close()
        return "Erro: veiculo nao encontrado"

    veiculo_id = veiculo["id"]
    cliente_id = veiculo["cliente_id"] if veiculo["cliente_id"] else None

    c.execute(
        """
        SELECT id
        FROM servicos
        WHERE empresa_id=?
          AND veiculo_id=?
          AND COALESCE(TRIM(UPPER(status)), '')='EM ANDAMENTO'
        ORDER BY id DESC
        LIMIT 1
        """,
        (empresa_id, veiculo_id),
    )
    atendimento_aberto = c.fetchone()
    if atendimento_aberto:
        conn.close()
        definir_feedback_painel(
            "erro",
            f"A placa {placa} ja possui atendimento em andamento. Finalize ou reabra o atendimento existente antes de iniciar outro.",
        )
        return redirect("/painel")

    # ðŸ”¥ BUSCAR TIPO DE SERVIÃ‡O
    tipo_nome = data["tipo"]

    c.execute("SELECT id, valor FROM tipos_servico WHERE nome=?", (tipo_nome,))
    tipo = c.fetchone()

    if not tipo:
        conn.close()
        return "Erro: tipo nao encontrado"

    tipo_id = tipo["id"]
    valor_base = converter_valor_numerico(tipo["valor"])
    valor_adicional = converter_valor_numerico(data.get("valor_adicional"))
    valor_total = valor_base + valor_adicional
    entrega_prevista = interpretar_hora_brasilia(data.get("entrega_prevista"))
    entrega_prevista_iso = (
        entrega_prevista.isoformat(timespec="seconds")
        if entrega_prevista else None
    )

    # ðŸ”¥ PRIORIDADE
    c.execute("""
        SELECT MAX(prioridade) FROM servicos 
        WHERE empresa_id=? AND COALESCE(TRIM(UPPER(status)), '')='EM ANDAMENTO'
    """, (empresa_id,))

    resultado = c.fetchone()[0]

    if resultado is None:
        nova_prioridade = 0
    else:
        nova_prioridade = resultado + 1

    perfil_cliente_atendimento, motivo_perfil_cliente, atendimentos_anteriores = classificar_perfil_cliente_atendimento(
        c,
        empresa_id,
        veiculo_id,
        placa,
    )

    # ðŸ”¥ INSERIR SERVIÃ‡O (NOVO MODELO)
    c.execute("""
        INSERT INTO servicos 
        (
            empresa_id, veiculo_id, tipo_id, valor, valor_adicional, entrada, entrega_prevista, status, prioridade,
            observacoes, origem, guarita, pneu, cera, hidro_lataria, hidro_vidros,
            perfil_cliente_atendimento, etapa_atual, etapa_atual_iniciada_em, lavagem_iniciada_em, lavagem_segundos, finalizacao_segundos,
            criado_por_usuario, criado_por_nome
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        empresa_id,
        veiculo_id,
        tipo_id,
        valor_total,
        valor_adicional,
        agora,
        entrega_prevista_iso,
        "EM ANDAMENTO",
        nova_prioridade,
        normalizar_texto_campo(data.get("observacoes")),
        normalizar_texto_campo(data.get("origem")),
        normalizar_texto_campo(data.get("guarita")),
        normalizar_texto_campo(data.get("pneu")),
        normalizar_flag_sim_nao(data.get("cera")),
        normalizar_flag_sim_nao(data.get("hidro_lataria")),
        normalizar_flag_sim_nao(data.get("hidro_vidros")),
        perfil_cliente_atendimento,
        "LAVAGEM",
        agora,
        agora,
        0,
        0,
        normalizar_texto_campo(usuario_info.get("usuario")),
        normalizar_texto_campo(usuario_info.get("nome")),
    ))

    servico_id = c.lastrowid

    sincronizar_resumo_veiculo_cliente(
        c,
        veiculo_id,
        placa=placa,
        cliente_id=cliente_id,
        status_atendimento="EM ANDAMENTO",
        entrada=agora,
        entrega=entrega_prevista_iso,
    )

    # ðŸ“¸ FOTOS
    fotos_entrada = request.files.getlist("foto_entrada")
    fotos_detalhe = request.files.getlist("foto_detalhe")
    entrada_salvas = salvar_fotos_servico(c, servico_id, fotos_entrada, "entrada")
    detalhe_salvas = salvar_fotos_servico(c, servico_id, fotos_detalhe, "detalhe")

    conn.commit()
    conn.close()
    limpar_cache_painel()

    registrar_auditoria(
        "iniciou_atendimento",
        "servico",
        entidade_id=servico_id,
        placa=placa,
        detalhes={
            "tipo_servico": tipo_nome,
            "valor_base": valor_base,
            "valor_adicional": valor_adicional,
            "valor_total": valor_total,
            "perfil_cliente_atendimento": perfil_cliente_atendimento,
            "motivo_perfil_cliente": motivo_perfil_cliente,
            "atendimentos_anteriores": atendimentos_anteriores,
            "entrega_prevista": entrega_prevista_iso,
            "fotos_entrada": entrada_salvas,
            "fotos_detalhe": detalhe_salvas,
        },
        usuario=usuario_info,
    )

    definir_feedback_painel("sucesso", f"Atendimento da placa {placa} iniciado com sucesso.")
    return redirect("/painel")

@app.route("/painel/servico/<int:id>/operacional", methods=["POST"])
def salvar_operacional_painel(id):
    if not session.get("usuario"):
        return redirect("/login")

    usuario_info = resumo_usuario_logado()
    conn = conectar()
    c = conn.cursor()
    servico_db = consultar_servico_operacional_domain(c, empresa_atual_id(), id)

    if not servico_db:
        conn.close()
        definir_feedback_painel("erro", "Atendimento nao encontrado.")
        return redirect("/painel")

    atualizar_campos_operacionais_servico(c, id, request.form, usuario_info=usuario_info)
    fotos_entrada = request.files.getlist("foto_entrada")
    fotos_detalhe = request.files.getlist("foto_detalhe")
    entradas_salvas = salvar_fotos_servico(c, id, fotos_entrada, "entrada")
    detalhes_salvos = salvar_fotos_servico(c, id, fotos_detalhe, "detalhe")

    acao = (request.form.get("acao") or "salvar").strip().lower()
    placa = servico_db["placa"] or "-"

    if acao == "finalizar":
        servico_db = registrar_transicao_etapa_servico(c, servico_db, "FINALIZACAO")
        conn.commit()
        conn.close()
        limpar_cache_painel()
        registrar_auditoria(
            "abriu_checklist_finalizacao",
            "servico",
            entidade_id=id,
            placa=placa,
            detalhes={
                "fotos_entrada_adicionadas": entradas_salvas,
                "fotos_detalhe_adicionadas": detalhes_salvos,
            },
            usuario=usuario_info,
        )
        mensagem = f"Checklist aberto para a placa {placa}."
        if entradas_salvas:
            mensagem += f" {entradas_salvas} foto(s) de entrada salva(s)."
        if detalhes_salvos:
            mensagem += f" {detalhes_salvos} foto(s) de detalhe salva(s)."
        definir_feedback_checklist("sucesso", mensagem)
        return redirect(f"/painel/servico/{id}/checklist")

    mensagem = f"Dados operacionais da placa {placa} salvos."

    if entradas_salvas:
        mensagem += f" {entradas_salvas} foto(s) de entrada adicionada(s)."
    if detalhes_salvos:
        mensagem += f" {detalhes_salvos} foto(s) de detalhe adicionada(s)."

    conn.commit()
    conn.close()
    limpar_cache_painel()

    registrar_auditoria(
        "salvou_operacional",
        "servico",
        entidade_id=id,
        placa=placa,
        detalhes={
            "origem": normalizar_texto_campo(request.form.get("origem")),
            "guarita": normalizar_texto_campo(request.form.get("guarita")),
            "pneu": normalizar_texto_campo(request.form.get("pneu")),
            "cera": normalizar_flag_sim_nao(request.form.get("cera")),
            "hidro_lataria": normalizar_flag_sim_nao(request.form.get("hidro_lataria")),
            "hidro_vidros": normalizar_flag_sim_nao(request.form.get("hidro_vidros")),
            "fotos_entrada_adicionadas": entradas_salvas,
            "fotos_detalhe_adicionadas": detalhes_salvos,
        },
        usuario=usuario_info,
    )

    definir_feedback_painel("sucesso", mensagem)
    return redirect("/painel")


@app.route("/painel/servico/<int:id>/etapa", methods=["POST"])
def trocar_etapa_servico_painel(id):
    if not session.get("usuario"):
        return redirect("/login")

    usuario_info = resumo_usuario_logado()
    etapa_destino = request.form.get("etapa_destino")
    conn = conectar()
    c = conn.cursor()
    servico = consultar_servico_operacional_domain(c, empresa_atual_id(), id)

    if not servico:
        conn.close()
        definir_feedback_painel("erro", "Atendimento nao encontrado.")
        return redirect("/painel")

    if normalizar_texto_campo(servico["status"]).upper() == "FINALIZADO":
        conn.close()
        definir_feedback_painel("erro", "Nao e possivel trocar a etapa de um atendimento finalizado.")
        return redirect("/painel")

    servico_atualizado = registrar_transicao_etapa_servico(c, servico, etapa_destino)
    c.execute(
        """
        UPDATE servicos
        SET operacional_por_usuario=?, operacional_por_nome=?
        WHERE empresa_id=? AND id=?
        """,
        (
            normalizar_texto_campo(usuario_info.get("usuario")),
            normalizar_texto_campo(usuario_info.get("nome")),
            empresa_atual_id(),
            id,
        ),
    )
    conn.commit()
    conn.close()
    limpar_cache_painel()

    servico_atualizado = enriquecer_etapas_operacionais_servico(dict(servico_atualizado or {}))
    registrar_auditoria(
        "alterou_etapa_operacional",
        "servico",
        entidade_id=id,
        placa=servico.get("placa"),
        detalhes={
            "etapa_destino": servico_atualizado.get("etapa_atual_normalizada"),
            "lavagem_segundos": servico_atualizado.get("lavagem_segundos_total"),
            "finalizacao_segundos": servico_atualizado.get("finalizacao_segundos_total"),
        },
        usuario=usuario_info,
    )
    definir_feedback_painel(
        "sucesso",
        f"Placa {servico.get('placa') or '-'} movida para a etapa de {servico_atualizado.get('etapa_atual_exibicao', 'Lavagem')}.",
    )
    return redirect("/painel")

@app.route("/painel/servico/<int:id>/cobranca-extra", methods=["POST"])
def adicionar_cobranca_extra_painel(id):
    if not session.get("usuario"):
        return redirect("/login")

    usuario_info = resumo_usuario_logado()
    descricao = normalizar_texto_campo(request.form.get("descricao_extra"))
    valor_extra = converter_valor_numerico(request.form.get("valor_extra"))

    if not descricao:
        definir_feedback_painel("erro", "Informe a descricao da cobranca extra.")
        return redirect("/painel")

    if valor_extra <= 0:
        definir_feedback_painel("erro", "Informe um valor valido para a cobranca extra.")
        return redirect("/painel")

    conn = conectar()
    c = conn.cursor()
    empresa_id = empresa_atual_id()
    servico = consultar_servico_operacional_domain(c, empresa_id, id)

    if not servico:
        conn.close()
        definir_feedback_painel("erro", "Atendimento nao encontrado.")
        return redirect("/painel")

    if normalizar_texto_campo(servico["status"]).upper() == "FINALIZADO":
        conn.close()
        definir_feedback_painel("erro", "Nao e possivel adicionar cobranca extra em atendimento finalizado.")
        return redirect("/painel")

    novo_total = converter_valor_numerico(servico["valor"]) + valor_extra

    c.execute("""
        INSERT INTO servico_cobrancas_extras (
            servico_id, descricao, valor, criado_em, criado_por_usuario, criado_por_nome
        )
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        id,
        descricao,
        valor_extra,
        agora_iso(),
        normalizar_texto_campo(usuario_info.get("usuario")),
        normalizar_texto_campo(usuario_info.get("nome")),
    ))
    c.execute(
        "UPDATE servicos SET valor=? WHERE empresa_id=? AND id=?",
        (novo_total, empresa_id, id),
    )
    conn.commit()
    conn.close()
    limpar_cache_painel()

    registrar_auditoria(
        "adicionou_cobranca_extra",
        "servico",
        entidade_id=id,
        placa=servico["placa"],
        detalhes={
            "descricao": descricao,
            "valor_extra": valor_extra,
            "valor_total": novo_total,
        },
        usuario=usuario_info,
    )

    definir_feedback_painel(
        "sucesso",
        f"Cobranca extra adicionada: {descricao} (R$ {formatar_valor_monetario(valor_extra)}).",
    )
    return redirect("/painel")

@app.route("/painel/servico/<int:id>/checklist", methods=["GET", "POST"])
def checklist_servico(id):
    if not session.get("usuario"):
        return redirect("/login")

    usuario_info = resumo_usuario_logado()
    servico = buscar_servico_operacional(id)

    if not servico:
        definir_feedback_painel("erro", "Atendimento nao encontrado.")
        return redirect("/painel")

    if servico.get("status") == "FINALIZADO":
        definir_feedback_painel("erro", "Esse atendimento ja foi finalizado.")
        return redirect("/painel")

    servico["cliente_nome"] = servico.get("cliente_nome") or "Sem cliente"
    servico["cliente_telefone"] = servico.get("cliente_telefone") or ""
    servico["placa"] = servico.get("placa") or "-"
    servico["modelo"] = servico.get("modelo") or ""
    servico["cor"] = servico.get("cor") or ""
    servico["tipo_nome"] = servico.get("tipo_nome") or "Servico"
    servico["valor_exibicao"] = formatar_valor_monetario(servico.get("valor"))
    enriquecer_responsaveis_servico(servico)
    enriquecer_entrega_servico(servico)
    enriquecer_etapas_operacionais_servico(servico)
    servico["cobrancas_extras_info"] = listar_cobrancas_extras_servico(id)
    entrada = interpretar_datahora_sistema(servico.get("entrada"))
    servico["entrada_exibicao"] = (
        entrada.strftime("%d/%m/%Y %H:%M")
        if entrada else (servico.get("entrada") or "-")
    )

    itens = listar_itens_checklist(apenas_ativos=True)
    checked_ids = {
        item.get("item_id")
        for item in listar_checklist_servico(id)
        if item.get("marcado")
    }
    checked_ids = {item_id for item_id in checked_ids if item_id is not None}
    feedback = session.pop("checklist_feedback", None)

    if request.method == "POST":
        checked_ids = {
            int(valor)
            for valor in request.form.getlist("item_ids")
            if str(valor).isdigit()
        }
        ids_ativos = {item["id"] for item in itens}
        fotos_saida = request.files.getlist("foto_saida")

        if not itens:
            feedback = {
                "tipo": "erro",
                "mensagem": "Cadastre pelo menos um item em Itens para usar o checklist de finalizacao.",
            }
        elif checked_ids != ids_ativos:
            faltantes = [item["nome"] for item in itens if item["id"] not in checked_ids]
            feedback = {
                "tipo": "erro",
                "mensagem": "Marque todos os itens obrigatorios antes de finalizar: " + ", ".join(faltantes),
            }
        elif contar_fotos_validas(fotos_saida) == 0:
            feedback = {
                "tipo": "erro",
                "mensagem": "Envie pelo menos uma foto de finalizacao para concluir o atendimento.",
            }
        else:
            conn = conectar()
            c = conn.cursor()
            substituir_checklist_servico_domain(c, id, itens)

            fotos_saida_salvas = salvar_fotos_servico(c, id, fotos_saida, "saida")
            servico = consolidar_tempo_etapa_atual_servico(c, servico, etapa_final="FINALIZACAO")
            c.execute("""
                UPDATE servicos
                SET status='FINALIZADO',
                    entrega=?,
                    finalizado_por_usuario=?,
                    finalizado_por_nome=?
                WHERE empresa_id=? AND id=?
            """, (
                agora_iso(),
                normalizar_texto_campo(usuario_info.get("usuario")),
                normalizar_texto_campo(usuario_info.get("nome")),
                empresa_atual_id(),
                id,
            ))

            veiculo_finalizado_id = servico["veiculo_id"]
            placa_finalizada = normalizar_texto_campo(servico.get("placa") or "-").upper()

            sincronizar_resumo_veiculo_cliente(
                c,
                veiculo_finalizado_id,
                placa=placa_finalizada,
                status_atendimento="FINALIZADO",
                entrega=agora_iso(),
            )
            conn.commit()
            conn.close()
            limpar_cache_painel()

            registrar_auditoria(
                "finalizou_atendimento",
                "servico",
                entidade_id=id,
                placa=servico["placa"],
                detalhes={
                    "checklist_itens": len(itens),
                    "fotos_saida": fotos_saida_salvas,
                },
                usuario=usuario_info,
            )

            definir_feedback_painel(
                "sucesso",
                f"Atendimento da placa {servico['placa']} finalizado com checklist completo."
            )
            return redirect("/painel")

    return render_template(
        "checklist_finalizacao.html",
        servico=servico,
        itens=itens,
        checked_ids=checked_ids,
        feedback=feedback,
    )

@app.route("/finalizar/<int:id>", methods=["POST"])
def finalizar(id):
    if not session.get("usuario"):
        return redirect("/login")
    definir_feedback_checklist("erro", "Use o checklist obrigatorio para finalizar o atendimento.")
    return redirect(f"/painel/servico/{id}/checklist")

@app.route("/detalhe/<int:id>", methods=["POST"])
def detalhe(id):
    if not session.get("usuario"):
        return redirect("/login")

    usuario_info = resumo_usuario_logado()
    servico = buscar_servico_operacional(id)
    if not servico:
        definir_feedback_painel("erro", "Atendimento nao encontrado.")
        return redirect("/painel")

    conn = conectar()
    c = conn.cursor()

    fotos_salvas = salvar_fotos_servico(c, id, request.files.getlist("foto_detalhe"), "detalhe")

    conn.commit()
    conn.close()
    limpar_cache_painel()
    registrar_auditoria(
        "adicionou_fotos_detalhe",
        "servico",
        entidade_id=id,
        placa=servico.get("placa"),
        detalhes={"fotos_detalhe": fotos_salvas},
        usuario=usuario_info,
    )
    definir_feedback_painel("sucesso", "Fotos de detalhe salvas no painel.")

    return redirect("/painel")

@app.route("/prioridade/<int:id>/<acao>")
def prioridade(id, acao):
    if not session.get("usuario"):
        return redirect("/login")
    conn = conectar()
    c = conn.cursor()
    empresa_id = empresa_atual_id()

    # pega prioridade atual
    c.execute("SELECT prioridade FROM servicos WHERE empresa_id=? AND id=?", (empresa_id, id))
    atual = c.fetchone()

    if not atual:
        conn.close()
        return redirect("/painel")

    atual = atual[0]

    if acao == "up":
        c.execute("""
        SELECT id, prioridade FROM servicos
        WHERE empresa_id=? AND prioridade < ? AND COALESCE(TRIM(UPPER(status)), '')='EM ANDAMENTO'
        ORDER BY prioridade DESC LIMIT 1
        """, (empresa_id, atual))

    elif acao == "down":
        c.execute("""
        SELECT id, prioridade FROM servicos
        WHERE empresa_id=? AND prioridade > ? AND COALESCE(TRIM(UPPER(status)), '')='EM ANDAMENTO'
        ORDER BY prioridade ASC LIMIT 1
        """, (empresa_id, atual))

    else:
        conn.close()
        return redirect("/painel")

    outro = c.fetchone()

    # se existir outro, troca posiÃ§Ã£o
    if outro:
        outro_id, outro_prio = outro

        c.execute("UPDATE servicos SET prioridade=? WHERE empresa_id=? AND id=?", (outro_prio, empresa_id, id))
        c.execute("UPDATE servicos SET prioridade=? WHERE empresa_id=? AND id=?", (atual, empresa_id, outro_id))

        conn.commit()

    conn.close()
    return redirect("/painel")


@app.route("/cadastrar_servico", methods=["GET", "POST"])
def cadastrar_servico():
    if not session.get("usuario"):
        return redirect("/login")

    if request.method == "GET":
        preparar_rotinas_interface_logada()
    conn = conectar()
    c = conn.cursor()

    if request.method == "POST":
        nome = request.form["nome"]
        valor = converter_valor_numerico(request.form["valor"])

        c.execute("INSERT INTO tipos_servico (nome, valor) VALUES (?, ?)", (nome, valor))
        conn.commit()

    c.execute("SELECT * FROM tipos_servico")
    servicos_lista = c.fetchall()

    conn.close()

    return render_template("cadastro_servico.html", servicos=servicos_lista)

@app.route("/pneu", methods=["GET", "POST"])
def cadastrar_pneu():
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    if request.method == "POST":
        nome = request.form["nome"]
        c.execute("INSERT INTO produtos_pneu (nome) VALUES (?)", (nome,))
        conn.commit()

    # ðŸ”¥ LISTAR (ANTES ESTAVA FALTANDO)
    c.execute("SELECT * FROM produtos_pneu")
    lista = c.fetchall()

    conn.close()

    return render_template("pneu.html", produtos=lista)

@app.route("/itens", methods=["GET", "POST"])
def itens_checklist():
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    if request.method == "POST":
        nome = normalizar_texto_campo(request.form.get("nome"))

        if not nome:
            definir_feedback_itens("erro", "Informe o nome do item do checklist.")
            conn.close()
            return redirect("/itens")

        c.execute("SELECT COALESCE(MAX(ordem), 0) + 1 FROM checklist_itens")
        ordem = c.fetchone()[0]
        c.execute(
            "INSERT INTO checklist_itens (nome, ativo, ordem) VALUES (?, 1, ?)",
            (nome, ordem)
        )
        conn.commit()
        conn.close()
        definir_feedback_itens("sucesso", f"Item '{nome}' adicionado ao checklist.")
        return redirect("/itens")

    conn.close()

    return render_template(
        "itens_checklist.html",
        itens=listar_itens_checklist(),
        feedback=session.pop("itens_feedback", None),
    )

@app.route("/itens/<int:item_id>/alternar", methods=["POST"])
def alternar_item_checklist(item_id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id, nome, ativo FROM checklist_itens WHERE id=?", (item_id,))
    item = c.fetchone()

    if not item:
        conn.close()
        definir_feedback_itens("erro", "Item do checklist nao encontrado.")
        return redirect("/itens")

    novo_ativo = 0 if item["ativo"] else 1
    c.execute("UPDATE checklist_itens SET ativo=? WHERE id=?", (novo_ativo, item_id))
    conn.commit()
    conn.close()

    definir_feedback_itens(
        "sucesso",
        f"Item '{item['nome']}' {'ativado' if novo_ativo else 'pausado'}."
    )
    return redirect("/itens")

@app.route("/itens/<int:item_id>/excluir", methods=["POST"])
def excluir_item_checklist(item_id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT nome FROM checklist_itens WHERE id=?", (item_id,))
    item = c.fetchone()

    if not item:
        conn.close()
        definir_feedback_itens("erro", "Item do checklist nao encontrado.")
        return redirect("/itens")

    c.execute("DELETE FROM checklist_itens WHERE id=?", (item_id,))
    conn.commit()
    conn.close()

    definir_feedback_itens("sucesso", f"Item '{item['nome']}' removido.")
    return redirect("/itens")

@app.route("/base_dados")
def base_dados():
    if not session.get("usuario"):
        return redirect("/login")

    payload = obter_payload_base_dados()

    return render_template(
        "base_dados.html",
        registros=payload["dados"],
        resumo=payload["resumo"],
        feedback=session.pop("base_dados_feedback", None),
    )

@app.route("/base_dados/upload", methods=["POST"])
def base_dados_upload():
    if not session.get("usuario"):
        return redirect("/login")

    arquivo = request.files.get("arquivo")

    if not arquivo:
        definir_feedback_base_dados("erro", "Selecione uma planilha para importar.")
        return redirect("/base_dados")

    try:
        df, filename = ler_dataframe_arquivo_planilha(arquivo)
        mapeamento = sugerir_mapeamento_colunas(list(df.columns))

        if not mapeamento.get("placa"):
            raise ValueError("Nao encontrei uma coluna de placa na planilha enviada.")

        estatisticas = importar_clientes_dataframe(df, mapeamento)
        limpar_cache_clientes()
        mensagem = f"Upload '{filename}' importado com sucesso. {resumir_importacao_clientes(estatisticas)}"
        definir_feedback_base_dados("sucesso", mensagem)
        salvar_notificacao(mensagem, "sucesso")
    except Exception as e:
        definir_feedback_base_dados("erro", f"Erro ao importar planilha: {e}")

    return redirect("/base_dados")


@app.route("/confirmar_importacao", methods=["POST"])
def confirmar_importacao_legado():
    if not session.get("usuario"):
        return redirect("/login")

    definir_feedback_base_dados(
        "erro",
        "Fluxo antigo de pre-visualizacao desativado. Envie a planilha novamente pela tela Base de Dados.",
    )
    return redirect("/base_dados")


@app.route("/api/base_dados")
def api_base_dados():
    if not session.get("usuario"):
        return jsonify({"erro": "nao autorizado"}), 401

    return jsonify(obter_payload_base_dados())

@app.route("/api/base_dados/salvar", methods=["POST"])
@app.route("/api/salvar_base", methods=["POST"])
def api_salvar_base():
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    payload = request.get_json(silent=True) or {}
    linhas = payload.get("linhas") or payload.get("dados") or []

    try:
        estatisticas = salvar_linhas_base_dados(linhas)
        mensagem = resumir_salvamento_base_dados(estatisticas)
        limpar_cache_clientes()
        payload = obter_payload_base_dados(force=True)
        return jsonify({
            "status": "ok",
            "mensagem": mensagem,
            "dados": payload["dados"],
            "resumo": payload["resumo"],
        })
    except Exception as e:
        return jsonify({
            "status": "erro",
            "mensagem": str(e),
        }), 400


def listar_servicos_em_andamento_voz():
    if not INIT_DB_EXECUTADO or (
        modo_banco_preferido() == "postgres" and not SCHEMA_BANCO_ONLINE_GARANTIDO
    ):
        return []

    def carregar(conn):
        c = conn.cursor()
        return consultar_servicos_em_andamento_voz_domain(c, empresa_atual_id())

    servicos_db = executar_leitura_resiliente(
        carregar,
        descricao="OPERACIONAL VOZ",
        padrao=[],
    ) or []

    agora_atual = datetime.now(ZoneInfo("America/Sao_Paulo"))
    servicos = []

    for row in servicos_db:
        item = dict(row)
        entrada = interpretar_datahora_sistema(item.get("entrada"))

        try:
            minutos = int((agora_atual - entrada).total_seconds() / 60) if entrada else 0
        except Exception:
            minutos = 0

        minutos = max(0, minutos)
        horas = minutos // 60
        minutos_restantes = minutos % 60

        if horas > 0:
            tempo_exibicao = f"{horas}h {minutos_restantes}min"
        else:
            tempo_exibicao = f"{minutos_restantes}min"

        item_saida = {
            "id": item.get("id"),
            "placa": item.get("placa") or "",
            "modelo": item.get("modelo") or "",
            "cor": item.get("cor") or "",
            "cliente_nome": item.get("cliente_nome") or "",
            "servico": item.get("tipo_nome") or "Servico",
            "minutos_em_andamento": minutos,
            "tempo_exibicao": tempo_exibicao,
            "entrada_exibicao": (
                entrada.strftime("%d/%m/%Y %H:%M")
                if entrada else (item.get("entrada") or "-")
            ),
            "entrega_prevista": item.get("entrega_prevista"),
            "valor_adicional": item.get("valor_adicional"),
        }
        enriquecer_entrega_servico(item_saida, referencia=agora_atual)
        servicos.append(item_saida)

    return servicos


@app.route("/api/operacional/voz")
def api_operacional_voz():
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    usuario_cache = str(session.get("usuario") or "")
    agora_cache_ts = time.time()
    if (
        VOZ_CACHE.get("resultado") is not None
        and VOZ_CACHE.get("usuario") == usuario_cache
        and agora_cache_ts - float(VOZ_CACHE.get("testado_em") or 0.0) < VOZ_CACHE_TTL
    ):
        return jsonify(VOZ_CACHE["resultado"])

    try:
        resultado = {
            "status": "ok",
            "gerado_em": agora_iso(),
            "servicos": listar_servicos_em_andamento_voz(),
        }
        VOZ_CACHE["testado_em"] = agora_cache_ts
        VOZ_CACHE["usuario"] = usuario_cache
        VOZ_CACHE["resultado"] = dict(resultado)
        return jsonify(resultado)
    except Exception as e:
        log_info("ERRO OPERACIONAL VOZ:", e)
        resultado = {
            "status": "ok",
            "gerado_em": agora_iso(),
            "servicos": [],
        }
        VOZ_CACHE["testado_em"] = agora_cache_ts
        VOZ_CACHE["usuario"] = usuario_cache
        VOZ_CACHE["resultado"] = dict(resultado)
        return jsonify(resultado)


def _carregar_dados_painel(conn):
    c = conn.cursor()
    empresa_id = empresa_atual_id()
    servicos_db = medir_consulta_sql(
        "/painel",
        "servicos_em_andamento",
        lambda: [dict(row) for row in consultar_servicos_em_andamento_domain(c, empresa_id)],
        detalhes="servicos + veiculos + clientes",
    )
    pendentes_perfil = [
        servico
        for servico in servicos_db
        if not normalizar_texto_campo(servico.get("perfil_cliente_atendimento"))
        and converter_inteiro(servico.get("veiculo_id"), 0)
        and converter_inteiro(servico.get("id"), 0)
    ]
    historico_por_servico = {}
    if pendentes_perfil:
        ids = [converter_inteiro(servico.get("id"), 0) for servico in pendentes_perfil]
        placeholders = ",".join(["?"] * len(ids))

        def carregar_historico_perfil():
            c.execute(
                f"""
                SELECT atual.id AS servico_id, COUNT(anteriores.id) AS anteriores
                FROM servicos atual
                LEFT JOIN servicos anteriores
                  ON anteriores.empresa_id=atual.empresa_id
                 AND anteriores.veiculo_id=atual.veiculo_id
                 AND anteriores.id < atual.id
                WHERE atual.empresa_id=?
                  AND atual.id IN ({placeholders})
                GROUP BY atual.id
                """,
                (empresa_id, *ids),
            )
            return {
                converter_inteiro(row["servico_id"], 0): converter_inteiro(row["anteriores"], 0)
                for row in c.fetchall()
            }

        historico_por_servico = medir_consulta_sql(
            "/painel",
            "perfil_novo_retorno_agregado",
            carregar_historico_perfil,
            detalhes="substitui N consultas por 1 agregacao",
        )

    for servico in servicos_db:
        if normalizar_texto_campo(servico.get("perfil_cliente_atendimento")):
            continue
        servico_id = converter_inteiro(servico.get("id"), 0)
        if not servico_id:
            servico["perfil_cliente_atendimento"] = "NOVO"
            continue
        anteriores = historico_por_servico.get(servico_id, 0)
        servico["perfil_cliente_atendimento"] = "RETORNO" if anteriores > 0 else "NOVO"
    agora_cache_ts = time.time()
    produtos_cache = PRODUTOS_PNEU_CACHE.get("resultado")
    if produtos_cache is not None and (
        agora_cache_ts - float(PRODUTOS_PNEU_CACHE.get("testado_em") or 0.0) < PRODUTOS_PNEU_CACHE_TTL
    ):
        registrar_metrica_consulta_sql("/painel", "produtos_pneu", 0, origem="cache", cache_hit=True)
        produtos_pneu = list(produtos_cache)
    else:
        produtos_pneu = medir_consulta_sql(
            "/painel",
            "produtos_pneu",
            lambda: (c.execute("SELECT nome FROM produtos_pneu ORDER BY nome"), [row[0] for row in c.fetchall()])[1],
        )
        PRODUTOS_PNEU_CACHE["testado_em"] = agora_cache_ts
        PRODUTOS_PNEU_CACHE["resultado"] = list(produtos_pneu)
    ids_servicos = [row["id"] for row in servicos_db]
    return {
        "servicos_db": servicos_db,
        "produtos_pneu": produtos_pneu,
        "resumo_fotos_por_servico": {},
        "resumo_extras_por_servico": {},
        "detalhes_sob_demanda": bool(ids_servicos),
    }


@app.route("/painel")
def painel():
    if not session.get("usuario"):
        return redirect("/login")

    preparar_rotinas_interface_logada()
    usuario_cache = str(session.get("usuario") or "")
    empresa_id = empresa_atual_id()
    chave_cache = f"{usuario_cache}|{empresa_id}"
    leitura_painel = obter_cache_consulta(
        PAINEL_CONTEXT_CACHE,
        chave_cache,
        PAINEL_CONTEXT_CACHE_TTL,
    )
    if leitura_painel is not None:
        registrar_metrica_consulta_sql("/painel", "snapshot_painel", 0, origem="cache", cache_hit=True)
    if leitura_painel is None:
        leitura_painel = executar_leitura_resiliente(
            lambda conn: _carregar_dados_painel(conn),
            descricao="PAINEL",
            padrao={
                "servicos_db": [],
                "produtos_pneu": [],
                "resumo_fotos_por_servico": {},
                "resumo_extras_por_servico": {},
            },
        ) or {
            "servicos_db": [],
            "produtos_pneu": [],
            "resumo_fotos_por_servico": {},
            "resumo_extras_por_servico": {},
        }
        salvar_cache_consulta(PAINEL_CONTEXT_CACHE, chave_cache, leitura_painel)

    servicos_db = leitura_painel["servicos_db"]
    produtos_pneu = leitura_painel["produtos_pneu"]
    resumo_fotos_por_servico = leitura_painel.get("resumo_fotos_por_servico") or leitura_painel.get("fotos_por_servico", {})
    resumo_extras_por_servico = leitura_painel.get("resumo_extras_por_servico") or leitura_painel.get("extras_por_servico", {})

    servicos = []

    for s in servicos_db:
        s_dict = dict(s)

        # ðŸ”¥ PRIORIDADE IA
        prioridade_ia = calcular_prioridade_inteligente(s_dict)
        s_dict["prioridade_ia"] = prioridade_ia

        # ðŸ”¥ TEMPO DE ESPERA
        entrada = interpretar_datahora_sistema(s_dict.get("entrada"))

        try:
            agora_atual = datetime.now(ZoneInfo("America/Sao_Paulo"))

            diff = agora_atual - entrada
            minutos = int(diff.total_seconds() / 60)

            horas = minutos // 60
            mins = minutos % 60

            if horas > 0:
                tempo_str = f"{horas}h {mins}min"
            else:
                tempo_str = f"{mins}min"

        except:
            minutos = 0
            tempo_str = "N/A"

        s_dict["tempo_espera"] = tempo_str
        s_dict["tempo_espera_minutos"] = max(0, minutos)
        s_dict["entrada_exibicao"] = (
            entrada.strftime("%d/%m/%Y %H:%M")
            if entrada else (s_dict.get("entrada") or "-")
        )
        s_dict["valor_exibicao"] = formatar_valor_monetario(s_dict.get("valor"))
        s_dict["cliente_nome"] = s_dict.get("cliente_nome") or "Sem cliente"
        s_dict["cliente_telefone"] = s_dict.get("cliente_telefone") or ""
        s_dict["perfil_cliente_atendimento"] = normalizar_perfil_cliente_atendimento(
            s_dict.get("perfil_cliente_atendimento"),
            fallback="NOVO",
        )
        s_dict["perfil_cliente_atendimento_exibicao"] = perfil_cliente_atendimento_exibicao(
            s_dict["perfil_cliente_atendimento"]
        )
        s_dict["placa"] = s_dict.get("placa") or "-"
        s_dict["modelo"] = s_dict.get("modelo") or ""
        s_dict["cor"] = s_dict.get("cor") or ""
        s_dict["tipo_nome"] = s_dict.get("tipo_nome") or "Servico"
        s_dict["origem"] = s_dict.get("origem") or ""
        s_dict["guarita"] = s_dict.get("guarita") or ""
        s_dict["observacoes"] = s_dict.get("observacoes") or ""
        s_dict["pneu"] = s_dict.get("pneu") or ""
        s_dict["cera"] = s_dict.get("cera") or "Nao"
        s_dict["hidro_lataria"] = s_dict.get("hidro_lataria") or "Nao"
        s_dict["hidro_vidros"] = s_dict.get("hidro_vidros") or "Nao"
        s_dict["galeria_fotos"] = {}
        resumo_fotos = resumo_fotos_por_servico.get(s_dict["id"], {})
        s_dict["fotos_entrada"] = (
            len(resumo_fotos.get("entrada", []))
            if isinstance(resumo_fotos.get("entrada"), list)
            else converter_inteiro(resumo_fotos.get("entrada"), 0)
        )
        s_dict["fotos_detalhe"] = (
            len(resumo_fotos.get("detalhe", []))
            if isinstance(resumo_fotos.get("detalhe"), list)
            else converter_inteiro(resumo_fotos.get("detalhe"), 0)
        )
        s_dict["fotos_saida"] = (
            len(resumo_fotos.get("saida", []))
            if isinstance(resumo_fotos.get("saida"), list)
            else converter_inteiro(resumo_fotos.get("saida"), 0)
        )
        s_dict["tem_fotos"] = bool(s_dict["fotos_entrada"] or s_dict["fotos_detalhe"] or s_dict["fotos_saida"])
        s_dict["cobrancas_extras_info"] = resumo_extras_por_servico.get(s_dict["id"], {
            "itens": [],
            "quantidade": 0,
            "total": 0.0,
            "total_exibicao": "0.00",
        })
        s_dict["tem_cobrancas_extras"] = bool(
            s_dict["cobrancas_extras_info"].get("quantidade")
            or s_dict["cobrancas_extras_info"].get("itens")
        )
        enriquecer_responsaveis_servico(s_dict)
        enriquecer_entrega_servico(s_dict)
        enriquecer_etapas_operacionais_servico(s_dict)

        servicos.append(s_dict)

    servicos = ordenar_servicos_fluxo_atendimento(servicos)
    servicos_lavagem = [item for item in servicos if item.get("etapa_atual_normalizada") == "LAVAGEM"]
    servicos_finalizacao = [item for item in servicos if item.get("etapa_atual_normalizada") == "FINALIZACAO"]
    resumo_fluxo = montar_resumo_fluxo_atendimento(servicos)

    return render_template(
        "painel.html",
        servicos=servicos,
        servicos_lavagem=servicos_lavagem,
        servicos_finalizacao=servicos_finalizacao,
        resumo_fluxo=resumo_fluxo,
        produtos_pneu=produtos_pneu,
        feedback=session.pop("painel_feedback", None),
    )

@app.route("/api/painel/servico/<int:id>/detalhes")
def api_painel_servico_detalhes(id):
    if not session.get("usuario"):
        return jsonify({"ok": False, "erro": "Sessao expirada. Entre novamente para carregar os detalhes."}), 401

    empresa_id = empresa_atual_id()

    def carregar(conn):
        c = conn.cursor()
        servico = consultar_servico_operacional_domain(c, empresa_id, id)
        if not servico:
            return None
        fotos = listar_fotos_servicos([id], cursor=c).get(id, {})
        extras = listar_cobrancas_extras_servicos([id], cursor=c).get(
            id,
            {"itens": [], "total": 0.0, "total_exibicao": "0.00"},
        )
        return {
            "fotos": fotos,
            "cobrancas_extras": extras,
        }

    try:
        detalhes = executar_leitura_resiliente(
            carregar,
            descricao="PAINEL_DETALHES_SERVICO",
            padrao=None,
        )
    except Exception:
        app.logger.exception("Falha ao carregar detalhes do painel")
        detalhes = None

    if detalhes is None:
        return jsonify({
            "ok": False,
            "erro": "Nao foi possivel carregar os detalhes agora. Tente novamente.",
        }), 503

    return jsonify({
        "ok": True,
        "servico_id": id,
        **detalhes,
    })

@app.route("/historico", methods=["GET", "POST"])
def pagina_historico():
    if not session.get("usuario"):
        return redirect("/login")

    preparar_rotinas_interface_logada()
    busca = (request.form.get("busca") or request.args.get("busca") or "").strip()
    usuario_cache = str(session.get("usuario") or "")
    empresa_id = empresa_atual_id()
    chave_cache = f"{usuario_cache}|{empresa_id}|{busca}"
    historico = obter_cache_consulta(
        HISTORICO_CONTEXT_CACHE,
        chave_cache,
        HISTORICO_CONTEXT_CACHE_TTL,
    )
    if historico is None:
        historico = listar_historico_servicos(busca=busca)
        salvar_cache_consulta(HISTORICO_CONTEXT_CACHE, chave_cache, historico)
    return render_template(
        "historico.html",
        historico=historico,
        busca=busca,
        feedback=session.pop("historico_feedback", None),
    )

@app.route("/historico/servico/<int:id>/editar", methods=["GET", "POST"])
def editar_atendimento_historico(id):
    if not session.get("usuario"):
        return redirect("/login")

    servico = buscar_servico_operacional(id)
    if not servico:
        definir_feedback_historico("erro", "Atendimento nao encontrado.")
        return redirect("/historico")

    redirect_to = (
        request.form.get("redirect_to")
        if request.method == "POST"
        else request.args.get("redirect_to")
    )
    redirect_to = normalizar_redirect_interno(redirect_to, "/historico")

    conn = conectar()
    c = conn.cursor()
    tipos_servico, produtos_pneu = carregar_recursos_edicao_historico_domain(c)
    conn.close()

    if request.method == "POST":
        usuario_info = resumo_usuario_logado()
        try:
            tipo_id = converter_inteiro(request.form.get("tipo_id"), 0)
            if not tipo_id:
                raise ValueError("Selecione um tipo de servico valido.")

            valor = converter_valor_numerico(request.form.get("valor"))
            valor_adicional = converter_valor_numerico(request.form.get("valor_adicional"))
            entrada_iso = normalizar_datahora_formulario(request.form.get("entrada"), obrigatoria=True)
            entrega_prevista_iso = normalizar_datahora_formulario(request.form.get("entrega_prevista"))
            status = normalizar_texto_campo(request.form.get("status")).upper() or "EM ANDAMENTO"
            etapa_destino = normalizar_etapa_operacional(request.form.get("etapa_atual"), fallback=servico.get("etapa_atual") or "LAVAGEM")
            entrega_iso = normalizar_datahora_formulario(request.form.get("entrega"))

            finalizado_por_usuario = servico.get("finalizado_por_usuario")
            finalizado_por_nome = servico.get("finalizado_por_nome")
            if status == "FINALIZADO":
                if not entrega_iso:
                    entrega_iso = servico.get("entrega") or agora_iso()
                if not normalizar_texto_campo(finalizado_por_usuario):
                    finalizado_por_usuario = normalizar_texto_campo(usuario_info.get("usuario"))
                    finalizado_por_nome = normalizar_texto_campo(usuario_info.get("nome"))
            else:
                entrega_iso = None
                finalizado_por_usuario = None
                finalizado_por_nome = None

            conn = conectar()
            c = conn.cursor()
            servico = aplicar_fluxo_etapa_atendimento_em_edicao(
                c,
                servico,
                status,
                etapa_destino,
            )
            c.execute(
                """
                UPDATE servicos
                SET tipo_id=?, valor=?, valor_adicional=?, entrada=?, entrega_prevista=?,
                    entrega=?, status=?, observacoes=?, origem=?, guarita=?, pneu=?,
                    cera=?, hidro_lataria=?, hidro_vidros=?, finalizado_por_usuario=?,
                    finalizado_por_nome=?
                WHERE empresa_id=? AND id=?
                """,
                (
                    tipo_id,
                    valor,
                    valor_adicional,
                    entrada_iso,
                    entrega_prevista_iso,
                    entrega_iso,
                    status,
                    normalizar_texto_campo(request.form.get("observacoes")),
                    normalizar_texto_campo(request.form.get("origem")),
                    normalizar_texto_campo(request.form.get("guarita")),
                    normalizar_texto_campo(request.form.get("pneu")),
                    normalizar_texto_campo(request.form.get("cera")) or "Nao",
                    normalizar_texto_campo(request.form.get("hidro_lataria")) or "Nao",
                    normalizar_texto_campo(request.form.get("hidro_vidros")) or "Nao",
                    finalizado_por_usuario,
                    finalizado_por_nome,
                    empresa_atual_id(),
                    id,
                ),
            )
            recalcular_resumo_veiculo_por_servicos(c, servico["veiculo_id"])
            conn.commit()
            conn.close()
            registrar_auditoria(
                "editou_atendimento_historico",
                "servico",
                entidade_id=id,
                placa=servico.get("placa"),
                detalhes={
                    "status": status,
                    "etapa_atual": etapa_destino,
                    "tipo_id": tipo_id,
                    "valor": valor,
                    "valor_adicional": valor_adicional,
                },
                usuario=usuario_info,
            )
            definir_feedback_por_destino(
                redirect_to,
                "sucesso",
                f"Atendimento da placa {servico.get('placa') or '-'} atualizado com sucesso.",
            )
            return redirect(redirect_to)
        except Exception as e:
            definir_feedback_historico("erro", f"Nao foi possivel atualizar o atendimento: {e}")
            return redirect(f"/historico/servico/{id}/editar?redirect_to={quote(redirect_to, safe='/?=&')}")

    servico["entrada_input"] = formatar_datahora_input(servico.get("entrada"))
    servico["entrega_prevista_input"] = formatar_datahora_input(servico.get("entrega_prevista"))
    servico["entrega_input"] = formatar_datahora_input(servico.get("entrega"))
    servico["valor_exibicao"] = formatar_valor_monetario(servico.get("valor"))
    servico["valor_adicional_exibicao"] = formatar_valor_monetario(servico.get("valor_adicional"))
    servico["galeria_fotos"] = listar_fotos_servicos([id]).get(id, {})
    enriquecer_entrega_servico(servico)
    enriquecer_responsaveis_servico(servico)
    enriquecer_etapas_operacionais_servico(servico)
    servico["etapa_atual_form"] = servico.get("etapa_atual_normalizada") or "LAVAGEM"

    return render_template(
        "editar_atendimento.html",
        servico=servico,
        tipos_servico=tipos_servico,
        produtos_pneu=produtos_pneu,
        redirect_to=redirect_to,
        feedback=session.pop("historico_feedback", None),
    )

@app.route("/historico/servico/<int:id>/fotos", methods=["POST"])
def enviar_fotos_historico(id):
    if not session.get("usuario"):
        return redirect("/login")

    redirect_to = normalizar_redirect_interno(request.form.get("redirect_to"), "/historico")
    tipo_foto = normalizar_texto_campo(request.form.get("tipo_foto")).lower()
    if tipo_foto not in {"entrada", "detalhe", "saida"}:
        definir_feedback_por_destino(redirect_to, "erro", "Tipo de foto invalido.")
        return redirect(redirect_to)

    servico = buscar_servico_operacional(id)
    if not servico:
        definir_feedback_por_destino(redirect_to, "erro", "Atendimento nao encontrado.")
        return redirect(redirect_to)

    conn = conectar()
    c = conn.cursor()
    total = salvar_fotos_servico(c, id, request.files.getlist("fotos"), tipo_foto)
    conn.commit()
    conn.close()

    if total <= 0:
        definir_feedback_por_destino(redirect_to, "erro", "Nenhuma foto valida foi enviada.")
        return redirect(redirect_to)

    registrar_auditoria(
        "adicionou_fotos_atendimento",
        "servico",
        entidade_id=id,
        placa=servico.get("placa"),
        detalhes={"tipo_foto": tipo_foto, "quantidade": total},
    )
    definir_feedback_por_destino(
        redirect_to,
        "sucesso",
        f"{total} foto(s) de {tipo_foto} enviada(s) para a placa {servico.get('placa') or '-'}."
    )
    return redirect(redirect_to)


@app.route("/historico/servico/<int:id>/fotos/<int:foto_id>/excluir", methods=["POST"])
def excluir_foto_historico(id, foto_id):
    if not session.get("usuario"):
        return redirect("/login")

    redirect_to = normalizar_redirect_interno(
        request.form.get("redirect_to"),
        f"/historico/servico/{id}/editar",
    )
    servico = buscar_servico_operacional(id)
    if not servico:
        definir_feedback_por_destino(redirect_to, "erro", "Atendimento nao encontrado.")
        return redirect(redirect_to)

    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    c.execute(
        """
        SELECT id, servico_id, tipo, caminho, arquivo_nome
        FROM fotos
        WHERE empresa_id=? AND servico_id=? AND id=?
        """,
        (empresa_id, id, foto_id),
    )
    foto = c.fetchone()

    if not foto:
        conn.close()
        definir_feedback_por_destino(redirect_to, "erro", "Foto nao encontrada para este atendimento.")
        return redirect(redirect_to)

    caminho_foto = foto["caminho"]
    tipo_foto = normalizar_texto_campo(foto["tipo"]) or "foto"
    nome_arquivo = str(foto["arquivo_nome"] or "").strip() or os.path.basename(str(caminho_foto or "").replace("\\", "/"))
    c.execute("DELETE FROM fotos WHERE empresa_id=? AND servico_id=? AND id=?", (empresa_id, id, foto_id))
    conn.commit()
    conn.close()
    limpar_cache_painel()
    remover_foto_servico_local(caminho_foto)

    registrar_auditoria(
        "removeu_foto_atendimento",
        "foto",
        entidade_id=foto_id,
        placa=servico.get("placa"),
        detalhes={
            "servico_id": id,
            "tipo_foto": tipo_foto,
            "arquivo_nome": nome_arquivo,
        },
    )
    definir_feedback_por_destino(
        redirect_to,
        "sucesso",
        f"Foto de {tipo_foto} removida do atendimento da placa {servico.get('placa') or '-'}."
    )
    return redirect(redirect_to)


@app.route("/historico/servico/<int:id>/reabrir", methods=["POST"])
def reabrir_atendimento_historico(id):
    if not session.get("usuario"):
        return redirect("/login")

    servico = buscar_servico_operacional(id)
    redirect_to = normalizar_redirect_interno(request.form.get("redirect_to"), "/historico")

    if not servico:
        definir_feedback_por_destino(redirect_to, "erro", "Atendimento nao encontrado.")
        return redirect(redirect_to)

    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    c.execute(
        "SELECT COALESCE(MAX(prioridade), 0) FROM servicos WHERE empresa_id=? AND COALESCE(TRIM(UPPER(status)), '')='EM ANDAMENTO'",
        (empresa_id,),
    )
    maior_prioridade = converter_inteiro((c.fetchone() or [0])[0], 0)
    c.execute(
        """
        UPDATE servicos
        SET status='EM ANDAMENTO',
            entrega=NULL,
            prioridade=?,
            etapa_atual='FINALIZACAO',
            etapa_atual_iniciada_em=?,
            finalizacao_iniciada_em=COALESCE(finalizacao_iniciada_em, ?),
            finalizado_por_usuario=NULL,
            finalizado_por_nome=NULL
        WHERE empresa_id=? AND id=?
        """,
        (maior_prioridade + 1, agora_iso(), agora_iso(), empresa_id, id),
    )
    recalcular_resumo_veiculo_por_servicos(c, servico["veiculo_id"])
    conn.commit()
    conn.close()

    registrar_auditoria(
        "reabriu_atendimento",
        "servico",
        entidade_id=id,
        placa=servico.get("placa"),
    )
    definir_feedback_por_destino(
        redirect_to,
        "sucesso",
        f"Atendimento da placa {servico.get('placa') or '-'} voltou para EM ANDAMENTO.",
    )
    return redirect(redirect_to)

@app.route("/historico/servico/<int:id>/excluir", methods=["POST"])
def excluir_atendimento_historico(id):
    if not session.get("usuario"):
        return redirect("/login")

    redirect_to = normalizar_redirect_interno(request.form.get("redirect_to"), "/historico")
    servico = buscar_servico_operacional(id)
    if not servico:
        definir_feedback_por_destino(redirect_to, "erro", "Atendimento nao encontrado.")
        return redirect(redirect_to)

    empresa_id = empresa_atual_id()
    conn = conectar()
    c = conn.cursor()
    c.execute(
        "SELECT caminho FROM fotos WHERE empresa_id=? AND servico_id=?",
        (empresa_id, id),
    )
    caminhos_fotos = [row["caminho"] for row in c.fetchall()]
    excluir_dependencias_historico_servico_domain(c, empresa_id, id)
    c.execute("DELETE FROM servicos WHERE empresa_id=? AND id=?", (empresa_id, id))
    recalcular_resumo_veiculo_por_servicos(c, servico["veiculo_id"])
    conn.commit()
    conn.close()
    limpar_cache_painel()
    for caminho_foto in caminhos_fotos:
        remover_foto_servico_local(caminho_foto)

    registrar_auditoria(
        "excluiu_atendimento",
        "servico",
        entidade_id=id,
        placa=servico.get("placa"),
        detalhes={"status_anterior": servico.get("status")},
    )
    definir_feedback_por_destino(
        redirect_to,
        "sucesso",
        f"Atendimento da placa {servico.get('placa') or '-'} excluido com sucesso.",
    )
    return redirect(redirect_to)


@app.route("/clientes", methods=["GET", "POST"])
def clientes():
    if not session.get("usuario"):
        return redirect("/login")

    preparar_rotinas_interface_logada()
    limpar = bool(request.args.get("limpar"))
    busca = (request.form.get("busca") or request.args.get("busca") or "").strip()

    return renderizar_pagina_clientes(busca=busca, limpar=limpar)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)


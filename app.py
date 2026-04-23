from flask import Flask, render_template, request, redirect, session, jsonify, has_request_context
import csv
import json
import sqlite3
import socket
from zoneinfo import ZoneInfo
import os
import shutil
import time
import hashlib
import re
import secrets
import string
import tempfile
import zipfile
from threading import Thread
from io import BytesIO
from threading import Lock, Thread
import unicodedata
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse
import bcrypt  # 👈 se já adicionou
import pandas as pd
import requests
from werkzeug.utils import secure_filename
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from flask import send_file
from reportlab.lib.units import cm
from reportlab.platypus import Flowable
from reportlab.lib.utils import ImageReader
from xml.sax.saxutils import escape as xml_escape

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
    or ""
).strip().lower()

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
    "fotos",
    "produtos_pneu",
    "checklist_itens",
    "servico_checklist",
    "historico_lavagens_sync",
    "retornos_clientes",
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

        # 🔥 cria caminho circular
        path = c.beginPath()
        path.circle(self.size/2, self.size/2, self.size/2)

        # 🔥 aplica máscara corretamente
        c.clipPath(path, stroke=0, fill=0)

        # 🔥 desenha imagem dentro do círculo
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

        # 🔥 pega a lista principal
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
    
    if isinstance(obj, dt_time):  # 👈 corrigido
        return obj.strftime("%H:%M:%S")
    
    return obj

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def agora():
    return datetime.now(ZoneInfo("America/Sao_Paulo"))

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
    return texto

class CursorCompat:
    def __init__(self, cursor, backend):
        self._cursor = cursor
        self.backend = backend
        self.lastrowid = getattr(cursor, "lastrowid", None)

    def execute(self, sql, params=None):
        sql = str(sql or "")
        if self.backend == "postgres":
            sql = traduzir_sql_para_postgres(sql)
            parametros = tuple(params or ())
            sql_exec = re.sub(r"\?", "%s", sql)
            if sql_exec.lstrip().upper().startswith("INSERT") and "RETURNING" not in sql_exec.upper():
                sql_exec = f"{sql_exec} RETURNING id"
                self._cursor.execute(sql_exec, parametros)
                resultado = self._cursor.fetchone()
                self.lastrowid = resultado[0] if resultado else None
                return self
            self._cursor.execute(sql_exec, parametros)
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
        self._cursor.executemany(sql, seq_of_params)
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
        "frequencia": "diario",
        "tipo_backup": BACKUP_TIPO_PADRAO,
        "retencao_arquivos": BACKUP_RETENCAO_PADRAO,
        "destino_externo_ativo": 0,
        "destino_externo_pasta": "",
        "atualizado_em": "",
    }

def obter_configuracao_backup():
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM configuracao_backup WHERE id=1")
    row = c.fetchone()
    conn.close()

    dados = configuracao_backup_padrao()
    if row:
        dados.update(dict(row))

    frequencia = str(dados.get("frequencia") or "diario").strip().lower()
    dados["frequencia"] = frequencia if frequencia in {"diario", "semanal", "mensal"} else "diario"
    dados["tipo_backup"] = normalizar_tipo_backup(dados.get("tipo_backup"))
    dados["destino_externo_ativo"] = 1 if bool_config_ativo(dados.get("destino_externo_ativo")) else 0
    dados["destino_externo_pasta"] = normalizar_caminho_destino_externo(
        dados.get("destino_externo_pasta"),
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
    destino_externo_pasta = normalizar_caminho_destino_externo(
        form.get("destino_externo_pasta"),
    )

    try:
        retencao = int(form.get("retencao_arquivos") or BACKUP_RETENCAO_PADRAO)
    except Exception:
        retencao = BACKUP_RETENCAO_PADRAO

    retencao = max(1, min(120, retencao))

    conn = conectar()
    c = conn.cursor()
    c.execute("""
        INSERT INTO configuracao_backup (
            id, frequencia, tipo_backup, retencao_arquivos,
            destino_externo_ativo, destino_externo_pasta, atualizado_em
        )
        VALUES (1, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            frequencia=excluded.frequencia,
            tipo_backup=excluded.tipo_backup,
            retencao_arquivos=excluded.retencao_arquivos,
            destino_externo_ativo=excluded.destino_externo_ativo,
            destino_externo_pasta=excluded.destino_externo_pasta,
            atualizado_em=excluded.atualizado_em
    """, (
        frequencia,
        tipo_backup,
        retencao,
        destino_externo_ativo,
        destino_externo_pasta,
        agora_iso(),
    ))
    conn.commit()
    conn.close()
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

def listar_arquivos_backup_banco(tipo_backup=None):
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
    pasta = preparar_destino_externo_backup(configuracao, criar=ativo)

    if not ativo:
        return {
            "ativo": False,
            "sucesso": False,
            "pasta": "",
            "mensagem": "Destino externo desativado.",
        }

    if not pasta:
        return {
            "ativo": True,
            "sucesso": False,
            "pasta": "",
            "mensagem": "Selecione uma pasta sincronizada para enviar o backup.",
        }

    if not pasta_escrevivel(pasta):
        return {
            "ativo": True,
            "sucesso": False,
            "pasta": pasta,
            "arquivos": [],
            "mensagem": "A pasta sincronizada nao esta gravavel neste momento.",
        }

    copiados = []

    try:
        if caminho_backup and os.path.exists(caminho_backup):
            destino_backup = os.path.join(pasta, os.path.basename(caminho_backup))
            copiar_arquivo_para_destino(caminho_backup, destino_backup)
            tipo_backup = identificar_tipo_backup_por_nome(os.path.basename(caminho_backup))
            limpar_backups_antigos_em_pasta(pasta, tipo_backup)
            copiados.append(os.path.basename(destino_backup))

        if incluir_snapshot:
            destino_banco = os.path.join(
                pasta,
                BACKUP_ARQUIVO_POSTGRES_ATUAL_ZIP if banco_online_ativo() else BACKUP_ARQUIVO_BANCO_ATUAL,
            )
            gerar_snapshot_banco_para_arquivo(destino_banco)
            copiados.append(os.path.basename(destino_banco))

        return {
            "ativo": True,
            "sucesso": True,
            "pasta": pasta,
            "arquivos": copiados,
            "mensagem": "Sincronizacao externa atualizada com sucesso.",
        }
    except Exception as e:
        return {
            "ativo": True,
            "sucesso": False,
            "pasta": pasta,
            "arquivos": copiados,
            "mensagem": f"Falha ao copiar para a pasta sincronizada: {e}",
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
                snapshot["tabelas"][tabela] = [dict(row) for row in c.fetchall()]
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
            "manifesto_backup.json",
            json.dumps(
                {
                    "tipo_backup": "banco",
                    "gerado_em": snapshot["gerado_em"],
                    "arquivos": [BACKUP_ARQUIVO_POSTGRES_ATUAL],
                },
                ensure_ascii=False,
                indent=2,
            ),
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
                valores = [registro.get(coluna) for coluna in colunas]
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
    origem_cursor = origem.cursor()

    conn = conectar()
    c = conn.cursor()
    backend = "postgres" if banco_online_ativo() else "sqlite"

    try:
        limpar_tabelas_sistema(c, backend)
        for tabela in TABELAS_SISTEMA_ORDENADAS:
            try:
                origem_cursor.execute(f"SELECT * FROM {tabela} ORDER BY id")
                registros = origem_cursor.fetchall()
            except Exception:
                continue

            for registro in registros:
                registro_dict = dict(registro)
                if not registro_dict:
                    continue

                colunas = list(registro_dict.keys())
                valores = [registro_dict.get(coluna) for coluna in colunas]
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
            origem.close()
        except Exception:
            pass
        try:
            conn.close()
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

    manifesto = {
        "tipo_backup": "completo",
        "gerado_em": agora_iso(),
        "arquivos": [],
    }

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
                        manifesto["arquivos"].append(arcname)
                continue

            arquivo_zip.write(origem_path, destino_rel)
            manifesto["arquivos"].append(destino_rel)

        if banco_online_ativo():
            escrever_snapshot_banco_no_zip(arquivo_zip)
            manifesto["arquivos"].append(BACKUP_ARQUIVO_POSTGRES_ATUAL)

        arquivo_zip.writestr(
            "manifesto_backup.json",
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
        "destino_externo_pasta": destino_externo_pasta,
        "destino_externo_disponivel": bool(destino_externo_pasta and os.path.isdir(destino_externo_pasta)),
        "destino_externo_escrevivel": pasta_escrevivel(destino_externo_pasta),
        "destino_externo_quantidade": len(backups_externos),
        "ultimo_destino_externo_nome": ultimo_externo["nome"] if ultimo_externo else "",
        "ultimo_destino_externo_em_fmt": (
            ultimo_externo["modificado_em_fmt"] if ultimo_externo else "Nenhum envio externo ainda"
        ),
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

    if not sync_lock.acquire(blocking=False):
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

def url_foto_usuario(caminho):
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
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM manutencao_arquivos WHERE id=1")
    row = c.fetchone()
    conn.close()
    return dict(row) if row else {
        "id": 1,
        "ultimo_executado_em": "",
        "ultima_mensagem": "",
        "ultimo_resultado_json": "",
    }

def salvar_status_manutencao_arquivos(resultado, mensagem):
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        INSERT INTO manutencao_arquivos (id, ultimo_executado_em, ultima_mensagem, ultimo_resultado_json)
        VALUES (1, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            ultimo_executado_em=excluded.ultimo_executado_em,
            ultima_mensagem=excluded.ultima_mensagem,
            ultimo_resultado_json=excluded.ultimo_resultado_json
    """, (
        agora_iso(),
        mensagem,
        json.dumps(resultado or {}, ensure_ascii=False, default=sanitizar_para_json),
    ))
    conn.commit()
    conn.close()

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
    while True:
        try:
            sucesso, mensagem, _ = executar_manutencao_arquivos(
                force=False,
                registrar_log=False,
                usuario=usuario_sistema_interno(),
            )
            if sucesso and "concluida" in mensagem.lower():
                print(f"ARQUIVOS: {mensagem}")
        except Exception as e:
            print("ERRO WORKER ARQUIVOS:", e)

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

    conn = conectar()
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
    conn.close()

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

    return {
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

def loop_worker_backup_banco():
    while True:
        try:
            sucesso, mensagem, caminho = criar_backup_banco(force=False)
            if caminho and "criado com sucesso" in mensagem.lower():
                print(f"BACKUP: {mensagem} -> {caminho}")
        except Exception as e:
            print("ERRO WORKER BACKUP:", e)

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

        # ⏱️ tempo de espera
        if tempo_espera > 2:
            prioridade += 3
        elif tempo_espera > 1:
            prioridade += 2
        elif tempo_espera > 0.5:
            prioridade += 1

    except:
        pass

    # 💰 valor do serviço
    try:
        if float(valor) >= 150:
            prioridade += 3
        elif float(valor) >= 80:
            prioridade += 2
        elif float(valor) >= 40:
            prioridade += 1
    except:
        pass

    # 🧽 tipo de serviço
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

# 📁 CONFIG UPLOAD
UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# 🔐 SEGURANÇA UPLOAD
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
    try:
        cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN {definicao_coluna}")
    except Exception:
        pass

app = Flask(__name__)
app.config["SESSION_PERMANENT"] = True
app.config["SESSION_TYPE"] = "filesystem"
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = False
app.secret_key = "wagen_super_segura_123"

APP_VERSION = "Versão: 0.7.5-alpha (Em Desenvolvimento)"
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
    "modelo": ["modelo", "carro", "veiculo", "veículo"],
    "cor": ["cor"],
    "servico": ["servico", "serviço", "servi?o", "servi o", "tipo servico", "tipo", "lavagem"],
    "data": ["data", "data lavagem", "dia", "data servico"],
}


@app.context_processor
def inject_global_template_context():
    return {"app_version": APP_VERSION}


sync_lock = Lock()
sync_worker_iniciado = False
backup_lock = Lock()
backup_worker_iniciado = False
maintenance_lock = Lock()
maintenance_worker_iniciado = False
BANCO_ONLINE_STATUS_CACHE = {
    "testado_em": 0.0,
    "resultado": {},
}
BANCO_ONLINE_STATUS_CACHE_TTL = 30


def atualizar_configuracao_banco_runtime(database_url=None, senha=None, backend=None):
    global DATABASE_URL_RAW, SUPABASE_DB_PASSWORD, DATABASE_BACKEND_RAW

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

    BANCO_ONLINE_STATUS_CACHE["testado_em"] = 0.0
    BANCO_ONLINE_STATUS_CACHE["resultado"] = {}


def modo_banco_preferido():
    modo = str(DATABASE_BACKEND_RAW or "").strip().lower()
    if modo in {"sqlite", "local"}:
        return "sqlite"
    if modo in {"postgres", "supabase", "online"}:
        return "postgres"
    return "postgres" if DATABASE_URL_RAW else "sqlite"


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
        url = f"{url}{separador}connect_timeout=10"

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
    agora_ts = time.time()
    cache = BANCO_ONLINE_STATUS_CACHE.get("resultado") or {}
    testado_em = float(BANCO_ONLINE_STATUS_CACHE.get("testado_em") or 0.0)
    if (
        not force
        and cache
        and agora_ts - testado_em < BANCO_ONLINE_STATUS_CACHE_TTL
    ):
        return dict(cache)

    resultado = {
        "configurado": banco_online_configurado(),
        "ativo": False,
        "conectado": False,
        "backend": "postgres" if banco_online_configurado() else "sqlite",
        "backend_label": "Supabase / PostgreSQL" if banco_online_configurado() else "SQLite local",
        "mensagem": "Modo local selecionado.",
        "url_masked": mascarar_url_postgres(url_postgres_ajustada()) if banco_online_configurado() else "",
        "host": "",
        "porta": "",
        "database": "",
        "usuario": "",
    }

    if not banco_online_configurado():
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

    BANCO_ONLINE_STATUS_CACHE["testado_em"] = agora_ts
    BANCO_ONLINE_STATUS_CACHE["resultado"] = dict(resultado)
    return resultado


def banco_online_ativo():
    return bool(diagnosticar_banco_online().get("conectado"))


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
            return psycopg2.connect(dsn, hostaddr=hostaddr)
        except Exception as erro:
            ultimo_erro = erro

    try:
        return psycopg2.connect(dsn)
    except Exception as erro:
        if ultimo_erro is not None:
            raise ultimo_erro
        raise erro


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


def obter_configuracao_banco_form():
    status = diagnosticar_banco_online()
    origem = url_postgres_ajustada() or DATABASE_URL_RAW
    partes = quebrar_url_postgres(origem)
    partes.update({
        "modo": modo_banco_preferido(),
        "url_masked": status.get("url_masked") or "",
        "senha_preenchida": bool(SUPABASE_DB_PASSWORD),
    })
    return partes


def salvar_configuracao_banco_form(form):
    modo = str(form.get("database_backend") or "sqlite").strip().lower()
    if modo not in {"sqlite", "postgres"}:
        modo = "sqlite"

    configuracao_atual = obter_configuracao_banco_form()
    url_completa = normalizar_texto_campo(form.get("database_url"))
    host = normalizar_texto_campo(form.get("database_host")) or configuracao_atual.get("host") or ""
    porta = normalizar_texto_campo(form.get("database_port")) or configuracao_atual.get("porta") or "5432"
    database = normalizar_texto_campo(form.get("database_name")) or configuracao_atual.get("database") or "postgres"
    usuario = normalizar_texto_campo(form.get("database_user")) or configuracao_atual.get("usuario") or "postgres"
    senha = form.get("database_password") or SUPABASE_DB_PASSWORD

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

    atualizar_configuracao_banco_runtime(url_atual, senha_atual, modo)
    salvar_env_local({
        "DATABASE_BACKEND": modo,
        "DATABASE_URL": url_atual,
        "SUPABASE_DB_PASSWORD": senha_atual,
        "SUPABASE_DATABASE_URL": url_atual,
    })

    diagnosticar_banco_online(force=True)
    return obter_status_banco_online()


def obter_status_banco_online():
    status = diagnosticar_banco_online()
    status["modo"] = modo_banco_preferido()
    status["modo_label"] = "Supabase / PostgreSQL" if status["modo"] == "postgres" else "SQLite local"
    status["dsn_masked"] = status.get("url_masked") or ""
    status["backend"] = "postgres" if status.get("conectado") else "sqlite"
    status["backend_label"] = "Supabase / PostgreSQL" if status.get("conectado") else "SQLite local"
    return status

def conectar():
    if banco_online_ativo():
        dsn = url_postgres_ajustada()
        try:
            conn = conectar_postgres_com_fallback(dsn)
            return ConexaoCompat(conn, "postgres")
        except Exception as e:
            BANCO_ONLINE_STATUS_CACHE["testado_em"] = 0.0
            BANCO_ONLINE_STATUS_CACHE["resultado"] = {
                "configurado": True,
                "ativo": False,
                "conectado": False,
                "backend": "postgres",
                "backend_label": "Supabase / PostgreSQL",
                "mensagem": f"Falha ao abrir conexao online. Usando SQLite local temporariamente: {e}",
                "url_masked": mascarar_url_postgres(dsn),
            }
            print("AVISO:", BANCO_ONLINE_STATUS_CACHE["resultado"]["mensagem"])

    conn = sqlite3.connect(DATABASE_FILE)
    conn.row_factory = sqlite3.Row  # 🔥 ESSENCIAL
    return ConexaoCompat(conn, "sqlite")

def salvar_notificacao(mensagem, tipo="info"):
    try:
        conn = conectar()
        c = conn.cursor()

        c.execute("""
            INSERT INTO notificacoes (mensagem, tipo, criada_em)
            VALUES (?, ?, ?)
        """, (mensagem, tipo, agora_iso()))

        conn.commit()

    except Exception as e:
        print("ERRO AO SALVAR NOTIFICACAO:", e)

    finally:
        try:
            conn.close()
        except:
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

    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "ultimo_hash TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "colunas_ultima_sync TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "campo_servico TEXT")
    adicionar_coluna_se_preciso(c, "sincronizacoes_clientes", "campo_data TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "origem TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "guarita TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "pneu TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "cera TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "hidro_lataria TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "hidro_vidros TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "criado_por_usuario TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "criado_por_nome TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "operacional_por_usuario TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "operacional_por_nome TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "finalizado_por_usuario TEXT")
    adicionar_coluna_se_preciso(c, "servicos", "finalizado_por_nome TEXT")
    adicionar_coluna_se_preciso(c, "fotos", "usuario TEXT")
    adicionar_coluna_se_preciso(c, "fotos", "usuario_nome TEXT")
    adicionar_coluna_se_preciso(c, "fotos", "tamanho_bytes INTEGER")
    adicionar_coluna_se_preciso(c, "fotos", "largura INTEGER")
    adicionar_coluna_se_preciso(c, "fotos", "altura INTEGER")
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
    adicionar_coluna_se_preciso(c, "configuracao_backup", "frequencia TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "tipo_backup TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "retencao_arquivos INTEGER")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "destino_externo_ativo INTEGER")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "destino_externo_pasta TEXT")
    adicionar_coluna_se_preciso(c, "configuracao_backup", "atualizado_em TEXT")
    adicionar_coluna_se_preciso(c, "manutencao_arquivos", "ultimo_executado_em TEXT")
    adicionar_coluna_se_preciso(c, "manutencao_arquivos", "ultima_mensagem TEXT")
    adicionar_coluna_se_preciso(c, "manutencao_arquivos", "ultimo_resultado_json TEXT")
    adicionar_coluna_se_preciso(c, "integracao_fiscal", "token_api TEXT")

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
        placa TEXT NOT NULL UNIQUE,
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
        atualizado_em TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS orcamentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    conn.close()

def init_db():
    criar_todas_tabelas()
    atualizar_banco()

def criar_todas_tabelas():
    conn = conectar()
    c = conn.cursor()

    # 🔔 NOTIFICAÇÕES
    c.execute("""
    CREATE TABLE IF NOT EXISTS notificacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mensagem TEXT,
        tipo TEXT,
        lida INTEGER DEFAULT 0,
        criada_em TEXT
    )
    """)

    # 🔄 SINCRONIZAÇÕES
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
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 🔐 USUÁRIOS
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
        foto_perfil TEXT
    )
    """)

    # 👤 CLIENTES
    c.execute("""
    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        telefone TEXT
    )
    """)

    # 🚗 VEÍCULOS
    c.execute("""
    CREATE TABLE IF NOT EXISTS veiculos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        placa TEXT UNIQUE NOT NULL,
        modelo TEXT,
        cor TEXT,
        cliente_id INTEGER,
        FOREIGN KEY(cliente_id) REFERENCES clientes(id)
    )
    """)

    # 🔔 NOTIFICAÇÕES
    c.execute("""
    CREATE TABLE IF NOT EXISTS notificacoes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        mensagem TEXT,
        tipo TEXT,
        lida INTEGER DEFAULT 0,
        criada_em TEXT
    )
    """)

    # 🔄 SINCRONIZAÇÕES
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
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    # 🔥 TIPOS DE SERVIÇO
    c.execute("""
    CREATE TABLE IF NOT EXISTS tipos_servico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT,
        valor REAL
    )
    """)

    # 🔥 SERVIÇOS
    c.execute("""
    CREATE TABLE IF NOT EXISTS servicos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        veiculo_id INTEGER,
        tipo_id INTEGER,
        valor REAL,
        entrada TEXT,
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
        criado_por_usuario TEXT,
        criado_por_nome TEXT,
        operacional_por_usuario TEXT,
        operacional_por_nome TEXT,
        finalizado_por_usuario TEXT,
        finalizado_por_nome TEXT,
        FOREIGN KEY(veiculo_id) REFERENCES veiculos(id),
        FOREIGN KEY(tipo_id) REFERENCES tipos_servico(id)
    )
    """)

    # 🔥 ADICIONAIS
    c.execute("""
    CREATE TABLE IF NOT EXISTS adicionais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT
    )
    """)

    # 🔥 RELAÇÃO SERVIÇO ↔ ADICIONAIS
    c.execute("""
    CREATE TABLE IF NOT EXISTS servico_adicionais (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        servico_id INTEGER,
        adicional_id INTEGER,
        FOREIGN KEY(servico_id) REFERENCES servicos(id),
        FOREIGN KEY(adicional_id) REFERENCES adicionais(id)
    )
    """)

    # 🔥 FOTOS (melhorado)
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
        placa TEXT NOT NULL UNIQUE,
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

    # ⚡ ÍNDICES (performance)
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
        atualizado_em TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS orcamentos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        id INTEGER PRIMARY KEY CHECK (id = 1),
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
        id INTEGER PRIMARY KEY CHECK (id = 1),
        frequencia TEXT DEFAULT 'diario',
        tipo_backup TEXT DEFAULT 'completo',
        retencao_arquivos INTEGER DEFAULT 15,
        destino_externo_ativo INTEGER DEFAULT 0,
        destino_externo_pasta TEXT,
        atualizado_em TEXT
    )
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS manutencao_arquivos (
        id INTEGER PRIMARY KEY CHECK (id = 1),
        ultimo_executado_em TEXT,
        ultima_mensagem TEXT,
        ultimo_resultado_json TEXT
    )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_servico_status ON servicos(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_servico_entrada ON servicos(entrada)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_veiculo_placa ON veiculos(placa)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_servico_checklist_servico ON servico_checklist(servico_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_fotos_servico_tipo ON fotos(servico_id, tipo)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_auditoria_entidade ON auditoria(entidade, entidade_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_auditoria_criado_em ON auditoria(criado_em)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_historico_sync_placa ON historico_lavagens_sync(placa)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_historico_sync_data ON historico_lavagens_sync(data_lavagem)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_retornos_clientes_placa ON retornos_clientes(placa)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_retornos_clientes_status ON retornos_clientes(status, proximo_contato_em)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orcamentos_numero ON orcamentos(numero)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orcamentos_criado_em ON orcamentos(criado_em)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_orcamento_itens_orcamento ON orcamento_itens(orcamento_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_notas_fiscais_rps ON notas_fiscais(rps_numero)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_notas_fiscais_criado_em ON notas_fiscais(criado_em)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_nota_fiscal_itens_nota ON nota_fiscal_itens(nota_fiscal_id)")
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_sync_clientes_proximo
        ON sincronizacoes_clientes(ativo, proximo_sync_em)
    """)

    conn.commit()
    conn.close()


init_db()

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

def definir_feedback_itens(tipo, mensagem):
    session["itens_feedback"] = {"tipo": tipo, "mensagem": mensagem}

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
    session["usuario"] = usuario_row["usuario"]
    session["usuario_id"] = usuario_row["id"]
    session["usuario_nome"] = (usuario_row["nome"] or usuario_row["usuario"])
    session["usuario_iniciais"] = obter_iniciais_usuario(
        usuario_row["nome"],
        usuario_row["usuario"],
    )
    session["usuario_foto"] = str(usuario_row["foto_perfil"] or "").strip()
    session["usuario_foto_url"] = url_foto_usuario(usuario_row["foto_perfil"])
    session["usuario_perfil"] = (
        usuario_row["perfil"] or
        ("admin" if usuario_row["usuario"] == "admin" else "funcionario")
    )
    session["senha_alteracao_obrigatoria"] = usuario_precisa_trocar_senha(usuario_row)
    session.permanent = True

def sincronizar_sessao_usuario():
    if not session.get("usuario"):
        return

    if (
        session.get("usuario_id") and
        session.get("usuario_nome") and
        session.get("usuario_perfil") and
        "usuario_iniciais" in session and
        "usuario_foto_url" in session and
        "senha_alteracao_obrigatoria" in session
    ):
        return

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM usuarios WHERE usuario=?", (session.get("usuario"),))
    usuario = c.fetchone()
    conn.close()

    if usuario:
        preencher_sessao_usuario(usuario, limpar=False)

def usuario_admin():
    return (
        session.get("usuario_perfil") == "admin" or
        session.get("usuario") == "admin"
    )

def normalizar_perfil_usuario(valor):
    return "admin" if str(valor or "").strip().lower() == "admin" else "funcionario"

def normalizar_periodo_financeiro(valor):
    periodo = str(valor or "mes").strip().lower()
    return periodo if periodo in MAPA_PERIODOS_FINANCEIRO else "mes"

def filtrar_servicos_por_periodo(servicos, periodo, data_referencia):
    if periodo == "hoje":
        return [
            item for item in servicos
            if item["entrega_dt"].date() == data_referencia
        ]

    if periodo == "7dias":
        inicio = data_referencia - timedelta(days=6)
        return [
            item for item in servicos
            if inicio <= item["entrega_dt"].date() <= data_referencia
        ]

    if periodo == "30dias":
        inicio = data_referencia - timedelta(days=29)
        return [
            item for item in servicos
            if inicio <= item["entrega_dt"].date() <= data_referencia
        ]

    return [
        item for item in servicos
        if (
            item["entrega_dt"].year == data_referencia.year and
            item["entrega_dt"].month == data_referencia.month
        )
    ]

def dias_do_periodo_financeiro(periodo, data_referencia):
    if periodo == "hoje":
        return 1
    if periodo == "7dias":
        return 7
    if periodo == "30dias":
        return 30
    return max(1, data_referencia.day)

def obter_label_intervalo_sincronizacao(minutos):
    return MAPA_INTERVALOS_SINCRONIZACAO.get(minutos, f"{minutos} min")

def formatar_tempo_restante(valor_iso):
    if not valor_iso:
        return "Sem agendamento"

    try:
        diferenca = int((datetime.fromisoformat(valor_iso) - agora()).total_seconds())
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

def interpretar_datahora_sistema(valor):
    if not valor:
        return None

    if isinstance(valor, datetime):
        return valor

    texto = str(valor).strip()

    for parser in (
        lambda item: datetime.fromisoformat(item),
        lambda item: datetime.strptime(item, "%d/%m/%Y %H:%M"),
    ):
        try:
            return parser(texto)
        except Exception:
            continue

    return None

def formatar_valor_monetario(valor):
    try:
        numero = float(str(valor or 0).replace(",", "."))
    except Exception:
        numero = 0.0

    return f"{numero:.2f}"

def converter_valor_numerico(valor):
    try:
        return float(str(valor or 0).replace(",", "."))
    except Exception:
        return 0.0

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
    }

def obter_configuracao_empresa():
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM configuracao_empresa WHERE id=1")
    item = c.fetchone()
    conn.close()

    dados = empresa_snapshot_padrao()

    if item:
        dados.update(dict(item))

    dados["cnpj_formatado"] = formatar_documento_fiscal(dados.get("cnpj"))
    dados["cep_formatado"] = formatar_cep(dados.get("cep"))
    dados["aliquota_padrao"] = converter_valor_numerico(dados.get("aliquota_padrao"))
    return dados

def salvar_configuracao_empresa_form(form):
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        INSERT INTO configuracao_empresa (
            id, razao_social, nome_fantasia, cnpj, inscricao_municipal, inscricao_estadual,
            regime_tributario, email, telefone, endereco, numero, complemento, bairro,
            cidade, uf, cep, codigo_servico_padrao, aliquota_padrao, atualizado_em
        )
        VALUES (
            1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(id) DO UPDATE SET
            razao_social=excluded.razao_social,
            nome_fantasia=excluded.nome_fantasia,
            cnpj=excluded.cnpj,
            inscricao_municipal=excluded.inscricao_municipal,
            inscricao_estadual=excluded.inscricao_estadual,
            regime_tributario=excluded.regime_tributario,
            email=excluded.email,
            telefone=excluded.telefone,
            endereco=excluded.endereco,
            numero=excluded.numero,
            complemento=excluded.complemento,
            bairro=excluded.bairro,
            cidade=excluded.cidade,
            uf=excluded.uf,
            cep=excluded.cep,
            codigo_servico_padrao=excluded.codigo_servico_padrao,
            aliquota_padrao=excluded.aliquota_padrao,
            atualizado_em=excluded.atualizado_em
    """, (
        normalizar_texto_campo(form.get("razao_social")),
        normalizar_texto_campo(form.get("nome_fantasia")),
        normalizar_documento_fiscal(form.get("cnpj")),
        normalizar_texto_campo(form.get("inscricao_municipal")),
        normalizar_texto_campo(form.get("inscricao_estadual")),
        normalizar_texto_campo(form.get("regime_tributario")),
        normalizar_texto_campo(form.get("email")),
        normalizar_texto_campo(form.get("telefone")),
        normalizar_texto_campo(form.get("endereco")),
        normalizar_texto_campo(form.get("numero")),
        normalizar_texto_campo(form.get("complemento")),
        normalizar_texto_campo(form.get("bairro")),
        normalizar_texto_campo(form.get("cidade")),
        normalizar_texto_campo(form.get("uf")).upper()[:2],
        re.sub(r"\D", "", str(form.get("cep") or "")),
        normalizar_texto_campo(form.get("codigo_servico_padrao")),
        converter_valor_numerico(form.get("aliquota_padrao")),
        agora_iso(),
    ))
    conn.commit()
    conn.close()

def integracao_fiscal_padrao():
    return {
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
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM integracao_fiscal WHERE id=1")
    item = c.fetchone()
    conn.close()

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
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        INSERT INTO integracao_fiscal (
            id, tipo_integracao, provedor_nome, ambiente, municipio_codigo_ibge, municipio_nome, uf,
            endpoint_emissao, endpoint_consulta, endpoint_cancelamento, autenticacao_tipo, usuario_api,
            senha_api, client_id, client_secret, token_api, token_url, certificado_tipo,
            certificado_arquivo, certificado_senha, serie_rps, serie_nfe, ativo, ultimo_status,
            ultima_mensagem, atualizado_em
        )
        VALUES (
            1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        ON CONFLICT(id) DO UPDATE SET
            tipo_integracao=excluded.tipo_integracao,
            provedor_nome=excluded.provedor_nome,
            ambiente=excluded.ambiente,
            municipio_codigo_ibge=excluded.municipio_codigo_ibge,
            municipio_nome=excluded.municipio_nome,
            uf=excluded.uf,
            endpoint_emissao=excluded.endpoint_emissao,
            endpoint_consulta=excluded.endpoint_consulta,
            endpoint_cancelamento=excluded.endpoint_cancelamento,
            autenticacao_tipo=excluded.autenticacao_tipo,
            usuario_api=excluded.usuario_api,
            senha_api=excluded.senha_api,
            client_id=excluded.client_id,
            client_secret=excluded.client_secret,
            token_api=excluded.token_api,
            token_url=excluded.token_url,
            certificado_tipo=excluded.certificado_tipo,
            certificado_arquivo=excluded.certificado_arquivo,
            certificado_senha=excluded.certificado_senha,
            serie_rps=excluded.serie_rps,
            serie_nfe=excluded.serie_nfe,
            ativo=excluded.ativo,
            ultimo_status=excluded.ultimo_status,
            ultima_mensagem=excluded.ultima_mensagem,
            atualizado_em=excluded.atualizado_em
    """, (
        normalizar_texto_campo(form.get("tipo_integracao")) or "manual",
        normalizar_texto_campo(form.get("provedor_nome")),
        normalizar_texto_campo(form.get("ambiente")) or "homologacao",
        re.sub(r"\D", "", str(form.get("municipio_codigo_ibge") or "")),
        normalizar_texto_campo(form.get("municipio_nome")),
        normalizar_texto_campo(form.get("uf")).upper()[:2],
        normalizar_texto_campo(form.get("endpoint_emissao")),
        normalizar_texto_campo(form.get("endpoint_consulta")),
        normalizar_texto_campo(form.get("endpoint_cancelamento")),
        normalizar_texto_campo(form.get("autenticacao_tipo")) or "nenhuma",
        normalizar_texto_campo(form.get("usuario_api")),
        normalizar_texto_campo(form.get("senha_api")),
        normalizar_texto_campo(form.get("client_id")),
        normalizar_texto_campo(form.get("client_secret")),
        normalizar_texto_campo(form.get("token_api")),
        normalizar_texto_campo(form.get("token_url")),
        normalizar_texto_campo(form.get("certificado_tipo")) or "nenhum",
        normalizar_texto_campo(form.get("certificado_arquivo")),
        normalizar_texto_campo(form.get("certificado_senha")),
        normalizar_texto_campo(form.get("serie_rps")),
        normalizar_texto_campo(form.get("serie_nfe")),
        1 if form.get("ativo") else 0,
        normalizar_texto_campo(form.get("ultimo_status")),
        normalizar_texto_campo(form.get("ultima_mensagem")),
        agora_iso(),
    ))
    conn.commit()
    conn.close()

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
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT id, numero, cliente_nome, placa, modelo, total, status, criado_em
        FROM orcamentos
        ORDER BY numero DESC
        LIMIT ?
    """, (limit,))
    registros = [dict(item) for item in c.fetchall()]
    conn.close()

    for item in registros:
        item["numero_formatado"] = formatar_numero_documento(item.get("numero"))
        item["total_exibicao"] = formatar_valor_brl(item.get("total"))
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))

    return registros

def buscar_orcamento_completo(orcamento_id):
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM orcamentos WHERE id=?", (orcamento_id,))
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
    conn = conectar()
    c = conn.cursor()
    numero = proximo_numero_documento_sql(c, "orcamentos", "numero")
    criado_em = agora_iso()
    subtotal = round(sum(item["valor_total"] for item in itens), 2)
    desconto = max(0.0, converter_valor_numerico(dados.get("desconto")))
    total = max(subtotal - desconto, 0.0)
    empresa_snapshot = serializar_empresa_snapshot(montar_empresa_snapshot(empresa))

    c.execute("""
        INSERT INTO orcamentos (
            numero, cliente_nome, cliente_documento, email, telefone, placa, modelo,
            validade_dias, forma_pagamento, observacoes, subtotal, desconto, total,
            status, empresa_snapshot, criado_em, atualizado_em, usuario
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'GERADO', ?, ?, ?, ?)
    """, (
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
    conn = conectar()
    c = conn.cursor()
    query = """
        SELECT id, rps_numero, numero_nota, serie, ambiente, status, cliente_nome, valor_total, criado_em
        FROM notas_fiscais
    """
    params = []

    if somente_emitidas:
        query += " WHERE COALESCE(NULLIF(numero_nota, ''), '') <> ''"
    elif somente_pendentes:
        query += " WHERE COALESCE(NULLIF(numero_nota, ''), '') = ''"

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
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM notas_fiscais WHERE id=?", (nota_id,))
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
    conn = conectar()
    c = conn.cursor()
    rps_numero = proximo_numero_documento_sql(c, "notas_fiscais", "rps_numero")
    criado_em = agora_iso()
    valor_servicos = round(sum(item["valor_total"] for item in itens), 2)
    desconto = max(0.0, converter_valor_numerico(dados.get("desconto")))
    base_calculo = max(valor_servicos - desconto, 0.0)
    aliquota_iss = max(0.0, converter_valor_numerico(dados.get("aliquota_iss")))
    valor_iss = round((base_calculo * aliquota_iss) / 100, 2)
    valor_total = base_calculo
    numero_nota = normalizar_texto_campo(dados.get("numero_nota"))
    empresa_snapshot = serializar_empresa_snapshot(montar_empresa_snapshot(empresa))

    c.execute("""
        INSERT INTO notas_fiscais (
            rps_numero, numero_nota, serie, ambiente, tipo_documento, status,
            cliente_nome, cliente_documento, email, telefone, placa, modelo,
            endereco, numero_endereco, complemento, bairro, cidade, uf, cep,
            codigo_servico, discriminacao, observacoes, aliquota_iss, valor_servicos,
            desconto, valor_iss, valor_total, empresa_snapshot, criado_em, atualizado_em,
            usuario, origem_orcamento_id
        )
        VALUES (?, ?, ?, ?, 'NFS-e', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
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
    if not orcamento:
        return None

    empresa = obter_configuracao_empresa()
    discriminacao = "; ".join(item["descricao"] for item in orcamento.get("itens", []))
    return {
        "origem_orcamento_id": orcamento["id"],
        "cliente_nome": orcamento.get("cliente_nome", ""),
        "cliente_documento": orcamento.get("cliente_documento", ""),
        "email": orcamento.get("email", ""),
        "telefone": orcamento.get("telefone", ""),
        "placa": orcamento.get("placa", ""),
        "modelo": orcamento.get("modelo", ""),
        "codigo_servico": empresa.get("codigo_servico_padrao", ""),
        "aliquota_iss": empresa.get("aliquota_padrao", 0),
        "discriminacao": discriminacao,
        "desconto": orcamento.get("desconto", 0),
        "itens": orcamento.get("itens", []),
    }

def montar_tabela_itens_pdf(itens):
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
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
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
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=28, bottomMargin=28)
    styles = getSampleStyleSheet()
    titulo_style = ParagraphStyle(
        "DocTitulo",
        parent=styles["Heading1"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=18,
        textColor=colors.HexColor("#111827"),
        spaceAfter=6,
    )
    subtitulo_style = ParagraphStyle(
        "DocSubtitulo",
        parent=styles["BodyText"],
        alignment=TA_CENTER,
        textColor=colors.HexColor("#4b5563"),
        spaceAfter=12,
    )
    normal = styles["BodyText"]
    elementos = []

    try:
        logo = ImagemRedonda("static/logo.jpg", size=72)
        tabela_logo = Table([[logo]], colWidths=[523])
        tabela_logo.setStyle([("ALIGN", (0, 0), (-1, -1), "CENTER")])
        elementos.append(tabela_logo)
        elementos.append(Spacer(1, 10))
    except Exception:
        pass

    nome_empresa = empresa.get("nome_fantasia") or empresa.get("razao_social") or "Wagen Estetica Automotiva"
    elementos.append(Paragraph(xml_escape(nome_empresa), titulo_style))

    linhas_empresa = []
    if empresa.get("razao_social"):
        linhas_empresa.append(f"Razao social: {empresa['razao_social']}")
    if empresa.get("cnpj"):
        linhas_empresa.append(f"CNPJ: {formatar_documento_fiscal(empresa['cnpj'])}")
    linhas_empresa.extend(montar_endereco_empresa(empresa))
    if empresa.get("telefone") or empresa.get("email"):
        linhas_empresa.append(
            " | ".join(
                parte for parte in [
                    normalizar_texto_campo(empresa.get("telefone")),
                    normalizar_texto_campo(empresa.get("email")),
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
    elementos.append(montar_tabela_itens_pdf(orcamento.get("itens", [])))
    elementos.append(Spacer(1, 14))

    totais = Table([
        ["Subtotal", f"R$ {orcamento['subtotal_exibicao']}"],
        ["Desconto", f"R$ {orcamento['desconto_exibicao']}"],
        ["Total", f"R$ {orcamento['total_exibicao']}"],
    ], colWidths=[120, 130], hAlign="RIGHT")
    totais.setStyle(TableStyle([
        ("BACKGROUND", (0, 2), (-1, 2), colors.HexColor("#facc15")),
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
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4, leftMargin=36, rightMargin=36, topMargin=28, bottomMargin=28)
    styles = getSampleStyleSheet()
    titulo_style = ParagraphStyle(
        "FiscalTitulo",
        parent=styles["Heading1"],
        alignment=TA_CENTER,
        fontName="Helvetica-Bold",
        fontSize=18,
        textColor=colors.HexColor("#111827"),
        spaceAfter=6,
    )
    subtitulo_style = ParagraphStyle(
        "FiscalSubtitulo",
        parent=styles["BodyText"],
        alignment=TA_CENTER,
        textColor=colors.HexColor("#4b5563"),
        spaceAfter=10,
    )
    normal = styles["BodyText"]
    elementos = []

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
        ["Emitente", empresa.get("razao_social") or empresa.get("nome_fantasia") or "-"],
        ["CNPJ", formatar_documento_fiscal(empresa.get("cnpj")) or "-"],
        ["IM / IE", " / ".join(
            parte for parte in [
                normalizar_texto_campo(empresa.get("inscricao_municipal")),
                normalizar_texto_campo(empresa.get("inscricao_estadual")),
            ] if parte
        ) or "-"],
        ["Endereco", " | ".join(montar_endereco_empresa(empresa)) or "-"],
        ["Contato", " | ".join(
            parte for parte in [
                normalizar_texto_campo(empresa.get("telefone")),
                normalizar_texto_campo(empresa.get("email")),
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
    elementos.append(montar_tabela_itens_pdf(nota.get("itens", [])))
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
        ("BACKGROUND", (0, 5), (-1, 5), colors.HexColor("#facc15")),
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

criar_itens_checklist_padrao()

def normalizar_texto_campo(valor):
    return str(valor or "").strip()

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
            return caminho_relativo_usuario_foto(destino)
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
    return caminho_relativo_usuario_foto(destino)

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

def salvar_fotos_servico(cursor, servico_id, fotos, tipo):
    total_salvas = 0
    usuario_info = resumo_usuario_logado()

    for foto in fotos or []:
        if not foto or not foto.filename or not arquivo_permitido(foto.filename):
            continue

        arquivo_salvo = salvar_arquivo_imagem_otimizado(foto)
        caminho = arquivo_salvo["caminho"]

        cursor.execute(
            """
            INSERT INTO fotos (
                servico_id, tipo, caminho, usuario, usuario_nome, tamanho_bytes, largura, altura
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                servico_id,
                tipo,
                caminho,
                normalizar_texto_campo(usuario_info.get("usuario")),
                normalizar_texto_campo(usuario_info.get("nome")),
                arquivo_salvo.get("tamanho_bytes"),
                arquivo_salvo.get("largura"),
                arquivo_salvo.get("altura"),
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

def listar_fotos_servicos(ids_servicos):
    ids = [int(item) for item in (ids_servicos or []) if item]

    if not ids:
        return {}

    conn = conectar()
    c = conn.cursor()
    placeholders = ",".join(["?"] * len(ids))
    c.execute(f"""
        SELECT id, servico_id, tipo, caminho, criado_em, usuario, usuario_nome, tamanho_bytes, largura, altura
        FROM fotos
        WHERE servico_id IN ({placeholders})
        ORDER BY
            CASE tipo
                WHEN 'entrada' THEN 0
                WHEN 'detalhe' THEN 1
                WHEN 'saida' THEN 2
                ELSE 3
            END,
            id DESC
    """, ids)

    fotos_por_servico = {}
    labels = {
        "entrada": "Entrada",
        "detalhe": "Detalhe",
        "saida": "Finalizacao",
    }

    for row in c.fetchall():
        foto = dict(row)
        foto["url"] = caminho_foto_para_url(foto.get("caminho"))
        foto["arquivo_nome"] = os.path.basename(str(foto.get("caminho") or "").replace("\\", "/"))
        foto["tipo_label"] = labels.get(foto.get("tipo"), "Foto")
        foto["criado_em_fmt"] = formatar_datahora(foto.get("criado_em"))
        foto["usuario_nome_exibicao"] = formatar_usuario_exibicao(
            foto.get("usuario_nome"),
            foto.get("usuario"),
            fallback="Nao identificado",
        )
        foto["tamanho_fmt"] = formatar_tamanho_arquivo(foto.get("tamanho_bytes"))

        if not foto["url"]:
            continue

        grupos = fotos_por_servico.setdefault(foto["servico_id"], {})
        grupos.setdefault(foto["tipo"], []).append(foto)

    conn.close()
    return fotos_por_servico

def contar_fotos_validas(fotos):
    return sum(
        1
        for foto in (fotos or [])
        if foto and foto.filename and arquivo_permitido(foto.filename)
    )

def atualizar_campos_operacionais_servico(cursor, servico_id, form, usuario_info=None):
    usuario_info = usuario_info or resumo_usuario_logado()
    cursor.execute("""
        UPDATE servicos
        SET origem=?, guarita=?, observacoes=?, pneu=?, cera=?, hidro_lataria=?, hidro_vidros=?,
            operacional_por_usuario=?, operacional_por_nome=?
        WHERE id=?
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
    c.execute("""
        SELECT
            servicos.*,
            tipos_servico.nome AS tipo_nome,
            veiculos.placa,
            veiculos.modelo,
            veiculos.cor,
            clientes.nome AS cliente_nome,
            clientes.telefone AS cliente_telefone
        FROM servicos
        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
        WHERE servicos.id=?
    """, (servico_id,))
    servico = c.fetchone()
    conn.close()
    return dict(servico) if servico else None

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
                raise Exception("Não foi possível identificar o ID da planilha.")

            return montar_url_google_sheets_csv(sheet_id, extrair_gid_url(url))

        raise Exception("Link não suportado. Use uma planilha do Google Sheets.")

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
            "O link retornou uma página HTML em vez da planilha. "
            "Verifique se a planilha do Google está acessível pelo link."
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

    raise ultimo_erro or ValueError("Não foi possível interpretar o CSV.")

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

    # 🔥 DETECTAR SHEETY
    if "sheety.co" in url:
        df = ler_planilha_sheety(url, intervalo_minutos=intervalo_minutos)
        return df, url
    if not url:

        raise ValueError("Informe um link de planilha válido.")

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
                    "O link abriu uma página de visualização em vez do arquivo da planilha. "
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
        "Não consegui acessar um arquivo de planilha válido por esse link."
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

def salvar_historico_lavagens_sync(sync_id, registros):
    conn = conectar()
    c = conn.cursor()
    agora_atual = agora_iso()

    c.execute("DELETE FROM historico_lavagens_sync WHERE sync_id=?", (sync_id,))

    for item in registros:
        c.execute("""
            INSERT INTO historico_lavagens_sync (
                sync_id, placa, cliente, carro, cor, servico,
                data_lavagem, data_original, criado_em, atualizado_em
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sync_id,
            item.get("placa"),
            item.get("cliente"),
            item.get("carro"),
            item.get("cor"),
            item.get("servico"),
            item.get("data_lavagem"),
            item.get("data_original"),
            agora_atual,
            agora_atual,
        ))

    conn.commit()
    conn.close()

def buscar_ultima_lavagem_sync_placa(placa):
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT *
        FROM historico_lavagens_sync
        WHERE placa=?
        ORDER BY
            CASE WHEN data_lavagem IS NULL OR data_lavagem='' THEN 1 ELSE 0 END,
            data_lavagem DESC,
            id DESC
        LIMIT 1
    """, (placa.upper(),))
    row = c.fetchone()
    conn.close()
    return dict(row) if row else None

def buscar_ultima_lavagem_local_placa(placa):
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT
            servicos.entrega,
            tipos_servico.nome AS servico
        FROM servicos
        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
        WHERE veiculos.placa=?
          AND servicos.status='FINALIZADO'
          AND servicos.entrega IS NOT NULL
        ORDER BY servicos.entrega DESC, servicos.id DESC
        LIMIT 1
    """, (placa.upper(),))
    row = c.fetchone()
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
    c.execute("""
        SELECT
            placa,
            cliente,
            carro,
            cor,
            servico,
            data_lavagem,
            data_original,
            id
        FROM historico_lavagens_sync
        WHERE placa IS NOT NULL
          AND TRIM(placa) <> ''
        ORDER BY
            UPPER(placa) ASC,
            CASE
                WHEN data_lavagem IS NULL OR data_lavagem = '' THEN 1
                ELSE 0
            END,
            data_lavagem DESC,
            id DESC
    """)
    rows = c.fetchall()
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
    c.execute("""
        SELECT
            veiculos.placa,
            clientes.nome AS cliente,
            veiculos.modelo AS carro,
            veiculos.cor AS cor,
            tipos_servico.nome AS servico,
            servicos.entrega,
            servicos.id
        FROM servicos
        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
        WHERE servicos.status = 'FINALIZADO'
          AND servicos.entrega IS NOT NULL
          AND veiculos.placa IS NOT NULL
          AND TRIM(veiculos.placa) <> ''
        ORDER BY
            UPPER(veiculos.placa) ASC,
            servicos.entrega DESC,
            servicos.id DESC
    """)
    rows = c.fetchall()
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
    placeholders = ",".join(["?"] * len(placas))
    c.execute(f"""
        SELECT
            UPPER(veiculos.placa) AS placa,
            clientes.nome AS cliente_nome,
            clientes.telefone,
            veiculos.modelo AS carro,
            veiculos.cor AS cor
        FROM veiculos
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
        WHERE UPPER(veiculos.placa) IN ({placeholders})
    """, tuple(placas))
    rows = [dict(item) for item in c.fetchall()]
    conn.close()

    return {
        item["placa"]: item
        for item in rows
        if item.get("placa")
    }

def carregar_estados_retornos(placas=None):
    conn = conectar()
    c = conn.cursor()

    if placas:
        placas = [normalizar_texto_campo(item).upper() for item in placas if normalizar_texto_campo(item)]
        if not placas:
            conn.close()
            return {}

        placeholders = ",".join(["?"] * len(placas))
        c.execute(
            f"SELECT * FROM retornos_clientes WHERE placa IN ({placeholders})",
            tuple(placas),
        )
    else:
        c.execute("SELECT * FROM retornos_clientes")

    rows = [dict(item) for item in c.fetchall()]
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
    c = conn.cursor()
    c.execute("""
        INSERT INTO retornos_clientes (
            placa, status, observacao, proximo_contato_em, ultimo_contato_em,
            ultima_acao, reagendado_dias, usuario, usuario_nome, criado_em, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(placa) DO UPDATE SET
            status=excluded.status,
            observacao=excluded.observacao,
            proximo_contato_em=excluded.proximo_contato_em,
            ultimo_contato_em=excluded.ultimo_contato_em,
            ultima_acao=excluded.ultima_acao,
            reagendado_dias=excluded.reagendado_dias,
            usuario=excluded.usuario,
            usuario_nome=excluded.usuario_nome,
            atualizado_em=excluded.atualizado_em
    """, (
        normalizar_texto_campo(placa).upper(),
        normalizar_status_retorno(status),
        normalizar_texto_campo(observacao),
        normalizar_texto_campo(proximo_contato_em),
        normalizar_texto_campo(ultimo_contato_em),
        normalizar_texto_campo(ultima_acao),
        int(reagendado_dias or 0),
        normalizar_texto_campo(usuario_info.get("usuario")),
        normalizar_texto_campo(usuario_info.get("nome")),
        agora_iso(),
        agora_iso(),
    ))
    conn.commit()
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

def importar_clientes_dataframe(df, mapeamento):
    conn = conectar()
    c = conn.cursor()

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

        c.execute("""
            SELECT id, placa, modelo, cor, cliente_id
            FROM veiculos
            WHERE placa=?
        """, (placa,))
        veiculo_existente = c.fetchone()

        cliente_id = veiculo_existente["cliente_id"] if veiculo_existente and veiculo_existente["cliente_id"] else None
        cliente_existente = None

        if telefone:
            c.execute("SELECT id, nome, telefone FROM clientes WHERE telefone=?", (telefone,))
            cliente_existente = c.fetchone()

            if cliente_existente:
                cliente_id = cliente_existente["id"]
        elif cliente_id:
            c.execute("SELECT id, nome, telefone FROM clientes WHERE id=?", (cliente_id,))
            cliente_existente = c.fetchone()

        if cliente_existente:
            novo_nome = nome or cliente_existente["nome"] or ""
            novo_telefone = telefone or cliente_existente["telefone"] or ""

            if (
                novo_nome != (cliente_existente["nome"] or "") or
                novo_telefone != (cliente_existente["telefone"] or "")
            ):
                c.execute("""
                    UPDATE clientes
                    SET nome=?, telefone=?
                    WHERE id=?
                """, (novo_nome, novo_telefone, cliente_existente["id"]))
                estatisticas["clientes_atualizados"] += 1
        elif nome or telefone:
            c.execute("""
                INSERT INTO clientes (nome, telefone)
                VALUES (?, ?)
            """, (nome or "Sem nome", telefone))
            cliente_id = c.lastrowid
            estatisticas["clientes_novos"] += 1

        if veiculo_existente:
            novo_modelo = modelo or veiculo_existente["modelo"] or ""
            nova_cor = cor or veiculo_existente["cor"] or ""
            novo_cliente_id = cliente_id if cliente_id is not None else veiculo_existente["cliente_id"]

            if (
                novo_modelo != (veiculo_existente["modelo"] or "") or
                nova_cor != (veiculo_existente["cor"] or "") or
                novo_cliente_id != veiculo_existente["cliente_id"]
            ):
                c.execute("""
                    UPDATE veiculos
                    SET modelo=?, cor=?, cliente_id=?
                    WHERE placa=?
                """, (novo_modelo, nova_cor, novo_cliente_id, placa))
                estatisticas["veiculos_atualizados"] += 1
        else:
            c.execute("""
                INSERT INTO veiculos (placa, modelo, cor, cliente_id)
                VALUES (?, ?, ?, ?)
            """, (placa, modelo, cor, cliente_id))
            estatisticas["veiculos_novos"] += 1

        estatisticas["linhas_processadas"] += 1

    conn.commit()
    conn.close()

    return estatisticas

def resumir_importacao_clientes(estatisticas):
    resumo = (
        f"{estatisticas['linhas_processadas']} linha(s) processada(s), "
        f"{estatisticas['veiculos_novos']} veículo(s) novo(s), "
        f"{estatisticas['veiculos_atualizados']} veículo(s) atualizado(s), "
        f"{estatisticas['clientes_novos']} cliente(s) novo(s), "
        f"{estatisticas['clientes_atualizados']} cliente(s) atualizado(s)"
    )

    if "historico_linhas" in estatisticas:
        resumo += (
            f", {estatisticas['historico_linhas']} lavagem(ns) no historico"
        )

    return resumo

def buscar_sincronizacao_cliente(sync_id):
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM sincronizacoes_clientes WHERE id=?", (sync_id,))
    sync = c.fetchone()
    conn.close()
    return sync

def executar_sincronizacao_cliente(sync_id):
    sync = buscar_sincronizacao_cliente(sync_id)

    if not sync:
        return False, "Sincronização não encontrada."

    try:
        url_base = sync["url"]

        # 🔥 SHEETY (FLUXO DIRETO)
        if "sheety.co" in url_base:
            df = ler_planilha_sheety(url_base, intervalo_minutos=sync["intervalo_minutos"])

            print("🔥 DADOS SHEETY:")
            print(df.tail(5))

            hash_atual = hashlib.md5(
                df.to_csv(index=False).encode("utf-8")
            ).hexdigest()
            url_usada = url_base

        # 🔽 PLANILHA NORMAL (GOOGLE, CSV, EXCEL)
        else:
            url_base = corrigir_link_google_sheets(url_base)

            url_base = url_base + "?_=" + str(time.time())
            resposta = requests.get(url_base, timeout=20)
            resposta.raise_for_status()

            print("URL FINAL:", url_base)
            print("STATUS:", resposta.status_code)
            print("CONTENT TYPE:", resposta.headers.get("content-type"))

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

            print("🔥 DADOS PLANILHA:")
            print(df.tail(5))

            url_usada = url_base

        if sync["ultimo_hash"] == hash_atual:
            agora_atual = agora_iso()
            mensagem = "Sem alterações na planilha."
            conn = conectar()
            c = conn.cursor()
            c.execute("""
                UPDATE sincronizacoes_clientes
                SET ultimo_sync_em=?,
                    proximo_sync_em=?,
                    ultimo_status=?,
                    ultima_mensagem=?,
                    atualizado_em=?
                WHERE id=?
            """, (
                agora_atual,
                somar_minutos_iso(sync["intervalo_minutos"]),
                "OK",
                mensagem,
                agora_atual,
                sync_id,
            ))
            conn.commit()
            conn.close()
            return True, mensagem

        # 🔽 MAPEAMENTO
        mapeamento = {
            campo["key"]: sync[f"campo_{campo['key']}"] or ""
            for campo in CAMPOS_SINCRONIZACAO_CLIENTES
        }
        mapeamento["telefone"] = sync["campo_telefone"] or ""
        mapeamento["data"] = sync["campo_data"] or ""

        # 🔽 IMPORTAÇÃO
        estatisticas = importar_clientes_dataframe(df, mapeamento)
        registros_historico, estatisticas_historico = montar_registros_historico_lavagens(df, mapeamento)
        estatisticas.update(estatisticas_historico)
        salvar_historico_lavagens_sync(sync_id, registros_historico)
        mensagem = resumir_importacao_clientes(estatisticas)

        agora_atual = agora_iso()

        # 🔽 SALVAR RESULTADO
        conn = conectar()
        c = conn.cursor()

        c.execute("""
            UPDATE sincronizacoes_clientes
            SET url=?,
                ultimo_sync_em=?,
                proximo_sync_em=?,
                ultimo_status=?,
                ultima_mensagem=?,
                ultimo_hash=?,
                atualizado_em=?
            WHERE id=?
        """, (
            url_usada,
            agora_atual,
            somar_minutos_iso(sync["intervalo_minutos"]),
            "OK",
            mensagem,
            hash_atual,
            agora_atual,
            sync_id
        ))

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

        c.execute("""
            UPDATE sincronizacoes_clientes
            SET ultimo_status=?,
                ultima_mensagem=?,
                ativo=?,
                proximo_sync_em=?,
                atualizado_em=?
            WHERE id=?
        """, (
            "PAUSADA" if fatal else "ERRO",
            mensagem,
            0 if fatal else sync["ativo"],
            None if fatal else somar_minutos_iso(sync["intervalo_minutos"]),
            agora_iso(),
            sync_id
        ))

        conn.commit()
        conn.close()

        salvar_notificacao(mensagem, "erro")

        return False, mensagem

def importar_planilha_local():
    try:
        caminho = os.path.join("static", "CONTROLE LAVAGENS.xlsx")

        if not os.path.exists(caminho):
            return False, "Arquivo clientes.csv não encontrado."

        df = pd.read_excel(caminho)

        df = df.fillna("")
        df.columns = [str(col).strip().lower() for col in df.columns]

        # 🔽 MAPEAMENTO SIMPLES
        mapeamento = {
            "placa": "placa",
            "nome": "nome",
            "telefone": "telefone",
            "modelo": "modelo",
            "cor": "cor"
        }

        estatisticas = importar_clientes_dataframe(df, mapeamento)
        mensagem = resumir_importacao_clientes(estatisticas)

        salvar_notificacao(mensagem, "sucesso")

        return True, mensagem

    except Exception as e:
        mensagem = f"Erro ao importar planilha local: {e}"
        salvar_notificacao(mensagem, "erro")
        return False, mensagem

def listar_registros_clientes(busca=""):
    conn = conectar()
    c = conn.cursor()

    if busca:
        termo = f"%{busca}%"
        c.execute("""
            SELECT
                veiculos.id AS veiculo_id,
                veiculos.placa,
                veiculos.modelo,
                veiculos.cor,
                clientes.id AS cliente_id,
                clientes.nome,
                clientes.telefone
            FROM veiculos
            LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
            WHERE veiculos.placa LIKE ? OR veiculos.modelo LIKE ? OR clientes.nome LIKE ?
            ORDER BY veiculos.id DESC
        """, (termo, termo, termo))
    else:
        c.execute("""
            SELECT
                veiculos.id AS veiculo_id,
                veiculos.placa,
                veiculos.modelo,
                veiculos.cor,
                clientes.id AS cliente_id,
                clientes.nome,
                clientes.telefone
            FROM veiculos
            LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
            ORDER BY veiculos.id DESC
        """)

    registros = []

    for row in c.fetchall():
        item = dict(row)
        item["nome"] = item.get("nome") or ""
        item["telefone"] = item.get("telefone") or ""
        item["modelo"] = item.get("modelo") or ""
        item["cor"] = item.get("cor") or ""
        item["placa_original"] = item.get("placa") or ""
        registros.append(item)

    conn.close()
    return registros

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

def salvar_cliente_veiculo(placa, nome="", telefone="", modelo="", cor="", placa_original=None):
    placa_nova = limpar_valor_planilha(placa).upper()
    placa_referencia = limpar_valor_planilha(placa_original).upper() or placa_nova
    nome = limpar_valor_planilha(nome)
    telefone = limpar_valor_planilha(telefone)
    modelo = limpar_valor_planilha(modelo)
    cor = limpar_valor_planilha(cor)

    if not placa_nova:
        raise ValueError("Informe a placa do veiculo.")

    conn = conectar()
    c = conn.cursor()

    try:
        c.execute("""
            SELECT id, placa, modelo, cor, cliente_id
            FROM veiculos
            WHERE placa=?
        """, (placa_referencia,))
        veiculo_existente = c.fetchone()

        if placa_referencia != placa_nova:
            c.execute("SELECT id FROM veiculos WHERE placa=?", (placa_nova,))
            conflito = c.fetchone()

            if conflito and (not veiculo_existente or conflito["id"] != veiculo_existente["id"]):
                raise ValueError("Ja existe um veiculo cadastrado com essa placa.")

        cliente_existente = None

        if veiculo_existente and veiculo_existente["cliente_id"]:
            c.execute("SELECT id, nome, telefone FROM clientes WHERE id=?", (veiculo_existente["cliente_id"],))
            cliente_existente = c.fetchone()
        elif telefone:
            c.execute("SELECT id, nome, telefone FROM clientes WHERE telefone=?", (telefone,))
            cliente_existente = c.fetchone()

        cliente_id = cliente_existente["id"] if cliente_existente else None

        if cliente_existente:
            c.execute("""
                UPDATE clientes
                SET nome=?, telefone=?
                WHERE id=?
            """, (nome or "Sem nome", telefone, cliente_id))
        elif nome or telefone:
            c.execute("""
                INSERT INTO clientes (nome, telefone)
                VALUES (?, ?)
            """, (nome or "Sem nome", telefone))
            cliente_id = c.lastrowid

        if veiculo_existente:
            c.execute("""
                UPDATE veiculos
                SET placa=?, modelo=?, cor=?, cliente_id=?
                WHERE id=?
            """, (placa_nova, modelo, cor, cliente_id, veiculo_existente["id"]))
        else:
            c.execute("""
                INSERT INTO veiculos (placa, modelo, cor, cliente_id)
                VALUES (?, ?, ?, ?)
            """, (placa_nova, modelo, cor, cliente_id))

        conn.commit()

        return {
            "placa": placa_nova,
            "acao": "atualizado" if veiculo_existente else "novo",
            "cliente_id": cliente_id,
        }
    finally:
        conn.close()

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
            conn = conectar()
            c = conn.cursor()
            c.execute("""
                SELECT id
                FROM sincronizacoes_clientes
                WHERE ativo=1
                  AND (proximo_sync_em IS NULL OR proximo_sync_em<=?)
                ORDER BY id
                LIMIT 1
            """, (agora_iso(),))
            pendente = c.fetchone()
            conn.close()

            if not pendente:
                break

            executar_sincronizacao_cliente(pendente["id"])
    finally:
        sync_lock.release()

def loop_worker_sincronizacao():
    while True:
        try:
            sincronizar_fontes_pendentes()
        except Exception as e:
            print("ERRO WORKER SYNC:", e)

        time.sleep(60)

def iniciar_worker_sincronizacao():
    global sync_worker_iniciado

    if sync_worker_iniciado:
        return

    sync_worker_iniciado = True
    Thread(target=loop_worker_sincronizacao, daemon=True).start()

def loop_importacao():
    while True:
        importar_planilha_local()
        time.sleep(3600)  # atualiza a cada 1 minuto


def carregar_contexto_clientes(busca="", limpar=False):
    busca_aplicada = "" if limpar else busca
    clientes = listar_registros_clientes(busca_aplicada)

    conn = conectar()
    c = conn.cursor()

    c.execute("""
        SELECT *
        FROM sincronizacoes_clientes
        ORDER BY id DESC
    """)
    sincronizacoes_raw = c.fetchall()
    conn.close()

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

    return clientes, sincronizacoes

@app.before_request
def preparar_sincronizacoes():
    if request.endpoint == "static":
        return

    iniciar_worker_backup_banco()
    iniciar_worker_manutencao_arquivos()
    iniciar_worker_sincronizacao()

    if session.get("usuario"):
        sincronizar_fontes_pendentes()

@app.before_request
def exigir_troca_senha_obrigatoria():
    endpoint = request.endpoint or ""

    if endpoint == "static" or not session.get("usuario"):
        return

    sincronizar_sessao_usuario()

    if not session.get("senha_alteracao_obrigatoria"):
        return

    if endpoint in {"configuracoes", "atualizar_minha_senha", "logout"}:
        return

    definir_feedback_configuracoes(
        "erro",
        "Por seguranca, troque sua senha antes de continuar usando o sistema."
    )
    return redirect("/configuracoes")

@app.route("/api/cliente/<placa>")
def buscar_cliente_api(placa):
    if not session.get("usuario"):
        return {"erro": "nao autorizado"}

    conn = conectar()
    c = conn.cursor()

    c.execute("""
        SELECT 
            veiculos.placa,
            veiculos.modelo,
            clientes.nome,
            clientes.telefone
        FROM veiculos
        LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
        WHERE veiculos.placa=?
    """, (placa.upper(),))

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

    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.enums import TA_CENTER  # 🔥 ADICIONA ISSO

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        leftMargin=0,
        rightMargin=0
    )

    styles = getSampleStyleSheet()
    style_centro = styles["Normal"]
    style_centro.alignment = TA_CENTER
    elementos = []

    # 🔥 LOGO
    try:
        logo = ImagemRedonda("static/logo.jpg", size=80)

        tabela_logo = Table(
            [[logo]],
            colWidths=[500]  # 🔥 ESSENCIAL
        )

        tabela_logo.setStyle([
            ("ALIGN", (0,0), (-1,-1), "LEFT")
        ])

        elementos.append(tabela_logo)

    except:
        pass

    elementos.append(Spacer(1, 20))

    # 🔥 DADOS
    nome = request.form.get("nome")
    telefone = request.form.get("telefone")
    placa = request.form.get("placa")
    modelo = request.form.get("modelo")
    observacoes = request.form.get("observacoes")

    elementos.append(Paragraph(f"<b>Cliente:</b> {nome}", style_centro))
    elementos.append(Paragraph(f"<b>Telefone:</b> {telefone}", style_centro))
    elementos.append(Paragraph(f"<b>Veículo:</b> {modelo} - {placa}", style_centro))

    elementos.append(Spacer(1, 20))

    # 🔥 TABELA
    servicos = request.form.getlist("servico[]")
    valores = request.form.getlist("valor[]")

    dados_tabela = [["Serviço", "Valor (R$)"]]

    total = 0

    for s, v in zip(servicos, valores):
        try:
            valor = float(v)
        except:
            valor = 0

        dados_tabela.append([s, f"{valor:.2f}"])
        total += valor

    dados_tabela.append(["TOTAL", f"{total:.2f}"])

    tabela = Table(dados_tabela)
    tabela.hAlign = "CENTER"

    tabela.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.black),
        ("TEXTCOLOR", (0,0), (-1,0), colors.white),

        ("BACKGROUND", (0,-1), (-1,-1), colors.lightgrey),

        ("GRID", (0,0), (-1,-1), 1, colors.black),
        ("FONTNAME", (0,0), (-1,0), "Helvetica-Bold"),
    ]))

    elementos.append(tabela)

    # 🔥 OBSERVAÇÕES
    if observacoes:
        elementos.append(Spacer(1, 20))
        elementos.append(Paragraph("<b>Observações:</b>", styles["Normal"]))
        elementos.append(Paragraph(observacoes, styles["Normal"]))

    doc.build(elementos)
    buffer.seek(0)

    return send_file(buffer, as_attachment=True, download_name="orcamento.pdf", mimetype="application/pdf")

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
    try:
        import requests

        url = "https://api.open-meteo.com/v1/forecast?latitude=-29.68&longitude=-51.13&current_weather=true"
        resposta = requests.get(url, timeout=5)

        if resposta.status_code != 200:
            return {"erro": "api offline"}

        dados = resposta.json()

        print("CLIMA DEBUG:", dados)

        cw = dados.get("current_weather")

        if not cw:
            return {"erro": "sem dados"}

        temp = cw.get("temperature", 0)
        codigo = cw.get("weathercode", 0)

        # 🔥 LÓGICA
        if codigo >= 61:
            icone = "🌧️"
            clima = "Chuva"
            sugestao = "💡 Lavagem interna"
        elif codigo <= 3:
            icone = "☀️"
            clima = "Tempo limpo"
            sugestao = "💡 Lavagem completa"
        else:
            icone = "⛅"
            clima = "Nublado"
            sugestao = "💡 Lavagem simples"

        return {
            "clima": clima,
            "temp": temp,
            "icone": icone,
            "sugestao": sugestao
        }

    except Exception as e:
        print("ERRO CLIMA:", e)
        return {"erro": str(e)}

@app.route("/clientes/importar-local")
def importar_local():
    sucesso, mensagem = importar_planilha_local()
    definir_feedback_clientes("sucesso" if sucesso else "erro", mensagem)
    return redirect("/clientes")


@app.route("/api/notificacoes")
def api_notificacoes():
    if not session.get("usuario"):
        return jsonify([])

    conn = conectar()
    c = conn.cursor()

    c.execute("""
        SELECT * FROM notificacoes
        ORDER BY id DESC
        LIMIT 20
    """)

    dados = c.fetchall()
    conn.close()

    return jsonify([dict(row) for row in dados])

@app.route("/api/notificacoes/lida/<int:id>", methods=["POST"])
def marcar_notificacao_lida(id):
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    conn = conectar()
    c = conn.cursor()

    c.execute("UPDATE notificacoes SET lida=1 WHERE id=?", (id,))

    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})

@app.route("/api/notificacoes/limpar", methods=["POST"])
def limpar_notificacoes():
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM notificacoes")
    total = c.fetchone()[0]
    c.execute("DELETE FROM notificacoes")
    conn.commit()
    conn.close()

    return jsonify({"status": "ok", "removidas": total})

@app.route("/api/agenda-retornos")
def api_agenda_retorno():
    if not session.get("usuario"):
        return jsonify({"erro": "nao autorizado"}), 401

    try:
        dados = listar_agenda_retorno_lavagens()
        return jsonify(dados)
    except Exception as e:
        print("ERRO AGENDA RETORNOS:", e)
        return jsonify({
            "erro": "nao foi possivel carregar a agenda agora",
            "detalhe": str(e),
        }), 500

@app.route("/api/hud")
def api_hud():
    if not session.get("usuario"):
        return {"erro": "nao autorizado"}

    from datetime import datetime
    from zoneinfo import ZoneInfo

    conn = conectar()
    c = conn.cursor()

    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y")

    # 💰 faturamento
    c.execute("""
        SELECT SUM(valor) FROM servicos 
        WHERE status='FINALIZADO' AND entrega LIKE ?
    """, (hoje + "%",))

    total = c.fetchone()[0] or 0

    # ⚙️ em andamento
    c.execute("SELECT COUNT(*) FROM servicos WHERE status='EM ANDAMENTO'")
    andamento = c.fetchone()[0]

    # 📦 finalizados hoje
    c.execute("""
        SELECT COUNT(*) FROM servicos 
        WHERE status='FINALIZADO' AND entrega LIKE ?
    """, (hoje + "%",))

    quantidade = c.fetchone()[0]

    # 💵 ticket médio
    ticket = total / quantidade if quantidade > 0 else 0

    # 🚨 atrasados (>2h)
    c.execute("SELECT entrada FROM servicos WHERE status='EM ANDAMENTO'")
    servicos = c.fetchall()

    atrasados = 0
    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))

    for s in servicos:
        try:
            entrada = datetime.fromisoformat(s["entrada"])
            diff = (agora - entrada).total_seconds()

            if diff > 7200:
                atrasados += 1
        except:
            pass

    conn.close()
    itens_retornos = []
    retornos_acao_agora = 0
    retornos_reagendados_vencidos = 0
    retornos_contatados_hoje = 0

    try:
        itens_retornos = montar_itens_retornos_comerciais()
        hoje_data = agora.date()
        retornos_acao_agora = sum(
            1 for item in itens_retornos if item.get("mostrar_na_agenda")
        )
        retornos_reagendados_vencidos = sum(
            1 for item in itens_retornos if item.get("reagendamento_vencido")
        )
        retornos_contatados_hoje = sum(
            1
            for item in itens_retornos
            if (
                item.get("status_retorno") == "contatado" and
                interpretar_datahora_sistema(item.get("ultimo_contato_em")) and
                interpretar_datahora_sistema(item.get("ultimo_contato_em")).date() == hoje_data
            )
        )
    except Exception as erro:
        print("ERRO HUD RETORNOS:", erro)

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

    return {
        "total": round(total, 2),
        "andamento": andamento,
        "atrasados": atrasados,
        "ticket": round(ticket, 2),
        "retornos_acao_agora": retornos_acao_agora,
        "retornos_reagendados_vencidos": retornos_reagendados_vencidos,
        "retornos_contatados_hoje": retornos_contatados_hoje,
        "retornos_mensagem": mensagem_retornos_hud,
        "versao": APP_VERSION,
        "usuario": session.get("usuario") or "",
        "usuario_nome": session.get("usuario_nome") or session.get("usuario") or "",
        "usuario_iniciais": session.get("usuario_iniciais") or obter_iniciais_usuario(
            session.get("usuario_nome"),
            session.get("usuario"),
        ),
        "usuario_foto_url": session.get("usuario_foto_url") or "",
    }

@app.route("/status_sync")
def status_sync():
    if not session.get("usuario"):
        return jsonify({"status": "erro"})

    conn = conectar()
    c = conn.cursor()

    c.execute("""
        SELECT id, ultima_mensagem, ultimo_status
        FROM sincronizacoes_clientes
        ORDER BY id DESC
        LIMIT 1
    """)

    row = c.fetchone()

    if not row:
        conn.close()
        return jsonify({"status": "vazio"})

    # 🔥 limpa a mensagem depois de ler (ANTI-SPAM)
    c.execute("""
        UPDATE sincronizacoes_clientes
        SET ultima_mensagem=NULL
        WHERE id=?
    """, (row["id"],))

    conn.commit()
    conn.close()

    return jsonify({
        "status": row["ultimo_status"],
        "mensagem": row["ultima_mensagem"]
    })

@app.route("/editar_servico_inline/<int:id>", methods=["POST"])
def editar_servico_inline(id):
    data = request.get_json()

    nome = data.get("nome")
    valor = data.get("valor")

    conn = conectar()
    c = conn.cursor()

    c.execute("UPDATE tipos_servico SET nome=?, valor=? WHERE id=?", (nome, valor, id))
    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})

@app.route("/excluir_servico/<int:id>", methods=["POST"])
def excluir_servico(id):
    conn = conectar()
    c = conn.cursor()

    c.execute("DELETE FROM tipos_servico WHERE id=?", (id,))
    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})


@app.route("/editar_servico/<int:id>", methods=["GET", "POST"])
def editar_servico(id):
    conn = conectar()
    c = conn.cursor()

    if request.method == "POST":
        nome = request.form['nome']
        valor = request.form['valor']

        c.execute("UPDATE tipos_servico SET nome=?, valor=? WHERE id=?", (nome, valor, id))
        conn.commit()
        conn.close()

        return redirect("/cadastro_servico")

    c.execute("SELECT * FROM tipos_servico WHERE id=?", (id,))
    servico = c.fetchone()

    conn.close()

    return render_template("editar_servico.html", servico=servico)

# 🔐 CRIAR ADMIN PADRÃO
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
        elif senha_padrao_admin_ativa(admin_atualizado) or not senha_usa_bcrypt(admin_atualizado["senha"]):
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
        print(f"ADMIN criado/recuperado com senha temporaria segura: admin / {senha_temporaria}")
        print("ATENCAO: troque essa senha no primeiro login.")
    elif aviso_troca:
        print("ATENCAO: senha antiga/padrao do administrador detectada. Troca obrigatoria ativada.")

def carregar_usuarios_configuracao():
    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT id, usuario, nome, perfil, ativo, criado_em,
               tentativas_login, bloqueado_ate, ultimo_login_em,
               senha_alteracao_obrigatoria, foto_perfil
        FROM usuarios
        ORDER BY
            CASE WHEN perfil='admin' THEN 0 ELSE 1 END,
            nome COLLATE NOCASE,
            usuario COLLATE NOCASE
    """)
    usuarios = [dict(row) for row in c.fetchall()]
    conn.close()

    for item in usuarios:
        item["nome"] = item.get("nome") or item.get("usuario")
        item["perfil"] = normalizar_perfil_usuario(item.get("perfil"))
        item["ativo"] = int(item.get("ativo") or 0)
        item["criado_em_fmt"] = formatar_datahora(item.get("criado_em"))
        item["ultimo_login_em_fmt"] = formatar_datahora(item.get("ultimo_login_em"))
        item["tentativas_login"] = int(item.get("tentativas_login") or 0)
        item["troca_senha_obrigatoria"] = bool(int(item.get("senha_alteracao_obrigatoria") or 0))
        item["iniciais"] = obter_iniciais_usuario(item.get("nome"), item.get("usuario"))
        item["foto_url"] = url_foto_usuario(item.get("foto_perfil"))
        bloqueado_ate = usuario_bloqueado_ate(item)
        item["bloqueado"] = bool(bloqueado_ate)
        item["bloqueado_ate_fmt"] = formatar_datahora(item.get("bloqueado_ate"))
        item["bloqueado_restante"] = formatar_tempo_restante(item.get("bloqueado_ate")) if bloqueado_ate else ""

    return usuarios

criar_admin()

@app.route("/login", methods=["GET", "POST"])
def login():

    if session.get("usuario"):
        sincronizar_sessao_usuario()
        if session.get("senha_alteracao_obrigatoria"):
            return redirect("/configuracoes")
        return redirect("/")

    if request.method == "POST":
        usuario = (request.form.get("usuario") or "").strip()
        senha = request.form.get("senha") or ""

        if not usuario or not senha:
            return render_template("login.html", erro="Informe usuario e senha.")

        conn = conectar()
        c = conn.cursor()

        c.execute("SELECT * FROM usuarios WHERE usuario=?", (usuario,))
        user = c.fetchone()

        if not user:
            conn.close()
            return render_template("login.html", erro="Usuario ou senha invalidos.")

        if not int(user["ativo"] if user["ativo"] is not None else 1):
            conn.close()
            return render_template("login.html", erro="Este acesso esta desativado.")

        bloqueado_ate = usuario_bloqueado_ate(user)
        if bloqueado_ate:
            conn.close()
            return render_template(
                "login.html",
                erro=(
                    "Login bloqueado temporariamente. "
                    f"{formatar_tempo_restante(bloqueado_ate.isoformat(timespec='seconds'))} "
                    "para tentar de novo."
                )
            )

        if not verificar_senha_usuario(senha, user["senha"]):
            novo_bloqueio = registrar_falha_login(c, user)
            conn.commit()
            conn.close()
            if novo_bloqueio:
                return render_template(
                    "login.html",
                    erro=f"Muitas tentativas invalidas. Login bloqueado por {MINUTOS_BLOQUEIO_LOGIN} minutos."
                )
            return render_template("login.html", erro="Usuario ou senha invalidos.")

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

        conn.close()
        preencher_sessao_usuario(user)
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

    return render_template("login.html")

@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.route("/configuracoes")
def configuracoes():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    pode_gerenciar_usuarios = usuario_admin() and not session.get("senha_alteracao_obrigatoria")
    usuarios = carregar_usuarios_configuracao() if pode_gerenciar_usuarios else []
    banco_status = obter_status_banco_online()
    banco_config = obter_configuracao_banco_form()
    backup_config = obter_configuracao_backup()
    arquivos_status = obter_status_arquivos()

    return render_template(
        "configuracoes.html",
        feedback=session.pop("configuracoes_feedback", None),
        usuario_logado={
            "id": session.get("usuario_id"),
            "usuario": session.get("usuario"),
            "nome": session.get("usuario_nome") or session.get("usuario"),
            "iniciais": session.get("usuario_iniciais") or obter_iniciais_usuario(
                session.get("usuario_nome"),
                session.get("usuario"),
            ),
            "foto_url": session.get("usuario_foto_url") or "",
            "perfil": session.get("usuario_perfil") or (
                "admin" if session.get("usuario") == "admin" else "funcionario"
            ),
            "senha_alteracao_obrigatoria": bool(session.get("senha_alteracao_obrigatoria")),
        },
        usuarios=usuarios,
        admin_logado=pode_gerenciar_usuarios,
        banco_status=banco_status,
        banco_config=banco_config,
        backup_status=obter_status_backup_banco(),
        backup_config=backup_config,
        frequencias_backup=FREQUENCIAS_BACKUP,
        tipos_backup=TIPOS_BACKUP,
        backups_disponiveis=listar_arquivos_backup_banco(),
        arquivos_status=arquivos_status,
        pastas_sync_sugeridas=listar_pastas_sincronizadas_sugeridas(),
    )

@app.route("/configuracoes/banco", methods=["POST"])
def salvar_configuracao_banco():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem alterar o banco online.")
        return redirect("/configuracoes")

    try:
        status = salvar_configuracao_banco_form(request.form)
    except ValueError as e:
        definir_feedback_configuracoes("erro", str(e))
        return redirect("/configuracoes")

    registrar_auditoria(
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
        definir_feedback_configuracoes(
            "sucesso",
            "Configuracao do banco salva e conexao com o Supabase validada com sucesso.",
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
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem testar o banco online.")
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
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem migrar o banco para o Supabase.")
        return redirect("/configuracoes")

    status = diagnosticar_banco_online(force=True)
    if not status.get("conectado"):
        definir_feedback_configuracoes(
            "erro",
            "Antes de migrar, a conexao com o Supabase precisa estar ativa.",
        )
        return redirect("/configuracoes")

    try:
        importar_sqlite_para_banco_atual(caminho_banco_absoluto())
        criar_backup_banco(force=True, tipo_backup="banco")
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
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem alterar a rotina de backup.")
        return redirect("/configuracoes")

    destino_externo_ativo = 1 if bool_config_ativo(request.form.get("destino_externo_ativo")) else 0
    destino_externo_pasta = normalizar_caminho_destino_externo(
        request.form.get("destino_externo_pasta"),
    )
    if destino_externo_ativo and not destino_externo_pasta:
        definir_feedback_configuracoes(
            "erro",
            "Informe a pasta sincronizada do Google Drive antes de ativar a copia externa.",
        )
        return redirect("/configuracoes")

    if destino_externo_ativo:
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
            "destino_externo_pasta": configuracao["destino_externo_pasta"],
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
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem gerar backups manuais.")
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
    return redirect("/configuracoes")

@app.route("/configuracoes/backup/restaurar", methods=["POST"])
def restaurar_backup_configuracoes():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem restaurar backups.")
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
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem executar a manutencao de arquivos.")
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

    sincronizar_sessao_usuario()
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
        nova_foto = salvar_foto_perfil_usuario(
            foto,
            identificador=f"{usuario['usuario']}_{usuario['id']}",
        )
        c.execute("UPDATE usuarios SET foto_perfil=? WHERE id=?", (nova_foto, usuario["id"]))
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
    definir_feedback_configuracoes("sucesso", "Foto do seu perfil atualizada com sucesso.")
    return redirect("/configuracoes")

@app.route("/configuracoes/senha", methods=["POST"])
def atualizar_minha_senha():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    senha_atual = request.form.get("senha_atual") or ""
    nova_senha = request.form.get("nova_senha") or ""
    confirmar_senha = request.form.get("confirmar_senha") or ""

    if not senha_atual or not nova_senha or not confirmar_senha:
        definir_feedback_configuracoes("erro", "Preencha todos os campos para alterar a senha.")
        return redirect("/configuracoes")

    if nova_senha != confirmar_senha:
        definir_feedback_configuracoes("erro", "A confirmacao da nova senha nao confere.")
        return redirect("/configuracoes")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM usuarios WHERE id=?", (session.get("usuario_id"),))
    usuario = c.fetchone()

    if not usuario or not verificar_senha_usuario(senha_atual, usuario["senha"]):
        conn.close()
        definir_feedback_configuracoes("erro", "A senha atual informada esta incorreta.")
        return redirect("/configuracoes")

    erro_forca = validar_forca_senha(nova_senha, session.get("usuario"))
    if erro_forca:
        conn.close()
        definir_feedback_configuracoes("erro", erro_forca)
        return redirect("/configuracoes")

    if verificar_senha_usuario(nova_senha, usuario["senha"]):
        conn.close()
        definir_feedback_configuracoes("erro", "Escolha uma senha diferente da atual.")
        return redirect("/configuracoes")

    c.execute(
        """
        UPDATE usuarios
        SET senha=?, senha_alteracao_obrigatoria=0, senha_atualizada_em=?, tentativas_login=0, bloqueado_ate=NULL
        WHERE id=?
        """,
        (senha_hash_bcrypt(nova_senha), agora_iso(), usuario["id"])
    )
    conn.commit()
    c.execute("SELECT * FROM usuarios WHERE id=?", (usuario["id"],))
    usuario_atualizado = c.fetchone()
    conn.close()

    preencher_sessao_usuario(usuario_atualizado)
    registrar_auditoria(
        "alterou_propria_senha",
        "usuario",
        entidade_id=usuario["id"],
        detalhes={"usuario_alvo": usuario["usuario"]},
    )
    definir_feedback_configuracoes(
        "sucesso",
        (
            "Senha atualizada com sucesso. Seu acesso agora esta protegido "
            "com hash bcrypt e politica forte."
        ),
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/usuarios", methods=["POST"])
def criar_usuario_funcionario():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem criar novos acessos.")
        return redirect("/configuracoes")

    nome = (request.form.get("nome") or "").strip()
    usuario = (request.form.get("usuario") or "").strip().lower()
    senha = request.form.get("senha") or ""
    perfil = normalizar_perfil_usuario(request.form.get("perfil"))
    foto_perfil = request.files.get("foto_perfil")

    if not nome or not usuario or not senha:
        definir_feedback_configuracoes("erro", "Informe nome, login e senha para criar o usuario.")
        return redirect("/configuracoes")

    erro_forca = validar_forca_senha(senha, usuario)
    if erro_forca:
        definir_feedback_configuracoes("erro", erro_forca)
        return redirect("/configuracoes")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id FROM usuarios WHERE usuario=?", (usuario,))
    existente = c.fetchone()

    if existente:
        conn.close()
        definir_feedback_configuracoes("erro", "Ja existe um acesso com esse login.")
        return redirect("/configuracoes")

    nova_foto = ""

    try:
        c.execute("""
            INSERT INTO usuarios (
                usuario, senha, nome, perfil, ativo, criado_em,
                tentativas_login, bloqueado_ate, ultimo_login_em,
                senha_alteracao_obrigatoria, senha_atualizada_em
            )
            VALUES (?, ?, ?, ?, 1, ?, 0, NULL, NULL, 1, ?)
        """, (
            usuario,
            senha_hash_bcrypt(senha),
            nome,
            perfil,
            agora_iso(),
            agora_iso(),
        ))
        usuario_id = c.lastrowid

        if foto_perfil and str(foto_perfil.filename or "").strip():
            nova_foto = salvar_foto_perfil_usuario(
                foto_perfil,
                identificador=f"{usuario}_{usuario_id}",
            )
            c.execute("UPDATE usuarios SET foto_perfil=? WHERE id=?", (nova_foto, usuario_id))

        conn.commit()
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
        definir_feedback_configuracoes("erro", "Nao foi possivel criar o usuario agora.")
        return redirect("/configuracoes")

    conn.close()

    registrar_auditoria(
        "criou_usuario",
        "usuario",
        detalhes={"usuario_alvo": usuario, "perfil": perfil, "com_foto": bool(nova_foto)},
    )
    definir_feedback_configuracoes(
        "sucesso",
        (
            f"Usuario {usuario} criado com sucesso. "
            "A troca de senha sera obrigatoria no primeiro login."
        ),
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/usuarios/<int:usuario_id>/senha", methods=["POST"])
def redefinir_senha_usuario(usuario_id):
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem redefinir senhas.")
        return redirect("/configuracoes")

    nova_senha = request.form.get("nova_senha") or ""

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id, usuario, perfil FROM usuarios WHERE id=?", (usuario_id,))
    alvo = c.fetchone()

    if not alvo:
        conn.close()
        definir_feedback_configuracoes("erro", "Usuario nao encontrado.")
        return redirect("/configuracoes")

    erro_forca = validar_forca_senha(nova_senha, alvo["usuario"])
    if erro_forca:
        conn.close()
        definir_feedback_configuracoes("erro", erro_forca)
        return redirect("/configuracoes")

    c.execute(
        """
        UPDATE usuarios
        SET senha=?, senha_alteracao_obrigatoria=1, senha_atualizada_em=?, tentativas_login=0, bloqueado_ate=NULL
        WHERE id=?
        """,
        (senha_hash_bcrypt(nova_senha), agora_iso(), usuario_id)
    )
    conn.commit()
    conn.close()

    registrar_auditoria(
        "redefiniu_senha_usuario",
        "usuario",
        entidade_id=usuario_id,
        detalhes={"usuario_alvo": alvo["usuario"]},
    )
    definir_feedback_configuracoes(
        "sucesso",
        (
            f"Senha do usuario {alvo['usuario']} atualizada. "
            "Ele vai precisar trocar a senha no proximo login."
        ),
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/usuarios/<int:usuario_id>/foto", methods=["POST"])
def atualizar_foto_usuario(usuario_id):
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem atualizar fotos de acessos.")
        return redirect("/configuracoes")

    foto = request.files.get("foto_perfil")
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT * FROM usuarios WHERE id=?", (usuario_id,))
    alvo = c.fetchone()

    if not alvo:
        conn.close()
        definir_feedback_configuracoes("erro", "Usuario nao encontrado.")
        return redirect("/configuracoes")

    antiga_foto = alvo["foto_perfil"]
    nova_foto = ""

    try:
        nova_foto = salvar_foto_perfil_usuario(
            foto,
            identificador=f"{alvo['usuario']}_{alvo['id']}",
        )
        c.execute("UPDATE usuarios SET foto_perfil=? WHERE id=?", (nova_foto, usuario_id))
        conn.commit()
        c.execute("SELECT * FROM usuarios WHERE id=?", (usuario_id,))
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
        f"Foto do usuario {alvo['usuario']} atualizada com sucesso."
    )
    return redirect("/configuracoes")

@app.route("/configuracoes/usuarios/<int:usuario_id>/alternar", methods=["POST"])
def alternar_status_usuario(usuario_id):
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem alterar o status de acessos.")
        return redirect("/configuracoes")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id, usuario, perfil, ativo FROM usuarios WHERE id=?", (usuario_id,))
    alvo = c.fetchone()

    if not alvo:
        conn.close()
        definir_feedback_configuracoes("erro", "Usuario nao encontrado.")
        return redirect("/configuracoes")

    if alvo["usuario"] == "admin" or normalizar_perfil_usuario(alvo["perfil"]) == "admin":
        conn.close()
        definir_feedback_configuracoes("erro", "O acesso administrador principal nao pode ser desativado.")
        return redirect("/configuracoes")

    novo_status = 0 if int(alvo["ativo"] or 0) else 1
    c.execute("UPDATE usuarios SET ativo=? WHERE id=?", (novo_status, usuario_id))
    conn.commit()
    conn.close()

    definir_feedback_configuracoes(
        "sucesso",
        f"Usuario {alvo['usuario']} {'ativado' if novo_status else 'pausado'} com sucesso."
    )
    return redirect("/configuracoes")

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

    estado_atual = carregar_estados_retornos([placa]).get(placa, {})
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
    definir_feedback_retornos("sucesso", mensagem_sucesso)
    return redirect(retorno_url)

@app.route("/auditoria")
def pagina_auditoria():
    if not session.get("usuario"):
        return redirect("/login")

    sincronizar_sessao_usuario()
    if not usuario_admin():
        definir_feedback_configuracoes("erro", "Somente administradores podem acessar a auditoria.")
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

@app.route("/clima")
def clima():
    if not session.get("usuario"):
        return redirect("/login")

    return render_template("clima.html")

@app.route("/financeiro")
def financeiro():
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()
    c.execute("""
        SELECT
            servicos.*,
            tipos_servico.nome AS tipo_nome,
            veiculos.placa,
            veiculos.modelo,
            clientes.nome AS cliente_nome
        FROM servicos
        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
        ORDER BY servicos.id DESC
    """)
    servicos_raw = [dict(row) for row in c.fetchall()]
    conn.close()

    agora_atual = agora()
    hoje = agora_atual.date()
    ano_atual = hoje.year
    mes_atual = hoje.month
    periodo_atual = normalizar_periodo_financeiro(request.args.get("periodo"))
    periodo_label = MAPA_PERIODOS_FINANCEIRO[periodo_atual]

    finalizados = []
    em_andamento = 0

    for item in servicos_raw:
        if item.get("status") == "EM ANDAMENTO":
            em_andamento += 1

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

    finalizados_hoje = [item for item in finalizados if item["entrega_dt"].date() == hoje]
    finalizados_mes = [
        item for item in finalizados
        if item["entrega_dt"].year == ano_atual and item["entrega_dt"].month == mes_atual
    ]
    finalizados_periodo = filtrar_servicos_por_periodo(finalizados, periodo_atual, hoje)

    total_hoje = sum(item["valor_num"] for item in finalizados_hoje)
    total_mes = sum(item["valor_num"] for item in finalizados_mes)
    total_geral = sum(item["valor_num"] for item in finalizados)
    quantidade_hoje = len(finalizados_hoje)
    quantidade_mes = len(finalizados_mes)
    quantidade_geral = len(finalizados)
    quantidade_periodo = len(finalizados_periodo)
    ticket_hoje = total_hoje / quantidade_hoje if quantidade_hoje else 0
    ticket_mes = total_mes / quantidade_mes if quantidade_mes else 0
    ticket_geral = total_geral / quantidade_geral if quantidade_geral else 0
    media_dia_mes = total_mes / max(1, hoje.day)
    total_periodo = sum(item["valor_num"] for item in finalizados_periodo)
    ticket_periodo = total_periodo / quantidade_periodo if quantidade_periodo else 0
    media_periodo = total_periodo / dias_do_periodo_financeiro(periodo_atual, hoje)

    base_ranking = finalizados_periodo
    ranking_servicos = {}

    for item in base_ranking:
        nome_servico = item["tipo_nome"]
        resumo = ranking_servicos.setdefault(nome_servico, {
            "nome": nome_servico,
            "quantidade": 0,
            "valor_total": 0.0,
        })
        resumo["quantidade"] += 1
        resumo["valor_total"] += item["valor_num"]

    ranking_faturamento = sorted(
        ranking_servicos.values(),
        key=lambda item: (item["valor_total"], item["quantidade"]),
        reverse=True
    )

    ranking_quantidade = sorted(
        ranking_servicos.values(),
        key=lambda item: (item["quantidade"], item["valor_total"]),
        reverse=True
    )

    referencia_ranking = ranking_faturamento[0]["valor_total"] if ranking_faturamento else 0
    referencia_quantidade = ranking_quantidade[0]["quantidade"] if ranking_quantidade else 0

    for item in ranking_faturamento:
        item["valor_exibicao"] = formatar_valor_monetario(item["valor_total"])
        item["ticket_exibicao"] = formatar_valor_monetario(
            item["valor_total"] / item["quantidade"] if item["quantidade"] else 0
        )
        item["percentual"] = round(
            (item["valor_total"] / referencia_ranking) * 100
        ) if referencia_ranking else 0

    ranking_quantidade_formatado = []
    for item in ranking_quantidade[:5]:
        ranking_quantidade_formatado.append({
            "nome": item["nome"],
            "quantidade": item["quantidade"],
            "valor_exibicao": formatar_valor_monetario(item["valor_total"]),
            "percentual": round((item["quantidade"] / referencia_quantidade) * 100) if referencia_quantidade else 0,
        })

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
        ultimos_7_dias.append({
            "label": data_ref.strftime("%d/%m"),
            "valor": total_dia,
            "valor_exibicao": formatar_valor_monetario(total_dia),
            "percentual": round((total_dia / referencia_7_dias) * 100) if referencia_7_dias else 0,
        })

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
        ultimos_6_meses.append({
            "label": f"{MESES_CURTOS_PT[mes_ref - 1]}/{str(ano_ref)[-2:]}",
            "valor": total_mes_ref,
            "valor_exibicao": formatar_valor_monetario(total_mes_ref),
        })

    referencia_6_meses = max(totais_por_mes, default=0)
    for item in ultimos_6_meses:
        item["percentual"] = round((item["valor"] / referencia_6_meses) * 100) if referencia_6_meses else 0

    ultimos_finalizados = finalizados_periodo[:8]
    servico_campeao = ranking_faturamento[0] if ranking_faturamento else None
    periodo_descricao = {
        "hoje": "Resultados fechados hoje.",
        "7dias": "Panorama consolidado dos ultimos 7 dias.",
        "30dias": "Panorama consolidado dos ultimos 30 dias.",
        "mes": "Fechamentos acumulados no mes atual.",
    }[periodo_atual]

    return render_template(
        "financeiro.html",
        periodo_atual=periodo_atual,
        periodo_label=periodo_label,
        periodo_descricao=periodo_descricao,
        periodos_financeiro=PERIODOS_FINANCEIRO,
        total_periodo=formatar_valor_monetario(total_periodo),
        quantidade_periodo=quantidade_periodo,
        ticket_periodo=formatar_valor_monetario(ticket_periodo),
        media_periodo=formatar_valor_monetario(media_periodo),
        total_hoje=formatar_valor_monetario(total_hoje),
        total_mes=formatar_valor_monetario(total_mes),
        total_geral=formatar_valor_monetario(total_geral),
        quantidade_hoje=quantidade_hoje,
        quantidade_mes=quantidade_mes,
        quantidade_geral=quantidade_geral,
        ticket_hoje=formatar_valor_monetario(ticket_hoje),
        ticket_mes=formatar_valor_monetario(ticket_mes),
        ticket_geral=formatar_valor_monetario(ticket_geral),
        media_dia_mes=formatar_valor_monetario(media_dia_mes),
        em_andamento=em_andamento,
        ranking_faturamento=ranking_faturamento[:5],
        ranking_quantidade=ranking_quantidade_formatado,
        ultimos_7_dias=ultimos_7_dias,
        ultimos_6_meses=ultimos_6_meses,
        ultimos_finalizados=ultimos_finalizados,
        servico_campeao=servico_campeao,
        referencia_ranking_periodo=periodo_label,
    )


@app.route("/", methods=["GET", "POST"])
def index():
    if not session.get("usuario"):
        return redirect("/login")

    dados = None
    historico = []
    buscou = False
    lavagem_info = None

    conn = conectar()
    c = conn.cursor()

    # 🔥 LISTAS FIXAS
    c.execute("SELECT * FROM tipos_servico")
    servicos_lista = c.fetchall()

    c.execute("SELECT * FROM produtos_pneu")
    produtos_pneu = c.fetchall()

    # 🔥 POST → REDIRECT
    if request.method == "POST":
        placa = request.form.get("placa", "").upper()
        return redirect(f"/?placa={placa}")

    # 🔥 GET (AQUI ESTÁ O SEGREDO)
    placa = request.args.get("placa", "").upper()

    if placa:
        buscou = True
        lavagem_info = montar_contexto_lavagem_placa(placa)

        # 🔥 CLIENTE
        c.execute("""
        SELECT 
            veiculos.placa,
            veiculos.modelo,
            veiculos.cor,
            clientes.nome,
            clientes.telefone
        FROM veiculos
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
        WHERE veiculos.placa=?
        """, (placa,))

        dados = c.fetchone()

        if dados:
            # 🔥 HISTÓRICO
            c.execute("SELECT id FROM veiculos WHERE placa=?", (placa,))
            veiculo = c.fetchone()

            if veiculo:
                veiculo_id = veiculo[0]

                c.execute("""
                    SELECT
                        servicos.id,
                        servicos.valor,
                        servicos.entrada,
                        servicos.entrega,
                        servicos.status,
                        servicos.observacoes,
                        servicos.origem,
                        servicos.guarita,
                        servicos.pneu,
                        servicos.cera,
                        servicos.hidro_lataria,
                        servicos.hidro_vidros,
                        servicos.criado_por_usuario,
                        servicos.criado_por_nome,
                        servicos.operacional_por_usuario,
                        servicos.operacional_por_nome,
                        servicos.finalizado_por_usuario,
                        servicos.finalizado_por_nome,
                        tipos_servico.nome AS tipo_nome
                    FROM servicos
                    LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
                    WHERE servicos.veiculo_id=?
                    ORDER BY servicos.id DESC
                """, (veiculo_id,))

                historico_db = c.fetchall()
                fotos_por_servico = listar_fotos_servicos([row["id"] for row in historico_db])

                # 🔥 FORMATAR HISTÓRICO
                historico_formatado = []

                for s in historico_db:
                    s_dict = dict(s)
                    entrada = interpretar_datahora_sistema(s_dict.get("entrada"))
                    entrega = interpretar_datahora_sistema(s_dict.get("entrega"))

                    try:
                        if entrada and entrega:
                            tempo = entrega - entrada
                            tempo_str = str(tempo)
                        elif s_dict.get("status") == "EM ANDAMENTO":
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
                    s_dict["valor_exibicao"] = formatar_valor_monetario(
                        s_dict.get("valor")
                    )
                    c.execute("""
                        SELECT item_nome
                        FROM servico_checklist
                        WHERE servico_id=?
                        ORDER BY id ASC
                    """, (s_dict["id"],))
                    checklist_itens = [row["item_nome"] for row in c.fetchall()]
                    s_dict["checklist_itens"] = checklist_itens
                    s_dict["checklist_resumo"] = ", ".join(checklist_itens)
                    s_dict["galeria_fotos"] = fotos_por_servico.get(s_dict["id"], {})
                    s_dict["fotos_entrada"] = len(s_dict["galeria_fotos"].get("entrada", []))
                    s_dict["fotos_detalhe"] = len(s_dict["galeria_fotos"].get("detalhe", []))
                    s_dict["fotos_saida"] = len(s_dict["galeria_fotos"].get("saida", []))
                    enriquecer_responsaveis_servico(s_dict)

                    historico_formatado.append({
                        "servico": s_dict,
                        "tempo_str": tempo_str,
                    })

                historico = historico_formatado

    conn.close()

    return render_template(
        "index.html",
        dados=dados,
        historico=historico,
        buscou=buscou,
        placa=placa,
        lavagem_info=lavagem_info,
        version=APP_VERSION,
        feedback_index=session.pop("index_feedback", None),
        servicos_lista=servicos_lista,
        produtos_pneu=produtos_pneu
    )

def renderizar_pagina_clientes(busca="", limpar=False):
    clientes_lista, sincronizacoes = carregar_contexto_clientes(busca=busca, limpar=limpar)

    return render_template(
        "clientes.html",
        clientes=clientes_lista,
        sincronizacoes=sincronizacoes,
        feedback=session.pop("clientes_feedback", None),
        preview_sync=session.get("clientes_sync_preview"),
        campos_sync=CAMPOS_SINCRONIZACAO_CLIENTES,
        intervalos_sync=INTERVALOS_SINCRONIZACAO,
        busca=busca,
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
            raise ValueError("Não encontrei colunas válidas nessa planilha.")

        # 🔥 CORREÇÃO CRÍTICA AQUI
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
            "amostra": amostra_tratada,  # ✅ agora seguro
            "total_linhas": len(df.index),
            "intervalo_label": obter_label_intervalo_sincronizacao(intervalo_minutos),
            "proximo_sync_previsto": proximo_sync_previsto,
            "proximo_sync_previsto_fmt": formatar_datahora(proximo_sync_previsto),
            "proximo_sync_previsto_relativo": formatar_tempo_restante(proximo_sync_previsto),
        }

        definir_feedback_clientes(
            "sucesso",
            f"Planilha carregada. {len(df.index)} linha(s) encontrada(s) para configurar a sincronização."
        )

    except Exception as e:
        limpar_preview_sincronizacao()
        definir_feedback_clientes("erro", f"Não consegui ler a planilha: {e}")

    return redirect("/clientes")

@app.route("/clientes/sincronizacao/cancelar_preview", methods=["POST"])
def cancelar_preview_sincronizacao_clientes():
    if not session.get("usuario"):
        return redirect("/login")

    limpar_preview_sincronizacao()
    definir_feedback_clientes("sucesso", "Pré-visualização cancelada.")
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
        # 🔥 1. LER PLANILHA
        df, url_normalizada = ler_dataframe_link_planilha(
            url,
            intervalo_minutos=intervalo_minutos,
        )

        # 🔥 2. MAPEAR COLUNAS AUTOMATICAMENTE
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
            raise Exception("Não consegui identificar a coluna de PLACA automaticamente.")

        # 🔥 3. IMPORTAR DIRETO PRO BANCO
        estatisticas = importar_clientes_dataframe(df, mapeamento)
        registros_historico, estatisticas_historico = montar_registros_historico_lavagens(df, mapeamento)
        estatisticas.update(estatisticas_historico)

        # 🔥 4. SALVAR CONFIG DE SINCRONIZAÇÃO
        conn = conectar()
        c = conn.cursor()

        agora_atual = agora_iso()

        c.execute("""
        INSERT INTO sincronizacoes_clientes (
            nome, url, intervalo_minutos,
            campo_placa, campo_nome, campo_telefone, campo_modelo, campo_cor, campo_servico, campo_data,
            ativo, ultimo_status, proximo_sync_em, criado_em, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nome or "Planilha automática",
            url_normalizada,
            intervalo_minutos,
            mapeamento.get("placa"),
            mapeamento.get("nome"),
            mapeamento.get("telefone"),
            mapeamento.get("modelo"),
            mapeamento.get("cor"),
            mapeamento.get("servico"),
            mapeamento.get("data"),
            1,
            "OK",
            somar_minutos_iso(intervalo_minutos),
            agora_atual,
            agora_atual,
        ))
        sync_id = c.lastrowid

        conn.commit()
        conn.close()

        salvar_historico_lavagens_sync(sync_id, registros_historico)

        definir_feedback_clientes(
            "sucesso",
            f"Importação concluída: {resumir_importacao_clientes(estatisticas)}"
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

        estatisticas = importar_clientes_dataframe(df, mapeamento)
        registros_historico, estatisticas_historico = montar_registros_historico_lavagens(df, mapeamento)
        estatisticas.update(estatisticas_historico)
        mensagem_importacao = resumir_importacao_clientes(estatisticas)
        agora_atual = agora_iso()
        proximo_sync_em = somar_minutos_iso(intervalo_minutos)
        hash_atual = hashlib.md5(df.to_csv(index=False).encode("utf-8")).hexdigest()

        conn = conectar()
        c = conn.cursor()
        c.execute("""
            INSERT INTO sincronizacoes_clientes (
                nome, url, intervalo_minutos,
                campo_placa, campo_nome, campo_telefone, campo_modelo, campo_cor, campo_servico, campo_data,
                ativo, ultimo_sync_em, proximo_sync_em, ultimo_status, ultima_mensagem,
                criado_em, atualizado_em, ultimo_hash, colunas_ultima_sync
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nome or "Planilha automatica",
            url_normalizada,
            intervalo_minutos,
            mapeamento.get("placa"),
            mapeamento.get("nome"),
            mapeamento.get("telefone"),
            mapeamento.get("modelo"),
            mapeamento.get("cor"),
            mapeamento.get("servico"),
            mapeamento.get("data"),
            1,
            agora_atual,
            proximo_sync_em,
            "OK",
            mensagem_importacao,
            agora_atual,
            agora_atual,
            hash_atual,
            ",".join(colunas),
        ))
        sync_id = c.lastrowid
        conn.commit()
        conn.close()

        salvar_historico_lavagens_sync(sync_id, registros_historico)

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

    sucesso, mensagem = executar_sincronizacao_cliente(sync_id)
    definir_feedback_clientes("sucesso" if sucesso else "erro", mensagem)
    return redirect("/clientes")

@app.route("/clientes/sincronizacao/<int:sync_id>/alternar", methods=["POST"])
def alternar_sync_clientes(sync_id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT id, ativo, intervalo_minutos FROM sincronizacoes_clientes WHERE id=?", (sync_id,))
    sync = c.fetchone()

    if not sync:
        conn.close()
        definir_feedback_clientes("erro", "Sincronização não encontrada.")
        return redirect("/clientes")

    novo_ativo = 0 if sync["ativo"] else 1
    novo_status = "AGENDADO" if novo_ativo else "PAUSADO"
    proximo_sync = agora_iso() if novo_ativo else None
    atualizado_em = agora_iso()

    c.execute("""
        UPDATE sincronizacoes_clientes
        SET ativo=?, ultimo_status=?, proximo_sync_em=?, atualizado_em=?
        WHERE id=?
    """, (novo_ativo, novo_status, proximo_sync, atualizado_em, sync_id))
    conn.commit()
    conn.close()

    definir_feedback_clientes(
        "sucesso",
        "Sincronização ativada." if novo_ativo else "Sincronização pausada."
    )
    return redirect("/clientes")

@app.route("/clientes/sincronizacao/<int:sync_id>/excluir", methods=["POST"])
def excluir_sync_clientes(sync_id):
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()
    c.execute("DELETE FROM sincronizacoes_clientes WHERE id=?", (sync_id,))
    conn.commit()
    conn.close()

    definir_feedback_clientes("sucesso", "Sincronização removida.")
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
            modelo=request.form.get("modelo", ""),
            cor=request.form.get("cor", ""),
        )
        definir_feedback_index(
            "sucesso",
            f"Cadastro da placa {resultado['placa']} salvo com sucesso."
        )
        placa = resultado["placa"]
    except Exception as e:
        print("ERRO CADASTRO:", e)
        definir_feedback_index("erro", f"Erro ao salvar a placa {placa}: {e}")

    return redirect(f"/?placa={placa}")

@app.route("/editar_cliente", methods=["POST"])
def editar_cliente():
    if not session.get("usuario"):
        return redirect("/login")

    placa_original = (request.form.get("placa_original") or request.form.get("placa") or "").strip()
    placa = (request.form.get("placa") or placa_original).strip()
    redirect_to = (request.form.get("redirect_to") or "").strip()

    if not redirect_to:
        redirect_to = f"/?placa={placa.upper()}"

    try:
        resultado = salvar_cliente_veiculo(
            placa=placa,
            nome=request.form.get("nome", ""),
            telefone=request.form.get("telefone", ""),
            modelo=request.form.get("modelo", ""),
            cor=request.form.get("cor", ""),
            placa_original=placa_original,
        )
        mensagem = f"Cadastro da placa {resultado['placa']} salvo com sucesso."

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

    data = request.form
    usuario_info = resumo_usuario_logado()

    from datetime import datetime
    agora = datetime.now(ZoneInfo("America/Sao_Paulo")).isoformat()

    conn = conectar()
    c = conn.cursor()

    # 🔥 BUSCAR VEÍCULO PELA PLACA
    placa = data["placa"].upper()

    c.execute("SELECT id FROM veiculos WHERE placa=?", (placa,))
    veiculo = c.fetchone()

    if not veiculo:
        conn.close()
        return "Erro: veículo não encontrado"

    veiculo_id = veiculo["id"]

    # 🔥 BUSCAR TIPO DE SERVIÇO
    tipo_nome = data["tipo"]

    c.execute("SELECT id, valor FROM tipos_servico WHERE nome=?", (tipo_nome,))
    tipo = c.fetchone()

    if not tipo:
        conn.close()
        return "Erro: tipo não encontrado"

    tipo_id = tipo["id"]
    valor = tipo["valor"]

    # 🔥 PRIORIDADE
    c.execute("""
        SELECT MAX(prioridade) FROM servicos 
        WHERE status='EM ANDAMENTO'
    """)

    resultado = c.fetchone()[0]

    if resultado is None:
        nova_prioridade = 0
    else:
        nova_prioridade = resultado + 1

    # 🔥 INSERIR SERVIÇO (NOVO MODELO)
    c.execute("""
        INSERT INTO servicos 
        (
            veiculo_id, tipo_id, valor, entrada, status, prioridade,
            observacoes, origem, guarita, pneu, cera, hidro_lataria, hidro_vidros,
            criado_por_usuario, criado_por_nome
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        veiculo_id,
        tipo_id,
        valor,
        agora,
        "EM ANDAMENTO",
        nova_prioridade,
        normalizar_texto_campo(data.get("observacoes")),
        normalizar_texto_campo(data.get("origem")),
        normalizar_texto_campo(data.get("guarita")),
        normalizar_texto_campo(data.get("pneu")),
        normalizar_flag_sim_nao(data.get("cera")),
        normalizar_flag_sim_nao(data.get("hidro_lataria")),
        normalizar_flag_sim_nao(data.get("hidro_vidros")),
        normalizar_texto_campo(usuario_info.get("usuario")),
        normalizar_texto_campo(usuario_info.get("nome")),
    ))

    servico_id = c.lastrowid

    # 📸 FOTOS
    fotos_entrada = request.files.getlist("foto_entrada")
    fotos_detalhe = request.files.getlist("foto_detalhe")
    entrada_salvas = salvar_fotos_servico(c, servico_id, fotos_entrada, "entrada")
    detalhe_salvas = salvar_fotos_servico(c, servico_id, fotos_detalhe, "detalhe")

    conn.commit()
    conn.close()

    registrar_auditoria(
        "iniciou_atendimento",
        "servico",
        entidade_id=servico_id,
        placa=placa,
        detalhes={
            "tipo_servico": tipo_nome,
            "valor": valor,
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

    c.execute("""
        SELECT servicos.id, veiculos.placa
        FROM servicos
        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
        WHERE servicos.id=?
    """, (id,))
    servico_db = c.fetchone()

    if not servico_db:
        conn.close()
        definir_feedback_painel("erro", "Atendimento nao encontrado.")
        return redirect("/painel")

    atualizar_campos_operacionais_servico(c, id, request.form, usuario_info=usuario_info)
    fotos_detalhe = request.files.getlist("foto_detalhe")
    detalhes_salvos = salvar_fotos_servico(c, id, fotos_detalhe, "detalhe")

    acao = (request.form.get("acao") or "salvar").strip().lower()
    placa = servico_db["placa"] or "-"

    if acao == "finalizar":
        conn.commit()
        conn.close()
        registrar_auditoria(
            "abriu_checklist_finalizacao",
            "servico",
            entidade_id=id,
            placa=placa,
            detalhes={
                "fotos_detalhe_adicionadas": detalhes_salvos,
            },
            usuario=usuario_info,
        )
        mensagem = f"Checklist aberto para a placa {placa}."
        if detalhes_salvos:
            mensagem += f" {detalhes_salvos} foto(s) de detalhe salva(s)."
        definir_feedback_checklist("sucesso", mensagem)
        return redirect(f"/painel/servico/{id}/checklist")

    mensagem = f"Dados operacionais da placa {placa} salvos."

    if detalhes_salvos:
        mensagem += f" {detalhes_salvos} foto(s) de detalhe adicionada(s)."

    conn.commit()
    conn.close()

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
            "fotos_detalhe_adicionadas": detalhes_salvos,
        },
        usuario=usuario_info,
    )

    definir_feedback_painel("sucesso", mensagem)
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
            c.execute("DELETE FROM servico_checklist WHERE servico_id=?", (id,))

            for item in itens:
                c.execute("""
                    INSERT INTO servico_checklist (servico_id, item_id, item_nome, marcado)
                    VALUES (?, ?, ?, 1)
                """, (id, item["id"], item["nome"]))

            fotos_saida_salvas = salvar_fotos_servico(c, id, fotos_saida, "saida")
            c.execute("""
                UPDATE servicos
                SET status='FINALIZADO', entrega=?, finalizado_por_usuario=?, finalizado_por_nome=?
                WHERE id=?
            """, (
                agora_iso(),
                normalizar_texto_campo(usuario_info.get("usuario")),
                normalizar_texto_campo(usuario_info.get("nome")),
                id,
            ))
            conn.commit()
            conn.close()

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
    conn = conectar()
    c = conn.cursor()

    fotos_salvas = salvar_fotos_servico(c, id, request.files.getlist("foto_detalhe"), "detalhe")

    conn.commit()
    conn.close()
    registrar_auditoria(
        "adicionou_fotos_detalhe",
        "servico",
        entidade_id=id,
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

    # pega prioridade atual
    c.execute("SELECT prioridade FROM servicos WHERE id=?", (id,))
    atual = c.fetchone()

    if not atual:
        conn.close()
        return redirect("/painel")

    atual = atual[0]

    if acao == "up":
        c.execute("""
        SELECT id, prioridade FROM servicos
        WHERE prioridade < ? AND status='EM ANDAMENTO'
        ORDER BY prioridade DESC LIMIT 1
        """, (atual,))

    elif acao == "down":
        c.execute("""
        SELECT id, prioridade FROM servicos
        WHERE prioridade > ? AND status='EM ANDAMENTO'
        ORDER BY prioridade ASC LIMIT 1
        """, (atual,))

    else:
        conn.close()
        return redirect("/painel")

    outro = c.fetchone()

    # se existir outro, troca posição
    if outro:
        outro_id, outro_prio = outro

        c.execute("UPDATE servicos SET prioridade=? WHERE id=?", (outro_prio, id))
        c.execute("UPDATE servicos SET prioridade=? WHERE id=?", (atual, outro_id))

        conn.commit()

    conn.close()
    return redirect("/painel")


@app.route("/cadastrar_servico", methods=["GET", "POST"])
def cadastrar_servico():
    conn = conectar()
    c = conn.cursor()

    if request.method == "POST":
        nome = request.form["nome"]
        valor = request.form["valor"]

        c.execute("INSERT INTO tipos_servico (nome, valor) VALUES (?, ?)", (nome, valor))
        conn.commit()

    c.execute("SELECT * FROM tipos_servico")
    servicos_lista = c.fetchall()

    conn.close()

    return render_template("cadastro_servico.html", servicos=servicos_lista)

@app.route("/pneu", methods=["GET", "POST"])
def cadastrar_pneu():
    conn = conectar()
    c = conn.cursor()

    if request.method == "POST":
        nome = request.form["nome"]
        c.execute("INSERT INTO produtos_pneu (nome) VALUES (?)", (nome,))
        conn.commit()

    # 🔥 LISTAR (ANTES ESTAVA FALTANDO)
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

    registros = listar_registros_clientes()
    resumo = montar_resumo_base_dados(registros)

    return render_template(
        "base_dados.html",
        registros=registros,
        resumo=resumo,
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
        mensagem = f"Upload '{filename}' importado com sucesso. {resumir_importacao_clientes(estatisticas)}"
        definir_feedback_base_dados("sucesso", mensagem)
        salvar_notificacao(mensagem, "sucesso")
    except Exception as e:
        definir_feedback_base_dados("erro", f"Erro ao importar planilha: {e}")

    return redirect("/base_dados")

@app.route("/api/base_dados")
def api_base_dados():
    if not session.get("usuario"):
        return jsonify({"erro": "nao autorizado"}), 401

    registros = listar_registros_clientes()
    resumo = montar_resumo_base_dados(registros)
    return jsonify({"dados": registros, "resumo": resumo})

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
        registros = listar_registros_clientes()
        resumo = montar_resumo_base_dados(registros)
        return jsonify({
            "status": "ok",
            "mensagem": mensagem,
            "dados": registros,
            "resumo": resumo,
        })
    except Exception as e:
        return jsonify({
            "status": "erro",
            "mensagem": str(e),
        }), 400


def listar_servicos_em_andamento_voz():
    conn = conectar()
    c = conn.cursor()
    c.execute(
        """
        SELECT
            servicos.id,
            servicos.entrada,
            tipos_servico.nome AS tipo_nome,
            veiculos.placa,
            veiculos.modelo,
            veiculos.cor,
            clientes.nome AS cliente_nome
        FROM servicos
        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
        WHERE servicos.status='EM ANDAMENTO'
        ORDER BY servicos.id DESC
        """
    )
    servicos_db = c.fetchall()
    conn.close()

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

        servicos.append(
            {
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
            }
        )

    return servicos


@app.route("/api/operacional/voz")
def api_operacional_voz():
    if not session.get("usuario"):
        return jsonify({"status": "erro", "mensagem": "nao autorizado"}), 401

    return jsonify(
        {
            "status": "ok",
            "gerado_em": agora_iso(),
            "servicos": listar_servicos_em_andamento_voz(),
        }
    )



@app.route("/painel")
def painel():
    if not session.get("usuario"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    c.execute("""
        SELECT
            servicos.*,
            tipos_servico.nome AS tipo_nome,
            veiculos.placa,
            veiculos.modelo,
            veiculos.cor,
            clientes.nome AS cliente_nome,
            clientes.telefone AS cliente_telefone
        FROM servicos
        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
        LEFT JOIN veiculos ON servicos.veiculo_id = veiculos.id
        LEFT JOIN clientes ON veiculos.cliente_id = clientes.id
        WHERE status='EM ANDAMENTO'
        ORDER BY servicos.id DESC
    """)

    servicos_db = c.fetchall()
    c.execute("SELECT nome FROM produtos_pneu ORDER BY nome")
    produtos_pneu = [row[0] for row in c.fetchall()]

    ids_servicos = [row["id"] for row in servicos_db]
    conn.close()
    fotos_por_servico = listar_fotos_servicos(ids_servicos)

    servicos = []

    for s in servicos_db:
        s_dict = dict(s)

        # 🔥 PRIORIDADE IA
        prioridade_ia = calcular_prioridade_inteligente(s_dict)
        s_dict["prioridade_ia"] = prioridade_ia

        # 🔥 TEMPO DE ESPERA
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
        s_dict["galeria_fotos"] = fotos_por_servico.get(s_dict["id"], {})
        s_dict["fotos_entrada"] = len(s_dict["galeria_fotos"].get("entrada", []))
        s_dict["fotos_detalhe"] = len(s_dict["galeria_fotos"].get("detalhe", []))
        s_dict["fotos_saida"] = len(s_dict["galeria_fotos"].get("saida", []))
        s_dict["tem_fotos"] = bool(s_dict["fotos_entrada"] or s_dict["fotos_detalhe"] or s_dict["fotos_saida"])
        enriquecer_responsaveis_servico(s_dict)

        servicos.append(s_dict)

    # 🔥 ORDENA PELA IA
    servicos.sort(key=lambda x: x["prioridade_ia"], reverse=True)

    return render_template(
        "painel.html",
        servicos=servicos,
        produtos_pneu=produtos_pneu,
        feedback=session.pop("painel_feedback", None),
    )


@app.route("/clientes", methods=["GET", "POST"])
def clientes():
    if not session.get("usuario"):
        return redirect("/login")

    limpar = bool(request.args.get("limpar"))
    busca = (request.form.get("busca") or request.args.get("busca") or "").strip()

    return renderizar_pagina_clientes(busca=busca, limpar=limpar)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=5000, debug=False)

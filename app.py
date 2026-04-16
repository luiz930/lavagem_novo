from flask import Flask, render_template, request, redirect, session, jsonify
import csv
import sqlite3
from zoneinfo import ZoneInfo
import os
import time
import hashlib
from threading import Thread
from io import BytesIO
from threading import Lock, Thread
import unicodedata
from urllib.parse import parse_qs, quote, urlencode, urlparse, urlunparse
import bcrypt  # 👈 se já adicionou
import pandas as pd
import requests
from werkzeug.utils import secure_filename
UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

from datetime import datetime, date, time as dt_time

def ler_planilha_sheety(url):
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

    except Exception as e:
        raise Exception(f"Erro ao ler Sheety: {e}")

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

# 📁 CONFIG UPLOAD
UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# 🔐 SEGURANÇA UPLOAD
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg"}

def arquivo_permitido(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

app = Flask(__name__)
app.secret_key = "wagen_super_segura_123"

APP_VERSION = "Versão: 0.1.7-alpha"

CAMPOS_SINCRONIZACAO_CLIENTES = [
    {"key": "placa", "label": "Placa", "required": True},
    {"key": "nome", "label": "Nome", "required": False},
    {"key": "telefone", "label": "Telefone", "required": False},
    {"key": "modelo", "label": "Modelo", "required": False},
    {"key": "cor", "label": "Cor", "required": False},
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

MAPA_LABEL_CAMPOS_SYNC = {
    item["key"]: item["label"] for item in CAMPOS_SINCRONIZACAO_CLIENTES
}

ALIASES_CAMPOS_SYNC = {
    "placa": ["placa"],
    "nome": ["nome", "cliente"],
    "telefone": ["telefone", "fone", "celular", "whatsapp", "contato"],
    "modelo": ["modelo", "carro", "veiculo", "veículo"],
    "cor": ["cor"],
}

sync_lock = Lock()
sync_worker_iniciado = False

def conectar():
    conn = sqlite3.connect("database_v2.db")
    conn.row_factory = sqlite3.Row  # 🔥 ESSENCIAL
    return conn

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

    try:
        c.execute("ALTER TABLE sincronizacoes_clientes ADD COLUMN ultimo_hash TEXT")
    except:
        pass

    try:
        c.execute("ALTER TABLE sincronizacoes_clientes ADD COLUMN colunas_ultima_sync TEXT")
    except:
        pass

    conn.commit()
    conn.close()

def init_db():
    conn = conectar()
    c = conn.cursor()
    atualizar_banco()

def criar_todas_tabelas():
    conn = sqlite3.connect("database_v2.db")
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
        ativo INTEGER NOT NULL DEFAULT 1,
        ultimo_sync_em TEXT,
        proximo_sync_em TEXT,
        ultimo_status TEXT,
        ultima_mensagem TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()

    # 🔐 USUÁRIOS
    c.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        usuario TEXT UNIQUE,
        senha TEXT
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
        ativo INTEGER NOT NULL DEFAULT 1,
        ultimo_sync_em TEXT,
        proximo_sync_em TEXT,
        ultimo_status TEXT,
        ultima_mensagem TEXT,
        criado_em TEXT DEFAULT CURRENT_TIMESTAMP,
        atualizado_em TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()

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

    # ⚡ ÍNDICES (performance)
    c.execute("CREATE INDEX IF NOT EXISTS idx_servico_status ON servicos(status)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_servico_entrada ON servicos(entrada)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_veiculo_placa ON veiculos(placa)")
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

def limpar_preview_sincronizacao():
    session.pop("clientes_sync_preview", None)

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

def normalizar_link_planilha(url):
    url = (url or "").strip()

    if "docs.google.com/spreadsheets" not in url or "export?format=" in url:
        return url

    if "/d/" not in url:
        return url

    sheet_id = url.split("/d/")[1].split("/")[0]
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    fragment = parse_qs(parsed.fragment)
    gid = query.get("gid", fragment.get("gid", ["0"]))[0]

    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def corrigir_link_google_sheets(url):
    try:
        # ❌ BLOQUEIA LINK ERRADO (googleusercontent)
        if "googleusercontent.com" in url:
            raise Exception("Link inválido. Use o link original do Google Sheets (docs.google.com).")

        # ✅ TRATA LINK DO GOOGLE SHEETS
        if "docs.google.com" in url and "/spreadsheets/d/" in url:

            # extrair ID da planilha
            partes = url.split("/d/")
            if len(partes) < 2:
                raise Exception("Não foi possível identificar o ID da planilha.")

            resto = partes[1]
            sheet_id = resto.split("/")[0]

            # pegar gid (aba)
            gid = "0"
            if "gid=" in url:
                gid = url.split("gid=")[-1].split("&")[0]

            # montar link final LIMPO
            novo_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

            return novo_url

        # ❌ NÃO É GOOGLE SHEETS
        raise Exception("Link não suportado. Use uma planilha do Google Sheets.")

    except Exception as e:
        raise Exception(f"Erro ao processar link: {e}")

    except Exception:
        return url

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

def ler_dataframe_link_planilha(url):

    # 🔥 DETECTAR SHEETY
    if "sheety.co" in url:
        df = ler_planilha_sheety(url)
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

        nome = limpar_valor_planilha(row.get(mapeamento.get("nome", ""), "")) if mapeamento.get("nome") else ""
        telefone = limpar_valor_planilha(row.get(mapeamento.get("telefone", ""), "")) if mapeamento.get("telefone") else ""
        modelo = limpar_valor_planilha(row.get(mapeamento.get("modelo", ""), "")) if mapeamento.get("modelo") else ""
        cor = limpar_valor_planilha(row.get(mapeamento.get("cor", ""), "")) if mapeamento.get("cor") else ""

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

            if novo_nome != (cliente_existente["nome"] or "") or novo_telefone != (cliente_existente["telefone"] or ""):
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
    return (
        f"{estatisticas['linhas_processadas']} linha(s) processada(s), "
        f"{estatisticas['veiculos_novos']} veículo(s) novo(s), "
        f"{estatisticas['veiculos_atualizados']} veículo(s) atualizado(s), "
        f"{estatisticas['clientes_novos']} cliente(s) novo(s), "
        f"{estatisticas['clientes_atualizados']} cliente(s) atualizado(s)"
    )

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
            df = ler_planilha_sheety(url_base)

            print("🔥 DADOS SHEETY:")
            print(df.tail(5))

            # força atualização sempre
            hash_atual = str(time.time())
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

        # 🔥 SE NÃO FOR SHEETY, VERIFICA HASH
        if "sheety.co" not in sync["url"]:
            if sync["ultimo_hash"] == hash_atual:
                return True, "Sem alterações na planilha."

        # 🔽 MAPEAMENTO
        mapeamento = {
            campo["key"]: sync[f"campo_{campo['key']}"] or ""
            for campo in CAMPOS_SINCRONIZACAO_CLIENTES
        }

        # 🔽 IMPORTAÇÃO
        estatisticas = importar_clientes_dataframe(df, mapeamento)
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
        mensagem = f"Erro ao sincronizar: {e}"

        conn = conectar()
        c = conn.cursor()

        c.execute("""
            UPDATE sincronizacoes_clientes
            SET ultimo_status=?,
                ultima_mensagem=?,
                proximo_sync_em=?,
                atualizado_em=?
            WHERE id=?
        """, (
            "ERRO",
            mensagem,
            somar_minutos_iso(sync["intervalo_minutos"]),
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

Thread(target=loop_importacao, daemon=True).start()

def carregar_contexto_clientes(busca="", limpar=False):
    conn = conectar()
    c = conn.cursor()

    if limpar:
        clientes = []
    else:
        if busca:
            c.execute("""
                SELECT
                    veiculos.id,
                    veiculos.placa,
                    veiculos.modelo,
                    veiculos.cor,
                    veiculos.cliente_id,
                    clientes.nome AS cliente_nome,
                    clientes.telefone AS cliente_telefone
                FROM veiculos
                LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
                WHERE veiculos.placa LIKE ? OR veiculos.modelo LIKE ? OR clientes.nome LIKE ?
                ORDER BY veiculos.id DESC
            """, (f"%{busca}%", f"%{busca}%", f"%{busca}%"))
        else:
            c.execute("""
                SELECT
                    veiculos.id,
                    veiculos.placa,
                    veiculos.modelo,
                    veiculos.cor,
                    veiculos.cliente_id,
                    clientes.nome AS cliente_nome,
                    clientes.telefone AS cliente_telefone
                FROM veiculos
                LEFT JOIN clientes ON clientes.id = veiculos.cliente_id
                ORDER BY veiculos.id DESC
            """)

        clientes = c.fetchall()

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
        item["ultimo_sync_em_fmt"] = formatar_datahora(item["ultimo_sync_em"])
        item["proximo_sync_em_fmt"] = formatar_datahora(item["proximo_sync_em"])
        item["campos"] = descrever_campos_sincronizados(item)
        sincronizacoes.append(item)

    return clientes, sincronizacoes

@app.before_request
def preparar_sincronizacoes():
    if request.endpoint == "static":
        return

    iniciar_worker_sincronizacao()

    if session.get("logado"):
        sincronizar_fontes_pendentes()

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
    if not session.get("logado"):
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
    conn = conectar()
    c = conn.cursor()

    c.execute("UPDATE notificacoes SET lida=1 WHERE id=?", (id,))

    conn.commit()
    conn.close()

    return jsonify({"status": "ok"})

@app.route("/api/hud")
def api_hud():
    if not session.get("logado"):
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

    return {
        "total": round(total, 2),
        "andamento": andamento,
        "atrasados": atrasados,
        "ticket": round(ticket, 2)
    }

@app.route("/status_sync")
def status_sync():
    if not session.get("logado"):
        return jsonify({"status": "erro"})

    conn = conectar()
    c = conn.cursor()

    c.execute("""
        SELECT ultima_mensagem, ultimo_status
        FROM sincronizacoes_clientes
        ORDER BY id DESC
        LIMIT 1
    """)

    row = c.fetchone()
    conn.close()

    if not row:
        return jsonify({"status": "vazio"})

    return jsonify({
        "status": row[1],
        "mensagem": row[0]
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

    senha_hash = bcrypt.hashpw("admin123".encode(), bcrypt.gensalt()).decode()

    try:
        c.execute("INSERT INTO usuarios (usuario, senha) VALUES (?, ?)", ("admin", senha_hash))
        conn.commit()
        print("✅ Admin criado: admin / admin123")
    except:
        pass

    conn.close()

criar_admin()

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario")
        senha = request.form.get("senha")

        conn = conectar()
        c = conn.cursor()

        c.execute("SELECT * FROM usuarios WHERE usuario=?", (usuario,))
        user = c.fetchone()

        conn.close()

        if user and bcrypt.checkpw(senha.encode(), user["senha"].encode()):
            session["logado"] = True
            return redirect("/")

        return render_template("login.html", erro="Login inválido")

    return render_template("login.html")

@app.route("/clima")
def clima():
    if not session.get("logado"):
        return redirect("/login")

    return render_template("clima.html")

@app.route("/financeiro")
def financeiro():
    if not session.get("logado"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    from datetime import datetime
    hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y")

    # 💰 TOTAL HOJE
    c.execute("""
    SELECT SUM(valor) FROM servicos 
    WHERE status='FINALIZADO' AND entrega LIKE ?
    """, (hoje + "%",))

    total = c.fetchone()[0]
    if total is None:
        total = 0

    # 📦 QUANTIDADE
    c.execute("""
    SELECT COUNT(*) FROM servicos 
    WHERE status='FINALIZADO' AND entrega LIKE ?
    """, (hoje + "%",))

    quantidade = c.fetchone()[0]

    # 💵 TICKET MÉDIO
    if quantidade > 0:
        ticket = total / quantidade
    else:
        ticket = 0

    conn.close()

    return render_template(
        "financeiro.html",
        total=round(total, 2),
        quantidade=quantidade,
        ticket=round(ticket, 2)
    )


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")

@app.route("/", methods=["GET", "POST"])
def index():
    if not session.get("logado"):
        return redirect("/login")

    dados = None
    historico = []
    buscou = False

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
                    SELECT * FROM servicos 
                    WHERE veiculo_id=? 
                    ORDER BY id DESC
                """, (veiculo_id,))

                historico_db = c.fetchall()

                # 🔥 FORMATAR HISTÓRICO
                from datetime import datetime
                historico_formatado = []

                for s in historico_db:
                    try:
                        entrada = datetime.fromisoformat(s["entrada"])

                        if s["entrega"]:
                            entrega = datetime.fromisoformat(s["entrega"])
                            tempo = entrega - entrada
                            tempo_str = str(tempo)
                        else:
                            tempo_str = "Em andamento"

                    except:
                        tempo_str = "N/A"

                    historico_formatado.append((s, tempo_str))

                historico = historico_formatado

    conn.close()

    return render_template(
        "index.html",
        dados=dados,
        historico=historico,
        buscou=buscou,
        placa=placa,
        version=APP_VERSION,
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
    if not session.get("logado"):
        return redirect("/login")

    nome = (request.form.get("nome") or "").strip()
    url = (request.form.get("url") or "").strip()
    intervalo_minutos = normalizar_intervalo_sincronizacao(request.form.get("intervalo_minutos"))

    if not url:
        definir_feedback_clientes("erro", "Informe um link de planilha para continuar.")
        return redirect("/clientes")

    try:
        url = corrigir_link_google_sheets(url)
        df, url_normalizada = ler_dataframe_link_planilha(url)
        colunas = list(df.columns)
        mapeamento_sugerido = sugerir_mapeamento_colunas(colunas)

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
            "amostra": amostra_tratada,  # ✅ agora seguro
            "total_linhas": len(df.index),
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
    if not session.get("logado"):
        return redirect("/login")

    limpar_preview_sincronizacao()
    definir_feedback_clientes("sucesso", "Pré-visualização cancelada.")
    return redirect("/clientes")

@app.route("/clientes/sincronizacao/adicionar", methods=["POST"])
def adicionar_sincronizacao_clientes():
    if not session.get("logado"):
        return redirect("/login")

    nome = (request.form.get("nome") or "").strip()
    url = (request.form.get("url") or "").strip()
    intervalo_minutos = normalizar_intervalo_sincronizacao(request.form.get("intervalo_minutos"))

    if not url:
        definir_feedback_clientes("erro", "Informe um link de planilha.")
        return redirect("/clientes")

    try:
        # 🔥 1. LER PLANILHA
        df, url_normalizada = ler_dataframe_link_planilha(url)

        # 🔥 2. MAPEAR COLUNAS AUTOMATICAMENTE
        colunas = list(df.columns)
        mapeamento = sugerir_mapeamento_colunas(colunas)

        if not mapeamento.get("placa"):
            raise Exception("Não consegui identificar a coluna de PLACA automaticamente.")

        # 🔥 3. IMPORTAR DIRETO PRO BANCO
        estatisticas = importar_clientes_dataframe(df, mapeamento)

        # 🔥 4. SALVAR CONFIG DE SINCRONIZAÇÃO
        conn = conectar()
        c = conn.cursor()

        agora_atual = agora_iso()

        c.execute("""
        INSERT INTO sincronizacoes_clientes (
            nome, url, intervalo_minutos,
            campo_placa, campo_nome, campo_telefone, campo_modelo, campo_cor,
            ativo, ultimo_status, proximo_sync_em, criado_em, atualizado_em
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            nome or "Planilha automática",
            url_normalizada,
            intervalo_minutos,
            mapeamento.get("placa"),
            mapeamento.get("nome"),
            mapeamento.get("telefone"),
            mapeamento.get("modelo"),
            mapeamento.get("cor"),
            1,
            "OK",
            somar_minutos_iso(intervalo_minutos),
            agora_atual,
            agora_atual,
        ))

        conn.commit()
        conn.close()

        definir_feedback_clientes(
            "sucesso",
            f"Importação concluída: {resumir_importacao_clientes(estatisticas)}"
        )

    except Exception as e:
        definir_feedback_clientes("erro", f"Erro ao importar: {e}")

    return redirect("/clientes")

@app.route("/clientes/sincronizacao/<int:sync_id>/executar", methods=["POST"])
def executar_sync_clientes(sync_id):
    if not session.get("logado"):
        return redirect("/login")

    sucesso, mensagem = executar_sincronizacao_cliente(sync_id)
    definir_feedback_clientes("sucesso" if sucesso else "erro", mensagem)
    return redirect("/clientes")

@app.route("/clientes/sincronizacao/<int:sync_id>/alternar", methods=["POST"])
def alternar_sync_clientes(sync_id):
    if not session.get("logado"):
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
    if not session.get("logado"):
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
    if not session.get("logado"):
        return redirect("/login")

    data = request.form

    placa = data["placa"].upper()
    nome = data.get("nome", "")
    telefone = data.get("telefone", "")
    modelo = data.get("modelo", "")
    cor = data.get("cor", "")

    conn = conectar()
    c = conn.cursor()

    try:
        # 🔥 1. CLIENTE
        cliente_id = None

        if telefone:
            c.execute("SELECT id FROM clientes WHERE telefone=?", (telefone,))
            cliente = c.fetchone()

            if cliente:
                cliente_id = cliente["id"]

                c.execute("""
                    UPDATE clientes 
                    SET nome=? 
                    WHERE id=?
                """, (nome, cliente_id))
            else:
                c.execute("""
                    INSERT INTO clientes (nome, telefone)
                    VALUES (?, ?)
                """, (nome, telefone))

                cliente_id = c.lastrowid

        # 🔥 2. VEÍCULO
        c.execute("SELECT id FROM veiculos WHERE placa=?", (placa,))
        veiculo = c.fetchone()

        if veiculo:
            c.execute("""
                UPDATE veiculos 
                SET modelo=?, cor=?, cliente_id=? 
                WHERE placa=?
            """, (modelo, cor, cliente_id, placa))
        else:
            c.execute("""
                INSERT INTO veiculos (placa, modelo, cor, cliente_id)
                VALUES (?, ?, ?, ?)
            """, (placa, modelo, cor, cliente_id))

        conn.commit()

    except Exception as e:
        print("ERRO CADASTRO:", e)
    finally:
        conn.close()

    return redirect(f"/?placa={placa}")

@app.route("/servico", methods=["POST"])
def servico():
    if not session.get("logado"):
        return redirect("/login")

    data = request.form

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
        (veiculo_id, tipo_id, valor, entrada, status, prioridade, observacoes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        veiculo_id,
        tipo_id,
        valor,
        agora,
        "EM ANDAMENTO",
        nova_prioridade,
        data.get("observacoes", "")
    ))

    servico_id = c.lastrowid

    # 📸 FOTOS
    fotos_entrada = request.files.getlist("foto_entrada")
    fotos_detalhe = request.files.getlist("foto_detalhe")

    import time
    from werkzeug.utils import secure_filename

    for foto in fotos_entrada:
        if foto and arquivo_permitido(foto.filename):
            nome = str(int(time.time())) + "_" + secure_filename(foto.filename)
            caminho = os.path.join(UPLOAD_FOLDER, nome)
            foto.save(caminho)

            c.execute(
                "INSERT INTO fotos (servico_id, tipo, caminho) VALUES (?, ?, ?)",
                (servico_id, "entrada", caminho)
            )

    for foto in fotos_detalhe:
        if foto and arquivo_permitido(foto.filename):
            nome = str(int(time.time())) + "_" + secure_filename(foto.filename)
            caminho = os.path.join(UPLOAD_FOLDER, nome)
            foto.save(caminho)

            c.execute(
                "INSERT INTO fotos (servico_id, tipo, caminho) VALUES (?, ?, ?)",
                (servico_id, "detalhe", caminho)
            )

    conn.commit()
    conn.close()

    return redirect("/")

@app.route("/finalizar/<int:id>", methods=["POST"])
def finalizar(id):
    if not session.get("logado"):
        return redirect("/login")
    from datetime import datetime
    agora = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M")

    conn = conectar()
    c = conn.cursor()

    # 📸 PEGAR FOTOS DE SAÍDA
    foto = request.files.get("foto_saida")

    if foto and foto.filename != "":
        nome = secure_filename(foto.filename)

        import time
        nome = str(int(time.time())) + "_" + nome

        caminho = os.path.join(UPLOAD_FOLDER, nome)
        foto.save(caminho)

        c.execute(
            "INSERT INTO fotos (servico_id, tipo, caminho) VALUES (?, ?, ?)",
            (id, "saida", caminho)
        )

    # 🔥 FINALIZA O SERVIÇO
    c.execute("""
    UPDATE servicos 
    SET status='FINALIZADO', entrega=? 
    WHERE id=?
    """, (agora, id))

    conn.commit()
    conn.close()
    print(request.files)

    return redirect("/painel")

@app.route("/detalhe/<int:id>", methods=["POST"])
def detalhe(id):
    if not session.get("logado"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    fotos = request.files.getlist("foto_detalhe")

    for foto in fotos:
        if foto and foto.filename != "":
            import time
            nome = str(int(time.time())) + "_" + secure_filename(foto.filename)

            caminho = os.path.join(UPLOAD_FOLDER, nome)
            foto.save(caminho)

            c.execute(
                "INSERT INTO fotos (servico_id, tipo, caminho) VALUES (?, ?, ?)",
                (id, "detalhe", caminho)
            )

    conn.commit()
    conn.close()

    return redirect("/painel")

@app.route("/prioridade/<int:id>/<acao>")
def prioridade(id, acao):
    if not session.get("logado"):
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

@app.route("/painel")
def painel():
    if not session.get("logado"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    c.execute("""
        SELECT servicos.*, tipos_servico.nome as tipo_nome 
        FROM servicos
        LEFT JOIN tipos_servico ON servicos.tipo_id = tipos_servico.id
        WHERE status='EM ANDAMENTO'
        ORDER BY id DESC
    """)

    servicos_db = c.fetchall()
    conn.close()

    servicos = []

    for s in servicos_db:
        s_dict = dict(s)

        # 🔥 PRIORIDADE IA
        prioridade_ia = calcular_prioridade_inteligente(s_dict)
        s_dict["prioridade_ia"] = prioridade_ia

        # 🔥 TEMPO DE ESPERA
        try:
            entrada = datetime.fromisoformat(s_dict["entrada"])
            agora = datetime.now(ZoneInfo("America/Sao_Paulo"))

            diff = agora - entrada
            minutos = int(diff.total_seconds() / 60)

            horas = minutos // 60
            mins = minutos % 60

            if horas > 0:
                tempo_str = f"{horas}h {mins}min"
            else:
                tempo_str = f"{mins}min"

        except:
            tempo_str = "N/A"

        s_dict["tempo_espera"] = tempo_str

        servicos.append(s_dict)

    # 🔥 ORDENA PELA IA
    servicos.sort(key=lambda x: x["prioridade_ia"], reverse=True)

    return render_template("painel.html", servicos=servicos)


@app.route("/clientes", methods=["GET", "POST"])
def clientes():
    if not session.get("logado"):
        return redirect("/login")

    limpar = bool(request.args.get("limpar"))
    busca = (request.form.get("busca") or request.args.get("busca") or "").strip()

    return renderizar_pagina_clientes(busca=busca, limpar=limpar)

# 🔥 inicia thread
Thread(target=loop_importacao, daemon=True).start()

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)

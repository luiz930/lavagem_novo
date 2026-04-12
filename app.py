from flask import Flask, render_template, request, redirect, session, request, jsonify
import sqlite3
from zoneinfo import ZoneInfo
import os
import bcrypt  # 👈 se já adicionou
from werkzeug.utils import secure_filename
UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

from datetime import datetime
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

APP_VERSION = "Versão: 0.1.0-alpha"

def conectar():
    conn = sqlite3.connect("database_v2.db")
    conn.row_factory = sqlite3.Row  # 🔥 ESSENCIAL
    return conn

def init_db():
    conn = conectar()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usuario TEXT UNIQUE,
    senha TEXT
    )
    """)

    # 🔥 CLIENTES
    c.execute("""
    CREATE TABLE IF NOT EXISTS clientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nome TEXT NOT NULL,
        telefone TEXT
    )
    """)

    # 🔥 VEÍCULOS
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

    conn.commit()
    conn.close()


init_db()

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

@app.route("/importar_clientes", methods=["POST"])
def importar_clientes():
    if not session.get("logado"):
        return redirect("/login")

    arquivo = request.files.get("arquivo")

    if not arquivo:
        return "Nenhum arquivo enviado"

    import pandas as pd

    try:
        if arquivo.filename.endswith(".csv"):
            df = pd.read_csv(arquivo)
        else:
            df = pd.read_excel(arquivo)
    except Exception as e:
        return f"Erro ao ler arquivo: {e}"

    # 🔥 normaliza colunas
    df.columns = df.columns.str.lower()

    conn = conectar()
    c = conn.cursor()

    for _, row in df.iterrows():
        try:
            placa = str(row.get("placa", "")).upper().strip()
            nome = str(row.get("nome", "")).strip()
            telefone = str(row.get("telefone", "")).strip()
            modelo = str(row.get("modelo", "")).strip()
            cor = str(row.get("cor", "")).strip()

            if not placa or placa == "nan":
                continue

            # 🔥 1. CRIA OU ATUALIZA CLIENTE
            cliente_id = None

            if telefone:
                c.execute("SELECT id FROM clientes WHERE telefone=?", (telefone,))
                cliente = c.fetchone()

                if cliente:
                    cliente_id = cliente["id"]

                    # atualiza nome se mudou
                    c.execute("""
                        UPDATE clientes SET nome=? WHERE id=?
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

        except Exception as e:
            print("Erro linha:", e)

    conn.commit()
    conn.close()

    return redirect("/clientes?importado=1")

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


@app.route("/preview_importacao", methods=["POST"])
def preview_importacao():
    import pandas as pd

    arquivo = request.files.get("arquivo")

    if not arquivo:
        return "Nenhum arquivo enviado"

    if arquivo.filename.endswith(".csv"):
        df = pd.read_csv(arquivo)
    else:
        df = pd.read_excel(arquivo)

    df.columns = df.columns.str.lower()

    # salva temporário na sessão
    session["preview_dados"] = df.to_dict(orient="records")
    session["preview_colunas"] = list(df.columns)

    return render_template(
        "preview_importacao.html",
        colunas=df.columns,
        dados=df.head(10).to_dict(orient="records")  # só preview
    )

@app.route("/confirmar_importacao", methods=["POST"])
def confirmar_importacao():
    dados = session.get("preview_dados")

    placa_col = request.form.get("placa")
    nome_col = request.form.get("nome")
    telefone_col = request.form.get("telefone")
    modelo_col = request.form.get("modelo")
    cor_col = request.form.get("cor")

    conn = conectar()
    c = conn.cursor()

    for row in dados:
        placa = str(row.get(placa_col, "")).upper().strip()

        if not placa:
            continue

        nome = row.get(nome_col, "")
        telefone = row.get(telefone_col, "")
        modelo = row.get(modelo_col, "")
        cor = row.get(cor_col, "")

        # cliente
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

        # 🔥 verifica se veiculo existe
        c.execute("SELECT id FROM veiculos WHERE placa=?", (placa,))
        existe = c.fetchone()

        if existe:
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
    conn.close()

    return redirect("/clientes")

@app.route("/clientes", methods=["GET", "POST"])
def clientes():
    if not session.get("logado"):
        return redirect("/login")

    limpar = request.args.get("limpar")

    importado = request.args.get("importado")

    conn = conectar()
    c = conn.cursor()

    busca = request.form.get("busca", "")

    busca = request.form.get("busca", "")

    if limpar:
        clientes = []
    else:
        if busca:
            c.execute("""
                SELECT * FROM veiculos 
                WHERE placa LIKE ? OR modelo LIKE ?
            """, (f"%{busca}%", f"%{busca}%"))
        else:
            c.execute("SELECT * FROM veiculos ORDER BY id DESC")

        clientes = c.fetchall()

    conn.close()

    return render_template("clientes.html", clientes=clientes)

@app.route("/editar_cliente", methods=["POST"])
def editar_cliente():
    if not session.get("logado"):
        return redirect("/login")

    data = request.form

    conn = conectar()
    c = conn.cursor()

    placa = data["placa"].upper()
    modelo = data["modelo"]
    cor = data["cor"]
    telefone = data["telefone"]
    nome = data.get("nome", "")

    # 🔥 BUSCAR VEICULO
    c.execute("SELECT cliente_id FROM veiculos WHERE placa=?", (placa,))
    veiculo = c.fetchone()

    cliente_id = None

    if veiculo:
        cliente_id = veiculo[0]

    # 🔥 SE EXISTE CLIENTE → ATUALIZA
    if cliente_id:
        c.execute("""
            UPDATE clientes 
            SET nome=?, telefone=?
            WHERE id=?
        """, (nome, telefone, cliente_id))
    else:
        # 🔥 CRIA CLIENTE NOVO
        c.execute("""
            INSERT INTO clientes (nome, telefone)
            VALUES (?, ?)
        """, (nome, telefone))

        cliente_id = c.lastrowid

    # 🔥 ATUALIZA VEICULO
    c.execute("""
        UPDATE veiculos 
        SET modelo=?, cor=?, cliente_id=?
        WHERE placa=?
    """, (modelo, cor, cliente_id, placa))

    conn.commit()
    conn.close()

    return redirect(f"/?placa={placa}")

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
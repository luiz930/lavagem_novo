from flask import Flask, render_template, request, redirect, session, request, jsonify
import sqlite3
from zoneinfo import ZoneInfo
import os
from werkzeug.utils import secure_filename
UPLOAD_FOLDER = "static/uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.secret_key = "wagen_super_segura_123"

APP_VERSION = "Versão: 0.0.3-alpha"

def conectar():
    return sqlite3.connect("database_v2.db")

def init_db():
    conn = conectar()
    c = conn.cursor()

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

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario")
        senha = request.form.get("senha")

        if usuario == "wagenadmin" and senha == "wagen@2026":
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
    hoje = datetime.now().strftime("%d/%m/%Y")

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
    placa = ""

    conn = conectar()
    c = conn.cursor()

    # 🔥 SERVIÇOS
    c.execute("SELECT * FROM tipos_servico")
    servicos_lista = c.fetchall()

    # 🔥 PNEU (SEMPRE FORA DO IF)
    c.execute("SELECT * FROM produtos_pneu")
    produtos_pneu = c.fetchall()

    if request.method == "POST":
        placa = request.form.get("placa", "").upper()
        return redirect(f"/?placa={placa}")

        # CLIENTE
        c.execute("SELECT * FROM veiculos WHERE placa=?", (placa,))
        dados = c.fetchone()

        if not dados:
            dados = None

    # 🔥 buscar o id do veículo pela placa
    c.execute("SELECT id FROM veiculos WHERE placa=?", (placa,))
    veiculo = c.fetchone()

    if veiculo:
        veiculo_id = veiculo[0]

        c.execute("SELECT * FROM servicos WHERE veiculo_id=? ORDER BY id DESC", (veiculo_id,))
        historico_db = c.fetchall()
    else:
        historico_db = []

        # HISTÓRICO PREMIUM
        from datetime import datetime
        historico_formatado = []

        for s in historico_db:
            try:
                entrada = datetime.strptime(s[4], "%d/%m/%Y %H:%M")

                if s[5]:
                    entrega = datetime.strptime(s[5], "%d/%m/%Y %H:%M")
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

@app.route("/cadastrar", methods=["POST"])
def cadastrar():
    if not session.get("logado"):
        return redirect("/login")
    data = request.form

    conn = conectar()
    c = conn.cursor()

    c.execute("""
    INSERT INTO veiculos (placa, nome, telefone, modelo, cor)
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(placa) DO UPDATE SET
    nome=excluded.nome,
    telefone=excluded.telefone,
    modelo=excluded.modelo,
    cor=excluded.cor
    """, (
        data["placa"].upper(),
        data["nome"],
        data["telefone"],
        data["modelo"],
        data["cor"]
    ))

    conn.commit()
    conn.close()

    return redirect("/")

@app.route("/servico", methods=["POST"])
def servico():
    if not session.get("logado"):
        return redirect("/login")

    data = request.form

    from datetime import datetime
    agora = datetime.now().isoformat()

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
        if foto.filename != "":
            nome = str(int(time.time())) + "_" + secure_filename(foto.filename)
            caminho = os.path.join(UPLOAD_FOLDER, nome)
            foto.save(caminho)

            c.execute(
                "INSERT INTO fotos (servico_id, tipo, caminho) VALUES (?, ?, ?)",
                (servico_id, "entrada", caminho)
            )

    for foto in fotos_detalhe:
        if foto.filename != "":
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
    agora = datetime.now().strftime("%d/%m/%Y %H:%M")

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

    c.execute("SELECT * FROM servicos WHERE status='EM ANDAMENTO' ORDER BY prioridade ASC, id DESC")
    servicos = c.fetchall()

    conn.close()

    return render_template("painel.html", servicos=servicos)

@app.route("/clientes", methods=["GET", "POST"])
def clientes():
    if not session.get("logado"):
        return redirect("/login")

    conn = conectar()
    c = conn.cursor()

    busca = request.form.get("busca", "")

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

    c.execute("""
        UPDATE veiculos
        SET placa=?, modelo=?, cor=?, telefone=?
        WHERE id=?
    """, (
        data["placa"].upper(),
        data["modelo"],
        data["cor"],
        data["telefone"],
        data["id"]
    ))

    conn.commit()
    conn.close()

    return redirect("/clientes")

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
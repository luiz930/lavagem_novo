import argparse
import json
import sys
import statistics
import time
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import app as app_module


DEFAULT_ROUTES = ["/", "/painel", "/clientes", "/financeiro", "/configuracoes"]


def medir_rota(client, rota, repeticoes):
    amostras = []
    status = None
    tamanho = 0
    destino = ""
    for _ in range(repeticoes):
        inicio = time.perf_counter()
        response = client.get(rota)
        try:
            conteudo = response.get_data()
            amostras.append(int((time.perf_counter() - inicio) * 1000))
            status = int(response.status_code)
            tamanho = len(conteudo or b"")
            destino = response.headers.get("Location") or ""
        finally:
            response.close()

    return {
        "rota": rota,
        "status": status,
        "min_ms": min(amostras),
        "media_ms": int(statistics.mean(amostras)),
        "max_ms": max(amostras),
        "amostras": amostras,
        "tamanho_kb": round(tamanho / 1024, 1),
        "destino": destino,
    }


def configurar_sessao(client):
    with client.session_transaction() as sess:
        sess["usuario"] = "admin"
        sess["usuario_perfil"] = "desenvolvedor"
        sess["usuario_id"] = 1
        sess["empresa_id"] = 1
        sess["senha_alteracao_obrigatoria"] = False


def medir_rotas(rotas, repeticoes, bypass_guards=True):
    with app_module.app.test_client() as client:
        configurar_sessao(client)
        with ExitStack() as stack:
            if bypass_guards:
                stack.enter_context(patch.object(app_module, "sincronizar_sessao_usuario"))
                stack.enter_context(patch.object(app_module, "obter_contexto_licenca_empresa_cached", return_value={"bloqueada": False}))
            return [medir_rota(client, rota, repeticoes) for rota in rotas]


def main():
    parser = argparse.ArgumentParser(description="Mede tempo de resposta das telas principais pelo Flask test client.")
    parser.add_argument("--repeticoes", type=int, default=3)
    parser.add_argument("--rotas", nargs="*", default=DEFAULT_ROUTES)
    parser.add_argument("--sem-bypass", action="store_true", help="Nao ignora sincronizacao de sessao/licenca nos before_request.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--metricas-internas", action="store_true", help="Inclui metricas internas de consultas/contexto coletadas durante a medicao.")
    args = parser.parse_args()

    if args.metricas_internas:
        app_module.SQL_METRICAS_CONSULTAS.clear()

    resultados = medir_rotas(
        args.rotas,
        max(1, args.repeticoes),
        bypass_guards=not args.sem_bypass,
    )

    if args.json:
        payload = {"rotas": resultados}
        if args.metricas_internas:
            payload["metricas_internas"] = app_module.obter_metricas_consultas_sql(limite=80)
        print(json.dumps(payload if args.metricas_internas else resultados, ensure_ascii=False, indent=2))
        return

    print("Rota | Status | Min | Media | Max | KB | Destino")
    print("--- | ---: | ---: | ---: | ---: | ---: | ---")
    for item in resultados:
        print(
            f"{item['rota']} | {item['status']} | {item['min_ms']} ms | "
            f"{item['media_ms']} ms | {item['max_ms']} ms | {item['tamanho_kb']} | {item['destino']}"
        )

    if args.metricas_internas:
        print("\nTrecho | Rota | Media | Pico | Amostras | Cache")
        print("--- | --- | ---: | ---: | ---: | ---:")
        for item in app_module.obter_metricas_consultas_sql(limite=80)["ranking"]:
            print(
                f"{item['nome']} | {item['pagina']} | {item['media_ms']} ms | "
                f"{item['max_ms']} ms | {item['amostras']} | {item['cache_hits']}"
            )


if __name__ == "__main__":
    main()

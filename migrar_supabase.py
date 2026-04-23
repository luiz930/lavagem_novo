import os

import app


def main():
    if not app.banco_online_ativo():
        print(
            "O banco online ainda nao esta ativo. "
            "Defina DATABASE_URL e DATABASE_BACKEND=postgres com a senha real do Supabase."
        )
        return 1

    origem = app.caminho_banco_absoluto()
    if not os.path.isfile(origem):
        print(f"Banco SQLite de origem nao encontrado: {origem}")
        return 1

    print("Criando tabelas no banco online...")
    app.init_db()

    print("Migrando dados do SQLite local para o Supabase...")
    app.importar_sqlite_para_banco_atual(origem)

    print("Migracao concluida com sucesso.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

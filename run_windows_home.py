import os

from waitress import serve

from app import app


def main():
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "5000"))
    threads = int(os.environ.get("WAITRESS_THREADS", "6"))
    connection_limit = int(os.environ.get("WAITRESS_CONNECTION_LIMIT", "120"))

    serve(
        app,
        host=host,
        port=port,
        threads=threads,
        connection_limit=connection_limit,
        ident="wagen-casa",
    )


if __name__ == "__main__":
    main()

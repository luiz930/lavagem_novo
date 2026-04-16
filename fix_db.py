import sqlite3

conn = sqlite3.connect("database_v2.db")
c = conn.cursor()

c.execute("""
UPDATE sincronizacoes_clientes
SET url = 'https://docs.google.com/spreadsheets/d/168DIKfBPJ7IyVkQe5WA-1ec1izYeSie658A8vBVBaok/edit#gid=2069444486'
WHERE id = 11
""")

conn.commit()
conn.close()

print("OK - URL corrigida")
from src.database import DB_PATH, database_connection

with database_connection() as connection:
    rows = connection.execute("SELECT * FROM productos").fetchall()

print("Base de datos:", DB_PATH)
print("TOTAL productos:", len(rows))
for row in rows:
    print(row)
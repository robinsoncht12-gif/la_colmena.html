import os, json, sqlite3
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, unquote

BASE = os.path.dirname(os.path.abspath(__file__))
DB = os.path.join(BASE, "LaColmena.db")

# === SEGURIDAD OWASP / ZAP / SQLMAP / WAPITI ===
PATRONES_ATAQUE = ["' OR '", '" OR "', "UNION SELECT", "DROP TABLE", "<script", "SELECT * FROM", "1=1", "--"]

# Crea DB si no existe con tus datos reales
def init_db():
    con = sqlite3.connect(DB)
    cur = con.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS usuarios(id INTEGER PRIMARY KEY, nombre TEXT, email TEXT UNIQUE, password TEXT, rol TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS productos(id INTEGER PRIMARY KEY, nombre TEXT, categoria TEXT, precio REAL, stock INTEGER, minimo INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS ventas(id INTEGER PRIMARY KEY AUTOINCREMENT, cliente TEXT, total REAL, estado TEXT, fecha TEXT DEFAULT (datetime('now','localtime')))")
    cur.execute("CREATE TABLE IF NOT EXISTS pedidos(id INTEGER PRIMARY KEY AUTOINCREMENT, proveedor TEXT, producto TEXT, cantidad INTEGER, estado TEXT)")
    # Usuario demo
    cur.execute("INSERT OR IGNORE INTO usuarios VALUES(1,'Ana Martínez','demo@lacolmena.co','demo123','Administradora')")
    # Tus 7 productos reales
    productos = [
        (1,'Arroz 500g','Alimentos',5000,120,20),
        (2,'Leche 1L','Lácteos',4500,80,15),
        (3,'Cafe 500g','Alimentos',12000,45,10),
        (4,'Aceite 1L','Alimentos',9000,60,10),
        (5,'Huevos x12','Alimentos',8000,90,20),
        (6,'Jabon','Aseo',3500,100,20),
        (7,'Papel higienico','Aseo',6000,75,15),
    ]
    cur.executemany("INSERT OR IGNORE INTO productos VALUES(?,?,?,?,?,?)", productos)
    cur.execute("INSERT OR IGNORE INTO ventas(id,cliente,total,estado) VALUES(1048,'Salón Aura',486300,'Pagada')")
    cur.execute("INSERT OR IGNORE INTO ventas(id,cliente,total,estado) VALUES(1047,'Barber Club',238900,'Pendiente')")
    cur.execute("INSERT OR IGNORE INTO ventas(id,cliente,total,estado) VALUES(1046,'Studio 21',612500,'Pagada')")
    cur.execute("INSERT OR IGNORE INTO pedidos VALUES(1,'Proveedor A','Arroz 500g',50,'Por recibir')")
    cur.execute("INSERT OR IGNORE INTO pedidos VALUES(2,'Proveedor B','Leche 1L',30,'Por recibir')")
    cur.execute("CREATE TABLE IF NOT EXISTS auditoria_seguridad(id INTEGER PRIMARY KEY AUTOINCREMENT, ip TEXT, intento TEXT, herramienta TEXT, fecha TEXT DEFAULT (datetime('now','localtime')))")
    con.commit(); con.close()

init_db()

def query(sql, params=()):
    con = sqlite3.connect(DB); con.row_factory = sqlite3.Row
    cur = con.cursor(); cur.execute(sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    con.commit(); con.close(); return rows

with open(os.path.join(BASE, "index.html"), encoding="utf-8") as f:
    HTML = f.read()

class H(BaseHTTPRequestHandler):
        
    def es_ataque(self, texto):
        texto_low = texto.lower()
        for p in PATRONES_ATAQUE:
            if p.lower() in texto_low:
                herramienta = "sqlmap" if "union" in texto_low or "select" in texto_low else "ZAP/Wapiti"
                con = sqlite3.connect(DB); cur = con.cursor()
                cur.execute("INSERT INTO auditoria_seguridad (ip, intento, herramienta) VALUES (?,?,?)", (self.client_address[0], texto[:200], herramienta))
                con.commit(); con.close()
                return True
        return False

    def send_json(self, data, code=200):
        b = json.dumps(data).encode()
        self.send_response(code); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        p = urlparse(self.path).path
        if self.es_ataque(unquote(self.path)):
            self.send_json({"ok": False, "error": "Intento bloqueado por WAF - La Colmena Security"}, 403)
            return
        if p in ["/","/index.html"]:

            b = HTML.encode(); self.send_response(200); self.send_header("Content-Type","text/html; charset=utf-8"); self.send_header("Content-Length",str(len(b))); self.end_headers(); self.wfile.write(b)
        elif p == "/api/productos":
            self.send_json(query("SELECT * FROM productos"))
        elif p == "/api/usuarios":
            self.send_json(query("SELECT id,nombre,email,rol FROM usuarios"))
        elif p == "/api/ventas":
            self.send_json(query("SELECT * FROM ventas ORDER BY id DESC"))
        elif p == "/api/pedidos":
            self.send_json(query("SELECT * FROM pedidos"))
        elif p == "/api/seguridad":
            self.send_json(query("SELECT * FROM auditoria_seguridad ORDER BY id DESC LIMIT 20"))
        elif p == "/api/resumen":
            prod = query("SELECT SUM(stock) as stock, COUNT(*) as total FROM productos")[0]
            ventas = query("SELECT SUM(total) as total, COUNT(*) as n FROM ventas")[0]
            bajo = query("SELECT * FROM productos WHERE stock <= minimo")
            self.send_json({"stock": prod["stock"] or 0, "total_productos": prod["total"], "ventas_total": ventas["total"] or 0, "ventas_n": ventas["n"], "bajo_stock": bajo, "pedidos_abiertos": len(query("SELECT * FROM pedidos WHERE estado='Por recibir'"))})
        else:
            self.send_json({"error":"Ruta no encontrada"},404)
    def do_POST(self):
        p = urlparse(self.path).path
        n = int(self.headers.get("Content-Length",0)); data = json.loads(self.rfile.read(n) or b"{}")
        if self.es_ataque(str(data)):
            self.send_json({"ok": False, "error": "Intento bloqueado por WAF"}, 403)
            return
        if p == "/api/login":
            u = query("SELECT * FROM usuarios WHERE email=? AND password=?", (data.get("email"), data.get("password")))
            if u: self.send_json({"ok":True,"usuario":{"nombre":u[0]["nombre"],"email":u[0]["email"],"rol":u[0]["rol"]}})
            else: self.send_json({"ok":False,"error":"Correo o contraseña incorrectos"},401)
        elif p == "/api/usuarios":
            try:
                query("INSERT INTO usuarios (nombre, email, password, rol) VALUES (?,?,?,?)", (data.get("nombre"), data.get("email"), data.get("password"), data.get("rol","Vendedor")))
                self.send_json({"ok": True, "mensaje": "Usuario creado correctamente"})
            except Exception as e:
                self.send_json({"ok": False, "error": "Ese correo ya existe"})

        elif p == "/api/ventas":
            # data: {cliente, producto_id, cantidad}
            prod = query("SELECT * FROM productos WHERE id=?", (data.get("producto_id"),))[0]
            cant = int(data.get("cantidad",1))
            if prod["stock"] < cant: self.send_json({"ok":False,"error":"Stock insuficiente"},400); return
            total = prod["precio"]*cant
            query("UPDATE productos SET stock=stock-? WHERE id=?", (cant, prod["id"]))
            query("INSERT INTO ventas(cliente,total,estado) VALUES(?,?, 'Pagada')", (data.get("cliente","Cliente"), total))
            self.send_json({"ok":True,"total":total})
        elif p == "/api/productos/ajustar":
            query("UPDATE productos SET stock=? WHERE id=?", (data.get("stock"), data.get("id")))
            self.send_json({"ok":True})
        else: self.send_json({"error":"No encontrado"},404)
    def log_message(self,*a): pass

print("Corriendo en http://localhost:8086  usuario: demo@lacolmena.co / demo123")
HTTPServer(("0.0.0.0",8086), H).serve_forever()
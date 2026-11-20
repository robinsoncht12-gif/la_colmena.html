import json
import os
import secrets
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer as HTTPServer
import threading
from http.cookies import CookieError, SimpleCookie
from urllib.parse import urlparse

from argon2.exceptions import HashingError

from src.auth import (
    DUMMY_PASSWORD_HASH,
    LOGIN_RATE_LIMITER,
    MAX_PASSWORD_LENGTH,
    MIN_PASSWORD_LENGTH,
    SESSION_COOKIE_NAME,
    SESSION_STORE,
    clear_session_cookie,
    hash_password,
    password_hash_needs_rehash,
    session_cookie,
    verify_password,
)
from src.database import DB_PATH, database_connection
from src.events import crear_cloudevent
from src.logger import get_logger
from src.outbox import KafkaPublisher, OutboxWorker
from src.repositories import (
    SQLiteOutboxRepository,
    SQLitePedidoRepository,
    SQLiteProductoRepository,
    insertar_evento_outbox,
)
from src.services import (
    ColmenaService,
    PasarelaPagosMock,
    SagaOrquestadorMatriculaPedido,
)
from src.tracing import current_traceparent, start_trace
from src.validators import ValidationError, Validator

BASE = os.path.dirname(os.path.abspath(__file__))
DB = DB_PATH
MAX_REQUEST_BYTES = 16_384
ADMIN_ROLE = "Administrador"
VENDOR_ROLE = "Vendedor"
CLIENT_ROLE = "Cliente"
VALID_ROLES = (ADMIN_ROLE, VENDOR_ROLE, CLIENT_ROLE)
logger = get_logger("WebApp")
_SAGA_RUNTIME_DB = None
_SAGA_RUNTIME = None
_SAGA_RUNTIME_LOCK = threading.Lock()


def _actor_event_data(user):
    return {
        "user_id": user["id"],
        "name": user["nombre"],
        "role": user["rol"],
    }


def _create_actor_event(event_type, user, data):
    return crear_cloudevent(
        event_type,
        {**data, "actor": _actor_event_data(user)},
    )

def init_db():
    generated_initial_password = None
    with database_connection(DB) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                rol TEXT NOT NULL
            )
        """)
        connection.execute(
            "UPDATE usuarios SET rol = ? WHERE rol = ?",
            (ADMIN_ROLE, "Administradora"),
        )
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(usuarios)")
        }
        if "password_hash" not in columns:
            connection.execute("ALTER TABLE usuarios ADD COLUMN password_hash TEXT")
        if "password" in columns:
            legacy_users = connection.execute(
                "SELECT id, password, password_hash FROM usuarios"
            ).fetchall()
            for user_id, legacy_password, stored_hash in legacy_users:
                if legacy_password and not stored_hash:
                    try:
                        password_hash = hash_password(legacy_password)
                    except ValueError:
                        logger.warning(
                            "La contraseña heredada del usuario %s no pudo migrarse.",
                            user_id,
                        )
                    else:
                        connection.execute(
                            "UPDATE usuarios SET password_hash = ? WHERE id = ?",
                            (password_hash, user_id),
                        )
            connection.execute("UPDATE usuarios SET password = NULL")

        connection.execute("""
            CREATE TABLE IF NOT EXISTS productos (
                id INTEGER PRIMARY KEY,
                nombre TEXT NOT NULL,
                categoria TEXT NOT NULL,
                precio REAL NOT NULL,
                stock INTEGER NOT NULL,
                minimo INTEGER NOT NULL
            )
        """)
        connection.execute("""
            CREATE TABLE IF NOT EXISTS ventas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cliente TEXT NOT NULL,
                total REAL NOT NULL,
                estado TEXT NOT NULL,
                fecha TEXT DEFAULT (datetime('now','localtime'))
            )
        """)
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proveedor TEXT NOT NULL,
                producto TEXT NOT NULL,
                cantidad INTEGER NOT NULL,
                estado TEXT NOT NULL
            )
        """)
        connection.execute("""
            CREATE TABLE IF NOT EXISTS auditoria_seguridad (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ip TEXT,
                intento TEXT,
                herramienta TEXT,
                fecha TEXT DEFAULT (datetime('now','localtime'))
            )
        """)

        user_count = connection.execute(
            "SELECT COUNT(*) FROM usuarios"
        ).fetchone()[0]
        if not user_count:
            initial_password = os.environ.get("LACOLMENA_INITIAL_ADMIN_PASSWORD")
            if initial_password is None:
                initial_password = secrets.token_urlsafe(18)
                generated_initial_password = initial_password
            if (
                len(initial_password) < MIN_PASSWORD_LENGTH
                or len(initial_password) > MAX_PASSWORD_LENGTH
            ):
                raise ValueError(
                    "LACOLMENA_INITIAL_ADMIN_PASSWORD debe tener entre 12 y "
                    "1024 caracteres."
                )
            connection.execute(
                """
                INSERT INTO usuarios (id, nombre, email, password_hash, rol)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    1,
                    "Ana Martínez",
                    "demo@lacolmena.co",
                    hash_password(initial_password),
                    "Administrador",
                ),
            )

        productos = [
            (1, "Arroz 500g", "Alimentos", 5000, 120, 20),
            (2, "Leche 1L", "Lácteos", 4500, 80, 15),
            (3, "Cafe 500g", "Alimentos", 12000, 45, 10),
            (4, "Aceite 1L", "Alimentos", 9000, 60, 10),
            (5, "Huevos x12", "Alimentos", 8000, 90, 20),
            (6, "Jabon", "Aseo", 3500, 100, 20),
            (7, "Papel higienico", "Aseo", 6000, 75, 15),
        ]
        connection.executemany(
            "INSERT OR IGNORE INTO productos VALUES (?, ?, ?, ?, ?, ?)",
            productos,
        )
        connection.executemany(
            """
            INSERT OR IGNORE INTO ventas (id, cliente, total, estado)
            VALUES (?, ?, ?, ?)
            """,
            [
                (1048, "Salón Aura", 486300, "Pagada"),
                (1047, "Barber Club", 238900, "Pendiente"),
                (1046, "Studio 21", 612500, "Pagada"),
            ],
        )
        connection.executemany(
            """
            INSERT OR IGNORE INTO pedidos (id, proveedor, producto, cantidad, estado)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (1, "Proveedor A", "Arroz 500g", 50, "Por recibir"),
                (2, "Proveedor B", "Leche 1L", 30, "Por recibir"),
            ],
        )
    SQLiteOutboxRepository(DB)
    if generated_initial_password:
        logger.warning(
            "Cuenta inicial creada para demo@lacolmena.co. "
            "Contraseña temporal (guárdala ahora): %s",
            generated_initial_password,
        )

def query(sql, params=()):
    with database_connection(DB) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.execute(sql, params)
        return [dict(row) for row in cursor.fetchall()]


def _crear_saga_orquestador():
    global _SAGA_RUNTIME_DB, _SAGA_RUNTIME
    if _SAGA_RUNTIME is None or _SAGA_RUNTIME_DB != DB:
        producto_repo = SQLiteProductoRepository(DB)
        pedido_repo = SQLitePedidoRepository(DB)
        outbox_repo = SQLiteOutboxRepository(DB)
        service = ColmenaService(
            producto_repo, pedido_repo, outbox_repo, db_path=DB
        )
        try:
            delay_ms = max(0, min(30_000, int(os.environ.get("PASARELA_MOCK_DELAY_MS", "0"))))
        except ValueError:
            logger.warning("PASARELA_MOCK_DELAY_MS no es válido; se usará 0 ms.")
            delay_ms = 0
        _SAGA_RUNTIME = SagaOrquestadorMatriculaPedido(
            service, PasarelaPagosMock(delay_seconds=delay_ms / 1000)
        )
        _SAGA_RUNTIME_DB = DB
    return _SAGA_RUNTIME


def _saga_orquestador():
    with _SAGA_RUNTIME_LOCK:
        return _crear_saga_orquestador()


with open(os.path.join(BASE, "index.html"), encoding="utf-8") as f:
    HTML = f.read()

class H(BaseHTTPRequestHandler):
    def parse_request(self):
        parsed = super().parse_request()
        if parsed:
            start_trace(self.headers.get("traceparent"))
        return parsed

    def end_headers(self):
        traceparent = current_traceparent()
        if traceparent and not getattr(self, "_traceparent_header_sent", False):
            self.send_header("traceparent", traceparent)
            self._traceparent_header_sent = True
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
            "connect-src 'self'; object-src 'none'; base-uri 'self'; "
            "frame-ancestors 'none'",
        )
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
        self.send_header("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def send_json(self, data, code=200, headers=None):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def _session_token(self):
        cookie = SimpleCookie()
        try:
            cookie.load(self.headers.get("Cookie", ""))
        except CookieError:
            return None
        morsel = cookie.get(SESSION_COOKIE_NAME)
        return morsel.value if morsel else None

    def _authenticated_user(self):
        token = self._session_token()
        user = SESSION_STORE.get(token) if token else None
        return user if user and user.get("rol") in VALID_ROLES else None

    def _require_roles(self, user, roles):
        if user.get("rol") not in roles:
            self.send_json({"ok": False, "error": "Acceso denegado"}, 403)
            return False
        return True

    def _is_admin(self, user):
        return self._require_roles(user, (ADMIN_ROLE,))

    def _read_json(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError as error:
            raise ValueError("Content-Length no válido") from error
        if content_length < 0:
            raise ValueError("Content-Length no válido")
        if content_length > MAX_REQUEST_BYTES:
            self.send_json({"ok": False, "error": "Solicitud demasiado grande"}, 413)
            return None
        data = json.loads(self.rfile.read(content_length) or b"{}")
        if not isinstance(data, dict):
            raise ValueError("Se esperaba un objeto JSON")
        return data

    def _record_failed_login(self):
        with database_connection(DB) as connection:
            connection.execute(
                """
                INSERT INTO auditoria_seguridad (ip, intento, herramienta)
                VALUES (?, ?, ?)
                """,
                (self.client_address[0], "Credenciales inválidas", "Login"),
            )

    def _login(self, data):
        allowed, retry_after = LOGIN_RATE_LIMITER.consume(self.client_address[0])
        if not allowed:
            self.send_json(
                {"ok": False, "error": "Demasiados intentos. Inténtalo más tarde."},
                429,
                {"Retry-After": str(retry_after)},
            )
            return

        email = data.get("email")
        password = data.get("password")
        email = email.strip().lower() if isinstance(email, str) else ""
        if len(email) > 254 or not isinstance(password, str):
            password = ""

        users = query(
            """
            SELECT id, nombre, email, rol, password_hash
            FROM usuarios
            WHERE email = ? COLLATE NOCASE
            """,
            (email,),
        )
        user = users[0] if users else None
        stored_hash = user["password_hash"] if user else DUMMY_PASSWORD_HASH
        if not verify_password(password, stored_hash) or not user:
            self._record_failed_login()
            self.send_json(
                {"ok": False, "error": "Correo o contraseña incorrectos"}, 401
            )
            return

        if password_hash_needs_rehash(stored_hash):
            upgraded_hash = hash_password(password)
            query(
                "UPDATE usuarios SET password_hash = ? WHERE id = ?",
                (upgraded_hash, user["id"]),
            )

        login_event = _create_actor_event(
            "bina.la_colmena.autenticacion.login",
            user,
            {"action": "login"},
        )
        with database_connection(DB) as connection:
            insertar_evento_outbox(
                connection,
                f"USR-{user['id']}",
                login_event.to_dict(),
            )

        token = SESSION_STORE.create(user)
        self.send_json(
            {
                "ok": True,
                "usuario": {
                    "nombre": user["nombre"],
                    "email": user["email"],
                    "rol": user["rol"],
                },
            },
            headers={"Set-Cookie": session_cookie(token)},
        )

    def _logout(self):
        token = self._session_token()
        user = SESSION_STORE.get(token) if token else None
        if token:
            SESSION_STORE.revoke(token)
        if user:
            try:
                logout_event = _create_actor_event(
                    "bina.la_colmena.autenticacion.logout",
                    user,
                    {"action": "logout"},
                )
                with database_connection(DB) as connection:
                    insertar_evento_outbox(
                        connection,
                        f"USR-{user['id']}",
                        logout_event.to_dict(),
                    )
            except sqlite3.Error:
                logger.exception(
                    "No se pudo guardar el evento de cierre de sesión del usuario %s",
                    user["id"],
                )
        self.send_json(
            {"ok": True},
            headers={"Set-Cookie": clear_session_cookie()},
        )

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path in ("/", "/index.html"):
                body = HTML.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            user = self._authenticated_user()
            if path.startswith("/api/") and not user:
                self.send_json(
                    {"ok": False, "error": "Autenticación requerida"}, 401
                )
                return

            if path == "/api/circuit-breaker":
                if not self._require_roles(user, (ADMIN_ROLE, VENDOR_ROLE)):
                    return
                gateway = _saga_orquestador().pasarela
                circuit = gateway.circuit_breaker.snapshot()
                circuit.update({
                    "delay_ms": round(gateway.delay_seconds * 1000),
                    "component": "PasarelaPagosMock",
                })
                self.send_json({"ok": True, "circuit": circuit})
                return

            if path == "/api/productos":
                if user["rol"] == CLIENT_ROLE:
                    self.send_json(
                        query(
                            "SELECT id, nombre, categoria, precio FROM productos"
                        )
                    )
                else:
                    self.send_json(query("SELECT * FROM productos"))
            elif path == "/api/catalogo-saga":
                products = _saga_orquestador().Colmena_service.listar_catalogo()
                self.send_json(
                    [
                        {
                            "codigo": product.codigo,
                            "nombre": product.nombre,
                            "categoria": product.categoria,
                            "precio": product.precio,
                        }
                        for product in products
                    ]
                )
            elif path == "/api/usuarios":
                if self._is_admin(user):
                    self.send_json(
                        query("SELECT id, nombre, email, rol FROM usuarios")
                    )
            elif path == "/api/ventas":
                if self._require_roles(user, (ADMIN_ROLE, VENDOR_ROLE)):
                    self.send_json(query("SELECT * FROM ventas ORDER BY id DESC"))
            elif path == "/api/eventos":
                if not self._require_roles(user, (ADMIN_ROLE, VENDOR_ROLE)):
                    return
                events = query(
                    """
                    SELECT id, aggregate_id, event_type, payload_json, timestamp,
                           status, processed_at, attempts, last_error
                    FROM outbox_events
                    ORDER BY id DESC
                    LIMIT 100
                    """
                )
                for event in events:
                    payload = json.loads(event.pop("payload_json"))
                    event["event_id"] = payload.get("id")
                    event["data"] = payload.get("data", {})
                    event["cloud_event"] = payload
                self.send_json({"events": events})
            elif path == "/api/pedidos":
                if self._require_roles(user, (ADMIN_ROLE, VENDOR_ROLE)):
                    self.send_json(query("SELECT * FROM pedidos"))
            elif path == "/api/seguridad":
                if self._is_admin(user):
                    self.send_json(
                        query(
                            "SELECT * FROM auditoria_seguridad "
                            "ORDER BY id DESC LIMIT 20"
                        )
                    )
            elif path == "/api/outbox":
                if self._is_admin(user):
                    events = query(
                        """
                        SELECT id, event_type, payload_json, timestamp,
                               attempts, last_error
                        FROM outbox_events
                        WHERE status = 'PENDING' AND last_error IS NOT NULL
                        ORDER BY id DESC
                        LIMIT 10
                        """
                    )
                    for event in events:
                        event["event_id"] = json.loads(
                            event.pop("payload_json")
                        ).get("id")
                    self.send_json({"events": events})
            elif path == "/api/resumen":
                if not self._require_roles(user, (ADMIN_ROLE, VENDOR_ROLE)):
                    return
                product_summary = query(
                    "SELECT SUM(stock) AS stock, COUNT(*) AS total FROM productos"
                )[0]
                sales_summary = query(
                    "SELECT SUM(total) AS total, COUNT(*) AS n FROM ventas"
                )[0]
                low_stock = query("SELECT * FROM productos WHERE stock <= minimo")
                open_orders = query(
                    "SELECT COUNT(*) AS total FROM pedidos WHERE estado = ?",
                    ("Por recibir",),
                )[0]["total"]
                self.send_json(
                    {
                        "stock": product_summary["stock"] or 0,
                        "total_productos": product_summary["total"],
                        "ventas_total": sales_summary["total"] or 0,
                        "ventas_n": sales_summary["n"],
                        "bajo_stock": low_stock,
                        "pedidos_abiertos": open_orders,
                    }
                )
            else:
                self.send_json({"error": "Ruta no encontrada"}, 404)
        except sqlite3.Error:
            logger.exception("Error de base de datos al procesar GET %s", path)
            self.send_json(
                {"ok": False, "error": "Error interno del servidor"}, 500
            )

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            data = self._read_json()
            if data is None:
                return
        except ValueError:
            self.send_json({"ok": False, "error": "JSON o solicitud no válidos"}, 400)
            return

        try:
            if path == "/api/login":
                self._login(data)
                return
            if path == "/api/logout":
                self._logout()
                return

            user = self._authenticated_user()
            if path.startswith("/api/") and not user:
                self.send_json(
                    {"ok": False, "error": "Autenticación requerida"}, 401
                )
                return

            if path == "/api/usuarios":
                if not self._is_admin(user):
                    return
                name = data.get("nombre")
                email = data.get("email")
                password = data.get("password")
                role = data.get("rol", VENDOR_ROLE)
                if (
                    not isinstance(name, str)
                    or not name.strip()
                    or len(name) > 120
                    or not isinstance(email, str)
                    or len(email) > 254
                    or "@" not in email
                    or not isinstance(password, str)
                    or len(password) < MIN_PASSWORD_LENGTH
                    or role not in VALID_ROLES
                ):
                    self.send_json(
                        {
                            "ok": False,
                            "error": (
                                "Datos inválidos; la contraseña debe tener "
                                "al menos 12 caracteres"
                            ),
                        },
                        400,
                    )
                    return
                try:
                    password_hash = hash_password(password)
                except ValueError:
                    self.send_json(
                        {"ok": False, "error": "La contraseña no es válida"},
                        400,
                    )
                    return
                try:
                    with database_connection(DB) as connection:
                        cursor = connection.execute(
                            """
                            INSERT INTO usuarios (nombre, email, password_hash, rol)
                            VALUES (?, ?, ?, ?)
                            """,
                            (
                                name.strip(),
                                email.strip().lower(),
                                password_hash,
                                role,
                            ),
                        )
                        user_id = cursor.lastrowid
                        event = crear_cloudevent(
                            "bina.la_colmena.usuario.creado",
                            {
                                "user_id": user_id,
                                "role": role,
                                "action": "user.create",
                                "actor": _actor_event_data(user),
                            },
                        )
                        insertar_evento_outbox(
                            connection, str(user_id), event.to_dict()
                        )
                except sqlite3.IntegrityError:
                    self.send_json(
                        {"ok": False, "error": "Ese correo ya existe"}, 409
                    )
                    return
                self.send_json(
                    {"ok": True, "mensaje": "Usuario creado correctamente"}
                )
            elif path == "/api/ordenes":
                if not self._require_roles(user, VALID_ROLES):
                    return
                try:
                    idempotency_key = Validator.validar_idempotency_key(
                        self.headers.get("Idempotency-Key", "")
                    )
                    address = data.get("direccion")
                    if not isinstance(address, str):
                        raise ValidationError(
                            "El campo 'direccion' debe ser texto.",
                            campo="direccion",
                        )
                    address = Validator.validar_no_vacio(address, "direccion")
                    items = Validator.validar_items_pedido(data.get("items"))
                    simular_falla_pago = data.get(
                        "simular_falla_pago", False
                    )
                    if not isinstance(simular_falla_pago, bool):
                        raise ValidationError(
                            "El campo 'simular_falla_pago' debe ser booleano.",
                            campo="simular_falla_pago",
                        )
                except ValidationError as error:
                    self.send_json(
                        {"ok": False, "error": str(error)}, 400
                    )
                    return
                if simular_falla_pago and not self._is_admin(user):
                    return

                result = _saga_orquestador().ejecutar_saga_pedido(
                    cliente_id=f"USR-{user['id']}",
                    cliente_nombre=user["nombre"],
                    direccion=address,
                    items_req=items,
                    idempotency_key=idempotency_key,
                    simular_falla_pago=simular_falla_pago,
                    actor=_actor_event_data(user),
                )
                pedido = result.get("pedido")
                if pedido is None:
                    status = (
                        400
                        if result.get("error_type") == "ValidationError"
                        else 500
                    )
                    if status == 500:
                        logger.error(
                            "No se pudo crear la orden Saga: %s",
                            result.get("error"),
                        )
                    self.send_json(
                        {
                            "ok": False,
                            "error": result.get(
                                "error", "No se pudo procesar la orden"
                            ),
                            "log": result.get("log", []),
                        },
                        status,
                    )
                    return

                self.send_json(
                    {
                        "ok": result["exito"],
                        "pedido": {
                            "id": pedido.id_pedido,
                            "estado": pedido.estado.value,
                            "saga_status": pedido.estado_saga.value,
                            "total": pedido.total,
                        },
                        "error": result.get("error"),
                        "error_compensacion": result.get(
                            "error_compensacion"
                        ),
                        "log": result.get("log", []),
                    },
                    200 if result["exito"] else 409,
                )
            elif path == "/api/ventas":
                if not self._require_roles(
                    user, (ADMIN_ROLE, VENDOR_ROLE, CLIENT_ROLE)
                ):
                    return
                try:
                    product_id = int(data.get("producto_id"))
                    quantity = int(data.get("cantidad", 1))
                except (TypeError, ValueError):
                    self.send_json(
                        {"ok": False, "error": "Producto o cantidad no válidos"},
                        400,
                    )
                    return
                if quantity <= 0:
                    self.send_json(
                        {"ok": False, "error": "La cantidad debe ser mayor que cero"},
                        400,
                    )
                    return

                client = data.get("cliente", "Cliente")
                if not isinstance(client, str) or not client.strip():
                    client = "Cliente"
                if user["rol"] == CLIENT_ROLE:
                    client = user["email"]
                with database_connection(DB) as connection:
                    connection.row_factory = sqlite3.Row
                    product = connection.execute(
                        "SELECT id, precio, stock FROM productos WHERE id = ?",
                        (product_id,),
                    ).fetchone()
                    if not product:
                        self.send_json(
                            {"ok": False, "error": "Producto no encontrado"}, 404
                        )
                        return
                    update = connection.execute(
                        """
                        UPDATE productos SET stock = stock - ?
                        WHERE id = ? AND stock >= ?
                        """,
                        (quantity, product_id, quantity),
                    )
                    if update.rowcount != 1:
                        self.send_json(
                            {"ok": False, "error": "Stock insuficiente"}, 400
                        )
                        return
                    total = product["precio"] * quantity
                    sale_cursor = connection.execute(
                        """
                        INSERT INTO ventas (cliente, total, estado)
                        VALUES (?, ?, ?)
                        """,
                        (client.strip(), total, "Pagada"),
                    )
                    event = crear_cloudevent(
                        "bina.la_colmena.venta.registrada",
                        {
                            "sale_id": sale_cursor.lastrowid,
                            "product_id": product_id,
                            "quantity": quantity,
                            "unit_price": product["precio"],
                            "total": total,
                            "action": "sale.register",
                            "actor": _actor_event_data(user),
                        },
                    )
                    insertar_evento_outbox(
                        connection, str(sale_cursor.lastrowid), event.to_dict()
                    )
                self.send_json(
                    {
                        "ok": True,
                        "total": total,
                        "evento": event.to_dict(),
                    }
                )
            elif path == "/api/productos/ajustar":
                if not self._is_admin(user):
                    return
                try:
                    product_id = int(data.get("id"))
                    stock = int(data.get("stock"))
                except (TypeError, ValueError):
                    self.send_json(
                        {"ok": False, "error": "Producto o stock no válidos"}, 400
                    )
                    return
                if stock < 0:
                    self.send_json(
                        {"ok": False, "error": "El stock no puede ser negativo"},
                        400,
                    )
                    return
                with database_connection(DB) as connection:
                    connection.row_factory = sqlite3.Row
                    product = connection.execute(
                        "SELECT nombre, stock FROM productos WHERE id = ?",
                        (product_id,),
                    ).fetchone()
                    if not product:
                        self.send_json(
                            {"ok": False, "error": "Producto no encontrado"}, 404
                        )
                        return
                    update = connection.execute(
                        "UPDATE productos SET stock = ? WHERE id = ?",
                        (stock, product_id),
                    )
                    if update.rowcount != 1:
                        self.send_json(
                            {"ok": False, "error": "Producto no encontrado"}, 404
                        )
                        return
                    event = crear_cloudevent(
                        "bina.la_colmena.inventario.actualizado",
                        {
                            "product_id": product_id,
                            "product_name": product["nombre"],
                            "previous_stock": product["stock"],
                            "stock": stock,
                            "action": "inventory.adjust",
                            "actor": _actor_event_data(user),
                        },
                    )
                    insertar_evento_outbox(
                        connection, str(product_id), event.to_dict()
                    )
                self.send_json({"ok": True})
            else:
                self.send_json({"error": "Ruta no encontrada"}, 404)
        except HashingError:
            logger.exception(
                "No se pudo derivar la contraseña al procesar POST %s", path
            )
            self.send_json(
                {"ok": False, "error": "Error interno del servidor"}, 500
            )
        except sqlite3.Error:
            logger.exception("Error de base de datos al procesar POST %s", path)
            self.send_json(
                {"ok": False, "error": "Error interno del servidor"}, 500
            )

    def do_DELETE(self):
        path = urlparse(self.path).path
        prefix = "/api/usuarios/"
        if not path.startswith(prefix):
            self.send_json({"error": "Ruta no encontrada"}, 404)
            return

        user = self._authenticated_user()
        if not user:
            self.send_json(
                {"ok": False, "error": "Autenticación requerida"}, 401
            )
            return
        if not self._is_admin(user):
            return

        try:
            target_id = int(path[len(prefix):])
        except ValueError:
            self.send_json({"ok": False, "error": "Usuario no válido"}, 400)
            return
        if target_id <= 0:
            self.send_json({"ok": False, "error": "Usuario no válido"}, 400)
            return
        if target_id == user["id"]:
            self.send_json(
                {"ok": False, "error": "No puedes eliminar tu propia cuenta"},
                400,
            )
            return

        try:
            with database_connection(DB) as connection:
                target = connection.execute(
                    "SELECT id, rol FROM usuarios WHERE id = ?",
                    (target_id,),
                ).fetchone()
                if not target:
                    self.send_json(
                        {"ok": False, "error": "Usuario no encontrado"}, 404
                    )
                    return
                if target[1] == ADMIN_ROLE:
                    admin_count = connection.execute(
                        "SELECT COUNT(*) FROM usuarios WHERE rol = ?",
                        (ADMIN_ROLE,),
                    ).fetchone()[0]
                    if admin_count <= 1:
                        self.send_json(
                            {
                                "ok": False,
                                "error": "Debe existir al menos un administrador",
                            },
                            409,
                        )
                        return
                connection.execute(
                    "DELETE FROM usuarios WHERE id = ?",
                    (target_id,),
                )
                event = crear_cloudevent(
                    "bina.la_colmena.usuario.eliminado",
                    {
                        "user_id": target_id,
                        "role": target[1],
                        "action": "user.delete",
                        "actor": _actor_event_data(user),
                    },
                )
                insertar_evento_outbox(
                    connection, str(target_id), event.to_dict()
                )
            SESSION_STORE.revoke_user(target_id)
            self.send_json({"ok": True, "mensaje": "Usuario eliminado"})
        except sqlite3.Error:
            logger.exception("Error de base de datos al eliminar un usuario")
            self.send_json(
                {"ok": False, "error": "Error interno del servidor"}, 500
            )

    def log_message(self, format_string, *args):
        if (
            self.command == "GET"
            and urlparse(self.path).path in ("/api/outbox", "/api/eventos")
        ):
            return
        logger.info("%s - %s", self.address_string(), format_string % args)

def run_server(host="127.0.0.1", port=8086):
    init_db()
    server = HTTPServer((host, port), H)
    publisher = KafkaPublisher()
    worker = OutboxWorker(SQLiteOutboxRepository(DB), publisher)
    worker.start()
    print(f"Corriendo en http://localhost:{port}")
    try:
        server.serve_forever()
    finally:
        worker.stop()
        publisher.close()
        server.server_close()


if __name__ == "__main__":
    run_server()

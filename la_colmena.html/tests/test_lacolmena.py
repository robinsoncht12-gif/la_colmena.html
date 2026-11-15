"""

SUITE DE PRUEBAS UNITARIAS Y DE INTEGRACIÓN (unittest de Python)
Cátedra: Ingeniería de Software 3 [IF0129] - Corporación Universitaria Remington
Pruebas: SOLID, ACID, Idempotencia, Saga Compensación Inversa, Validaciones Fail-Fast
Dependencias de pruebas: Python Standard Library `unittest`, `tempfile`, `os`, `shutil`

"""
import unittest
import tempfile
import os
import json
import uuid
import sys
import sqlite3
import threading
import base64
import hashlib
from http.client import HTTPConnection
from contextlib import closing
from unittest.mock import patch

# Asegurar path para ejecución directa
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.models import Producto, Pedido, ItemPedido, EstadoPedido
from src.validators import Validator, ValidationError
from src.repositories import SQLiteProductoRepository, SQLitePedidoRepository, SQLiteOutboxRepository
from src.services import ColmenaService, PasarelaPagosMock, SagaOrquestadorMatriculaPedido
from src import app
from src.auth import TokenBucketRateLimiter, hash_password, verify_password
from src.events import crear_cloudevent, event_bus_global
from src.outbox import OutboxWorker, RabbitMQPublisher


class TestAplicacionWeb(unittest.TestCase):
    def test_inicializa_la_base_de_datos_configurada(self):
        self.assertEqual(app.DB, os.path.join(BASE_DIR, "LaColmena.db"))
        with tempfile.TemporaryDirectory() as test_dir:
            db_path = os.path.join(test_dir, "LaColmena.db")
            with patch.object(app, "DB", db_path), patch.object(app, "HTTPServer") as server, patch.dict(
                os.environ, {"LACOLMENA_INITIAL_ADMIN_PASSWORD": "LocalAdminPass2026!"}
            ):
                app.run_server(host="127.0.0.1", port=0)
                server.assert_called_once_with(("127.0.0.1", 0), app.H)
                server.return_value.serve_forever.assert_called_once()

            SQLiteProductoRepository(db_path)
            SQLitePedidoRepository(db_path)
            SQLiteOutboxRepository(db_path)

            with closing(sqlite3.connect(db_path)) as conn:
                productos = conn.execute("SELECT COUNT(*) FROM productos").fetchone()[0]
                self.assertEqual(productos, 7)
                tablas = {
                    row[0] for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                self.assertTrue({
                    "productos",
                    "pedidos",
                    "catalogo_productos",
                    "ordenes_pedidos",
                    "outbox_events",
                }.issubset(tablas))
                self.assertIsNotNone(
                    conn.execute(
                        "SELECT 1 FROM usuarios WHERE email = ?",
                        ("demo@lacolmena.co",)
                    ).fetchone()
                )

class TestAutenticacionSegura(unittest.TestCase):
    def test_hash_usa_salt_y_verifica_sin_guardar_la_contrasena(self):
        first_hash = hash_password("UnaClaveSegura2026!")
        second_hash = hash_password("UnaClaveSegura2026!")

        self.assertNotEqual(first_hash, second_hash)
        self.assertTrue(first_hash.startswith("$argon2id$"))
        self.assertNotIn("UnaClaveSegura2026!", first_hash)
        self.assertTrue(verify_password("UnaClaveSegura2026!", first_hash))
        self.assertFalse(verify_password("otra-clave", first_hash))

    def test_login_actualiza_hash_pbkdf2_a_argon2id(self):
        with tempfile.TemporaryDirectory() as test_dir:
            self._start_test_server(os.path.join(test_dir, "legacy_hash.db"))
            password = "LegacyLogin2026!"
            salt = b"legacy-salt-1234"
            iterations = 600_000
            derived_key = hashlib.pbkdf2_hmac(
                "sha256", password.encode("utf-8"), salt, iterations
            )

            def encode(value):
                return base64.urlsafe_b64encode(value).decode("ascii")

            legacy_hash = "$".join(
                (
                    "pbkdf2_sha256",
                    str(iterations),
                    encode(salt),
                    encode(derived_key),
                )
            )
            with closing(sqlite3.connect(self.db_path)) as connection:
                connection.execute(
                    "UPDATE usuarios SET password_hash = ? WHERE email = ?",
                    (legacy_hash, "demo@lacolmena.co"),
                )
                connection.commit()

            status, _, _ = self._request(
                "POST",
                "/api/login",
                {"email": "demo@lacolmena.co", "password": password},
            )
            self.assertEqual(status, 200)
            with closing(sqlite3.connect(self.db_path)) as connection:
                upgraded_hash = connection.execute(
                    "SELECT password_hash FROM usuarios WHERE email = ?",
                    ("demo@lacolmena.co",),
                ).fetchone()[0]
            self.assertTrue(upgraded_hash.startswith("$argon2id$"))
            self.assertTrue(verify_password(password, upgraded_hash))

    def test_token_bucket_devuelve_retry_after_y_recarga(self):
        now = [100.0]
        limiter = TokenBucketRateLimiter(
            capacity=2, refill_seconds=10, clock=lambda: now[0]
        )

        self.assertEqual(limiter.consume("127.0.0.1"), (True, 0))
        self.assertEqual(limiter.consume("127.0.0.1"), (True, 0))
        self.assertEqual(limiter.consume("127.0.0.1"), (False, 10))
        now[0] += 10
        self.assertEqual(limiter.consume("127.0.0.1"), (True, 0))

    def test_migra_contrasena_heredada_y_limpia_texto_plano(self):
        with tempfile.TemporaryDirectory() as test_dir:
            db_path = os.path.join(test_dir, "legacy.db")
            with closing(sqlite3.connect(db_path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE usuarios (
                        id INTEGER PRIMARY KEY,
                        nombre TEXT,
                        email TEXT UNIQUE,
                        password TEXT,
                        rol TEXT
                    )
                    """
                )
                connection.execute(
                    "INSERT INTO usuarios VALUES (?, ?, ?, ?, ?)",
                    (7, "Usuario legado", "legacy@example.com", "LegacyPass2026!", "Vendedor"),
                )
                connection.commit()

            with patch.object(app, "DB", db_path):
                app.init_db()

            with closing(sqlite3.connect(db_path)) as connection:
                legacy_password, stored_hash = connection.execute(
                    "SELECT password, password_hash FROM usuarios WHERE id = ?",
                    (7,),
                ).fetchone()
            self.assertIsNone(legacy_password)
            self.assertTrue(stored_hash.startswith("$argon2id$"))
            self.assertTrue(verify_password("LegacyPass2026!", stored_hash))

    def test_login_sesion_cookie_proteccion_y_logout(self):
        with tempfile.TemporaryDirectory() as test_dir:
            self._start_test_server(os.path.join(test_dir, "auth.db"))
            status, _, _ = self._request("GET", "/api/productos")
            self.assertEqual(status, 401)

            status, failed_login, _ = self._request(
                "POST",
                "/api/login",
                {
                    "email": "' OR '1'='1",
                    "password": "LocalAdminPass2026!",
                },
            )
            self.assertEqual(status, 401)
            self.assertFalse(failed_login["ok"])

            status, _, headers = self._request(
                "POST",
                "/api/login",
                {"email": "demo@lacolmena.co", "password": "LocalAdminPass2026!"},
            )
            self.assertEqual(status, 200)
            self.assertIn("X-Content-Type-Options", headers)
            self.assertIn("Content-Security-Policy", headers)
            cookie = headers["Set-Cookie"]
            self.assertIn("HttpOnly", cookie)
            self.assertIn("Secure", cookie)
            self.assertIn("SameSite=Strict", cookie)
            cookie_pair = cookie.split(";", 1)[0]

            status, created, _ = self._request(
                "POST",
                "/api/usuarios",
                {
                    "nombre": "Prueba'); DROP TABLE productos; --",
                    "email": "seller@example.com",
                    "password": "SellerStrongPass2026!",
                },
                cookie=cookie_pair,
            )
            self.assertEqual(status, 200)
            self.assertTrue(created["ok"])
            with closing(sqlite3.connect(self.db_path)) as connection:
                password_hash = connection.execute(
                    "SELECT password_hash FROM usuarios WHERE email = ?",
                    ("seller@example.com",),
                ).fetchone()[0]
                product_count = connection.execute(
                    "SELECT COUNT(*) FROM productos"
                ).fetchone()[0]
            self.assertNotEqual(password_hash, "SellerStrongPass2026!")
            self.assertTrue(password_hash.startswith("$argon2id$"))
            self.assertTrue(verify_password("SellerStrongPass2026!", password_hash))
            self.assertEqual(product_count, 7)

            status, products, _ = self._request(
                "GET", "/api/productos", cookie=cookie_pair
            )
            self.assertEqual(status, 200)
            self.assertEqual(len(products), 7)

            with patch.object(
                app, "DB", os.path.join(test_dir, "missing", "auth.db")
            ):
                status, error, _ = self._request(
                    "GET", "/api/productos", cookie=cookie_pair
                )
            self.assertEqual(status, 500)
            self.assertEqual(error["error"], "Error interno del servidor")
            self.assertNotIn("Traceback", str(error))
            self.assertNotIn("OperationalError", str(error))

            status, _, logout_headers = self._request(
                "POST", "/api/logout", cookie=cookie_pair
            )
            self.assertEqual(status, 200)
            self.assertIn("Max-Age=0", logout_headers["Set-Cookie"])
            status, _, _ = self._request(
                "GET", "/api/productos", cookie=cookie_pair
            )
            self.assertEqual(status, 401)

    def test_login_limita_intentos_con_http_429(self):
        with tempfile.TemporaryDirectory() as test_dir:
            self._start_test_server(os.path.join(test_dir, "rate_limit.db"))
            for _ in range(5):
                status, _, _ = self._request(
                    "POST",
                    "/api/login",
                    {"email": "demo@lacolmena.co", "password": "incorrecta"},
                )
                self.assertEqual(status, 401)

            status, data, headers = self._request(
                "POST",
                "/api/login",
                {"email": "demo@lacolmena.co", "password": "incorrecta"},
            )
            self.assertEqual(status, 429)
            self.assertGreaterEqual(int(headers["Retry-After"]), 1)
            self.assertFalse(data["ok"])

    def test_permisos_admin_vendedor_y_cliente(self):
        with tempfile.TemporaryDirectory() as test_dir:
            self._start_test_server(os.path.join(test_dir, "roles.db"))
            status, _, admin_headers = self._request(
                "POST",
                "/api/login",
                {"email": "demo@lacolmena.co", "password": "LocalAdminPass2026!"},
            )
            self.assertEqual(status, 200)
            admin_cookie = admin_headers["Set-Cookie"].split(";", 1)[0]

            for name, email, password, role in (
                ("Vendedora", "seller@example.com", "SellerStrongPass2026!", "Vendedor"),
                ("Cliente", "customer@example.com", "CustomerStrongPass2026!", "Cliente"),
            ):
                status, result, _ = self._request(
                    "POST",
                    "/api/usuarios",
                    {
                        "nombre": name,
                        "email": email,
                        "password": password,
                        "rol": role,
                    },
                    cookie=admin_cookie,
                )
                self.assertEqual(status, 200)
                self.assertTrue(result["ok"])

            with closing(sqlite3.connect(self.db_path)) as connection:
                seller_id = connection.execute(
                    "SELECT id FROM usuarios WHERE email = ?",
                    ("seller@example.com",),
                ).fetchone()[0]

            status, _, seller_headers = self._request(
                "POST",
                "/api/login",
                {"email": "seller@example.com", "password": "SellerStrongPass2026!"},
            )
            self.assertEqual(status, 200)
            seller_cookie = seller_headers["Set-Cookie"].split(";", 1)[0]
            status, seller_products, _ = self._request(
                "GET", "/api/productos", cookie=seller_cookie
            )
            self.assertEqual(status, 200)
            self.assertIn("stock", seller_products[0])
            status, _, _ = self._request(
                "POST",
                "/api/productos/ajustar",
                {"id": 1, "stock": 1},
                cookie=seller_cookie,
            )
            self.assertEqual(status, 403)
            status, _, _ = self._request(
                "POST",
                "/api/usuarios",
                {
                    "nombre": "No autorizado",
                    "email": "blocked@example.com",
                    "password": "BlockedStrongPass2026!",
                },
                cookie=seller_cookie,
            )
            self.assertEqual(status, 403)

            status, _, client_headers = self._request(
                "POST",
                "/api/login",
                {
                    "email": "customer@example.com",
                    "password": "CustomerStrongPass2026!",
                },
            )
            self.assertEqual(status, 200)
            client_cookie = client_headers["Set-Cookie"].split(";", 1)[0]
            status, client_products, _ = self._request(
                "GET", "/api/productos", cookie=client_cookie
            )
            self.assertEqual(status, 200)
            self.assertEqual(
                set(client_products[0]),
                {"id", "nombre", "categoria", "precio"},
            )
            status, _, _ = self._request(
                "GET", "/api/resumen", cookie=client_cookie
            )
            self.assertEqual(status, 403)
            status, _, _ = self._request(
                "GET", "/api/ventas", cookie=client_cookie
            )
            self.assertEqual(status, 403)
            status, purchase, _ = self._request(
                "POST",
                "/api/ventas",
                {"producto_id": 1, "cantidad": 2, "cliente": "Cuenta ajena"},
                cookie=client_cookie,
            )
            self.assertEqual(status, 200)
            self.assertEqual(purchase["total"], 10000)
            with closing(sqlite3.connect(self.db_path)) as connection:
                buyer, stock = connection.execute(
                    """
                    SELECT ventas.cliente, productos.stock
                    FROM ventas CROSS JOIN productos
                    WHERE ventas.id = (SELECT MAX(id) FROM ventas)
                      AND productos.id = ?
                    """,
                    (1,),
                ).fetchone()
                sale_event = connection.execute(
                    """
                    SELECT payload_json FROM outbox_events
                    WHERE event_type = 'bina.la_colmena.venta.registrada'
                    ORDER BY id DESC LIMIT 1
                    """
                ).fetchone()[0]
            self.assertEqual(buyer, "customer@example.com")
            self.assertEqual(stock, 118)
            self.assertEqual(json.loads(sale_event)["source"], "bina/la-colmena")

            status, _, _ = self._request(
                "POST",
                "/api/productos/ajustar",
                {"id": 1, "stock": 130},
                cookie=admin_cookie,
            )
            self.assertEqual(status, 200)
            status, _, _ = self._request(
                "DELETE", f"/api/usuarios/{seller_id}", cookie=admin_cookie
            )
            self.assertEqual(status, 200)
            status, _, _ = self._request(
                "GET", "/api/productos", cookie=seller_cookie
            )
            self.assertEqual(status, 401)
            status, _, _ = self._request(
                "DELETE", "/api/usuarios/1", cookie=admin_cookie
            )
            self.assertEqual(status, 400)

    def test_venta_se_revierte_si_falla_el_insert_del_outbox(self):
        with tempfile.TemporaryDirectory() as test_dir:
            self._start_test_server(os.path.join(test_dir, "outbox_rollback.db"))
            status, _, headers = self._request(
                "POST",
                "/api/login",
                {"email": "demo@lacolmena.co", "password": "LocalAdminPass2026!"},
            )
            self.assertEqual(status, 200)
            admin_cookie = headers["Set-Cookie"].split(";", 1)[0]

            with closing(sqlite3.connect(self.db_path)) as connection:
                connection.execute(
                    """
                    CREATE TRIGGER reject_sale_outbox
                    BEFORE INSERT ON outbox_events
                    WHEN NEW.event_type = 'bina.la_colmena.venta.registrada'
                    BEGIN
                        SELECT RAISE(ABORT, 'outbox unavailable');
                    END
                    """
                )
                connection.commit()

            status, _, _ = self._request(
                "POST",
                "/api/ventas",
                {"producto_id": 1, "cantidad": 2},
                cookie=admin_cookie,
            )
            self.assertEqual(status, 500)
            with closing(sqlite3.connect(self.db_path)) as connection:
                stock = connection.execute(
                    "SELECT stock FROM productos WHERE id = ?", (1,)
                ).fetchone()[0]
                sales_count = connection.execute(
                    "SELECT COUNT(*) FROM ventas"
                ).fetchone()[0]
                event_count = connection.execute(
                    """
                    SELECT COUNT(*) FROM outbox_events
                    WHERE event_type = 'bina.la_colmena.venta.registrada'
                    """
                ).fetchone()[0]
            self.assertEqual(stock, 120)
            self.assertEqual(sales_count, 3)
            self.assertEqual(event_count, 0)

    def _start_test_server(self, db_path):
        self.db_path = db_path
        self.db_patch = patch.object(app, "DB", db_path)
        self.db_patch.start()
        self.rate_limiter = app.LOGIN_RATE_LIMITER
        app.LOGIN_RATE_LIMITER = TokenBucketRateLimiter()
        with patch.dict(
            os.environ,
            {"LACOLMENA_INITIAL_ADMIN_PASSWORD": "LocalAdminPass2026!"},
        ):
            app.init_db()
        self.server = app.HTTPServer(("127.0.0.1", 0), app.H)
        self.server_thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.server_thread.start()
        self.server_port = self.server.server_address[1]
        self.addCleanup(self._stop_test_server)

    def _request(self, method, path, data=None, cookie=None):
        connection = HTTPConnection("127.0.0.1", self.server_port, timeout=3)
        headers = {}
        body = None
        if data is not None:
            body = json.dumps(data).encode("utf-8")
            headers["Content-Type"] = "application/json"
        if cookie:
            headers["Cookie"] = cookie
        connection.request(method, path, body=body, headers=headers)
        response = connection.getresponse()
        payload = response.read()
        response_headers = dict(response.getheaders())
        connection.close()
        return response.status, json.loads(payload or b"{}"), response_headers

    def _stop_test_server(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)
        self.db_patch.stop()
        app.LOGIN_RATE_LIMITER = self.rate_limiter


class TestBusEventos(unittest.TestCase):
    def test_cloudevent_cumple_campos_canónicos_y_origen(self):
        event = crear_cloudevent(
            "bina.la_colmena.pedido.creado",
            {"order_id": "PED-TEST-001"},
        )
        payload = event.to_dict()

        self.assertEqual(
            set(payload),
            {
                "specversion",
                "id",
                "source",
                "type",
                "datacontenttype",
                "data",
            },
        )
        self.assertEqual(payload["specversion"], "1.0")
        self.assertEqual(uuid.UUID(payload["id"]).version, 4)
        self.assertTrue(payload["source"].startswith("bina"))
        self.assertEqual(payload["datacontenttype"], "application/json")
        self.assertEqual(json.loads(event.to_json()), payload)

    def test_publica_eventos_a_suscriptores(self):
        recibido = threading.Event()
        tipo_evento = f"prueba.{uuid.uuid4()}"
        event_bus_global.subscribe(tipo_evento, lambda evento: recibido.set())

        event_bus_global.publish(crear_cloudevent(tipo_evento, {}))

        self.assertTrue(recibido.wait(timeout=2))


class TestValidacionesNegocio(unittest.TestCase):
    """Pruebas para el módulo de validación defensiva (Fail-Fast)."""

    def test_cliente_id_valido_e_invalido(self):
        # Casos válidos
        self.assertEqual(Validator.validar_cliente_id("1098765432"), "1098765432")
        self.assertEqual(Validator.validar_cliente_id("CLI-001_A"), "CLI-001_A")

        # Casos inválidos (vacío, caracteres sospechosos, muy corto)
        with self.assertRaises(ValidationError):
            Validator.validar_cliente_id("")
        with self.assertRaises(ValidationError):
            Validator.validar_cliente_id("ab")  # Demasiado corto
        with self.assertRaises(ValidationError):
            Validator.validar_cliente_id("1098; DROP TABLE")  # Inyección o inválido

    def test_idempotency_key_validacion(self):
        self.assertEqual(Validator.validar_idempotency_key("IDEM-12345678"), "IDEM-12345678")
        with self.assertRaises(ValidationError):
            Validator.validar_idempotency_key("corta")  # Menos de 8 chars

    def test_items_pedido_validacion(self):
        # Lista vacía
        with self.assertRaises(ValidationError):
            Validator.validar_items_pedido([])

        # Cantidad negativa o cero
        with self.assertRaises(ValidationError):
            Validator.validar_items_pedido([{"codigo": "MED-001", "cantidad": 0}])

        # Códigos duplicados en misma orden
        with self.assertRaises(ValidationError):
            Validator.validar_items_pedido([
                {"codigo": "MED-001", "cantidad": 2},
                {"codigo": "MED-001", "cantidad": 1}
            ])

        # Exceder límite minorista
        with self.assertRaises(ValidationError):
            Validator.validar_items_pedido([{"codigo": "MED-001", "cantidad": 9999}])

class TestPersistenciaYACID(unittest.TestCase):
    """Pruebas de persistencia e integridad ACID en SQLite aislado."""

    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.test_dir.name, "test_lacolmena.db")
        self.prod_repo = SQLiteProductoRepository(self.db_path)
        self.ped_repo = SQLitePedidoRepository(self.db_path)
        self.outbox_repo = SQLiteOutboxRepository(self.db_path)

    def tearDown(self):
        self.test_dir.cleanup()

    def test_seed_productos_y_actualizacion_stock(self):
        prods = self.prod_repo.obtener_todos()
        self.assertEqual(len(prods), 7, "El catálogo inicial debe sembrar 7 productos")
        
        # Buscar por código
        arroz = self.prod_repo.buscar_por_codigo("SUP-001")
        self.assertIsNotNone(arroz)
        self.assertEqual(arroz.stock_disponible, 50)

        # Actualizar stock
        self.prod_repo.actualizar_stock("SUP-001", 45)
        arroz_actualizado = self.prod_repo.buscar_por_codigo("SUP-001")
        self.assertEqual(arroz_actualizado.stock_disponible, 45)

    def test_guardar_y_recuperar_pedido(self):
        pedido = Pedido(
            id_pedido="PED-TEST-001",
            cliente_id="1098765432",
            cliente_nombre="Carlos Alférez",
            cliente_direccion="Calle 45 # 12-30",
            items=[ItemPedido("SUP-001", 2, 5000.0)],
            estado=EstadoPedido.RESERVADO_SEMANTIC_LOCK,
            idempotency_key="IDEM-TEST-1111"
        )
        pedido.calcular_total()
        self.ped_repo.guardar(pedido)

        recuperado = self.ped_repo.buscar_por_id("PED-TEST-001")
        self.assertIsNotNone(recuperado)
        self.assertEqual(recuperado.total, 10000.0)
        self.assertEqual(recuperado.estado, EstadoPedido.RESERVADO_SEMANTIC_LOCK)
        self.assertEqual(len(recuperado.items), 1)

    def test_outbox_transactional_pattern(self):
        event = crear_cloudevent(
            "bina.la_colmena.pedido.creado",
            {"monto": 50000},
        )
        self.outbox_repo.registrar_evento(
            "PED-TEST-001", event.to_dict()
        )
        pendientes = self.outbox_repo.obtener_pendientes()
        self.assertEqual(len(pendientes), 1)
        self.assertEqual(pendientes[0]["aggregate_id"], "PED-TEST-001")
        self.assertEqual(pendientes[0]["payload"], event.to_dict())

        event_id = pendientes[0]["id"]
        self.outbox_repo.marcar_publicado(event_id)
        self.assertEqual(len(self.outbox_repo.obtener_pendientes()), 0)
        with closing(sqlite3.connect(self.db_path)) as connection:
            status, published, processed_at = connection.execute(
                """
                SELECT status, published, processed_at
                FROM outbox_events WHERE id = ?
                """,
                (event_id,),
            ).fetchone()
        self.assertEqual(status, "PROCESSED")
        self.assertEqual(published, 1)
        self.assertIsNotNone(processed_at)

    def test_migra_outbox_legacy_a_eventos_cloud(self):
        legacy_path = os.path.join(self.test_dir.name, "legacy_outbox.db")
        with closing(sqlite3.connect(legacy_path)) as connection:
            connection.execute(
                """
                CREATE TABLE outbox_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    published INTEGER DEFAULT 0
                )
                """
            )
            connection.executemany(
                """
                INSERT INTO outbox_events (
                    aggregate_id, event_type, payload_json, timestamp, published
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (
                        "PED-LEGACY-1",
                        "co.edu.uniremington.LaColmena.pedido.creado",
                        json.dumps({"order_id": "PED-LEGACY-1"}),
                        1.0,
                        0,
                    ),
                    (
                        "PED-LEGACY-2",
                        "co.edu.uniremington.LaColmena.pedido.creado",
                        json.dumps({"order_id": "PED-LEGACY-2"}),
                        2.0,
                        1,
                    ),
                ],
            )
            connection.commit()

        legacy_repo = SQLiteOutboxRepository(legacy_path)
        pending = legacy_repo.obtener_pendientes()
        self.assertEqual(len(pending), 1)
        self.assertEqual(
            pending[0]["payload"]["source"], "bina/la-colmena"
        )
        self.assertEqual(
            pending[0]["event_type"], "bina.la_colmena.pedido.creado"
        )
        self.assertEqual(
            pending[0]["payload"]["type"], "bina.la_colmena.pedido.creado"
        )
        self.assertEqual(
            pending[0]["payload"]["data"]["order_id"], "PED-LEGACY-1"
        )
        self.assertEqual(uuid.UUID(pending[0]["payload"]["id"]).version, 4)
        with closing(sqlite3.connect(legacy_path)) as connection:
            old_processed_status = connection.execute(
                "SELECT status FROM outbox_events WHERE id = 2"
            ).fetchone()[0]
        self.assertEqual(old_processed_status, "PROCESSED")


class TestOutboxWorker(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.test_dir.name, "outbox_worker.db")
        self.outbox_repo = SQLiteOutboxRepository(self.db_path)

    def tearDown(self):
        self.test_dir.cleanup()

    def test_publica_y_marca_processed_tras_confirmacion(self):
        event = crear_cloudevent(
            "bina.la_colmena.venta.registrada",
            {"sale_id": 1},
        )
        self.outbox_repo.registrar_evento("1", event.to_dict())

        class FakePublisher:
            def __init__(self):
                self.events = []

            def publish(self, published_event):
                self.events.append(published_event)

        publisher = FakePublisher()
        worker = OutboxWorker(self.outbox_repo, publisher)

        self.assertEqual(worker.process_pending(), 1)
        self.assertEqual(publisher.events, [event.to_dict()])
        self.assertEqual(self.outbox_repo.obtener_pendientes(), [])

    def test_fallo_de_publicacion_conserva_el_evento_pendiente(self):
        event = crear_cloudevent(
            "bina.la_colmena.venta.registrada",
            {"sale_id": 2},
        )
        event_id = self.outbox_repo.registrar_evento("2", event.to_dict())

        class FailingPublisher:
            def publish(self, published_event):
                raise OSError("broker no disponible")

        worker = OutboxWorker(self.outbox_repo, FailingPublisher())

        self.assertEqual(worker.process_pending(), 0)
        pending = self.outbox_repo.obtener_pendientes()
        self.assertEqual([row["id"] for row in pending], [event_id])
        self.assertEqual(pending[0]["attempts"], 1)
        self.assertEqual(pending[0]["last_error"], "OSError('broker no disponible')")

    def test_rabbitmq_publica_json_cloud_event_durable(self):
        event = crear_cloudevent(
            "bina.la_colmena.venta.registrada",
            {"sale_id": 3},
        ).to_dict()
        with patch("src.outbox.pika.BlockingConnection") as connection_factory:
            channel = connection_factory.return_value.channel.return_value
            publisher = RabbitMQPublisher("amqp://user:pass@localhost:5672/%2F")

            publisher.publish(event)

        channel.confirm_delivery.assert_called_once()
        publish_args = channel.basic_publish.call_args.kwargs
        self.assertEqual(publish_args["routing_key"], event["type"])
        self.assertTrue(publish_args["mandatory"])
        self.assertEqual(json.loads(publish_args["body"]), event)
        self.assertEqual(
            publish_args["properties"].content_type,
            "application/cloudevents+json",
        )
        self.assertEqual(publish_args["properties"].delivery_mode, 2)

class TestSagaYLogicaNegocio(unittest.TestCase):
    """Pruebas del Orquestador Saga, Idempotencia y Compensación Inversa C1."""

    def setUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.test_dir.name, "test_lacolmena_saga.db")
        self.prod_repo = SQLiteProductoRepository(self.db_path)
        self.ped_repo = SQLitePedidoRepository(self.db_path)
        self.outbox_repo = SQLiteOutboxRepository(self.db_path)
        self.service = ColmenaService(
            self.prod_repo,
            self.ped_repo,
            self.outbox_repo,
            db_path=self.db_path,
        )
        self.pasarela = PasarelaPagosMock()
        self.saga = SagaOrquestadorMatriculaPedido(self.service, self.pasarela)

    def tearDown(self):
        self.test_dir.cleanup()

    def test_pedido_y_stock_se_revierten_si_no_se_puede_guardar_evento(self):
        initial_stock = self.prod_repo.buscar_por_codigo("SUP-001").stock_disponible
        with closing(sqlite3.connect(self.db_path)) as connection:
            connection.execute(
                """
                CREATE TRIGGER reject_order_outbox
                BEFORE INSERT ON outbox_events
                WHEN NEW.event_type = 'bina.la_colmena.pedido.creado'
                BEGIN
                    SELECT RAISE(ABORT, 'outbox unavailable');
                END
                """
            )
            connection.commit()

        with self.assertRaises(sqlite3.IntegrityError):
            self.service.crear_pedido_monolito(
                cliente_id="1098765432",
                cliente_nombre="Prueba de rollback",
                direccion="Calle 1",
                items_req=[{"codigo": "SUP-001", "cantidad": 3}],
                idempotency_key="IDEM-OUTBOX-ROLLBACK",
            )

        self.assertEqual(
            self.prod_repo.buscar_por_codigo("SUP-001").stock_disponible,
            initial_stock,
        )
        self.assertIsNone(
            self.ped_repo.buscar_por_idempotency_key("IDEM-OUTBOX-ROLLBACK")
        )
        self.assertEqual(self.outbox_repo.obtener_pendientes(), [])

    def test_saga_flujo_feliz_completado(self):
        """Caso 1: T1 -> T2 -> T3 completado satisfactoriamente."""
        stock_inicial = self.prod_repo.buscar_por_codigo("SUP-002").stock_disponible
        
        resultado = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Laura Gómez",
            direccion="Carrera 15 # 45-20",
            items_req=[{"codigo": "SUP-002", "cantidad": 3}],
            idempotency_key="IDEM-EXITO-001",
            simular_falla_pago=False
        )

        self.assertTrue(resultado["exito"])
        pedido = resultado["pedido"]
        self.assertEqual(pedido.estado, EstadoPedido.COMPLETADO)

        # Stock descontado: 150 - 3 = 147
        stock_actual = self.prod_repo.buscar_por_codigo("SUP-002").stock_disponible
        self.assertEqual(stock_actual, stock_inicial - 3)
        event_types = {
            event["event_type"]
            for event in self.outbox_repo.obtener_pendientes()
        }
        self.assertTrue(
            {
                "bina.la_colmena.inventario.reservado",
                "bina.la_colmena.pedido.creado",
                "bina.la_colmena.pedido.pagado",
                "bina.la_colmena.pedido.completado",
            }.issubset(event_types)
        )

    def test_saga_compensacion_inversa_cuando_falla_pago(self):
        """Caso 2: T1 (Reserva) -> T2 Falla -> C1 (Compensación de stock y estado COMPENSADO)."""
        stock_inicial = self.prod_repo.buscar_por_codigo("SUP-001").stock_disponible

        resultado = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Marcos Ruiz",
            direccion="Avenida Santander # 10",
            items_req=[{"codigo": "SUP-001", "cantidad": 5}],
            idempotency_key="IDEM-FALLA-PAGO-001",
            simular_falla_pago=True  # Provoca rechazo en pasarela
        )

        self.assertFalse(resultado["exito"])
        pedido = resultado["pedido"]
        self.assertEqual(pedido.estado, EstadoPedido.COMPENSADO)

        # Verificar que el stock fue DEVUELTO intacto por C1
        stock_final = self.prod_repo.buscar_por_codigo("SUP-001").stock_disponible
        self.assertEqual(stock_final, stock_inicial, "El stock debe regresar al valor original tras la compensación C1")

    def test_garantia_de_idempotencia(self):
        """Caso 3: Dos peticiones con idéntica idempotency_key deben retornar el mismo pedido sin doble descuento."""
        stock_inicial = self.prod_repo.buscar_por_codigo("SUP-003").stock_disponible
        key_unica = "IDEM-REPEAT-777"

        res_1 = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Valeria Ríos",
            direccion="Calle 10 # 5-10",
            items_req=[{"codigo": "SUP-003", "cantidad": 2}],
            idempotency_key=key_unica,
            simular_falla_pago=False
        )

        # Segunda petición idéntica
        res_2 = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Valeria Ríos",
            direccion="Calle 10 # 5-10",
            items_req=[{"codigo": "SUP-003", "cantidad": 2}],
            idempotency_key=key_unica,
            simular_falla_pago=False
        )

        self.assertEqual(res_1["pedido"].id_pedido, res_2["pedido"].id_pedido)
        stock_final = self.prod_repo.buscar_por_codigo("SUP-003").stock_disponible
        # Debe haberse descontado SOLAMENTE 2 unidades, no 4
        self.assertEqual(stock_final, stock_inicial - 2)

if __name__ == "__main__":
    print("=" * 80)
    print("🧪 EJECUTANDO SUITE DE PRUEBAS AUTOMATIZADAS DE LaColmena")
    print("Cátedra: Ingeniería de Software 3 [IF0129] - Uniremington")
    print("=" * 80)
    unittest.main(verbosity=2)
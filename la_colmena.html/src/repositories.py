"""
========================================================================================
CAPA DE ACCESO A DATOS (PERSISTENCIA ACID CON SQLITE NATIVO - CERO LIBRERÍAS)
Cátedra: Ingeniería de Software 3 [IF0129] - Uniremington
Garantías: ACID (Atomicidad, Consistencia, Aislamiento, Durabilidad)
Patrón: Repository, Transactional Outbox Pattern
Dependencias: NINGUNA (Python Standard Library `sqlite3`, `json`, `time`, `typing`)
========================================================================================
"""
import sqlite3
import json
import time
from contextlib import nullcontext
from typing import Optional, List, Dict, Any
from src.models import Producto, Pedido, ItemPedido, EstadoPedido
from src.interfaces import IProductoRepository, IPedidoRepository, IOutboxRepository
from src.logger import get_logger
from src.database import DB_PATH, database_connection
from src.events import crear_cloudevent

logger = get_logger("Repositories")


def insertar_evento_outbox(
    connection: sqlite3.Connection,
    aggregate_id: str,
    event: Dict[str, Any],
) -> int:
    event_json = json.dumps(
        event,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )
    cursor = connection.execute(
        """
        INSERT INTO outbox_events (
            aggregate_id, event_type, payload_json, timestamp, published,
            status, attempts
        ) VALUES (?, ?, ?, ?, 0, 'PENDING', 0)
        """,
        (aggregate_id, event["type"], event_json, time.time()),
    )
    return cursor.lastrowid


class SQLiteProductoRepository(IProductoRepository):
    """
    Repositorio de Inventario y Catalogo de Productos.
    Maneja el stock disponible y las transacciones de reserva.
    """
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Inicializa la tabla y catalogo semilla con aislamiento WAL para alta concurrencia."""
        with database_connection(self.db_path) as conn:
            cursor = conn.cursor()
            # Habilitar WAL (Write-Ahead Logging) para concurrencia limpia
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS catalogo_productos (
                    codigo TEXT PRIMARY KEY,
                    nombre TEXT NOT NULL,
                    descripcion TEXT,
                    categoria TEXT,
                    precio REAL NOT NULL,
                    stock_disponible INTEGER NOT NULL,
                    requiere_receta INTEGER DEFAULT 0
                )
            """)
            conn.commit()
            self._seed_data(cursor, conn)

    def _seed_data(self, cursor: sqlite3.Cursor, conn: sqlite3.Connection):
        """Poblar datos iniciales canónicos si la tabla está vacía."""
        cursor.execute("SELECT COUNT(*) FROM catalogo_productos")
        if cursor.fetchone()[0] == 0:
            catalogo_inicial = [
                    ("SUP-001", "Arroz 500g", "Arroz blanco premium", "Alimentos", 5000.0, 50, 0),
                    ("SUP-002", "Leche 1L", "Leche entera pasteurizada", "Lacteos", 4500.0, 150, 0),
                    ("SUP-003", "Cafe 500g", "Cafe molido tradicional", "Alimentos", 12000.0, 80, 0),
                    ("SUP-004", "Aceite 1L", "Aceite vegetal", "Alimentos", 9000.0, 60, 0),
                    ("SUP-005", "Huevos x12", "Huevos frescos", "Alimentos", 8000.0, 40, 0),
                    ("SUP-006", "Jabon", "Jabon de baño", "Aseo", 3500.0, 100, 0),
                    ("SUP-007", "Papel higienico", "Papel higienico x4", "Hogar", 6000.0, 90, 0),
                ]
            cursor.executemany("""
                INSERT INTO catalogo_productos (codigo, nombre, descripcion, categoria, precio, stock_disponible, requiere_receta)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, catalogo_inicial)
            conn.commit()
            logger.info("Catalogo inicial sembrado con éxito (7 productos de prueba).")

    def obtener_todos(self) -> List[Producto]:
        """Consulta todos los productos disponibles."""
        with database_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT codigo, nombre, descripcion, categoria, precio, stock_disponible, requiere_receta FROM catalogo_productos")
            filas = cursor.fetchall()
            return [Producto(r[0], r[1], r[2], r[3], r[4], r[5], bool(r[6])) for r in filas]

    def buscar_por_codigo(
        self,
        codigo: str,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Optional[Producto]:
        """Búsqueda por clave primaria SKU."""
        context = (
            database_connection(self.db_path)
            if connection is None
            else nullcontext(connection)
        )
        with context as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT codigo, nombre, descripcion, categoria, precio, stock_disponible, requiere_receta FROM catalogo_productos WHERE codigo = ?", (codigo,))
            r = cursor.fetchone()
            if r:
                return Producto(r[0], r[1], r[2], r[3], r[4], r[5], bool(r[6]))
            return None

    def actualizar_stock(
        self,
        codigo: str,
        nuevo_stock: int,
        connection: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Actualiza el stock de forma atómica."""
        context = (
            database_connection(self.db_path)
            if connection is None
            else nullcontext(connection)
        )
        with context as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE catalogo_productos SET stock_disponible = ? WHERE codigo = ?", (nuevo_stock, codigo))
            return cursor.rowcount > 0


class SQLitePedidoRepository(IPedidoRepository):
    """
    Repositorio de Pedidos con persistencia de agregados completos en JSON.
    Garantiza durabilidad e integridad de la clave de idempotencia única.
    """
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with database_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ordenes_pedidos (
                    id_pedido TEXT PRIMARY KEY,
                    cliente_id TEXT NOT NULL,
                    cliente_nombre TEXT NOT NULL,
                    cliente_direccion TEXT NOT NULL,
                    estado TEXT NOT NULL,
                    total REAL NOT NULL,
                    idempotency_key TEXT UNIQUE NOT NULL,
                    timestamp REAL NOT NULL,
                    items_json TEXT NOT NULL,
                    historial_json TEXT NOT NULL
                )
            """)
            conn.commit()

    def guardar(
        self,
        pedido: Pedido,
        connection: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Guarda o actualiza el pedido en la base de datos."""
        context = (
            database_connection(self.db_path)
            if connection is None
            else nullcontext(connection)
        )
        with context as conn:
            cursor = conn.cursor()
            items_data = [{"codigo": it.producto_codigo, "cantidad": it.cantidad, "precio": it.precio_unitario} for it in pedido.items]
            cursor.execute("""
                INSERT OR REPLACE INTO ordenes_pedidos
                (id_pedido, cliente_id, cliente_nombre, cliente_direccion, estado, total, idempotency_key, timestamp, items_json, historial_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pedido.id_pedido,
                pedido.cliente_id,
                pedido.cliente_nombre,
                pedido.cliente_direccion,
                pedido.estado.value,
                pedido.total,
                pedido.idempotency_key,
                pedido.timestamp,
                json.dumps(items_data),
                json.dumps(pedido.historial_estados)
            ))
            return True

    def buscar_por_id(self, id_pedido: str) -> Optional[Pedido]:
        with database_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id_pedido, cliente_id, cliente_nombre, cliente_direccion, estado, total, idempotency_key, timestamp, items_json, historial_json 
                FROM ordenes_pedidos WHERE id_pedido = ?
            """, (id_pedido,))
            r = cursor.fetchone()
            if not r:
                return None
            items = [ItemPedido(it["codigo"], it["cantidad"], it["precio"]) for it in json.loads(r[8])]
            pedido = Pedido(
                id_pedido=r[0], cliente_id=r[1], cliente_nombre=r[2], cliente_direccion=r[3],
                items=items, estado=EstadoPedido(r[4]), total=r[5], idempotency_key=r[6],
                timestamp=r[7], historial_estados=json.loads(r[9])
            )
            return pedido

    def buscar_por_idempotency_key(
        self,
        key: str,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Optional[Pedido]:
        """Consulta deduplicadora para evitar reprocesamiento accidental."""
        context = (
            database_connection(self.db_path)
            if connection is None
            else nullcontext(connection)
        )
        with context as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id_pedido, cliente_id, cliente_nombre, cliente_direccion, estado, total, idempotency_key, timestamp, items_json, historial_json 
                FROM ordenes_pedidos WHERE idempotency_key = ?
            """, (key,))
            r = cursor.fetchone()
            if not r:
                return None
            items = [ItemPedido(it["codigo"], it["cantidad"], it["precio"]) for it in json.loads(r[8])]
            pedido = Pedido(
                id_pedido=r[0], cliente_id=r[1], cliente_nombre=r[2], cliente_direccion=r[3],
                items=items, estado=EstadoPedido(r[4]), total=r[5], idempotency_key=r[6],
                timestamp=r[7], historial_estados=json.loads(r[9])
            )
            return pedido

    def listar_todos(self) -> List[Pedido]:
        with database_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id_pedido, cliente_id, cliente_nombre, cliente_direccion, estado, total, idempotency_key, timestamp, items_json, historial_json 
                FROM ordenes_pedidos ORDER BY timestamp DESC
            """)
            filas = cursor.fetchall()
            pedidos = []
            for r in filas:
                items = [ItemPedido(it["codigo"], it["cantidad"], it["precio"]) for it in json.loads(r[8])]
                p = Pedido(
                    id_pedido=r[0], cliente_id=r[1], cliente_nombre=r[2], cliente_direccion=r[3],
                    items=items, estado=EstadoPedido(r[4]), total=r[5], idempotency_key=r[6],
                    timestamp=r[7], historial_estados=json.loads(r[9])
                )
                pedidos.append(p)
            return pedidos

    def actualizar_estado(self, id_pedido: str, nuevo_estado: str, motivo: str) -> bool:
        pedido = self.buscar_por_id(id_pedido)
        if not pedido:
            return False
        pedido.transicionar_a(EstadoPedido(nuevo_estado), motivo)
        return self.guardar(pedido)


class SQLiteOutboxRepository(IOutboxRepository):
    """
    Implementación del Transactional Outbox Pattern en SQLite.
    Garantiza que ningún evento de dominio se pierda o desincronice
    en arquitecturas orientadas a eventos (Dual-Write Prevention).
    """
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with database_connection(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    published INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    processed_at TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT
                )
            """)
            columns = {
                row[1] for row in cursor.execute(
                    "PRAGMA table_info(outbox_events)"
                )
            }
            additions = {
                "status": "TEXT NOT NULL DEFAULT 'PENDING'",
                "processed_at": "TEXT",
                "attempts": "INTEGER NOT NULL DEFAULT 0",
                "last_error": "TEXT",
            }
            for name, definition in additions.items():
                if name not in columns:
                    cursor.execute(
                        f"ALTER TABLE outbox_events ADD COLUMN {name} {definition}"
                    )
            if "published" in columns:
                cursor.execute(
                    """
                    UPDATE outbox_events SET status = 'PROCESSED'
                    WHERE published = 1 AND status = 'PENDING'
                    """
                )
            self._migrate_legacy_events(conn)

    def _migrate_legacy_events(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            "SELECT id, event_type, payload_json FROM outbox_events"
        ).fetchall()
        required_fields = {
            "specversion",
            "id",
            "source",
            "type",
            "datacontenttype",
            "data",
        }
        for row_id, event_type, payload_json in rows:
            payload = json.loads(payload_json)
            canonical_type = (
                isinstance(payload, dict)
                and isinstance(payload.get("type"), str)
                and payload["type"].startswith("bina.")
            )
            if (
                isinstance(payload, dict)
                and required_fields.issubset(payload)
                and isinstance(payload["source"], str)
                and payload["source"].startswith("bina")
                and canonical_type
            ):
                continue
            event_data = (
                payload["data"]
                if isinstance(payload, dict) and "data" in payload
                else payload
            )
            legacy_prefix = "co.edu.uniremington.LaColmena."
            if event_type.startswith(legacy_prefix):
                event_type = (
                    "bina.la_colmena."
                    + event_type[len(legacy_prefix):].lower()
                )
            elif not event_type.startswith("bina."):
                event_type = (
                    "bina.la_colmena." + event_type.strip(".").lower()
                )
            event = crear_cloudevent(event_type, event_data)
            connection.execute(
                """
                UPDATE outbox_events SET event_type = ?, payload_json = ?
                WHERE id = ?
                """,
                (event.type, event.to_json(), row_id),
            )

    def registrar_evento(
        self,
        aggregate_id: str,
        event: Dict[str, Any],
        connection: Optional[sqlite3.Connection] = None,
    ) -> int:
        context = (
            database_connection(self.db_path)
            if connection is None
            else nullcontext(connection)
        )
        with context as conn:
            return insertar_evento_outbox(conn, aggregate_id, event)

    def obtener_pendientes(self) -> List[Dict[str, Any]]:
        with database_connection(self.db_path) as conn:
            filas = conn.execute("""
                SELECT id, aggregate_id, event_type, payload_json, timestamp,
                       status, attempts, last_error
                FROM outbox_events
                WHERE status = 'PENDING'
                ORDER BY id ASC
            """).fetchall()
            return [{
                "id": r[0],
                "aggregate_id": r[1],
                "event_type": r[2],
                "payload": json.loads(r[3]),
                "timestamp": r[4],
                "status": r[5],
                "attempts": r[6],
                "last_error": r[7],
            } for r in filas]

    def marcar_publicado(self, event_id: int) -> bool:
        with database_connection(self.db_path) as conn:
            cursor = conn.execute(
                """
                UPDATE outbox_events
                SET published = 1, status = 'PROCESSED',
                    processed_at = datetime('now')
                WHERE id = ? AND status = 'PENDING'
                """,
                (event_id,),
            )
            return cursor.rowcount > 0

    def registrar_error(self, event_id: int, error: str) -> bool:
        with database_connection(self.db_path) as conn:
            cursor = conn.execute(
                """
                UPDATE outbox_events
                SET attempts = attempts + 1, last_error = ?
                WHERE id = ? AND status = 'PENDING'
                """,
                (error[:1000], event_id),
            )
            return cursor.rowcount > 0

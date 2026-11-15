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
from typing import Optional, List, Dict, Any
from src.models import Producto, Pedido, ItemPedido, EstadoPedido
from src.interfaces import IProductoRepository, IPedidoRepository, IOutboxRepository
from src.logger import get_logger

logger = get_logger("Repositories")

class SQLiteProductoRepository(IProductoRepository):
    """
    Repositorio de Inventario y Catalogo de Productos.
    Maneja el stock disponible y las transacciones de reserva.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        """Inicializa la tabla y catalogo semilla con aislamiento WAL para alta concurrencia."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Habilitar WAL (Write-Ahead Logging) para concurrencia limpia
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS productos (
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
        cursor.execute("SELECT COUNT(*) FROM productos")
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
                INSERT INTO productos (codigo, nombre, descripcion, categoria, precio, stock_disponible, requiere_receta)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, catalogo_inicial)
            conn.commit()
            logger.info("Catalogo inicial sembrado con éxito (7 productos de prueba).")

    def obtener_todos(self) -> List[Producto]:
        """Consulta todos los productos disponibles."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT codigo, nombre, descripcion, categoria, precio, stock_disponible, requiere_receta FROM productos")
            filas = cursor.fetchall()
            return [Producto(r[0], r[1], r[2], r[3], r[4], r[5], bool(r[6])) for r in filas]

    def buscar_por_codigo(self, codigo: str) -> Optional[Producto]:
        """Búsqueda por clave primaria SKU."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT codigo, nombre, descripcion, categoria, precio, stock_disponible, requiere_receta FROM productos WHERE codigo = ?", (codigo,))
            r = cursor.fetchone()
            if r:
                return Producto(r[0], r[1], r[2], r[3], r[4], r[5], bool(r[6]))
            return None

    def actualizar_stock(self, codigo: str, nuevo_stock: int) -> bool:
        """Actualiza el stock de forma atómica."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE productos SET stock_disponible = ? WHERE codigo = ?", (nuevo_stock, codigo))
            conn.commit()
            return cursor.rowcount > 0


class SQLitePedidoRepository(IPedidoRepository):
    """
    Repositorio de Pedidos con persistencia de agregados completos en JSON.
    Garantiza durabilidad e integridad de la clave de idempotencia única.
    """
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pedidos (
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

    def guardar(self, pedido: Pedido) -> bool:
        """Guarda o actualiza el pedido en la base de datos."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            items_data = [{"codigo": it.producto_codigo, "cantidad": it.cantidad, "precio": it.precio_unitario} for it in pedido.items]
            cursor.execute("""
                INSERT OR REPLACE INTO pedidos 
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
            conn.commit()
            return True

    def buscar_por_id(self, id_pedido: str) -> Optional[Pedido]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id_pedido, cliente_id, cliente_nombre, cliente_direccion, estado, total, idempotency_key, timestamp, items_json, historial_json 
                FROM pedidos WHERE id_pedido = ?
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

    def buscar_por_idempotency_key(self, key: str) -> Optional[Pedido]:
        """Consulta deduplicadora para evitar reprocesamiento accidental."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id_pedido, cliente_id, cliente_nombre, cliente_direccion, estado, total, idempotency_key, timestamp, items_json, historial_json 
                FROM pedidos WHERE idempotency_key = ?
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id_pedido, cliente_id, cliente_nombre, cliente_direccion, estado, total, idempotency_key, timestamp, items_json, historial_json 
                FROM pedidos ORDER BY timestamp DESC
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
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS outbox_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    aggregate_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    published INTEGER DEFAULT 0
                )
            """)
            conn.commit()

    def registrar_evento(self, aggregate_id: str, event_type: str, payload_json: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO outbox_events (aggregate_id, event_type, payload_json, timestamp, published)
                VALUES (?, ?, ?, ?, 0)
            """, (aggregate_id, event_type, payload_json, time.time()))
            conn.commit()
            return True

    def obtener_pendientes(self) -> List[Dict[str, Any]]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, aggregate_id, event_type, payload_json, timestamp, published 
                FROM outbox_events WHERE published = 0 ORDER BY id ASC
            """)
            filas = cursor.fetchall()
            return [{
                "id": r[0],
                "aggregate_id": r[1],
                "event_type": r[2],
                "payload": json.loads(r[3]),
                "timestamp": r[4],
                "published": bool(r[5])
            } for r in filas]

    def marcar_publicado(self, event_id: str) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE outbox_events SET published = 1 WHERE id = ?", (event_id,))
            conn.commit()
            return cursor.rowcount > 0

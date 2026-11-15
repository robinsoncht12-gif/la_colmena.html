"""
========================================================================================
DOMINIO Y ENTIDADES DE NEGOCIO: LaColmena
Cátedra: Ingeniería de Software 3 [IF0129] - Corporación Universitaria Remington
Buenas Prácticas: Single Responsibility (SRP), Tipado Fuerte, Encapsulamiento, SoD
Dependencias: NINGUNA (Python Standard Library `dataclasses`, `enum`, `time`, `uuid`)
========================================================================================
"""
from dataclasses import dataclass, field
from enum import Enum
import time
import uuid
from typing import List, Dict, Any

class EstadoPedido(Enum):
    """
    Máquina de Estados de un Pedido en la Arquitectura Distribuida:
    - PENDIENTE: Solicitud recibida en API Gateway / Controlador.
    - RESERVADO_SEMANTIC_LOCK: Stock bloqueado en la base de datos local (Paso T1 de la Saga).
    - PAGADO: Cobro debitado satisfactoriamente en pasarela financiera (Paso T2).
    - FACTURADO: Factura electrónica emitida según normativa tributaria.
    - COMPLETADO: Saga completada con éxito.
    - CANCELADO: Pedido rechazado antes de procesarse.
    - COMPENSADO: Reversión exitosa tras una falla en la Saga (Compensación C1).
    """
    PENDIENTE = "PENDIENTE"
    RESERVADO_SEMANTIC_LOCK = "RESERVADO_SEMANTIC_LOCK"
    PAGADO = "PAGADO"
    FACTURADO = "FACTURADO"
    COMPLETADO = "COMPLETADO"
    CANCELADO = "CANCELADO"
    COMPENSADO = "COMPENSADO"


class TipoPago(Enum):
    """Métodos de pago soportados por la pasarela de pagos."""
    TARJETA_CREDITO = "TARJETA_CREDITO"
    TRANSFERENCIA_PSE = "TRANSFERENCIA_PSE"
    EFECTIVO_CONTRAENTREGA = "EFECTIVO_CONTRAENTREGA"


@dataclass
class Producto:
    """
    Entidad de Negocio: Producto / producto en el catálogo de LaColmena.
    Representa el estado actual del inventario en almacén.
    """
    codigo: str
    nombre: str
    descripcion: str
    categoria: str
    precio: float
    stock_disponible: int
    requiere_receta: bool = False

    def tiene_stock(self, cantidad: int) -> bool:
        """Verifica disponibilidad sin mutar estado."""
        return self.stock_disponible >= cantidad


@dataclass
class ItemPedido:
    """
    Objeto de Valor (Value Object): Representa un renglón del carrito o pedido.
    Inmutable conceptualmente en el contexto de la orden.
    """
    producto_codigo: str
    cantidad: int
    precio_unitario: float

    @property
    def subtotal(self) -> float:
        """Calcula el valor parcial del renglón (precio * cantidad)."""
        return round(self.cantidad * self.precio_unitario, 2)


@dataclass
class Pedido:
    """
    Entidad Raíz del Agregado (Aggregate Root): Pedido
    Encapsula los ítems, el cálculo de totales, la clave de idempotencia
    y la trazabilidad auditable de todas las transiciones de estado.
    """
    id_pedido: str
    cliente_id: str
    cliente_nombre: str
    cliente_direccion: str
    items: List[ItemPedido]
    estado: EstadoPedido = EstadoPedido.PENDIENTE
    total: float = 0.0
    idempotency_key: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    historial_estados: List[Dict[str, Any]] = field(default_factory=list)

    def calcular_total(self) -> float:
        """Recalcula y asigna la suma de subtotales de todos los ítems."""
        self.total = round(sum(item.subtotal for item in self.items), 2)
        return self.total

    def transicionar_a(self, nuevo_estado: EstadoPedido, motivo: str = ""):
        """
        Cambia el estado del agregado registrando la estampa de tiempo
        y el motivo para garantizar auditoría forense (Trazabilidad SoD).
        """
        self.estado = nuevo_estado
        self.historial_estados.append({
            "estado": nuevo_estado.value,
            "motivo": motivo,
            "timestamp": time.time(),
            "hora_utc": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        })

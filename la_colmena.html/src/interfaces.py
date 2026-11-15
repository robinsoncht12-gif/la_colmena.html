"""
========================================================================================
INTERFACES Y ABSTRACCIONES (DIP - DEPENDENCY INVERSION PRINCIPLE)
Cátedra: Ingeniería de Software 3 [IF0129] - Uniremington
Garantiza el desacoplamiento entre Lógica de Negocio y Persistencia / IO
========================================================================================
"""
from abc import ABC, abstractmethod
import sqlite3
from typing import Optional, List, Dict, Any
from src.models import Producto, Pedido

class IProductoRepository(ABC):
    """Contrato abstracto para la persistencia del catálogo de productos."""
    
    @abstractmethod
    def obtener_todos(self) -> List[Producto]:
        """Recupera la totalidad de los productos en inventario."""
        pass

    @abstractmethod
    def buscar_por_codigo(
        self,
        codigo: str,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Optional[Producto]:
        """Obtiene un producto específico por su SKU o código único."""
        pass

    @abstractmethod
    def actualizar_stock(
        self,
        codigo: str,
        nuevo_stock: int,
        connection: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Actualiza el inventario físico disponible en bodega."""
        pass


class IPedidoRepository(ABC):
    """Contrato abstracto para la persistencia del agregado Pedido."""

    @abstractmethod
    def guardar(
        self,
        pedido: Pedido,
        connection: Optional[sqlite3.Connection] = None,
    ) -> bool:
        """Inserta o actualiza un pedido en el medio de almacenamiento."""
        pass

    @abstractmethod
    def buscar_por_id(self, id_pedido: str) -> Optional[Pedido]:
        """Busca un pedido por su identificador primario."""
        pass

    @abstractmethod
    def buscar_por_idempotency_key(
        self,
        key: str,
        connection: Optional[sqlite3.Connection] = None,
    ) -> Optional[Pedido]:
        """Permite deduplicar peticiones mediante la clave de idempotencia."""
        pass

    @abstractmethod
    def listar_todos(self) -> List[Pedido]:
        """Lista la totalidad de pedidos para reportería y auditoría."""
        pass

    @abstractmethod
    def actualizar_estado(self, id_pedido: str, nuevo_estado: str, motivo: str) -> bool:
        """Actualiza atómicamente el estado del pedido y su bitácora."""
        pass


class IOutboxRepository(ABC):
    """
    Contrato abstracto para el Transactional Outbox Pattern (Garantía Dual-Write).
    Permite publicar eventos en un broker sin riesgo de inconsistencia distribuida.
    """

    @abstractmethod
    def registrar_evento(
        self,
        aggregate_id: str,
        event: Dict[str, Any],
        connection: Optional[sqlite3.Connection] = None,
    ) -> int:
        """Inserta un evento en la tabla de Outbox dentro de la misma transacción local ACID."""
        pass

    @abstractmethod
    def obtener_pendientes(self) -> List[Dict[str, Any]]:
        """Recupera eventos cuyo estado continúa en PENDING."""
        pass

    @abstractmethod
    def marcar_publicado(self, event_id: int) -> bool:
        """Marca el evento como PROCESSED luego de confirmar su publicación."""
        pass

    @abstractmethod
    def registrar_error(self, event_id: int, error: str) -> bool:
        """Incrementa intentos y conserva el evento pendiente después de un fallo."""
        pass


class IPasarelaPagos(ABC):
    """
    Contrato abstracto para la integración con pasarelas de pago externas.
    Desacopla el servicio de la implementación concreta (PSE, Stripe, Wompi, etc.).
    """

    @abstractmethod
    def procesar_cobro(self, id_pedido: str, monto: float, idempotency_key: str, simular_falla: bool = False) -> bool:
        """Debita el monto solicitado retornando True si es exitoso o False si es rechazado."""
        pass

    @abstractmethod
    def reembolsar(
        self,
        id_pedido: str,
        monto: float,
        idempotency_key: str,
    ) -> bool:
        """Ejecuta la compensación financiera si se requiere devolver los fondos."""
        pass

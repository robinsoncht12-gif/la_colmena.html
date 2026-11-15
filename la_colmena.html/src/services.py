"""
========================================================================================
CAPA DE SERVICIOS Y LÓGICA DE NEGOCIO (SRP, SAGA, IDEMPOTENCIA, COMPENSACIÓN INVERSA)
Cátedra: Ingeniería de Software 3 [IF0129] - Uniremington
Patrón: Saga Pattern (Orchestrator), Idempotent Receiver, Fail-Fast con Validaciones
Buenas Prácticas: SoD, Inyección de Dependencias (DIP), Logging Estructurado
Dependencias: NINGUNA (Python Standard Library `time`, `json`, `uuid`, `typing`)
========================================================================================
"""
import time
import json
import uuid

from typing import Dict, Any, Optional, List
from src.models import Pedido, ItemPedido, EstadoPedido
from src.interfaces import IProductoRepository, IPedidoRepository, IOutboxRepository, IPasarelaPagos
from src.validators import Validator, ValidationError
from src.logger import get_logger
from src.events import crear_cloudevent, event_bus_global

logger = get_logger("Services")

class ColmenaService:
    """
    Servicio de Dominio de Colmena.
    Aplica Single Responsibility Principle (SRP) y garantiza consistencia transaccional
    en las operaciones locales de inventario y pedidos.
    """
    def __init__(self, 
                 producto_repo: IProductoRepository, 
                 pedido_repo: IPedidoRepository,
                 outbox_repo: IOutboxRepository):
        self.producto_repo = producto_repo
        self.pedido_repo = pedido_repo
        self.outbox_repo = outbox_repo

    def listar_catalogo(self):
        """Consulta el catálogo completo de productos disponibles."""
        return self.producto_repo.obtener_todos()

    def crear_pedido_monolito(self, 
                              cliente_id: str, 
                              cliente_nombre: str, 
                              direccion: str, 
                              items_req: list, 
                              idempotency_key: str) -> Pedido:
        """
        ====================================================================================
        FLUJO BASE LOCAL (ACID Monolítico + Validación Defensiva + Outbox):
        Etapa 1: Validación estricta Fail-Fast (Sintaxis y Contratos).
        Etapa 2: Comprobación de Idempotencia (Deduplicación).
        Etapa 3: Verificación de Stock en Bodega y Bloqueo Semántico.
        Etapa 4: Descuento atómico de Inventario.
        Etapa 5: Creación y Persistencia del Agregado Pedido.
        Etapa 6: Registro en Transactional Outbox (Prevención Dual-Write).
        ====================================================================================
        """
        # --- ETAPA 1: VALIDACIÓN FAIL-FAST ---
        c_id = Validator.validar_cliente_id(cliente_id)
        c_nombre = Validator.validar_no_vacio(cliente_nombre, "cliente_nombre")
        c_dir = Validator.validar_no_vacio(direccion, "direccion")
        i_key = Validator.validar_idempotency_key(idempotency_key)
        items_validados = Validator.validar_items_pedido(items_req)

        # --- ETAPA 2: COMPROBACIÓN DE IDEMPOTENCIA ---
        logger.info(f"Procesando pedido para cliente={c_id} con key={i_key}")
        existente = self.pedido_repo.buscar_por_idempotency_key(i_key)
        if existente:
            logger.warning(f"⚠️ [Idempotencia] Petición duplicada detectada. Retornando pedido existente {existente.id_pedido}")
            return existente

        # --- ETAPA 3: VERIFICACIÓN DE STOCK Y CONSTRUCCIÓN DE ITEMS ---
        items = []
        for it in items_validados:
            prod = self.producto_repo.buscar_por_codigo(it["codigo"])
            if not prod:
                logger.error(f"Validación de catálogo fallida: El código '{it['codigo']}' no existe.")
                raise ValidationError(f"Producto '{it['codigo']}' no existe en el catálogo farmacéutico.", campo="codigo")
            
            if not prod.tiene_stock(it["cantidad"]):
                logger.error(f"Stock insuficiente para {prod.nombre} (SKU: {prod.codigo}). Solicitado: {it['cantidad']}, Disponible: {prod.stock_disponible}")
                raise ValidationError(
                    f"Stock insuficiente para '{prod.nombre}'. Solicitado: {it['cantidad']}, Disponible: {prod.stock_disponible}",
                    campo="stock"
                )
            items.append(ItemPedido(prod.codigo, it["cantidad"], prod.precio))

        # --- ETAPA 4: INSTANCIACIÓN DEL AGREGADO PEDIDO ---
        pedido = Pedido(
            id_pedido=f"PED-{int(time.time()*1000)}",
            cliente_id=c_id,
            cliente_nombre=c_nombre,
            cliente_direccion=c_dir,
            items=items,
            estado=EstadoPedido.PENDIENTE,
            idempotency_key=i_key
        )
        pedido.calcular_total()
        pedido.transicionar_a(EstadoPedido.RESERVADO_SEMANTIC_LOCK, "Stock reservado preventivamente (Semantic Lock)")

        # --- ETAPA 5: DESCUENTO ATÓMICO DE STOCK ---
        for it in items:
            prod = self.producto_repo.buscar_por_codigo(it.producto_codigo)
            nuevo_stock = prod.stock_disponible - it.cantidad
            self.producto_repo.actualizar_stock(prod.codigo, nuevo_stock)
            logger.debug(f"Stock actualizado para {prod.codigo}: {prod.stock_disponible} -> {nuevo_stock}")

        # Persistir Pedido con estado RESERVADO_SEMANTIC_LOCK
        self.pedido_repo.guardar(pedido)
        logger.info(f"✅ Pedido {pedido.id_pedido} registrado exitosamente. Total=${pedido.total:,.2f}")

              # --- ETAPA 6: TRANSACTIONAL OUTBOX + EVENT BUS REACTIVO (Reto 3) ---
        evento_payload = {
            "id_pedido": pedido.id_pedido,
            "cliente_id": pedido.cliente_id,
            "total": pedido.total,
            "items_count": len(pedido.items),
            "idempotency_key": pedido.idempotency_key,
            "timestamp": pedido.timestamp
        }
        self.outbox_repo.registrar_evento(
            aggregate_id=pedido.id_pedido,
            event_type="co.edu.uniremington.LaColmena.pedido.creado", # <-- Sin .v1, norma CNCF
            payload_json=json.dumps(evento_payload)
        )
        logger.debug(f"Evento Outbox emitido para pedido {pedido.id_pedido}")

        # --- DESACOPLAMIENTO FINAL (Cumple T_async <=5ms) ---
        try:
            cloudevent = crear_cloudevent(
                tipo='co.edu.uniremington.LaColmena.pedido.creado',
                origen='/LaColmena/ordenes',
                payload=evento_payload
            )
            event_bus_global.publicar(cloudevent) # Publicación no bloqueante
            logger.info(f"📤 [EventBus] Evento {cloudevent.type} publicado con id={cloudevent.id}")
        except Exception as e:
            logger.error(f"Error publicando evento: {e}")

        return pedido


class PasarelaPagosMock(IPasarelaPagos):
    """
    Simulador de Pasarela Financiera Externa (Mock).
    Soporta Idempotencia en cobros y simulación deliberada de fallos para probar la Saga.
    """
    def __init__(self):
        self.cobros_procesados = {}

    def procesar_cobro(self, id_pedido: str, monto: float, idempotency_key: str, simular_falla: bool = False) -> bool:
        # Validación de monto positivo
        Validator.validar_monto(monto, "monto_cobro")

        if idempotency_key in self.cobros_procesados:
            logger.info(f"[Pasarela] Cobro idempotente ya registrado para key={idempotency_key}")
            return self.cobros_procesados[idempotency_key]["exito"]

        if simular_falla:
            logger.warning(f"❌ [Pasarela] Cobro RECHAZADO deliberadamente para pedido {id_pedido} (${monto:,.2f})")
            self.cobros_procesados[idempotency_key] = {"exito": False, "monto": monto, "motivo": "Fondos insuficientes"}
            return False

        logger.info(f"💳 [Pasarela] Cobro APROBADO para pedido {id_pedido} (${monto:,.2f})")
        self.cobros_procesados[idempotency_key] = {"exito": True, "monto": monto, "motivo": "Aprobado"}
        return True

    def reembolsar(self, id_pedido: str, monto: float) -> bool:
        logger.info(f"↩️ [Pasarela] Reembolso procesado para pedido {id_pedido} por ${monto:,.2f}")
        return True


class SagaOrquestadorMatriculaPedido:
    """
    Orquestador Centralizado de Saga (S4):
    Coordina transacciones distribuidas compensables con garantía de consistencia eventual.
    Pasos:
      - T1: Inventario / Pedido (Acción Compensable)
      - T2: Pasarela de Pagos (Punto de Decisión Pivot)
      - T3: Facturación y Despacho (Acción Retriable)
      - C1: Compensación Inversa (Devolución de stock en reversa si T2 falla)
    """
    def __init__(self, Colmena_service: ColmenaService, pasarela: PasarelaPagosMock):
        self.Colmena_service = Colmena_service
        self.pasarela = pasarela

    def ejecutar_saga_pedido(self, 
                             cliente_id: str, 
                             cliente_nombre: str, 
                             direccion: str, 
                             items_req: list, 
                             idempotency_key: str, 
                             simular_falla_pago: bool = False) -> dict:
        """
        Ejecuta el flujo orquestado de la Saga con compensación automática.
        """
        log_saga = []
        logger.info(f"🏁 Iniciando Saga para orden del cliente={cliente_id}")

        try:
            # =========================================================================
            # PASO T1: Crear Pedido y Reservar Inventario (Local ACID con Semantic Lock)
            # =========================================================================
            pedido = self.Colmena_service.crear_pedido_monolito(
                cliente_id, cliente_nombre, direccion, items_req, idempotency_key
            )
            msg_t1 = f"T1 [Inventario]: Pedido {pedido.id_pedido} creado con Semantic Lock (Total: ${pedido.total:,.2f})."
            log_saga.append(msg_t1)
            logger.info(msg_t1)

            # Si el pedido ya estaba completado o compensado previamente por idempotencia
            if pedido.estado in [EstadoPedido.COMPLETADO, EstadoPedido.COMPENSADO]:
                log_saga.append(f"ℹ️ [Idempotencia]: Pedido ya procesado previamente con estado {pedido.estado.value}.")
                return {"exito": pedido.estado == EstadoPedido.COMPLETADO, "pedido": pedido, "log": log_saga}

            # =========================================================================
            # PASO T2: Cobro Financiero en Pasarela Externa (Pivot Transaction)
            # =========================================================================
            pay_key = f"PAY-{idempotency_key}"
            pago_ok = self.pasarela.procesar_cobro(
                pedido.id_pedido, pedido.total, pay_key, simular_falla=simular_falla_pago
            )

            if not pago_ok:
                raise RuntimeError("Falla en Pasarela de Pagos (Rechazo Financiero). Disparando Compensación Inversa (C1).")

            pedido.transicionar_a(EstadoPedido.PAGADO, "Cobro aprobado por pasarela financiera")
            self.Colmena_service.pedido_repo.guardar(pedido)
            msg_t2 = f"T2 [Pagos]: Débito de ${pedido.total:,.2f} exitoso en pasarela."
            log_saga.append(msg_t2)
            logger.info(msg_t2)

            # =========================================================================
            # PASO T3: Facturación Electrónica y Despacho
            # =========================================================================
            pedido.transicionar_a(EstadoPedido.COMPLETADO, "Orden facturada y lista para despacho logístico")
            self.Colmena_service.pedido_repo.guardar(pedido)
            msg_t3 = "T3 [Facturación]: Factura electrónica emitida. Saga completada exitosamente."
            log_saga.append(msg_t3)
            logger.info(msg_t3)

            return {"exito": True, "pedido": pedido, "log": log_saga}

        except Exception as e:
            # =========================================================================
            # COMPENSACIÓN INVERSA (C1): Deshacer efectos secundarios en orden inverso
            # =========================================================================
            err_msg = f"🚨 ERROR EN SAGA: {str(e)}"
            log_saga.append(err_msg)
            logger.error(err_msg)

            if 'pedido' in locals() and pedido is not None:
                logger.warning(f"Iniciando Compensación C1 para Pedido {pedido.id_pedido}...")
                
                # C1: Devolver stock al almacén
                for it in pedido.items:
                    prod = self.Colmena_service.producto_repo.buscar_por_codigo(it.producto_codigo)
                    if prod:
                        stock_restaurado = prod.stock_disponible + it.cantidad
                        self.Colmena_service.producto_repo.actualizar_stock(prod.codigo, stock_restaurado)
                        logger.info(f"↩️ C1: Stock restaurado para {prod.codigo}: {prod.stock_disponible} -> {stock_restaurado}")

                pedido.transicionar_a(EstadoPedido.COMPENSADO, f"Aborto por falla en paso T2: {str(e)}")
                self.Colmena_service.pedido_repo.guardar(pedido)
                msg_c1 = "C1 [Compensación Inventario]: Stock devuelto a la Colmena. Estado: COMPENSADO."
                log_saga.append(msg_c1)
                logger.info(msg_c1)

            return {"exito": False, "pedido": locals().get('pedido', None), "log": log_saga, "error": str(e)}

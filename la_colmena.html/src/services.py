"""
========================================================================================
CAPA DE SERVICIOS Y LÓGICA DE NEGOCIO (SRP, SAGA, IDEMPOTENCIA, COMPENSACIÓN INVERSA)
Cátedra: Ingeniería de Software 3 [IF0129] - Uniremington
Patrón: Saga Pattern (Orchestrator), Idempotent Receiver, Fail-Fast con Validaciones
Buenas Prácticas: SoD, Inyección de Dependencias (DIP), Logging Estructurado
Dependencias: NINGUNA (Python Standard Library `time`, `json`)
========================================================================================
"""
import time
import copy
from src.models import Pedido, ItemPedido, EstadoPedido, EstadoSaga
from src.interfaces import IProductoRepository, IPedidoRepository, IOutboxRepository, IPasarelaPagos
from src.validators import Validator, ValidationError
from src.logger import get_logger
from src.events import crear_cloudevent
from src.database import DB_PATH, database_connection
from src.circuit_breaker import CircuitBreaker, CircuitOpenError

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
                 outbox_repo: IOutboxRepository,
                 db_path: str = DB_PATH):
        self.producto_repo = producto_repo
        self.pedido_repo = pedido_repo
        self.outbox_repo = outbox_repo
        self.db_path = db_path

    def _registrar_evento(
        self,
        connection,
        aggregate_id: str,
        event_type: str,
        data: dict,
        actor: dict = None,
    ) -> None:
        if actor is not None:
            data = {**data, "actor": actor}
        event = crear_cloudevent(event_type, data)
        self.outbox_repo.registrar_evento(
            aggregate_id, event.to_dict(), connection
        )

    @staticmethod
    def _datos_pedido(pedido: Pedido) -> dict:
        return {
            "order_id": pedido.id_pedido,
            "client_id": pedido.cliente_id,
            "total": pedido.total,
            "saga_status": pedido.estado_saga.value,
            "items": [
                {
                    "product_code": item.producto_codigo,
                    "quantity": item.cantidad,
                    "unit_price": item.precio_unitario,
                }
                for item in pedido.items
            ],
            "idempotency_key": pedido.idempotency_key,
        }

    def listar_catalogo(self):
        """Consulta el catálogo completo de productos disponibles."""
        return self.producto_repo.obtener_todos()

    def crear_pedido_monolito(self, 
                              cliente_id: str, 
                              cliente_nombre: str, 
                              direccion: str, 
                              items_req: list, 
                              idempotency_key: str,
                              actor: dict = None) -> Pedido:
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

        logger.info(f"Procesando pedido para cliente={c_id} con key={i_key}")
        with database_connection(self.db_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existente = self.pedido_repo.buscar_por_idempotency_key(
                i_key, connection
            )
            if existente:
                logger.warning(
                    "Petición duplicada; se retorna el pedido %s",
                    existente.id_pedido,
                )
                return existente

            productos = {}
            items = []
            for item in items_validados:
                product = self.producto_repo.buscar_por_codigo(
                    item["codigo"], connection
                )
                if not product:
                    raise ValidationError(
                        f"Producto '{item['codigo']}' no existe en el catálogo.",
                        campo="codigo",
                    )
                if not product.tiene_stock(item["cantidad"]):
                    raise ValidationError(
                        f"Stock insuficiente para '{product.nombre}'. "
                        f"Solicitado: {item['cantidad']}, "
                        f"Disponible: {product.stock_disponible}",
                        campo="stock",
                    )
                productos[product.codigo] = product
                items.append(
                    ItemPedido(
                        product.codigo,
                        item["cantidad"],
                        product.precio,
                    )
                )

            pedido = Pedido(
                id_pedido=f"PED-{int(time.time() * 1000)}",
                cliente_id=c_id,
                cliente_nombre=c_nombre,
                cliente_direccion=c_dir,
                items=items,
                estado=EstadoPedido.PENDIENTE,
                idempotency_key=i_key,
            )
            pedido.calcular_total()
            pedido.transicionar_a(
                EstadoPedido.RESERVADO_SEMANTIC_LOCK,
                "Stock reservado preventivamente (Semantic Lock)",
            )

            for item in items:
                product = productos[item.producto_codigo]
                new_stock = product.stock_disponible - item.cantidad
                if not self.producto_repo.actualizar_stock(
                    product.codigo, new_stock, connection
                ):
                    raise RuntimeError(
                        f"No se pudo actualizar el inventario de {product.codigo}."
                    )
                self._registrar_evento(
                    connection,
                    pedido.id_pedido,
                    "bina.la_colmena.inventario.reservado",
                    {
                        "order_id": pedido.id_pedido,
                        "product_code": product.codigo,
                        "quantity": item.cantidad,
                        "previous_stock": product.stock_disponible,
                        "stock": new_stock,
                    },
                    actor=actor,
                )

            self.pedido_repo.guardar(pedido, connection)
            self._registrar_evento(
                connection,
                pedido.id_pedido,
                "bina.la_colmena.pedido.creado",
                self._datos_pedido(pedido),
                actor=actor,
            )

        logger.info(
            "Pedido %s registrado. Total=$%.2f", pedido.id_pedido, pedido.total
        )
        return pedido

    def guardar_pedido_y_evento(
        self,
        pedido: Pedido,
        event_type: str,
        actor: dict = None,
    ) -> None:
        with database_connection(self.db_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            self.pedido_repo.guardar(pedido, connection)
            self._registrar_evento(
                connection,
                pedido.id_pedido,
                event_type,
                self._datos_pedido(pedido),
                actor=actor,
            )

    def iniciar_compensacion(
        self, pedido: Pedido, motivo: str, actor: dict = None
    ) -> Pedido:
        if pedido.estado_saga == EstadoSaga.COMPENSATED:
            return pedido
        if pedido.estado_saga != EstadoSaga.COMPENSATING:
            pedido.transicionar_saga_a(EstadoSaga.COMPENSATING, motivo)
            actor_kwargs = {"actor": actor} if actor is not None else {}
            self.guardar_pedido_y_evento(
                pedido, "bina.la_colmena.pedido.compensando", **actor_kwargs
            )
        return pedido

    def compensar_pedido(
        self, pedido: Pedido, motivo: str, actor: dict = None
    ) -> Pedido:
        actor_kwargs = {"actor": actor} if actor is not None else {}
        pedido = self.iniciar_compensacion(pedido, motivo, **actor_kwargs)
        if pedido.estado_saga == EstadoSaga.COMPENSATED:
            return pedido
        pedido_compensado = copy.deepcopy(pedido)
        with database_connection(self.db_path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            for item in pedido_compensado.items:
                product = self.producto_repo.buscar_por_codigo(
                    item.producto_codigo, connection
                )
                if not product:
                    raise RuntimeError(
                        f"No se encontró el producto {item.producto_codigo} "
                        "durante la compensación."
                    )
                restored_stock = product.stock_disponible + item.cantidad
                self.producto_repo.actualizar_stock(
                    product.codigo, restored_stock, connection
                )
                self._registrar_evento(
                    connection,
                    pedido_compensado.id_pedido,
                    "bina.la_colmena.inventario.restaurado",
                    {
                        "order_id": pedido_compensado.id_pedido,
                        "product_code": product.codigo,
                        "quantity": item.cantidad,
                        "previous_stock": product.stock_disponible,
                        "stock": restored_stock,
                    },
                    actor=actor,
                )
            pedido_compensado.transicionar_a(
                EstadoPedido.COMPENSADO, motivo
            )
            pedido_compensado.transicionar_saga_a(
                EstadoSaga.COMPENSATED, motivo
            )
            self.pedido_repo.guardar(pedido_compensado, connection)
            self._registrar_evento(
                connection,
                pedido_compensado.id_pedido,
                "bina.la_colmena.pedido.compensado",
                self._datos_pedido(pedido_compensado),
                actor=actor,
            )
        return pedido_compensado


class PasarelaPagosMock(IPasarelaPagos):
    """
    Simulador de Pasarela Financiera Externa (Mock).
    Soporta Idempotencia en cobros y simulación deliberada de fallos para probar la Saga.
    """
    def __init__(self, breaker=None, delay_seconds=0.0):
        self.cobros_procesados = {}
        self.reembolsos_procesados = {}
        self.circuit_breaker = breaker or CircuitBreaker(failure_threshold=3, recovery_seconds=10)
        self.delay_seconds = max(0.0, float(delay_seconds))

    def procesar_cobro(self, id_pedido: str, monto: float, idempotency_key: str, simular_falla: bool = False) -> bool:
        # Validación de monto positivo
        Validator.validar_monto(monto, "monto_cobro")

        if idempotency_key in self.cobros_procesados:
            logger.info(f"[Pasarela] Cobro idempotente ya registrado para key={idempotency_key}")
            return self.cobros_procesados[idempotency_key]["exito"]

        previous_state = self.circuit_breaker.snapshot()["state"]
        self.circuit_breaker.acquire()
        if previous_state == CircuitBreaker.HALF_OPEN:
            logger.warning("[CircuitBreaker] Periodo de recuperación cumplido; iniciando prueba HALF-OPEN.")
        payment_started = time.monotonic()
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        latency_ms = round((time.monotonic() - payment_started) * 1000)

        if simular_falla:
            logger.warning(f"❌ [Pasarela] Cobro RECHAZADO para pedido {id_pedido} (${monto:,.2f}); latencia={latency_ms}ms")
            self.cobros_procesados[idempotency_key] = {"exito": False, "monto": monto, "motivo": "Fondos insuficientes", "latency_ms": latency_ms}
            self.circuit_breaker.record_failure()
            return False

        logger.info(f"💳 [Pasarela] Cobro APROBADO para pedido {id_pedido} (${monto:,.2f}); latencia={latency_ms}ms")
        self.cobros_procesados[idempotency_key] = {"exito": True, "monto": monto, "motivo": "Aprobado", "latency_ms": latency_ms}
        self.circuit_breaker.record_success()
        return True

    def reembolsar(
        self,
        id_pedido: str,
        monto: float,
        idempotency_key: str,
    ) -> bool:
        if idempotency_key in self.reembolsos_procesados:
            return self.reembolsos_procesados[idempotency_key]
        logger.info(f"↩️ [Pasarela] Reembolso procesado para pedido {id_pedido} por ${monto:,.2f}")
        self.reembolsos_procesados[idempotency_key] = True
        return True


class SagaOrquestadorMatriculaPedido:
    """
    Orquestador Centralizado de Saga (S4):
    Coordina transacciones distribuidas compensables con garantía de consistencia eventual.
    Pasos:
      - T1: Inventario / Pedido (Acción Compensable)
      - T2: Pasarela de Pagos (Punto de Decisión Pivot)
      - T3: Facturación y Despacho (Acción Retriable)
      - C1: Compensación Inversa de inventario
      - C2: Reembolso idempotente si el cobro ya fue confirmado
    """
    def __init__(self, Colmena_service: ColmenaService, pasarela: PasarelaPagosMock):
        self.Colmena_service = Colmena_service
        self.pasarela = pasarela

    def _compensar(
        self,
        pedido: Pedido,
        motivo: str,
        log_saga: list,
        actor: dict = None,
    ) -> Pedido:
        actor_kwargs = {"actor": actor} if actor is not None else {}
        pedido = self.Colmena_service.iniciar_compensacion(
            pedido, motivo, **actor_kwargs
        )
        if pedido.estado in (EstadoPedido.PAGADO, EstadoPedido.COMPLETADO):
            refund_key = f"REFUND-{pedido.idempotency_key}"
            if not self.pasarela.reembolsar(
                pedido.id_pedido, pedido.total, refund_key
            ):
                raise RuntimeError(
                    "La pasarela no confirmó el reembolso; Saga en COMPENSATING."
                )
            self.Colmena_service.guardar_pedido_y_evento(
                pedido,
                "bina.la_colmena.pago.reembolsado",
                **actor_kwargs,
            )
            log_saga.append(
                f"C2 [Compensación de pago]: Reembolso confirmado para "
                f"{pedido.id_pedido}."
            )
        pedido = self.Colmena_service.compensar_pedido(
            pedido, motivo, **actor_kwargs
        )
        log_saga.append(
            "C1 [Compensación de inventario]: Stock restaurado; "
            "estado Saga COMPENSATED."
        )
        return pedido

    def ejecutar_saga_pedido(self, 
                             cliente_id: str, 
                             cliente_nombre: str, 
                             direccion: str, 
                             items_req: list, 
                             idempotency_key: str, 
                             simular_falla_pago: bool = False,
                             actor: dict = None) -> dict:
        """
        Ejecuta el flujo orquestado de la Saga con compensación automática.
        """
        log_saga = []
        actor_kwargs = {"actor": actor} if actor is not None else {}
        logger.info(f"🏁 Iniciando Saga para orden del cliente={cliente_id}")

        try:
            # =========================================================================
            # PASO T1: Crear Pedido y Reservar Inventario (Local ACID con Semantic Lock)
            # =========================================================================
            pedido = self.Colmena_service.crear_pedido_monolito(
                cliente_id, cliente_nombre, direccion, items_req, idempotency_key,
                **actor_kwargs,
            )
            msg_t1 = f"T1 [Inventario]: Pedido {pedido.id_pedido} creado con Semantic Lock (Total: ${pedido.total:,.2f})."
            log_saga.append(msg_t1)
            logger.info(msg_t1)

            if pedido.estado_saga == EstadoSaga.COMPENSATING:
                pedido = self._compensar(
                    pedido,
                    "Reanudación de compensación pendiente",
                    log_saga,
                    **actor_kwargs,
                )
                return {"exito": False, "pedido": pedido, "log": log_saga}
            if (
                pedido.estado_saga == EstadoSaga.COMPENSATED
                or pedido.estado == EstadoPedido.COMPENSADO
            ):
                log_saga.append(
                    f"ℹ️ [Idempotencia]: Pedido ya compensado "
                    f"({pedido.estado_saga.value})."
                )
                return {"exito": False, "pedido": pedido, "log": log_saga}
            if pedido.estado == EstadoPedido.COMPLETADO:
                log_saga.append(f"ℹ️ [Idempotencia]: Pedido ya procesado previamente con estado {pedido.estado.value}.")
                return {"exito": True, "pedido": pedido, "log": log_saga}

            # =========================================================================
            # PASO T2: Cobro Financiero en Pasarela Externa (Pivot Transaction)
            # =========================================================================
            if pedido.estado != EstadoPedido.PAGADO:
                pay_key = f"PAY-{idempotency_key}"
                pago_ok = self.pasarela.procesar_cobro(
                    pedido.id_pedido,
                    pedido.total,
                    pay_key,
                    simular_falla=simular_falla_pago,
                )

                if not pago_ok:
                    self.Colmena_service.guardar_pedido_y_evento(
                        pedido,
                        "bina.la_colmena.pago.rechazado",
                        **actor_kwargs,
                    )
                    log_saga.append(
                        "T2 [Pasarela de pagos]: Pago rechazado; no se realizó el débito."
                    )
                    raise RuntimeError(
                        "Falla en Pasarela de Pagos (Rechazo Financiero). "
                        "Disparando Compensación Inversa."
                    )

                pedido.transicionar_a(
                    EstadoPedido.PAGADO,
                    "Cobro aprobado por pasarela financiera",
                )
                pedido.transicionar_saga_a(
                    EstadoSaga.CONFIRMED,
                    "La pasarela confirmó el cobro",
                )
                self.Colmena_service.guardar_pedido_y_evento(
                    pedido, "bina.la_colmena.pedido.pagado", **actor_kwargs
                )
                msg_t2 = f"T2 [Pagos]: Débito de ${pedido.total:,.2f} exitoso en pasarela."
                log_saga.append(msg_t2)
                logger.info(msg_t2)

            # =========================================================================
            # PASO T3: Facturación Electrónica y Despacho
            # =========================================================================
            pedido.transicionar_a(EstadoPedido.COMPLETADO, "Orden facturada y lista para despacho logístico")
            self.Colmena_service.guardar_pedido_y_evento(
                pedido, "bina.la_colmena.pedido.completado", **actor_kwargs
            )
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
                try:
                    pedido = self._compensar(
                        pedido,
                        f"Compensación por fallo de Saga: {str(e)}",
                        log_saga,
                        **actor_kwargs,
                    )
                except Exception as compensation_error:
                    logger.exception(
                        "No se pudo completar la compensación del pedido %s",
                        pedido.id_pedido,
                    )
                    return {
                        "exito": False,
                        "pedido": pedido,
                        "log": log_saga,
                        "error": str(e),
                        "error_compensacion": str(compensation_error),
                        "error_type": type(e).__name__,
                    }

            return {
                "exito": False,
                "pedido": locals().get("pedido"),
                "log": log_saga,
                "error": str(e),
                "error_type": type(e).__name__,
            }

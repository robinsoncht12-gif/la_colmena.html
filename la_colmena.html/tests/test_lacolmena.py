"""

SUITE DE PRUEBAS UNITARIAS Y DE INTEGRACIÓN (TESTING NATIVO - CERO DEPENDENCIAS)
Cátedra: Ingeniería de Software 3 [IF0129] - Corporación Universitaria Remington
Pruebas: SOLID, ACID, Idempotencia, Saga Compensación Inversa, Validaciones Fail-Fast
Dependencias: NINGUNA (Python Standard Library `unittest`, `tempfile`, `os`, `shutil`)

"""
import unittest
import tempfile
import os
import shutil
import json
import uuid
import sys

# Asegurar path para ejecución directa
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.models import Producto, Pedido, ItemPedido, EstadoPedido
from src.validators import Validator, ValidationError
from src.repositories import SQLiteProductoRepository, SQLitePedidoRepository, SQLiteOutboxRepository
from src.services import ColmenaService, PasarelaPagosMock, SagaOrquestadorMatriculaPedido

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
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_farma.db")
        self.prod_repo = SQLiteProductoRepository(self.db_path)
        self.ped_repo = SQLitePedidoRepository(self.db_path)
        self.outbox_repo = SQLiteOutboxRepository(self.db_path)

    def tearDown(self):
        # Fix Windows: cerrar conexiones antes de borrar el archivo temporal
        for repo in [self.prod_repo, self.ped_repo, self.outbox_repo]:
            try:
                if hasattr(repo, 'conn') and repo.conn:
                    repo.conn.close()
                if hasattr(repo, '_conn') and repo._conn:
                    repo._conn.close()
                if hasattr(repo, 'engine'):
                    repo.engine.dispose()
            except:
                pass
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_seed_productos_y_actualizacion_stock(self):
        prods = self.prod_repo.obtener_todos()
        self.assertEqual(len(prods), 5, "El catálogo inicial debe sembrar 5 productos")
        
        # Buscar por código
        amox = self.prod_repo.buscar_por_codigo("MED-001")
        self.assertIsNotNone(amox)
        self.assertEqual(amox.stock_disponible, 50)

        # Actualizar stock
        self.prod_repo.actualizar_stock("MED-001", 45)
        amox_actualizado = self.prod_repo.buscar_por_codigo("MED-001")
        self.assertEqual(amox_actualizado.stock_disponible, 45)

    def test_guardar_y_recuperar_pedido(self):
        pedido = Pedido(
            id_pedido="PED-TEST-001",
            cliente_id="1098765432",
            cliente_nombre="Carlos Alférez",
            cliente_direccion="Calle 45 # 12-30",
            items=[ItemPedido("MED-001", 2, 25000.0)],
            estado=EstadoPedido.RESERVADO_SEMANTIC_LOCK,
            idempotency_key="IDEM-TEST-1111"
        )
        pedido.calcular_total()
        self.ped_repo.guardar(pedido)

        recuperado = self.ped_repo.buscar_por_id("PED-TEST-001")
        self.assertIsNotNone(recuperado)
        self.assertEqual(recuperado.total, 50000.0)
        self.assertEqual(recuperado.estado, EstadoPedido.RESERVADO_SEMANTIC_LOCK)
        self.assertEqual(len(recuperado.items), 1)

    def test_outbox_transactional_pattern(self):
        self.outbox_repo.registrar_evento("PED-TEST-001", "pedido.creado", json.dumps({"monto": 50000}))
        pendientes = self.outbox_repo.obtener_pendientes()
        self.assertEqual(len(pendientes), 1)
        self.assertEqual(pendientes[0]["aggregate_id"], "PED-TEST-001")

        # Marcar publicado
        event_id = pendientes[0]["id"]
        self.outbox_repo.marcar_publicado(event_id)
        self.assertEqual(len(self.outbox_repo.obtener_pendientes()), 0)

class TestSagaYLogicaNegocio(unittest.TestCase):
    """Pruebas del Orquestador Saga, Idempotencia y Compensación Inversa C1."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.test_dir, "test_farma_saga.db")
        self.prod_repo = SQLiteProductoRepository(self.db_path)
        self.ped_repo = SQLitePedidoRepository(self.db_path)
        self.outbox_repo = SQLiteOutboxRepository(self.db_path)
        self.service = ColmenaService(self.prod_repo, self.ped_repo, self.outbox_repo)
        self.pasarela = PasarelaPagosMock()
        self.saga = SagaOrquestadorMatriculaPedido(self.service, self.pasarela)

    def tearDown(self):
        # Fix Windows: cerrar conexiones antes de borrar el archivo temporal
        for repo in [self.prod_repo, self.ped_repo, self.outbox_repo]:
            try:
                if hasattr(repo, 'conn') and repo.conn:
                    repo.conn.close()
                if hasattr(repo, '_conn') and repo._conn:
                    repo._conn.close()
                if hasattr(repo, 'engine'):
                    repo.engine.dispose()
            except:
                pass
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_saga_flujo_feliz_completado(self):
        """Caso 1: T1 -> T2 -> T3 completado satisfactoriamente."""
        stock_inicial = self.prod_repo.buscar_por_codigo("MED-002").stock_disponible  # Acetaminofen (150)
        
        resultado = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Laura Gómez",
            direccion="Carrera 15 # 45-20",
            items_req=[{"codigo": "MED-002", "cantidad": 3}],
            idempotency_key="IDEM-EXITO-001",
            simular_falla_pago=False
        )

        self.assertTrue(resultado["exito"])
        pedido = resultado["pedido"]
        self.assertEqual(pedido.estado, EstadoPedido.COMPLETADO)

        # Stock descontado: 150 - 3 = 147
        stock_actual = self.prod_repo.buscar_por_codigo("MED-002").stock_disponible
        self.assertEqual(stock_actual, stock_inicial - 3)

    def test_saga_compensacion_inversa_cuando_falla_pago(self):
        """Caso 2: T1 (Reserva) -> T2 Falla -> C1 (Compensación de stock y estado COMPENSADO)."""
        stock_inicial = self.prod_repo.buscar_por_codigo("MED-001").stock_disponible  # 50

        resultado = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Marcos Ruiz",
            direccion="Avenida Santander # 10",
            items_req=[{"codigo": "MED-001", "cantidad": 5}],
            idempotency_key="IDEM-FALLA-PAGO-001",
            simular_falla_pago=True  # Provoca rechazo en pasarela
        )

        self.assertFalse(resultado["exito"])
        pedido = resultado["pedido"]
        self.assertEqual(pedido.estado, EstadoPedido.COMPENSADO)

        # Verificar que el stock fue DEVUELTO intacto por C1
        stock_final = self.prod_repo.buscar_por_codigo("MED-001").stock_disponible
        self.assertEqual(stock_final, stock_inicial, "El stock debe regresar al valor original tras la compensación C1")

    def test_garantia_de_idempotencia(self):
        """Caso 3: Dos peticiones con idéntica idempotency_key deben retornar el mismo pedido sin doble descuento."""
        stock_inicial = self.prod_repo.buscar_por_codigo("MED-003").stock_disponible  # 80
        key_unica = "IDEM-REPEAT-777"

        res_1 = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Valeria Ríos",
            direccion="Calle 10 # 5-10",
            items_req=[{"codigo": "MED-003", "cantidad": 2}],
            idempotency_key=key_unica,
            simular_falla_pago=False
        )

        # Segunda petición idéntica
        res_2 = self.saga.ejecutar_saga_pedido(
            cliente_id="1098765432",
            cliente_nombre="Valeria Ríos",
            direccion="Calle 10 # 5-10",
            items_req=[{"codigo": "MED-003", "cantidad": 2}],
            idempotency_key=key_unica,
            simular_falla_pago=False
        )

        self.assertEqual(res_1["pedido"].id_pedido, res_2["pedido"].id_pedido)
        stock_final = self.prod_repo.buscar_por_codigo("MED-003").stock_disponible
        # Debe haberse descontado SOLAMENTE 2 unidades, no 4
        self.assertEqual(stock_final, stock_inicial - 2)

if __name__ == "__main__":
    print("=" * 80)
    print("🧪 EJECUTANDO SUITE DE PRUEBAS AUTOMATIZADAS DE LaColmena")
    print("Cátedra: Ingeniería de Software 3 [IF0129] - Uniremington")
    print("=" * 80)
    unittest.main(verbosity=2)
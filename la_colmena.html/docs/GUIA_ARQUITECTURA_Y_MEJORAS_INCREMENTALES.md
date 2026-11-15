# 🏥 LaColmena MVP — GUÍA DE ARQUITECTURA Y ENTREGAS INCREMENTALES
**Asignatura:** [IF0129] Ingeniería de Software 3 | **Institución:** Corporación Universitaria Remington  
**Docente Titular:** Ing. Jhon Alexander Rosero Lasso, MSc en Inteligencia Artificial  
**Tecnología Base:** Python Estándar (3.10+) + Web Nativo; `argon2-cffi` para hashing y `confluent-kafka` para Apache Kafka.

---

## 🏛️ 1. Principios de Arquitectura Aplicados (IS1, IS2 e IS3)

El sistema actual es un **monolito modular en transición**. Sus límites de dominio están identificados, pero parte de la separación todavía no está impuesta por el código:
1. **SOLID:**
   - **SRP (Single Responsibility):** Modelos (`models.py`), Abstracciones (`interfaces.py`), Repositorios (`repositories.py`) y Servicios (`services.py`) están separados en el flujo Saga; las rutas heredadas aún concentran varias responsabilidades en `app.py`.
   - **DIP (Dependency Inversion):** `ColmenaService` usa contratos de repositorio, pero también abre conexiones SQLite directamente para coordinar la transacción Saga.
2. **ACID Local:** Persistencia atómica en SQLite con aislamiento y control transaccional nativo.
3. **SoD (Separation of Duties / Separation of Concerns):** Parcial. `app.py` contiene SQL y lógica de rutas; el flujo Saga usa servicios y repositorios.
4. **D2 — Modelo C4, Bounded Contexts e ISO 25010:** diagramas C1–C3, métricas Ca/Ce/I/D, propiedad de datos por contexto y metas ISO se documentan en [D2 — Modelo C4, acoplamiento, Bounded Contexts e ISO/IEC 25010](./D2_MODELADO_C4_ISO25010_BOUNDED_CONTEXTS.md). El documento registra explícitamente las brechas actuales de aislamiento.

## 🧩 2. Estructura del Código

- `main.py`: inicia el servidor y abre la interfaz.
- `src/app.py` y `src/index.html`: API HTTP e interfaz web principal.
- `src/database.py`: ruta única y ciclo de vida de las conexiones SQLite.
- `src/models.py`, `src/interfaces.py`, `src/repositories.py`, `src/services.py` y `src/validators.py`: dominio y flujo Saga.
- `src/events/`: CloudEvents y bus asíncrono.
- `tests/test_lacolmena.py`: pruebas de API, persistencia, validaciones y Saga.

---

## 🚀 3. Ejecución Rápida en Local
```bash
# Desde la raíz del repositorio:
cd la_colmena.html
python -m pip install -r requirements.txt
python main.py
python -m unittest discover -s tests -p "test_*.py"
```
El servidor levantará en `http://localhost:8086` con:
- Interfaz web principal en `src/index.html`, servida por `src/app.py`.
- Base SQLite compartida `LaColmena.db` en la raíz del proyecto; `src/database.py` centraliza la conexión y el archivo se crea al iniciar.
- Catálogo de productos y stock en tiempo real.
- Carrito y simulador transaccional.
- Monitor de la máquina de estados Saga.
- Tabla `outbox_table` para captura de eventos CDC.

---

## 📦 4. Hoja de Ruta de Mejoras Incrementales por Sesión

| Sesión | Hito Evaluativo | Mejora Incremental en la Aplicación LaColmena |
| :---: | :---: | :--- |
| **S1** | **Act1 (25% Seg.)** | **Diagnóstico y Modelo C4:** C1–C3, baseline Ca/Ce/I/D, Bounded Contexts y metas ISO 25010 documentados en [D2](./D2_MODELADO_C4_ISO25010_BOUNDED_CONTEXTS.md); el aislamiento estricto queda como brecha de implementación. |
| **S2** | **PRIMER PARCIAL (25%)** | **Descomposición y CloudEvents:** Separar el catálogo de productos del servicio de órdenes y emitir eventos bajo el estándar **CNCF CloudEvents v1.0** en formato JSON. |
| **S3** | **Act2 (25% Seg.)** | **Transactional Outbox y Rate Limiting:** Activar el procesamiento de la `outbox_table` para evitar la Doble Escritura (Dual-Write) y agregar limitación de tasa (Token Bucket) al login. |
| **S4** | **SEGUNDO PARCIAL (25%)** | **Orquestador Saga y Compensación Inversa:** Implementar la máquina de estados completa ($T_1 \to T_2 \to T_3$), probar la inyección de fallos con compensación ($C_1$) y verificar la Idempotency Key. |
| **S5** | **Act3 (25% Seg.)** | **Resiliencia (Circuit Breaker y OpenTelemetry):** Proteger la pasarela de pagos con Circuit Breaker (`CLOSED`, `OPEN`, `HALF-OPEN`) e inyectar cabeceras W3C `traceparent`. |
| **S6** | **EXAMEN FINAL (25%)** | **Evaluación ATAM y Cloud Native:** Sustentación formal de trade-offs arquitectónicos con el método ATAM del SEI y empaquetamiento en `docker-compose.yml`. |

## 🔐 5. Login seguro D1

- Las contraseñas nuevas se almacenan como Argon2id con salt aleatorio; las consultas SQL usan parámetros.
- Al iniciar, la aplicación migra contraseñas heredadas en texto plano a Argon2id y vacía la columna antigua `password`. Los hashes PBKDF2 existentes se actualizan a Argon2id tras un inicio de sesión correcto.
- El login limita intentos por IP (Token Bucket: cinco intentos y recarga de uno por minuto) y responde `429` con `Retry-After`.
- Las API requieren sesión; la cookie es `HttpOnly`, `Secure` y `SameSite=Strict`, expira en una hora y se invalida al cerrar sesión. Las sesiones viven en memoria y se revocan al reiniciar el servidor. La administración de usuarios y la auditoría se reservan al rol Administradora.
- El servidor escucha en `127.0.0.1` por defecto. No debe exponerse directamente por HTTP; para acceso remoto se requiere HTTPS, por ejemplo mediante un proxy inverso.
- En una base nueva se crea `demo@lacolmena.co` con una contraseña aleatoria que se muestra una sola vez en la consola; también puede definirse `LACOLMENA_INITIAL_ADMIN_PASSWORD` (mínimo 12 caracteres) antes del primer inicio. No se publica una contraseña demo en la interfaz.

### Roles y permisos

- **Administradora:** acceso completo, incluida la creación y eliminación de cuentas. No puede eliminar su propia cuenta ni la última cuenta administradora.
- **Vendedor:** consulta el catálogo y registra ventas; no puede ajustar el stock ni gestionar usuarios.
- **Cliente:** consulta el catálogo sin ver existencias y realiza compras. El servidor usa la cuenta autenticada como comprador y descuenta el stock de forma transaccional.
- Los permisos se verifican en la API; ocultar opciones en la interfaz no sustituye la autorización del servidor. Al eliminar una cuenta también se revocan sus sesiones activas.

## 📬 6. CloudEvents y Transactional Outbox D3

- Los eventos se guardan como JSON CloudEvents 1.0 con `specversion`, UUIDv4 `id`, `source` con prefijo `bina`, tipo en pasado, `datacontenttype` y `data`.
- Las ventas, ajustes de inventario, cuentas y transiciones de pedido escriben su cambio de negocio y el evento `PENDING` en una única transacción SQLite.
- El worker asíncrono publica CloudEvents JSON en el topic durable `bina.events` de Apache Kafka y solo marca `PROCESSED` tras la confirmación del broker (`acks=all`). Si Kafka no está disponible, el evento permanece pendiente en SQLite y se reintenta.
- La entrega es al menos una vez: un consumidor debe deduplicar usando el `id` CloudEvent.

Para levantar Kafka localmente con Docker Compose, configura la dirección del broker en PowerShell:

```powershell
$env:LACOLMENA_KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
docker compose up -d broker
docker compose ps
Test-NetConnection 127.0.0.1 -Port 9092
python main.py
```

Para inspeccionar los CloudEvents desde otra consola:

```powershell
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bina.events --from-beginning
```

Kafka debe estar disponible antes de procesar eventos; la aplicación puede iniciar sin el broker y conservará los eventos pendientes en SQLite. El Compose de desarrollo expone el listener solo en `127.0.0.1` y no habilita autenticación; no debe exponerse a redes no confiables.

## 🔁 7. Saga, compensación e idempotencia D4

### Decisión CAP

El teorema CAP establece que, durante una partición de red, un sistema distribuido no puede garantizar simultáneamente consistencia linealizable y disponibilidad para todas las solicitudes. CAP aplica a los límites distribuidos (por ejemplo, la comunicación con pagos y Kafka), no a SQLite ejecutándose localmente por sí solo. Ante una partición que impida obtener una confirmación fiable de la pasarela, La Colmena prioriza **consistencia (CP)** para el checkout: no declara la orden confirmada ni permite completar el flujo sin confirmar el cobro. La solicitud falla o queda en compensación; se prefiere perder disponibilidad temporal a vender sin stock reservado o registrar un pago incierto como exitoso.

El Outbox conserva disponibilidad para confirmar una transacción local y publicar sus eventos posteriormente, pero esa entrega eventual no equivale a consistencia fuerte entre SQLite, Kafka y la pasarela. En producción, un timeout con resultado de pago desconocido debe reconciliarse consultando la pasarela con la misma clave idempotente antes de liberar inventario.

### Máquina Saga y C1

Cada orden persiste un estado Saga independiente del estado comercial:

- `PENDING` puede avanzar a `CONFIRMED` o `COMPENSATING`.
- `CONFIRMED` puede avanzar a `COMPENSATING` si falla un paso posterior al cobro.
- `COMPENSATING` termina en `COMPENSATED` solo después de completar las compensaciones.

Las transiciones no permitidas se rechazan. C1 restaura el stock en una transacción SQLite; si el cobro ya fue confirmado, primero solicita un reembolso con una clave determinista. Si el reembolso falla, la Saga conserva `COMPENSATING` y no devuelve el stock hasta reanudar la compensación.

### Consumidor HTTP e idempotencia

`POST /api/ordenes` requiere una sesión autenticada, una cabecera `Idempotency-Key` (8–120 caracteres), `direccion` y una lista `items` con `codigo` y `cantidad`. La clave se persiste con restricción única en SQLite. Repetirla devuelve la misma orden sin volver a reservar stock ni duplicar el cobro; las claves de cobro y reembolso derivan de esa clave. La respuesta incluye `pedido.id`, `pedido.estado` y `pedido.saga_status`. Los errores de validación responden `400`; una orden compensada o con compensación pendiente responde `409`.

El simulador de pagos conserva sus claves en memoria; una pasarela real debe proporcionar deduplicación durable por clave para proteger también frente a reinicios del proceso.

### Despliegue

El archivo `docker-compose.yml` levanta un broker Kafka KRaft de un solo nodo para desarrollo local. La configuración no es un clúster de producción; para ese uso configura alta disponibilidad, retención y seguridad de transporte/autenticación.

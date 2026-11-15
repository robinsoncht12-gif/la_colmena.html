# 🏥 LaColmena MVP — GUÍA DE ARQUITECTURA Y ENTREGAS INCREMENTALES
**Asignatura:** [IF0129] Ingeniería de Software 3 | **Institución:** Corporación Universitaria Remington  
**Docente Titular:** Ing. Jhon Alexander Rosero Lasso, MSc en Inteligencia Artificial  
**Tecnología Base:** Python Estándar (3.10+) + Web Nativo; `argon2-cffi` para hashing y `pika` para RabbitMQ.

---

## 🏛️ 1. Principios de Arquitectura Aplicados (IS1, IS2 e IS3)

El sistema base está diseñado como un **Monolito Modular Limpio**, garantizando:
1. **SOLID:**
   - **SRP (Single Responsibility):** Modelos (`models.py`), Abstracciones (`interfaces.py`), Repositorios (`repositories.py`) y Servicios (`services.py`) estrictamente separados.
   - **DIP (Dependency Inversion):** Los servicios no dependen de SQLite directamente, sino de las interfaces `IProductoRepository` e `IPedidoRepository`.
2. **ACID Local:** Persistencia atómica en SQLite con aislamiento y control transaccional nativo.
3. **SoD (Separation of Duties / Separation of Concerns):** El plano de presentación web (`app.py`) no contiene lógica de negocio ni sentencias SQL.
4. **C4 Model Ready:** Estructura organizada para ser mapeada de forma natural en Nivel 1 (Contexto), Nivel 2 (Contenedor Web/DB) y Nivel 3 (Componentes).

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
| **S1** | **Act1 (25% Seg.)** | **Diagnóstico y Modelo C4:** Diagramar Contexto, Contenedores y Componentes del monolito actual y calcular métricas de acoplamiento $I = C_e / (C_a + C_e)$. |
| **S2** | **PRIMER PARCIAL (25%)** | **Descomposición y CloudEvents:** Separar el catálogo de productos del servicio de órdenes y emitir eventos bajo el estándar **CNCF CloudEvents v1.0** en formato JSON. |
| **S3** | **Act2 (25% Seg.)** | **Transactional Outbox y Rate Limiting:** Activar el procesamiento de la `outbox_table` para evitar la Doble Escritura (Dual-Write) y agregar limitación de tasa (Token Bucket) al login. |
| **S4** | **SEGUNDO PARCIAL (25%)** | **Orquestador Saga y Compensación Inversa:** Implementar la máquina de estados completa ($T_1 \to T_2 \to T_3$), probar la inyección de fallos con compensación ($C_1$) y verificar la Idempotency Key. |
| **S5** | **Act3 (25% Seg.)** | **Resiliencia (Circuit Breaker y OpenTelemetry):** Proteger la pasarela de pagos con Circuit Breaker (`CLOSED`, `OPEN`, `HALF-OPEN`) e inyectar cabeceras W3C `traceparent`. |
| **S6** | **EXAMEN FINAL (25%)** | **Evaluación ATAM y Cloud Native:** Sustentación formal de trade-offs arquitectónicos con el método ATAM del SEI y empaquetamiento en `docker-compose.yaml`. |

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
- El worker asíncrono publica al exchange/topic durable `bina.events` en RabbitMQ y solo marca `PROCESSED` tras la confirmación del broker. Si RabbitMQ no está disponible, el evento permanece pendiente y se reintenta.
- La entrega es al menos una vez: un consumidor debe deduplicar usando el `id` CloudEvent.

Para levantar RabbitMQ localmente con Docker Compose, configura credenciales locales y la URL AMQP en PowerShell. El volumen conserva las credenciales iniciales; reutiliza la misma contraseña mientras conserves ese volumen:

```powershell
$env:RABBITMQ_DEFAULT_USER = "lacolmena"
$env:RABBITMQ_DEFAULT_PASS = "REEMPLAZA_Y_GUARDA_TU_CLAVE_LOCAL"
$env:LACOLMENA_RABBITMQ_URL = "amqp://$($env:RABBITMQ_DEFAULT_USER):$($env:RABBITMQ_DEFAULT_PASS)@127.0.0.1:5672/%2F"
docker compose up -d rabbitmq
docker compose ps
Test-NetConnection 127.0.0.1 -Port 5672
python main.py
```

RabbitMQ debe estar disponible antes de procesar eventos; la aplicación puede iniciar sin el broker y conservará los eventos pendientes en SQLite.

# 🏥 LaColmena MVP — GUÍA DE ARQUITECTURA Y ENTREGAS INCREMENTALES
**Asignatura:** [IF0129] Ingeniería de Software 3 | **Institución:** Corporación Universitaria Remington  
**Docente Titular:** Ing. Jhon Alexander Rosero Lasso, MSc en Inteligencia Artificial  
**Tecnología Base:** Python Estándar (3.10+) + Web Nativo (**CERO Librerías Externas / Cero `pip install`**)

---

## 🏛️ 1. Principios de Arquitectura Aplicados (IS1, IS2 e IS3)

El sistema base está diseñado como un **Monolito Modular Limpio**, garantizando:
1. **SOLID:**
   - **SRP (Single Responsibility):** Modelos (`models.py`), Abstracciones (`interfaces.py`), Repositorios (`repositories.py`) y Servicios (`services.py`) estrictamente separados.
   - **DIP (Dependency Inversion):** Los servicios no dependen de SQLite directamente, sino de las interfaces `IProductoRepository` e `IPedidoRepository`.
2. **ACID Local:** Persistencia atómica en SQLite con aislamiento y control transaccional nativo.
3. **SoD (Separation of Duties / Separation of Concerns):** El plano de presentación web (`app.py`) no contiene lógica de negocio ni sentencias SQL.
4. **C4 Model Ready:** Estructura organizada para ser mapeada de forma natural en Nivel 1 (Contexto), Nivel 2 (Contenedor Web/DB) y Nivel 3 (Componentes).

---

## 🚀 2. Ejecución Rápida en Local
```bash
cd 03_Bloque_Sistemas_Complejos/Ingenieria_de_Software_3/04_Laboratorios_y_Codigo/proyecto_LaColmena_app
python3 main.py
```
El servidor levantará en `http://localhost:8086` con:
- Catálogo de productos y stock en tiempo real.
- Carrito y simulador transaccional.
- Monitor de la máquina de estados Saga.
- Tabla `outbox_table` para captura de eventos CDC.

---

## 📦 3. Hoja de Ruta de Mejoras Incrementales por Sesión

| Sesión | Hito Evaluativo | Mejora Incremental en la Aplicación LaColmena |
| :---: | :---: | :--- |
| **S1** | **Act1 (25% Seg.)** | **Diagnóstico y Modelo C4:** Diagramar Contexto, Contenedores y Componentes del monolito actual y calcular métricas de acoplamiento $I = C_e / (C_a + C_e)$. |
| **S2** | **PRIMER PARCIAL (25%)** | **Descomposición y CloudEvents:** Separar el catálogo de productos del servicio de órdenes y emitir eventos bajo el estándar **CNCF CloudEvents v1.0** en formato JSON. |
| **S3** | **Act2 (25% Seg.)** | **Transactional Outbox y Rate Limiting:** Activar el procesamiento de la `outbox_table` para evitar la Doble Escritura (Dual-Write) y agregar limitación de tasa (Token Bucket) en la API. |
| **S4** | **SEGUNDO PARCIAL (25%)** | **Orquestador Saga y Compensación Inversa:** Implementar la máquina de estados completa ($T_1 \to T_2 \to T_3$), probar la inyección de fallos con compensación ($C_1$) y verificar la Idempotency Key. |
| **S5** | **Act3 (25% Seg.)** | **Resiliencia (Circuit Breaker y OpenTelemetry):** Proteger la pasarela de pagos con Circuit Breaker (`CLOSED`, `OPEN`, `HALF-OPEN`) e inyectar cabeceras W3C `traceparent`. |
| **S6** | **EXAMEN FINAL (25%)** | **Evaluación ATAM y Cloud Native:** Sustentación formal de trade-offs arquitectónicos con el método ATAM del SEI y empaquetamiento en `docker-compose.yaml`. |

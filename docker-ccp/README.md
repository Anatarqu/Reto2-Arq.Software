# Infraestructura Docker Desktop — Validación de Arquitectura CCP (Reto 2)

Este paquete contiene la infraestructura completa y automatizada en **Docker Compose** para desplegar y validar localmente en **Docker Desktop** todos los componentes, tácticas y experimentos de los cuatro **Escenarios de Calidad (ASRs)** de la **Compañía Comercializadora de Productos (CCP)**.

---

## 1. Mapeo Arquitectónico de Contenedores

| Contenedor | Imagen / Tecnología | Puerto Expuesto | Rol Arquitectónico & Escenario Vinculado |
| :--- | :--- | :--- | :--- |
| **`ccp-api-gateway`** | Nginx 1.25 Alpine | `8080:80` | Punto de entrada perimetral, rate limiting, enrutamiento dinámico activo/standby (ASR DISP-02, AD-D1). |
| **`ccp-svc-ventas-main`** | Python 3.12 / FastAPI | `8001:8000` | Instancia Principal de Ventas con sondeo profundo `/health/deep` y transacciones pesimistas (ASR AD-D1, AD-R2). |
| **`ccp-svc-ventas-spare`** | Python 3.12 / FastAPI | `8002:8000` | Instancia Standby (Warm Backup Multi-AZ) para conmutación transparente (ASR AD-D1). |
| **`ccp-health-monitor`** | Python 3.12 Daemon | Interno | Daemon de sondeo funcional cada 5s, control de concesión (Lease) de 3 fallos y emisión de alertas AMQP (ASR AD-D1). |
| **`ccp-ueba-worker`** | Python 3.12 / FastAPI | `8005:8005` | Motor de analítica de comportamiento (UEBA) y evaluador de riesgo para revocación atómica en Redis (ASR SEC-01). |
| **`ccp-redis`** | Redis 7 Alpine | `6379:6379` | Almacén en memoria para Idempotencia (`SETNX`) y Lista Negra de Tokens (`ccp:blacklist:<token>`) (ASR AD-R2, SEC-01). |
| **`ccp-postgres`** | PostgreSQL 15 Alpine | `5432:5432` | Base de datos relacional con **Row-Level Security (RLS)** nativo para aislamiento 100% de carteras (ASR SEC-02, AD-R2). |
| **`ccp-pgbouncer`** | PgBouncer 1.21 | `6432:6432` | Pool de conexiones para optimizar el rendimiento y transacciones concurrentes (ASR AD-R2). |
| **`ccp-rabbitmq`** | RabbitMQ 3.12 Management | `5672`, `15672` | Broker de mensajería con colas preconfiguradas: `failover.events.v1`, `orders.dlq` y `sec.events.v1`. |

---

## 2. Requisitos Previos

* **Docker Desktop** (versión 4.20 o superior) instalado y en ejecución en Windows, macOS o Linux.
* Asignación recomendada en Docker Desktop: al menos 4 GB de memoria RAM y 2 CPUs.
* Python 3.10+ en la máquina host con la librería `requests` instalada:
  ```bash
  pip install requests
  ```

---

## 3. Instrucciones de Despliegue Rápido

1. **Descomprimir el archivo:**
   Extrae el contenido de `ccp-docker-infrastructure.zip` en tu carpeta de trabajo.

2. **Iniciar la infraestructura con Docker Compose:**
   Abre una terminal en la carpeta extraída y ejecuta:
   ```bash
   docker compose up -d --build
   ```

3. **Verificar que todos los servicios estén saludables:**
   ```bash
   docker compose ps
   ```
   *Nota: Espera unos 15-20 segundos mientras PostgreSQL y RabbitMQ completan su inicialización y ejecutan sus scripts de inicio.*

4. **Acceso a Interfaces Administrativas:**
   * **API Gateway (Ingress):** `http://localhost:8080`
   * **Consola RabbitMQ:** `http://localhost:15672` (Usuario: `ccp_admin`, Contraseña: `ccp_pass_2026`)
   * **Base de Datos PostgreSQL:** `localhost:5432` (Base de datos: `ccp_db`, Usuario: `ccp_app_user`, Pass: `ccp_secure_pass_2026`)

---

## 4. Ejecución de los Experimentos de Arquitectura

Para ejecutar toda la suite de experimentos automatizados en un solo paso:
```bash
python scripts/run_all_experiments.py
```

O puedes ejecutar cada experimento de forma individual para inspeccionar los resultados:

### Experimento E01 — Detección Oportuna y Conmutación por Error (ASR AD-D1)
Valida que al degradar la instancia principal, el monitor detecta la falla funcional en ≤ 15 segundos y el Traffic Router redirige las peticiones al nodo Standby sin pérdida de servicio.
```bash
python scripts/test_e01_failover.py
```

### Experimento E02 — Idempotencia y Prevención de Sobreventa (ASR AD-R2)
Dispara 20 hilos concurrentes compitiendo por un inventario limitado de 10 unidades, comprobando que `SELECT FOR UPDATE` previene la sobreventa (0 inconsistencias) y que `X-Idempotency-Key` en Redis absorbe los reintentos duplicados.
```bash
python scripts/test_e02_idempotency_concurrency.py
```

### Experimento E03 — Detección de Suplantación con UEBA y Lista Negra (ASR SEC-01)
Simula un viaje imposible (salto geográfico en 3 minutos) hacia el motor UEBA, verificando que calcula un score de riesgo ≥ 80 y revoca el token en Redis en ≤ 10 ms, bloqueando la siguiente llamada con HTTP 401 Unauthorized.
```bash
python scripts/test_e03_ueba_blacklist.py
```

### Experimento E04 — Aislamiento Multitenant con ABAC y PostgreSQL RLS (ASR SEC-02)
Valida que al consultar clientes sin cláusula WHERE, el motor PostgreSQL aplica físicamente el filtro de Row-Level Security restringiendo a los clientes del vendedor autenticado, y comprueba que un ataque IDOR cruzado recibe inmediatamente una respuesta HTTP 403 Forbidden (0% fuga de datos).
```bash
python scripts/test_e04_abac_rls.py
```

---

## 5. Detener la Infraestructura

Cuando termines tus pruebas:
```bash
docker compose down -v
```
*(El flag `-v` limpia los volúmenes de datos para permitir pruebas limpias en el siguiente inicio).*

# Experimento CCP — caída del Servicio de Ventas con redundancia pasiva

Guía paso a paso para correr el experimento en **Windows 11**, ver cómo ocurre el failover y sacar los datos para presentarlos.
Todo corre dentro de contenedores Docker: **no necesitas instalar Python, PostgreSQL, RabbitMQ ni etcd en Windows.**

---

## 0. Qué vas a tener funcionando

```
                        ┌────────────────────────── red ccp_coord ──────────────────────────┐
                        │   etcd1   etcd2   etcd3   (consenso Raft: líder, latidos, leases)  │
                        │                                                                    │
 runner ──► router ─────┼──► ventas-a (ACTIVO) ◄──┐                                          │
 (carga +   :8080       │    ventas-b (STANDBY)   │ latido cada 1 s ──► etcd                 │
 fallas)                │                         │                                          │
                        │    promotor  ◄── lee latidos, detecta, hace fencing y promueve     │
                        └────────────────────────────────────────────────────────────────────┘
          red ccp_data:   postgres (DB Core)    rabbitmq (DLQ)  ◄── reprocesador (backoff exponencial)
```

| Contenedor | Elemento de tu arquitectura | Qué hace en el experimento |
|---|---|---|
| `ventas-a`, `ventas-b` | Servicio de Ventas + **warm spare** | Solo el que es líder atiende `POST /pedidos` (pedido + reserva en **una** transacción ACID, con *idempotency key*). El otro espera. |
| `promotor` | Monitor de Heartbeat + Promotor de Failover | Cada 0,5 s mira si el líder sigue latiendo. Si no: **fencing** en la BD (sube el epoch) y publica el nuevo líder en etcd. |
| `etcd1-3` | Gestor de Lease (Raft) | Guarda quién es el líder (`/ccp/ventas/leader`, con su **epoch** = *fencing token*) y los latidos (`/ccp/hb/…`, lease de 3 s). |
| `router` | Balanceador / API Gateway (simplificado) | Reenvía al líder vigente. No reintenta: reintenta el cliente con la misma llave. |
| `postgres` | DB Core | Tablas `pedido`, `reserva`, `existencia`, `registro_idempotencia` (UNIQUE), `fencing`, `epoca`, `eventos`. |
| `rabbitmq` + `reprocesador` | DLQ / Manejo de fallos | Pedidos que el servicio no pudo confirmar quedan en cola y se reintentan con backoff 1-2-4-8-16 s. |
| `runner` | k6 + inyector de fallas + verificador | Genera la carga, rompe cosas, mide y verifica la integridad con SQL. |

### Los 5 casos

| Caso | Falla inyectada (a los 15 s de iniciada la carga) | Qué demuestra |
|---|---|---|
| **C0** | Ninguna (control, 10 min) | Que el sistema **no hace failovers falsos** bajo carga normal. |
| **C1** | `docker kill` al activo | Detección y recuperación ante caída abrupta. |
| **C2** | `docker pause` 20 s al activo, luego lo descongela | **Zombi**: el viejo líder despierta creyéndose activo y trata de escribir → el fencing lo rechaza. |
| **C3** | Desconecta al activo de la red de etcd 20 s (sigue viendo la BD) | Partición: el viejo líder sigue vivo y escribiendo con un epoch viejo → el fencing lo rechaza. |
| **C4** | `docker kill` **en el instante en que hay transacciones abiertas** | Que no queden pedidos a medias (todo o nada) y que los reintentos no dupliquen. |

### Lo que NO se pudo probar antes de entregártelo (léelo)

Probé **la misma lógica** (servicios, promotor, fencing, DLQ, generador de carga, verificación y análisis) contra **etcd (3 nodos), PostgreSQL 16 y RabbitMQ reales**, corriendo como procesos en Linux, con las fallas emuladas (`SIGKILL`, `SIGSTOP`, `iptables`). Los 5 casos funcionaron de punta a punta.
**No pude probar:** `docker compose up`/`build` en tu máquina, las llamadas del *runner* a Docker (kill/pause/desconectar red), el montaje de `docker.sock` en Windows y la imagen de etcd v3.5 (probé con v3.4, misma API). Por eso el **paso 5 (prueba rápida)** existe: si algo falla, falla ahí y barato. La sección 10 lista los problemas probables.

---

## 1. Requisitos (una sola vez)

1. **Windows 11** con virtualización habilitada (Administrador de tareas → Rendimiento → CPU → *Virtualización: Habilitado*).
2. **Docker Desktop** (https://www.docker.com/products/docker-desktop/) con el backend **WSL 2** y **contenedores Linux** (valores por defecto).
3. **Recursos para WSL 2.** Crea (o edita) `C:\Users\<tu-usuario>\.wslconfig`:
   ```ini
   [wsl2]
   memory=6GB
   processors=4
   ```
   y ejecuta en PowerShell: `wsl --shutdown`; vuelve a abrir Docker Desktop. (Con menos CPU aparecen *failovers falsos* en C0: es información válida, pero conviene saber la causa.)
4. ~10 GB libres en disco. Internet para bajar las imágenes la primera vez (~1,5 GB).
5. Verifica en PowerShell:
   ```powershell
   docker version
   docker compose version
   ```

> **Tip de medición:** cierra Teams/Chrome pesados durante la campaña larga y desactiva la suspensión (Configuración → Sistema → Inicio/apagado y batería → *Pantalla y suspensión → Nunca* mientras esté conectado).

## 2. Preparar la carpeta

Descomprime el zip en una ruta corta, **fuera de OneDrive** y sin espacios, por ejemplo `C:\ccp-exp`.

```powershell
cd C:\ccp-exp
dir -Force      # -Force muestra los archivos que empiezan con punto (.env)
```
Debes ver `docker-compose.yml`, `Dockerfile`, `.env`, `db\`, `src\` y `results\`.

Abre `.env`: ahí están los parámetros del experimento (heartbeat 1 s, lease 3 s, sondeo 0,5 s).

## 3. Levantar el sistema

```powershell
docker compose up -d --build
docker compose ps
```
La primera vez tarda unos minutos (descarga imágenes y construye la tuya). Espera ~20 s y comprueba:

```powershell
curl.exe http://localhost:8080/leader
```
Esperado: `{"id": "ventas-a", "epoch": 1}` (o `ventas-b`). Ese `epoch` es el **fencing token**.

## 4. Prueba manual: ver el failover con tus propios ojos (10 min)

Abre **tres** terminales PowerShell en `C:\ccp-exp`.

**Terminal 1 — el "proceso" en vivo (déjala abierta):**
```powershell
docker compose logs -f --tail 0 promotor router ventas-a ventas-b reprocesador
```

**Terminal 2 — acciones:**
```powershell
# a) datos limpios
docker compose run --rm runner python -m exp.seed

# b) un pedido (una "orden" de un vendedor a un tendero, 2 unidades del producto 1 en la bodega 1)
$body = '{"vendedor_id":1,"tendero_id":2,"lineas":[{"producto_id":1,"bodega_id":1,"cantidad":2}]}'
Invoke-RestMethod -Method Post -Uri http://localhost:8080/pedidos -ContentType "application/json" `
  -Headers @{"Idempotency-Key"="demo-1"} -Body $body
# Respuesta: pedido_id, replay=false, instancia, epoch

# c) el MISMO pedido otra vez (misma llave): NO se duplica
Invoke-RestMethod -Method Post -Uri http://localhost:8080/pedidos -ContentType "application/json" `
  -Headers @{"Idempotency-Key"="demo-1"} -Body $body
# Respuesta: el mismo pedido_id y replay=true

# d) mira qué guardó etcd (líder + latidos)
docker compose exec etcd1 etcdctl get --prefix /ccp/

# e) MATA al líder (cambia el nombre si el líder es ventas-b)
docker kill ventas-a
```
En la **Terminal 1** verás, en este orden: `FALLA DETECTADA` → `promoviendo ventas-b con epoch 2` → `líder publicado` → `nuevo líder` (router). Luego:

```powershell
curl.exe http://localhost:8080/leader          # ahora ventas-b, epoch 2
Invoke-RestMethod -Method Post -Uri http://localhost:8080/pedidos -ContentType "application/json" `
  -Headers @{"Idempotency-Key"="demo-2"} -Body $body
docker start ventas-a                          # vuelve como STANDBY (warm spare)
```

**Mirar por dentro (opcional pero muy útil para la presentación):**
- **RabbitMQ:** http://localhost:15672 (usuario `ccp`, clave `ccp`) → *Queues*: `pedidos.dlq` y las colas `retry`.
- **PostgreSQL:** con DBeaver/pgAdmin en `localhost:15432`, usuario `ccp`, clave `ccp`, base `ccp`:
  ```sql
  SELECT * FROM fencing;                              -- epoch mínimo aceptado
  SELECT * FROM epoca ORDER BY epoch;                 -- historia de líderes
  SELECT * FROM eventos ORDER BY ts DESC LIMIT 20;    -- falla_detectada, fencing_aplicado, lider_publicado
  SELECT estado, epoch, instancia, count(*) FROM pedido GROUP BY 1,2,3;
  ```
- **Docker Desktop → Containers:** verás `ventas-a` en rojo (killed) o con el ícono de pausa durante C2.

## 5. Prueba rápida del experimento automatizado (~12 min) — hazla antes de la campaña

```powershell
docker compose run --rm runner python -m exp.run --quick --case ALL
```
Imprime **una línea por segundo** (`ok`, `en_cola`, `fallos`, líder y epoch) y, al final de cada corrida, un `RESULTADO` con detección, recuperación y los contadores de integridad. Debes ver, en cada caso con falla, la caída de `ok` a 0 unos segundos y la recuperación después de ver cambiar el líder.

Después genera el informe:
```powershell
docker compose run --rm runner python -m exp.analyze
```
y abre `results\informe\`. Si todo se ve bien, **borra los resultados de prueba** antes de la campaña real:
```powershell
Remove-Item -Recurse -Force .\results\* -Exclude .gitkeep
```

## 6. La campaña completa (≈ 4 h)

```powershell
docker compose run --rm runner python -m exp.run --case ALL --reps 30 --rate 100
```
Orden: 3 corridas de control de 10 min (C0) y luego, intercalado, C1–C4 × 30 repeticiones de 60 s cada una (≈ 105 s por corrida contando reinicio y espera de la DLQ).

- **Se puede interrumpir** (Ctrl+C) y **continuar**: vuelve a ejecutar el mismo comando; salta las corridas ya registradas en `summary.csv`.
- Para un caso suelto: `--case C2 --reps 10`.
- Para repetir todo desde cero ignorando lo hecho: `--no-resume` (mejor vaciar `results\` antes).
- `docker compose logs -f` en otra terminal mientras corre, si quieres ver el proceso.

### Carga pico (800 pedidos/s)
Primero **calibra** que tu máquina la sostiene sin fallar (no queremos medir el límite de tu laptop):
```powershell
docker compose run --rm -e RESULTS_DIR=/results/pico800 runner python -m exp.run --case C0 --reps 1 --c0-duration 90 --rate 800
```
Mira `lat_p95_ms`, `intentos_fallidos` y `falsos_failovers` en `results\pico800\summary.csv`. (En mi sandbox de 2 núcleos, 800/s saturó incluso el control; en un portátil con 4+ núcleos asignados a WSL puede aguantar, pero compruébalo.) Si hay fallos sin haber inyectado nada, 800/s excede tu máquina (el servicio es 1 proceso Python): baja el `--rate` y repórtalo como limitación. Si está limpio:
```powershell
docker compose run --rm -e RESULTS_DIR=/results/pico800 runner python -m exp.run --case ALL --reps 10 --rate 800
docker compose run --rm -e RESULTS_DIR=/results/pico800 runner python -m exp.analyze
```

### Variante "parámetros de DIS-01" (para contrastar)
Tu documento tiene dos tiempos distintos (sondeo de 5 s × 3 fallos ≈ 15 s en DIS-01 vs. heartbeat de 1 s en la vista de despliegue). Para medir el segundo caso: edita `.env` con `HEARTBEAT_S=5` y `LEASE_TTL_S=15`, ejecuta `docker compose up -d --force-recreate`, y corre con `-e RESULTS_DIR=/results/dis01`. Lo esperable es una detección de ~10–15 s: el umbral de 10,8 s **no se cumpliría**, y eso es un resultado válido para tu presentación (muestra que el parámetro, no la arquitectura, decide el criterio). Recuerda volver `.env` a 1 y 3.

## 7. Qué se mide y cómo

| Columna (`summary.csv`) | Definición | Criterio de aceptación |
|---|---|---|
| `deteccion_s` | Desde la inyección hasta el evento `falla_detectada` del promotor (reloj único del host Docker). | p95 **< 10,8 s** |
| `promocion_s` | Hasta que el nuevo líder queda publicado en etcd (después del fencing). | informativo |
| `recuperacion_s` | Hasta la primera respuesta 200/201 de una instancia con **epoch mayor** al anterior. | p95 **≤ 30 s** |
| `max_gap_s` | Mayor intervalo sin ninguna confirmación a partir de la falla. | informativo |
| `perdidos` | Pedidos que el cliente recibió como aceptados (200/201/202) y **no existen** en la BD tras vaciarse la DLQ. | **0** |
| `reservas_duplicadas` | Unidades reservadas por encima de las pedidas (doble descuento de stock). | **0** |
| `stock_inconsistente`, `pedidos_incompletos` | `inicial ≠ disponible + reservado`, `reservado ≠ Σ reservas`, pedidos sin todas sus líneas/reservas. | **0** |
| `escrituras_epoch_viejo` | Pedidos confirmados con un epoch anterior al vigente en ese momento (auditoría en BD) **+** respuestas 201 de las escrituras directas del zombi. | **0** |
| `falsos_failovers` | En C0: cualquier `falla_detectada`. En C1–C4: detecciones además de la esperada. | **0** en C0 |
| `dlq_residual` | Mensajes que siguen en la DLQ/retry/parked tras esperar (máx. 5 min). | **0** |
| `tx_abiertas_al_matar` | (C4) Transacciones abiertas en el instante de la muerte: prueba de que se mató "a mitad". | informativo |
| `zombi_respuestas` | (C2/C3) Qué contestó el viejo líder a 10–20 escrituras directas: lo esperable es `202` (rechazado por epoch y enviado a la DLQ). | sin `201` |
| `lat_p50/p95/p99_ms`, `intentos_fallidos`, `no_confirmados` | Experiencia del cliente. `no_confirmados` = pedidos a los que el cliente renunció tras 8 intentos (indisponibilidad, no pérdida de datos). | informativo |

**Hallazgo que verás en C2:** `promocion_s` puede ser unos segundos mayor que `deteccion_s`. Razón: el zombi congelado puede tener una transacción *dentro de su ventana de commit* que sostiene el bloqueo que el fencing necesita; la BD la cancela a los 5 s (`idle_in_transaction_session_timeout`). Es el costo real de fencing estricto en la BD y es un buen punto de discusión (alternativas: fencing en el almacén de objetos, lease corto con *self-fencing* en el servicio).

## 8. Obtener y presentar los datos

```powershell
docker compose run --rm runner python -m exp.analyze
```

Todo queda en tu carpeta `C:\ccp-exp\results\`:

| Archivo | Para qué |
|---|---|
| `summary.csv` | Una fila por corrida. Es la fuente de todo. |
| `<corrida>\requests.csv` | **Cada intento HTTP** (tiempo de envío/fin, estado, instancia, epoch, latencia). |
| `<corrida>\orders.csv` | Una fila por pedido (intentos, estado final). |
| `<corrida>\events.csv` | Línea de tiempo: inyección, falla detectada, fencing, líder publicado. |
| `<corrida>\meta.json` | Parámetros y métricas de esa corrida. |
| `informe\criterios.csv` | **¿Se cumple la hipótesis?** una fila por criterio (SÍ/NO). |
| `informe\resumen_por_caso.csv` | Mediana / p95 / máximo por caso. |
| `informe\resultados.xlsx` | Las tablas anteriores + todas las corridas, listo para Excel. |
| `informe\fig1_deteccion_recuperacion.png` | Distribución de tiempos contra los umbrales (10,8 s y 30 s). |
| `informe\fig2_lineas_de_tiempo.png` | Qué vio el cliente segundo a segundo, con las marcas *inyección → falla detectada → nuevo líder*. **La mejor figura para explicar el failover.** (En C2 hay un hueco justo después de la inyección: las peticiones quedan colgadas contra el proceso congelado hasta el timeout de 2 s del router.) |
| `informe\fig3_impacto_cliente.png` | Intentos fallidos y mayor hueco sin servicio. |

**Cómo armar la presentación:** (1) hipótesis y criterios; (2) arquitectura (el diagrama de arriba); (3) `fig2` de un caso (C1) y luego la de C2/C3 para explicar el zombi y el fencing; (4) `fig1` + `criterios.csv`; (5) tabla de integridad (todo en 0) con la consulta SQL que la respalda; (6) limitaciones (sección 9) y la variante DIS-01.

## 9. Validez: qué NO demuestra este experimento

- **Un solo host:** todos los contenedores comparten CPU, disco y reloj. Los tiempos son válidos como orden de magnitud y comparativos entre casos, no como SLA de producción.
- **Sin red real:** `docker kill/pause/network disconnect` emulan fallas, no pérdidas de paquetes ni latencias de una región.
- **Servicio mínimo:** no hay API Gateway real, ABAC/RLS, UEBA ni auditoría WORM (no son parte de esta hipótesis).
- **Cambios frente al plan original** (por simplicidad/medición): el generador de carga es propio en Python (no k6) porque necesitaba el detalle de cada intento y reintentar con la misma llave; el router es un proxy Python en vez de HAProxy/Traefik; el Monitor y el Promotor son un solo proceso; no hay Redis (la unicidad en BD es la fuente de verdad de la idempotencia).
- **Fencing por BD:** protege lo que se escribe en PostgreSQL. Un efecto lateral fuera de la BD (enviar un correo) no estaría protegido.
- **Cliente que reintenta 8 veces (~14 s):** si el failover tardara más, aparecerían `no_confirmados` (indisponibilidad).

## 10. Si algo falla

| Síntoma | Causa probable / solución |
|---|---|
| `port is already allocated` (8080, 15672, 15432) | Otro programa usa el puerto. Cambia el número de la izquierda en `ports:` de `docker-compose.yml`. |
| `pull access denied` / timeout al bajar `quay.io/coreos/etcd` | Red corporativa o VPN. Prueba sin VPN, o cambia la imagen por `gcr.io/etcd-development/etcd:v3.5.17`. |
| El runner dice `Error while fetching server API version` o `permission denied` en `docker.sock` | Docker Desktop no está en modo Linux/WSL 2, o el socket no se montó. Verifica *Settings → General → Use the WSL 2 based engine* y reinicia Docker. |
| `el sistema no quedó sano a tiempo` | Mira `docker compose ps` y `docker compose logs --tail 50 ventas-a promotor etcd1`. Casi siempre un contenedor reiniciándose o etcd sin quórum. |
| Cambié `db\init.sql` y no se refleja | Postgres solo aplica el script al crear el contenedor: `docker compose down -v` y vuelve a `up -d --build`. |
| `falsos_failovers` > 0 en C0 | La máquina está saturada o el reloj de WSL se desfasó. Sube CPU en `.wslconfig`, cierra aplicaciones, `wsl --shutdown`. Reporta el dato: es un resultado (el heartbeat de 1 s/lease de 3 s no tolera esa carga). |
| Muchos `fallos` en C0 con 800/s | La carga excede un único proceso de ventas: baja `--rate` (ver calibración). |
| Quedó algo roto tras Ctrl+C | `docker compose down` y `docker compose up -d`; el runner también restaura contenedores pausados/desconectados al iniciar cada corrida. |
| Quiero empezar de cero | `docker compose down -v` y vaciar `results\`. |

## 11. Limpiar
```powershell
docker compose down -v        # detiene y borra contenedores, redes y datos de la BD
```
Tus resultados quedan en `results\`.

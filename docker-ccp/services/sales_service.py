import os
import time
import json
import logging
from typing import Optional
from fastapi import FastAPI, Header, HTTPException, Response, status
from pydantic import BaseModel
import redis
import psycopg2
from psycopg2.extras import RealDictCursor

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s")
logger = logging.getLogger("CCP-SalesService")

SERVICE_ROLE = os.getenv("SERVICE_ROLE", "PRIMARY")  # PRIMARY or SPARE
PORT = int(os.getenv("PORT", 8000))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "ccp_db")
DB_USER = os.getenv("DB_USER", "ccp_app_user")
DB_PASS = os.getenv("DB_PASS", "ccp_secure_pass_2026")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

app = FastAPI(title=f"CCP Sales Service ({SERVICE_ROLE})", version="2.0.0")

# Redis Client
r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Estado de simulación de falla (para E01)
simulated_failure = False

def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        connect_timeout=3
    )

class ItemPedido(BaseModel):
    producto_id: int
    cantidad: int

class PedidoRequest(BaseModel):
    cliente_id: int
    bodega_id: int
    items: list[ItemPedido]

@app.get("/health/deep")
def health_deep():
    """
    Endpoint de salud funcional profunda (ASR AD-D1).
    Valida estado de proceso y conectividad efectiva a PostgreSQL.
    """
    global simulated_failure
    if simulated_failure:
        logger.warning(f"[{SERVICE_ROLE}] Health check fallo: Simulación de falla activa.")
        raise HTTPException(status_code=503, detail="Simulated service failure active")

    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return {
            "status": "HEALTHY",
            "service_role": SERVICE_ROLE,
            "database": "CONNECTED",
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"[{SERVICE_ROLE}] Health check fallo conectividad BD: {str(e)}")
        raise HTTPException(status_code=503, detail=f"Database unreachable: {str(e)}")

@app.post("/simulate/failure")
def simulate_failure(action: str):
    """
    Permite inyectar o recuperar fallas programáticas para validar E01.
    """
    global simulated_failure
    if action.lower() == "fail":
        simulated_failure = True
        logger.warn(f"[{SERVICE_ROLE}] Simulación de falla ACTIVADA.")
        return {"status": "FAILURE_INJECTED", "simulated_failure": True}
    else:
        simulated_failure = False
        logger.info(f"[{SERVICE_ROLE}] Servicio RESTAURADO a estado normal.")
        return {"status": "RECOVERED", "simulated_failure": False}

@app.get("/api/v1/clientes")
def list_clientes(x_vendedor_id: Optional[str] = Header(None)):
    """
    Consulta de clientes protegida por PostgreSQL Row-Level Security (RLS) - (ASR SEC-02).
    Inyecta SET LOCAL app.current_vendedor_id y ejecuta consulta general SIN WHERE manual.
    Demuestra que el motor de BD garantiza 0% de fuga de datos entre vendedores.
    """
    if not x_vendedor_id:
        raise HTTPException(status_code=400, detail="Falta cabecera X-Vendedor-Id")

    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # 1. Inyectar contexto de sesión para RLS
        cur.execute("SET LOCAL app.current_vendedor_id = %s;", (x_vendedor_id,))
        # 2. Consulta sin cláusula WHERE manual (RLS filtra físicamente a nivel de disco)
        cur.execute("SELECT id, nombre, pais, vendedor_id, direccion, limite_credito FROM clientes;")
        clientes = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "vendedor_autenticado": int(x_vendedor_id),
            "total_registros_visibles": len(clientes),
            "clientes": clientes
        }
    except Exception as e:
        conn.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/v1/pedidos", status_code=status.HTTP_201_CREATED)
def crear_pedido(
    pedido: PedidoRequest,
    response: Response,
    x_idempotency_key: Optional[str] = Header(None),
    x_vendedor_id: Optional[str] = Header(None),
    authorization: Optional[str] = Header(None)
):
    """
    Procesamiento Transaccional Atómico de Pedidos:
    - Verificación de Idempotencia en Redis (SETNX) (ASR AD-R2)
    - Verificación de Lista Negra de Tokens en Redis (ASR SEC-01)
    - Evaluación de Autorización ABAC y Cortocircuito 403 Forbidden (ASR SEC-02)
    - Bloqueo Pesimista de Inventario (SELECT FOR UPDATE) y Rollback ACID (ASR AD-R2)
    """
    start_time = time.time()

    # 1. Validación de Blacklist en Redis (SEC-01)
    if authorization:
        token = authorization.replace("Bearer ", "").strip()
        if r_client.exists(f"ccp:blacklist:{token}"):
            logger.warning(f"Intento de acceso con token revocado: {token}")
            raise HTTPException(status_code=401, detail="Sesión revocada por motor de riesgo (Token en Lista Negra)")

    # 2. Control de Idempotencia con Redis SETNX (AD-R2)
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Cabecera X-Idempotency-Key es obligatoria")

    idemp_redis_key = f"ccp:idemp:{x_idempotency_key}"
    is_new = r_client.set(idemp_redis_key, "PROCESSING", nx=True, ex=86400)
    if not is_new:
        cached_val = r_client.get(idemp_redis_key)
        if cached_val and cached_val != "PROCESSING":
            logger.info(f"Petición idempotente duplicada interceptada: {x_idempotency_key}")
            response.status_code = status.HTTP_200_OK
            return json.loads(cached_val)
        else:
            raise HTTPException(status_code=409, detail="Transacción en proceso concurrente con la misma clave")

    if not x_vendedor_id:
        r_client.delete(idemp_redis_key)
        raise HTTPException(status_code=400, detail="Falta cabecera X-Vendedor-Id")

    conn = get_db_connection()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 3. Inyección de Contexto RLS y Validación ABAC (SEC-02)
        cur.execute("SET LOCAL app.current_vendedor_id = %s;", (x_vendedor_id,))
        cur.execute("SELECT id, vendedor_id, nombre FROM clientes WHERE id = %s;", (pedido.cliente_id,))
        cliente = cur.fetchone()

        if not cliente:
            conn.rollback()
            r_client.delete(idemp_redis_key)
            # Registrar bloqueo de auditoría
            logger.warning(f"ABAC Denegado: Vendedor {x_vendedor_id} intentó acceder a cliente ajeno {pedido.cliente_id}")
            raise HTTPException(status_code=403, detail="HTTP 403 Forbidden: No autorizado para acceder a este cliente (Aislamiento de Cartera)")

        total_pedido = 0.0
        detalles_insertar = []

        # 4. Transacción ACID con Bloqueo Pesimista (SELECT FOR UPDATE) (AD-R2)
        # Ordenar productos por ID para prevenir deadlocks
        items_sorted = sorted(pedido.items, key=lambda x: x.producto_id)

        for item in items_sorted:
            cur.execute("""
                SELECT p.precio_unitario, i.cantidad_disponible
                FROM inventario i
                JOIN productos p ON p.id = i.producto_id
                WHERE i.bodega_id = %s AND i.producto_id = %s
                FOR UPDATE OF i;
            """, (pedido.bodega_id, item.producto_id))

            inv_row = cur.fetchone()
            if not inv_row:
                conn.rollback()
                r_client.delete(idemp_redis_key)
                raise HTTPException(status_code=404, detail=f"Producto {item.producto_id} no registrado en bodega {pedido.bodega_id}")

            disponible = inv_row["cantidad_disponible"]
            precio = float(inv_row["precio_unitario"])

            if disponible < item.cantidad:
                # Rollback completo y prevención de sobreventa
                conn.rollback()
                r_client.delete(idemp_redis_key)
                logger.warning(f"Stock insuficiente en bodega {pedido.bodega_id} para SKU {item.producto_id}: disp={disponible}, req={item.cantidad}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Stock insuficiente para producto {item.producto_id}. Disponible: {disponible}, Solicitado: {item.cantidad}. Transacción revertida (Rollback)."
                )

            # Descontar inventario
            cur.execute("""
                UPDATE inventario
                SET cantidad_disponible = cantidad_disponible - %s, updated_at = NOW()
                WHERE bodega_id = %s AND producto_id = %s;
            """, (item.cantidad, pedido.bodega_id, item.producto_id))

            subtotal = precio * item.cantidad
            total_pedido += subtotal
            detalles_insertar.append((item.producto_id, item.cantidad, precio))

        # Insertar cabecera de pedido
        cur.execute("""
            INSERT INTO pedidos (vendedor_id, cliente_id, bodega_id, total, estado, idempotency_key)
            VALUES (%s, %s, %s, %s, 'CONFIRMADO', %s)
            RETURNING id, uuid_pedido, created_at;
        """, (int(x_vendedor_id), pedido.cliente_id, pedido.bodega_id, total_pedido, x_idempotency_key))
        nuevo_pedido = cur.fetchone()
        pedido_id = nuevo_pedido["id"]

        # Insertar detalles
        for prod_id, cant, precio in detalles_insertar:
            cur.execute("""
                INSERT INTO detalles_pedido (pedido_id, producto_id, cantidad, precio_unitario)
                VALUES (%s, %s, %s, %s);
            """, (pedido_id, prod_id, cant, precio))

        # Confirmar Transacción Atómica
        conn.commit()
        cur.close()
        conn.close()

        duracion_ms = (time.time() - start_time) * 1000
        resultado = {
            "status": "CONFIRMADO",
            "pedido_id": pedido_id,
            "uuid_pedido": str(nuevo_pedido["uuid_pedido"]),
            "vendedor_id": int(x_vendedor_id),
            "cliente_id": pedido.cliente_id,
            "bodega_id": pedido.bodega_id,
            "total": total_pedido,
            "idempotency_key": x_idempotency_key,
            "procesado_por": SERVICE_ROLE,
            "latencia_transaccion_ms": round(duracion_ms, 2)
        }

        # Guardar en Redis para futuras peticiones idempotentes
        r_client.set(idemp_redis_key, json.dumps(resultado), ex=86400)
        return resultado

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        r_client.delete(idemp_redis_key)
        logger.error(f"Error inesperado en transacción de pedido: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error en transacción: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

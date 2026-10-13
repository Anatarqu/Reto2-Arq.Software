"""Servicio de Ventas (una instancia). Se ejecutan dos: una activa y una en espera (warm spare).

Qué hace esta instancia:
  1. Late (heartbeat) hacia etcd cada HEARTBEAT_S segundos con un lease de LEASE_TTL_S.
  2. Lee quién es el líder en etcd. Si es ella -> role=active y toma el epoch del líder.
  3. Solo si es activa atiende POST /pedidos: pedido + reserva en UNA transacción ACID,
     con idempotency key y fencing por epoch (la BD rechaza epochs viejos).
  4. Si el fallo es recuperable (BD caída, epoch viejo...), encola el pedido en la DLQ (RabbitMQ).
"""
import asyncio
import json
import logging
import os
import time
import uuid

import aio_pika
import psycopg
import uvicorn
from fastapi import FastAPI, Header
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import BaseModel, Field

from . import config as C
from .etcd import Etcd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
log = logging.getLogger(C.INSTANCE_ID)


class State:
    role = "standby"          # standby | active
    epoch = 0                 # epoch del líder vigente (según la última lectura de etcd)
    last_coord_ok = 0.0       # monotonic de la última comunicación exitosa con etcd
    lease: str | None = None


S = State()
etcd = Etcd(C.ETCD_ENDPOINTS, timeout=1.0)
pool: AsyncConnectionPool
amqp_conn = None
amqp_channel = None


# --------------------------------------------------------------------------------------
# Coordinación: heartbeat + lectura del líder
# --------------------------------------------------------------------------------------
async def coord_loop():
    while True:
        t0 = time.monotonic()
        try:
            if S.lease:
                if await etcd.lease_keepalive(S.lease) <= 0:  # el lease expiró (estuve congelada/aislada)
                    log.warning("lease expirado; me re-registro")
                    S.lease = None
            if not S.lease:
                S.lease = await etcd.lease_grant(C.LEASE_TTL_S)
                await etcd.put(C.hb_key(C.INSTANCE_ID), json.dumps({"id": C.INSTANCE_ID}), S.lease)
            kv = await etcd.get(C.LEADER_KEY)
            if kv:
                d = kv.json()
                new_role = "active" if d["id"] == C.INSTANCE_ID else "standby"
                if new_role != S.role:
                    log.info("rol %s -> %s (epoch %s)", S.role, new_role, d["epoch"])
                S.role, S.epoch = new_role, d["epoch"]
            S.last_coord_ok = time.monotonic()
        except Exception as e:  # noqa: BLE001  (sin etcd conservo el estado: así nace un "zombi")
            log.warning("coordinación falló: %s", e)
        await asyncio.sleep(max(0.0, C.HEARTBEAT_S - (time.monotonic() - t0)))


# --------------------------------------------------------------------------------------
# DLQ
# --------------------------------------------------------------------------------------
async def amqp_connect():
    global amqp_conn, amqp_channel
    while True:
        try:
            amqp_conn = await aio_pika.connect_robust(C.AMQP_URL)
            amqp_channel = await amqp_conn.channel()
            await amqp_channel.declare_queue(C.DLQ, durable=True)
            log.info("conectado a RabbitMQ")
            return
        except Exception as e:  # noqa: BLE001
            log.warning("esperando RabbitMQ: %s", e)
            await asyncio.sleep(2)


async def enqueue_dlq(key: str, body: dict, motivo: str):
    msg = aio_pika.Message(
        json.dumps({"idem_key": key, "body": body, "motivo": motivo, "origen": C.INSTANCE_ID}).encode(),
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        content_type="application/json",
        headers={"attempt": 0},
    )
    await amqp_channel.default_exchange.publish(msg, routing_key=C.DLQ)  # espera confirmación del broker


# --------------------------------------------------------------------------------------
# Transacción de negocio
# --------------------------------------------------------------------------------------
class Linea(BaseModel):
    producto_id: int
    bodega_id: int
    cantidad: int = Field(gt=0)


class PedidoIn(BaseModel):
    vendedor_id: int
    tendero_id: int
    lineas: list[Linea] = Field(min_length=1)


class Fenced(Exception):
    pass


class SinStock(Exception):
    pass


async def procesar(key: str, p: PedidoIn, my_epoch: int, hold_ms: int):
    """Devuelve (pedido_id, replay). Todo ocurre en UNA transacción: o queda todo o no queda nada."""
    async with pool.connection() as conn:
        async with conn.transaction():
            cur = conn.cursor()
            pedido_id = uuid.uuid4()
            # 1) Idempotencia: la llave UNIQUE decide si este pedido ya existe.
            await cur.execute(
                "INSERT INTO registro_idempotencia(idem_key, pedido_id) VALUES (%s,%s) "
                "ON CONFLICT (idem_key) DO NOTHING RETURNING idem_key", (key, pedido_id))
            if await cur.fetchone() is None:
                await cur.execute("SELECT pedido_id FROM registro_idempotencia WHERE idem_key=%s", (key,))
                row = await cur.fetchone()
                return row[0], True
            # 2) Reserva de stock (se ordenan las líneas para evitar interbloqueos entre pedidos).
            lineas = sorted(p.lineas, key=lambda l: (l.producto_id, l.bodega_id))
            for l in lineas:
                await cur.execute(
                    "UPDATE existencia SET disponible = disponible - %s, reservado = reservado + %s "
                    "WHERE producto_id=%s AND bodega_id=%s AND disponible >= %s",
                    (l.cantidad, l.cantidad, l.producto_id, l.bodega_id, l.cantidad))
                if cur.rowcount == 0:
                    raise SinStock()
            # 3) Pedido, líneas y reservas.
            await cur.execute(
                "INSERT INTO pedido(id, vendedor_id, tendero_id, estado, epoch, instancia) "
                "VALUES (%s,%s,%s,'RESERVADO',%s,%s)",
                (pedido_id, p.vendedor_id, p.tendero_id, my_epoch, C.INSTANCE_ID))
            for l in lineas:
                await cur.execute("INSERT INTO linea_pedido VALUES (%s,%s,%s,%s)",
                                  (pedido_id, l.producto_id, l.bodega_id, l.cantidad))
                await cur.execute("INSERT INTO reserva VALUES (%s,%s,%s,%s)",
                                  (pedido_id, l.producto_id, l.bodega_id, l.cantidad))
            # (solo experimento) mantener la transacción abierta para poder matar el proceso "a mitad"
            if hold_ms > 0:
                await asyncio.sleep(hold_ms / 1000)
            # 4) FENCING, lo último antes del COMMIT: si mi epoch es viejo, la BD no me deja escribir.
            #    FOR SHARE hace que el promotor espere a que termine mi commit antes de subir el epoch.
            await cur.execute("SELECT epoch FROM fencing WHERE id = 1 FOR SHARE")
            (db_epoch,) = await cur.fetchone()
            if my_epoch < db_epoch:
                raise Fenced(f"epoch {my_epoch} < {db_epoch}")
            return pedido_id, False


# --------------------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------------------
async def lifespan(app: FastAPI):
    global pool
    pool = AsyncConnectionPool(
        C.DB_DSN, min_size=2, max_size=C.POOL_SIZE, open=False, timeout=3,
        kwargs={"autocommit": True, "options": "-c statement_timeout=8000 -c lock_timeout=8000"})
    await pool.open()
    await pool.wait(timeout=120)
    await amqp_connect()
    task = asyncio.create_task(coord_loop())
    log.info("instancia %s lista (heartbeat %ss, lease %ss)", C.INSTANCE_ID, C.HEARTBEAT_S, C.LEASE_TTL_S)
    yield
    task.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"instancia": C.INSTANCE_ID, "role": S.role, "epoch": S.epoch,
            "coord_age_s": round(time.monotonic() - S.last_coord_ok, 2) if S.last_coord_ok else None}


@app.post("/pedidos")
async def crear_pedido(
    p: PedidoIn,
    idempotency_key: str = Header(),
    x_tx_hold_ms: int = Header(default=0),
    x_reprocess: str = Header(default=""),
):
    if S.role != "active":
        return JSONResponse({"error": "standby", "instancia": C.INSTANCE_ID}, status_code=503)
    my_epoch = S.epoch  # el epoch con el que ESTA instancia cree estar escribiendo
    hold = x_tx_hold_ms if C.FAULT_HOOKS else 0
    meta = {"instancia": C.INSTANCE_ID, "epoch": my_epoch}
    try:
        pedido_id, replay = await procesar(idempotency_key, p, my_epoch, hold)
        return JSONResponse({"pedido_id": str(pedido_id), "replay": replay, **meta},
                            status_code=200 if replay else 201)
    except SinStock:
        return JSONResponse({"error": "sin_stock", **meta}, status_code=409)
    except (Fenced, psycopg.Error, PoolTimeout, OSError) as e:
        motivo = "stale_epoch" if isinstance(e, Fenced) else f"db:{type(e).__name__}"
        log.warning("pedido %s no se pudo procesar (%s)", idempotency_key, motivo)
        if x_reprocess:  # el reprocesador gestiona su propio backoff: no re-encolar (evita bucles)
            return JSONResponse({"error": motivo, **meta}, status_code=503)
        try:
            await enqueue_dlq(idempotency_key, p.model_dump(), motivo)
            return JSONResponse({"estado": "en_cola", "motivo": motivo, **meta}, status_code=202)
        except Exception as e2:  # noqa: BLE001
            log.error("no pude encolar en DLQ: %s", e2)
            return JSONResponse({"error": "dlq_no_disponible", **meta}, status_code=503)


if __name__ == "__main__":
    uvicorn.run(app, host=os.environ.get("BIND_HOST", "0.0.0.0"), port=8000,
                log_level="warning", access_log=False)

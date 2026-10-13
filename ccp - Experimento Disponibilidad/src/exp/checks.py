"""Verificación de invariantes (la parte que decide si la hipótesis se cumple) y utilidades de BD/RabbitMQ."""
import os
import time

import psycopg
import requests

from ccp import config as C

RABBIT_API = os.environ.get("RABBIT_API", "http://rabbitmq:15672")
RABBIT_AUTH = ("ccp", "ccp")
STOCK_INICIAL = 1_000_000
N_PRODUCTOS, N_BODEGAS = 50, 3


def connect():
    return psycopg.connect(C.DB_DSN, autocommit=True)


# ----------------------------------------------------------------------- preparación
def reset_datos():
    with connect() as c:
        c.execute("SET lock_timeout = '15s'")
        c.execute("TRUNCATE reserva, linea_pedido, pedido, registro_idempotencia")
        c.execute("TRUNCATE existencia")
        c.execute("INSERT INTO existencia SELECT p, b, %s, %s, 0 "
                  "FROM generate_series(1, %s) p, generate_series(1, %s) b",
                  (STOCK_INICIAL, STOCK_INICIAL, N_PRODUCTOS, N_BODEGAS))


def _queues():
    r = requests.get(f"{RABBIT_API}/api/queues/%2F", auth=RABBIT_AUTH, timeout=5)
    r.raise_for_status()
    return [q for q in r.json() if q["name"].startswith(C.DLQ)]


def purgar_colas():
    for q in _queues():
        requests.delete(f"{RABBIT_API}/api/queues/%2F/{q['name']}/contents", auth=RABBIT_AUTH, timeout=5)


def dlq_pendientes() -> int:
    """Mensajes sin resolver: en la cola principal, en las de espera, sin confirmar o 'parked'."""
    return sum(q.get("messages", 0) for q in _queues())


def esperar_dlq_vacia(timeout_s: float):
    """Espera a que la DLQ quede en 0 de forma estable. Devuelve (segundos_esperados, pendientes_finales)."""
    t0, ceros = time.time(), 0
    while True:
        try:
            n = dlq_pendientes()
        except Exception:  # noqa: BLE001
            n = -1
        ceros = ceros + 1 if n == 0 else 0
        if ceros >= 4:  # 4 lecturas seguidas en 0 (~8 s; el panel de RabbitMQ refresca cada 5 s)
            return round(time.time() - t0 - 8, 1), 0
        if time.time() - t0 > timeout_s:
            return round(time.time() - t0, 1), max(n, 0)
        time.sleep(2)


# ----------------------------------------------------------------------- invariantes
def verificar(ack_keys: set[str]) -> dict:
    with connect() as c:
        keys_db = {r[0] for r in c.execute("SELECT idem_key FROM registro_idempotencia")}
        perdidos = sorted(ack_keys - keys_db)

        # Stock: inicial = disponible + reservado, y reservado = suma de reservas.
        stock_incons = c.execute("""
            SELECT count(*) FROM existencia e
            WHERE e.inicial <> e.disponible + e.reservado
               OR e.reservado <> COALESCE((SELECT sum(r.cantidad) FROM reserva r
                                           WHERE r.producto_id = e.producto_id AND r.bodega_id = e.bodega_id), 0)
        """).fetchone()[0]

        # Reservas duplicadas: unidades reservadas por encima de las pedidas (doble descuento de stock).
        dup = c.execute("""
            SELECT COALESCE(sum(GREATEST(e.reservado - COALESCE(l.q, 0), 0)), 0)
            FROM existencia e
            LEFT JOIN (SELECT producto_id, bodega_id, sum(cantidad) q FROM linea_pedido GROUP BY 1, 2) l
                   USING (producto_id, bodega_id)
        """).fetchone()[0]

        incompletos = c.execute("""
            SELECT count(*) FROM pedido p
            WHERE (SELECT count(*) FROM linea_pedido l WHERE l.pedido_id = p.id)
               <> (SELECT count(*) FROM reserva r WHERE r.pedido_id = p.id)
               OR NOT EXISTS (SELECT 1 FROM registro_idempotencia i WHERE i.pedido_id = p.id)
        """).fetchone()[0]

        # Escrituras aceptadas con un epoch más viejo que el vigente en el momento de crearse.
        epoch_viejo = c.execute("""
            SELECT count(*) FROM pedido p
            WHERE p.epoch < (SELECT max(e.epoch) FROM epoca e WHERE e.desde <= p.creado_en)
        """).fetchone()[0]

        n_pedidos = c.execute("SELECT count(*) FROM pedido").fetchone()[0]
    return {"pedidos_db": n_pedidos, "perdidos": len(perdidos), "perdidos_keys": perdidos[:20],
            "reservas_duplicadas": int(dup), "stock_inconsistente": stock_incons,
            "pedidos_incompletos": incompletos, "escrituras_epoch_viejo": epoch_viejo}


def eventos_desde(t_unix: float) -> list[dict]:
    with connect() as c:
        rows = c.execute("SELECT extract(epoch FROM ts), componente, tipo, detalle::text FROM eventos "
                         "WHERE ts >= to_timestamp(%s) ORDER BY ts", (t_unix,)).fetchall()
    return [{"ts": float(r[0]), "componente": r[1], "tipo": r[2], "detalle": r[3]} for r in rows]


def registrar_evento(componente: str, tipo: str, t_unix: float, detalle: str = "{}"):
    with connect() as c:
        c.execute("INSERT INTO eventos(ts, componente, tipo, detalle) VALUES (to_timestamp(%s), %s, %s, %s)",
                  (t_unix, componente, tipo, detalle))


def transacciones_abiertas() -> int:
    """Transacciones de ventas 'a mitad' (idle in transaction) en este instante. Para C4."""
    with connect() as c:
        return c.execute("SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction'"
                         ).fetchone()[0]

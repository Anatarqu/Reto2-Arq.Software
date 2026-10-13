"""Monitor de Heartbeat + Promotor de Failover (un solo proceso, rol de la vista de disponibilidad).

Ciclo cada POLL_S segundos:
  - ¿existe líder en etcd? Si no, elige una instancia viva (elección inicial).
  - ¿sigue vivo el latido del líder? Si desapareció -> FALLA DETECTADA -> promover a la otra.

Promover (en este orden, para que nunca haya dos líderes escribiendo):
  1. FENCING en la BD:  fencing.epoch = epoch_nuevo.  (el líder viejo ya no puede confirmar escrituras)
  2. Publicar en etcd:  líder = {id, epoch_nuevo}.    (el router y la instancia nueva se enteran)
"""
import asyncio
import json
import logging
import time

import psycopg

from . import config as C
from .etcd import Etcd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [promotor] %(message)s")
log = logging.getLogger("promotor")


async def evento(db, tipo: str, detalle: dict):
    async with db.cursor() as cur:
        await cur.execute("INSERT INTO eventos(componente, tipo, detalle) VALUES ('promotor', %s, %s)",
                          (tipo, json.dumps(detalle)))


async def aplicar_fencing(db, nuevo_epoch: int, nuevo_lider: str):
    async with db.transaction():
        async with db.cursor() as cur:
            await cur.execute("SET LOCAL lock_timeout = '2s'")
            await cur.execute("UPDATE fencing SET epoch = GREATEST(epoch, %s) WHERE id = 1", (nuevo_epoch,))
            await cur.execute("INSERT INTO epoca(epoch, lider) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                              (nuevo_epoch, nuevo_lider))


async def main():
    etcd = Etcd(C.ETCD_ENDPOINTS, timeout=1.0)
    db = None
    detectado_para = None  # (lider, epoch) para registrar la detección una sola vez
    while True:
        t0 = time.monotonic()
        try:
            if db is None or db.closed:
                db = await psycopg.AsyncConnection.connect(C.DB_DSN, autocommit=True)
            kv = await etcd.get(C.LEADER_KEY)
            vivos = {i: (await etcd.get(C.hb_key(i))) is not None for i in C.INSTANCES}

            if kv is None:
                cand = [i for i in C.INSTANCES if vivos[i]]
                if cand:
                    await promover(etcd, db, None, 0, cand[0], "lider_inicial")
            else:
                lider = kv.json()
                if not vivos[lider["id"]]:
                    if detectado_para != (lider["id"], lider["epoch"]):
                        detectado_para = (lider["id"], lider["epoch"])
                        log.warning("FALLA DETECTADA: sin latido de %s (epoch %s)", lider["id"], lider["epoch"])
                        await evento(db, "falla_detectada", lider)
                    cand = [i for i in C.INSTANCES if i != lider["id"] and vivos[i]]
                    if cand:
                        await promover(etcd, db, kv, lider["epoch"], cand[0], "falla_lider")
                else:
                    detectado_para = None
        except Exception as e:  # noqa: BLE001
            log.warning("ciclo falló: %s", e)
            if db is not None and not db.closed:
                await db.close()  # se reconecta en el próximo ciclo
            db = None
        await asyncio.sleep(max(0.0, C.POLL_S - (time.monotonic() - t0)))


async def promover(etcd, db, kv, epoch_actual, nuevo_id, motivo):
    nuevo_epoch = epoch_actual + 1
    log.info("promoviendo %s con epoch %s (%s)", nuevo_id, nuevo_epoch, motivo)
    await aplicar_fencing(db, nuevo_epoch, nuevo_id)
    await evento(db, "fencing_aplicado", {"epoch": nuevo_epoch, "lider": nuevo_id})
    ok = await etcd.cas_put(C.LEADER_KEY, json.dumps({"id": nuevo_id, "epoch": nuevo_epoch}),
                            kv.mod_revision if kv else 0)
    await evento(db, "lider_publicado" if ok else "publicacion_fallida",
                 {"epoch": nuevo_epoch, "lider": nuevo_id, "motivo": motivo})
    log.info("líder publicado: %s epoch %s (ok=%s)", nuevo_id, nuevo_epoch, ok)


if __name__ == "__main__":
    asyncio.run(main())

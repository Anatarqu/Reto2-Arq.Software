"""Router: reenvía las peticiones al líder actual (lo lee de etcd cada ROUTER_POLL_S segundos).

No reintenta nada: si el líder no responde devuelve 502/504 y el CLIENTE reintenta con la misma
Idempotency-Key. Así lo que se mide es el comportamiento de la arquitectura, no del router.
"""
import asyncio
import logging

import aiohttp
from aiohttp import web

from . import config as C
from .etcd import Etcd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [router] %(message)s")
log = logging.getLogger("router")

LEADER = {"id": None, "epoch": 0}
FWD_HEADERS = ("Idempotency-Key", "Content-Type", "X-Tx-Hold-Ms", "X-Reprocess")


async def refresh(etcd: Etcd):
    while True:
        try:
            kv = await etcd.get(C.LEADER_KEY)
            if kv:
                d = kv.json()
                if d["id"] != LEADER["id"]:
                    log.info("nuevo líder: %s (epoch %s)", d["id"], d["epoch"])
                LEADER.update(id=d["id"], epoch=d["epoch"])
        except Exception as e:  # noqa: BLE001
            log.warning("no pude leer etcd: %s", e)
        await asyncio.sleep(C.ROUTER_POLL_S)


async def leader(_req):
    return web.json_response(LEADER)


async def proxy(req: web.Request):
    lid = LEADER["id"]
    if lid is None:
        return web.json_response({"error": "sin_lider"}, status=503)
    url = C.VENTAS_URL.format(id=lid) + req.path_qs
    headers = {h: req.headers[h] for h in FWD_HEADERS if h in req.headers}
    body = await req.read()
    try:
        async with req.app["http"].request(req.method, url, data=body, headers=headers) as r:
            return web.Response(body=await r.read(), status=r.status, content_type="application/json")
    except asyncio.TimeoutError:
        return web.json_response({"error": "timeout_upstream", "lider": lid}, status=504)
    except Exception as e:  # noqa: BLE001
        return web.json_response({"error": f"upstream:{type(e).__name__}", "lider": lid}, status=502)


async def on_start(app):
    app["http"] = aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=C.ROUTER_TIMEOUT_S),
        connector=aiohttp.TCPConnector(limit=0))
    app["etcd"] = Etcd(C.ETCD_ENDPOINTS, timeout=1.0)
    app["task"] = asyncio.create_task(refresh(app["etcd"]))


def make_app():
    app = web.Application()
    app.on_startup.append(on_start)
    app.router.add_get("/leader", leader)
    app.router.add_route("*", "/pedidos", proxy)
    return app


if __name__ == "__main__":
    import os
    web.run_app(make_app(), host=os.environ.get("BIND_HOST", "0.0.0.0"), port=8080, print=None, access_log=None)

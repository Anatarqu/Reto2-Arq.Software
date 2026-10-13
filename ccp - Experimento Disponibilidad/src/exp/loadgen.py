"""Generador de carga de lazo abierto (como k6 con `constant-arrival-rate`).

- Llega un pedido nuevo cada 1/rate segundos, aunque el sistema esté caído (así se ve el daño real).
- Cada pedido se reintenta con la MISMA Idempotency-Key hasta max_attempts (backoff 0.25, 0.5, 1, 2, 2...).
- El 20 % de los pedidos incluye el mismo producto y bodega ("hot"): eso fuerza contención de filas.
- Guarda UNA fila por intento: es la materia prima de las gráficas.
"""
import asyncio
import random
import time
from collections import defaultdict

import aiohttp

OK = (200, 201)          # confirmado por el líder (201 nuevo, 200 repetido)
ACK = (200, 201, 202)    # el sistema se comprometió a procesarlo (202 = quedó en la DLQ)
BACKOFF = [0.25, 0.5, 1, 2, 2, 2, 2, 2]
HOT = (1, 1)             # (producto, bodega) caliente


class Load:
    def __init__(self, url: str, run_id: str, rate: float, hold_ms: int = 0,
                 hot_ratio: float = 0.2, seed: int = 0, max_attempts: int = 8):
        self.url, self.run_id, self.rate = url, run_id, rate
        self.hold_ms, self.hot_ratio, self.max_attempts = hold_ms, hot_ratio, max_attempts
        self.rnd = random.Random(seed)
        self.rows: list[dict] = []                 # un dict por intento HTTP
        self.orders: dict[str, dict] = {}          # un dict por Idempotency-Key
        self.per_sec = defaultdict(lambda: defaultdict(int))  # segundo -> {ok, fail, cola}
        self.sent = 0
        self.session: aiohttp.ClientSession | None = None

    # ---- construcción del pedido ----
    def _body(self) -> dict:
        r = self.rnd
        lines = {}
        if r.random() < self.hot_ratio:
            lines[HOT] = r.randint(1, 3)
        for _ in range(r.randint(0 if lines else 1, 1)):
            lines[(r.randint(2, 50), r.randint(1, 3))] = r.randint(1, 3)
        return {"vendedor_id": r.randint(1, 3500), "tendero_id": r.randint(1, 1000),
                "lineas": [{"producto_id": p, "bodega_id": b, "cantidad": q} for (p, b), q in lines.items()]}

    # ---- un pedido con sus reintentos ----
    async def _order(self, session, key: str, body: dict, url: str, attempts: int, probe: bool = False):
        # C4: mantiene la transacción abierta SOLO en pedidos que no tocan el producto caliente; si no,
        # las filas calientes quedarían bloqueadas y el sistema se saturaría por una razón ajena a la falla.
        es_caliente = any((l["producto_id"], l["bodega_id"]) == HOT for l in body["lineas"])
        o = {"key": key, "t_first": time.time(), "attempts": 0, "final": "no_confirmado",
             "t_ack": None, "probe": probe}
        self.orders[key] = o
        for a in range(1, attempts + 1):
            t0 = time.time()
            status, data = 0, {}
            h = {"Idempotency-Key": key}
            if self.hold_ms and not es_caliente:   # el runner pone hold_ms=0 apenas inyecta la falla
                h["X-Tx-Hold-Ms"] = str(self.hold_ms)
            try:
                async with session.post(url, json=body, headers=h) as r:
                    status = r.status
                    data = await r.json(content_type=None)
            except Exception:  # noqa: BLE001  (timeout, conexión rechazada, reset...)
                pass
            t1 = time.time()
            o["attempts"] = a
            self.rows.append({"key": key, "attempt": a, "t_send": t0, "t_done": t1, "status": status,
                              "instancia": data.get("instancia", ""), "epoch": data.get("epoch", ""),
                              "replay": data.get("replay", ""), "lat_ms": round((t1 - t0) * 1000, 1),
                              "probe": int(probe)})
            sec = int(t1)
            if status in OK:
                self.per_sec[sec]["ok"] += 1
            elif status == 202:
                self.per_sec[sec]["cola"] += 1
            elif status != 409:
                self.per_sec[sec]["fail"] += 1
            if status in OK or status == 202 or status == 409:
                o["final"] = "ack_ok" if status in OK else ("ack_cola" if status == 202 else "sin_stock")
                o["t_ack"] = t1
                return
            if a < attempts:
                await asyncio.sleep(BACKOFF[min(a - 1, len(BACKOFF) - 1)])

    # ---- bucle principal ----
    async def run(self, duration: float):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=4), connector=aiohttp.TCPConnector(limit=0))
        tasks, tick, start = set(), 0.01, time.time()
        try:
            while (el := time.time() - start) < duration:
                target = int((el + tick) * self.rate)
                while self.sent < target:
                    self.sent += 1
                    key = f"{self.run_id}-{self.sent:06d}"
                    t = asyncio.create_task(self._order(self.session, key, self._body(), self.url + "/pedidos", self.max_attempts))
                    tasks.add(t)
                    t.add_done_callback(tasks.discard)
                await asyncio.sleep(tick)
            if tasks:  # esperar reintentos pendientes (máx. ~25 s)
                await asyncio.wait(tasks, timeout=25)
        finally:
            await self.session.close()

    async def probe_direct(self, base_url: str, tag: str, n: int):
        """Pedidos directos a una instancia concreta (para probar al líder viejo / 'zombi')."""
        sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=4))
        try:
            await asyncio.gather(*[
                self._order(sess, f"{self.run_id}-{tag}-{i:03d}", self._body(), base_url + "/pedidos", 1, probe=True)
                for i in range(n)])
        finally:
            await sess.close()

"""Cliente mínimo de etcd v3 usando su API JSON (gateway HTTP). Sin dependencias de gRPC."""
import base64
import json
from dataclasses import dataclass

import aiohttp


def _b64(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


def _unb64(s: str) -> str:
    return base64.b64decode(s).decode()


@dataclass
class KV:
    value: str
    mod_revision: int
    create_revision: int

    def json(self):
        return json.loads(self.value)


class Etcd:
    def __init__(self, endpoints, timeout: float = 1.0):
        self.endpoints = list(endpoints)
        self._i = 0
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: aiohttp.ClientSession | None = None

    async def close(self):
        if self._session:
            await self._session.close()

    async def _post(self, path: str, body: dict) -> dict:
        if self._session is None:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        last: Exception | None = None
        n = len(self.endpoints)
        for k in range(n):  # prueba cada endpoint, empezando por el último que funcionó
            ep = self.endpoints[(self._i + k) % n]
            try:
                async with self._session.post(ep + path, json=body) as r:
                    data = await r.json(content_type=None)
                    if r.status != 200:
                        raise RuntimeError(f"etcd {ep}{path} -> {r.status}: {data}")
                    self._i = (self._i + k) % n
                    return data
            except Exception as e:  # noqa: BLE001
                last = e
        raise last  # type: ignore[misc]

    # ---- leases (latidos) ----
    async def lease_grant(self, ttl: int) -> str:
        d = await self._post("/v3/lease/grant", {"TTL": ttl})
        return str(d["ID"])

    async def lease_keepalive(self, lease_id: str) -> int:
        d = await self._post("/v3/lease/keepalive", {"ID": lease_id})
        return int(d.get("result", {}).get("TTL", 0) or 0)  # 0 => el lease ya expiró

    # ---- llaves ----
    async def put(self, key: str, value: str, lease: str | None = None):
        body = {"key": _b64(key), "value": _b64(value)}
        if lease:
            body["lease"] = lease
        await self._post("/v3/kv/put", body)

    async def get(self, key: str) -> KV | None:
        d = await self._post("/v3/kv/range", {"key": _b64(key)})
        kvs = d.get("kvs")
        if not kvs:
            return None
        kv = kvs[0]
        return KV(_unb64(kv["value"]), int(kv["mod_revision"]), int(kv["create_revision"]))

    async def cas_put(self, key: str, value: str, expected_mod_revision: int) -> bool:
        """Escribe solo si nadie cambió la llave desde `expected_mod_revision` (0 = no existía)."""
        if expected_mod_revision == 0:
            cmp = {"target": "CREATE", "result": "EQUAL", "key": _b64(key), "create_revision": "0"}
        else:
            cmp = {"target": "MOD", "result": "EQUAL", "key": _b64(key),
                   "mod_revision": str(expected_mod_revision)}
        d = await self._post("/v3/kv/txn", {
            "compare": [cmp],
            "success": [{"request_put": {"key": _b64(key), "value": _b64(value)}}],
        })
        return bool(d.get("succeeded", False))

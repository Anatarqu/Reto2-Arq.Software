"""Topología y control de fallas. DockerEnv usa el socket de Docker (montado en el contenedor runner)."""
import asyncio
import time

import aiohttp

from ccp import config as C

NET_COORD = "ccp_coord"


async def _get(url: str, timeout: float = 2.0):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout)) as s:
        async with s.get(url) as r:
            return await r.json(content_type=None)


async def leader() -> dict:
    return await _get(C.ROUTER_URL + "/leader")


class Env:
    """Interfaz. Las subclases implementan cómo se rompe y se arregla cada cosa."""

    async def restore(self): ...
    async def kill(self, name: str): ...
    async def pause(self, name: str): ...
    async def unpause(self, name: str): ...
    async def partition(self, name: str): ...
    async def heal(self, name: str): ...

    async def ensure_ready(self, timeout: float = 120) -> dict:
        """Deja el sistema sano: ambas instancias vivas, un líder activo y un standby."""
        await self.restore()
        t0 = time.time()
        while time.time() - t0 < timeout:
            try:
                ld = await leader()
                hs = {i: await _get(C.VENTAS_URL.format(id=i) + "/health") for i in C.INSTANCES}
                if (ld.get("id") and hs[ld["id"]]["role"] == "active" and hs[ld["id"]]["epoch"] == ld["epoch"]
                        and all(h["role"] == "standby" for i, h in hs.items() if i != ld["id"])
                        and all((h["coord_age_s"] or 99) < 2.5 for h in hs.values())):
                    return ld
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(1)
        raise RuntimeError("el sistema no quedó sano a tiempo (revisa `docker compose ps` y los logs)")


class DockerEnv(Env):
    def __init__(self):
        import docker
        self.d = docker.from_env()

    def _c(self, name):
        return self.d.containers.get(name)

    async def restore(self):
        def _restore():
            for name in C.INSTANCES:
                c = self._c(name)
                c.reload()
                if c.status == "paused":
                    c.unpause()
                if c.status != "running":
                    c.start()
                    c.reload()
                if NET_COORD not in c.attrs["NetworkSettings"]["Networks"]:
                    self.d.networks.get(NET_COORD).connect(c)
        await asyncio.to_thread(_restore)

    async def kill(self, name):
        await asyncio.to_thread(lambda: self._c(name).kill())          # SIGKILL: sin despedida

    async def pause(self, name):
        await asyncio.to_thread(lambda: self._c(name).pause())         # congela el proceso (cgroup freezer)

    async def unpause(self, name):
        await asyncio.to_thread(lambda: self._c(name).unpause())

    async def partition(self, name):
        # Lo desconecta SOLO de la red de coordinación (etcd). Sigue viendo la BD y RabbitMQ: es el "zombi".
        await asyncio.to_thread(lambda: self.d.networks.get(NET_COORD).disconnect(self._c(name), force=True))

    async def heal(self, name):
        await asyncio.to_thread(lambda: self.d.networks.get(NET_COORD).connect(self._c(name)))

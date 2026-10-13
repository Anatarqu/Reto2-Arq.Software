"""Reprocesador DLQ (contenedor "Manejo de fallos").

Toma pedidos que el Servicio de Ventas no pudo confirmar y los reenvía al líder vigente (vía router)
con la MISMA Idempotency-Key, así que reintentar nunca duplica nada.

Backoff exponencial con colas de espera (TTL) de RabbitMQ:
    pedidos.dlq  --falla-->  pedidos.dlq.retry.1s -> (expira) -> pedidos.dlq   (luego 2s, 4s, 8s, 16s)
Tras DLQ_MAX_ATTEMPTS intentos el mensaje pasa a `pedidos.dlq.parked` (requiere intervención humana).
"""
import asyncio
import json
import logging

import aio_pika
import aiohttp

from . import config as C

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [reprocesador] %(message)s")
log = logging.getLogger("reprocesador")


async def main():
    while True:
        try:
            conn = await aio_pika.connect_robust(C.AMQP_URL)
            break
        except Exception as e:  # noqa: BLE001
            log.warning("esperando RabbitMQ: %s", e)
            await asyncio.sleep(2)
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=20)
    main_q = await ch.declare_queue(C.DLQ, durable=True)
    await ch.declare_queue(C.DLQ_PARKED, durable=True)
    for d in C.DLQ_RETRY_DELAYS:
        await ch.declare_queue(
            f"{C.DLQ}.retry.{d}s", durable=True,
            arguments={"x-message-ttl": d * 1000, "x-dead-letter-exchange": "",
                       "x-dead-letter-routing-key": C.DLQ})
    http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5))

    async def reintentar_despues(msg: aio_pika.IncomingMessage, data: dict, attempt: int, motivo: str):
        if attempt >= C.DLQ_MAX_ATTEMPTS:
            target = C.DLQ_PARKED
            log.error("pedido %s agotó intentos -> parked (%s)", data["idem_key"], motivo)
        else:
            d = C.DLQ_RETRY_DELAYS[min(attempt, len(C.DLQ_RETRY_DELAYS) - 1)]
            target = f"{C.DLQ}.retry.{d}s"
        await ch.default_exchange.publish(
            aio_pika.Message(msg.body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                             content_type="application/json", headers={"attempt": attempt + 1}),
            routing_key=target)

    async def handle(msg: aio_pika.IncomingMessage):
        data = json.loads(msg.body)
        attempt = int((msg.headers or {}).get("attempt", 0))
        try:
            async with http.post(C.ROUTER_URL + "/pedidos", json=data["body"],
                                 headers={"Idempotency-Key": data["idem_key"], "X-Reprocess": "1"}) as r:
                status = r.status
        except Exception as e:  # noqa: BLE001
            status, motivo = 0, type(e).__name__
        else:
            motivo = f"http {status}"
        if status in (200, 201, 409):  # creado, ya existía, o rechazo definitivo por falta de stock
            log.info("pedido %s resuelto (%s) tras %s intentos", data["idem_key"], status, attempt + 1)
            await msg.ack()
        else:
            await reintentar_despues(msg, data, attempt, motivo)
            await msg.ack()  # el reintento ya quedó publicado (si se cae aquí, se duplica: es idempotente)

    async def on_message(msg: aio_pika.IncomingMessage):
        asyncio.create_task(handle(msg))

    await main_q.consume(on_message)
    log.info("reprocesador escuchando %s", C.DLQ)
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())

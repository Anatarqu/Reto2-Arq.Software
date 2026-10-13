import os
import time
import json
import logging
import urllib.request
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MONITOR-LEASE] %(message)s")
logger = logging.getLogger("HealthMonitor")

TARGET_URL = os.getenv("TARGET_URL", "http://svc-ventas-main:8000/health/deep")
PROBE_INTERVAL = int(os.getenv("PROBE_INTERVAL", 5)) # 5 segundos
PROBE_TIMEOUT = int(os.getenv("PROBE_TIMEOUT", 3))   # 3 segundos
FAILURE_THRESHOLD = int(os.getenv("FAILURE_THRESHOLD", 3)) # 3 fallos = 15s

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "ccp_admin")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "ccp_pass_2026")

def publish_failover_event(reason: str):
    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
        parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        payload = {
            "evento": "SERVICE_VENTAS_DOWN",
            "motivo": reason,
            "timestamp": time.time(),
            "accion_requerida": "ACTIVATE_STANDBY_FAILOVER"
        }

        channel.basic_publish(
            exchange="ccp.topic",
            routing_key="failover.events.v1",
            body=json.dumps(payload),
            properties=pika.BasicProperties(delivery_mode=2) # Mensaje persistente
        )
        connection.close()
        logger.info(f"==> Evento de Failover publicado exitosamente en RabbitMQ: {payload}")
    except Exception as e:
        logger.error(f"Error publicando alerta en RabbitMQ: {str(e)}")

def run_monitor():
    consecutive_failures = 0
    failover_triggered = False

    logger.info(f"Iniciando Health-Check Probe Daemon hacia {TARGET_URL} (Intervalo={PROBE_INTERVAL}s, Timeout={PROBE_TIMEOUT}s)")

    while True:
        try:
            req = urllib.request.Request(TARGET_URL, headers={"User-Agent": "CCP-HealthCheckDaemon/1.0"})
            with urllib.request.urlopen(req, timeout=PROBE_TIMEOUT) as response:
                if response.status == 200:
                    if consecutive_failures > 0:
                        logger.info(f"Recuperación detectada. Reseteando contador de fallos (estaba en {consecutive_failures}).")
                    consecutive_failures = 0
                    failover_triggered = False
                else:
                    consecutive_failures += 1
                    logger.warning(f"Sondeo devolvió código HTTP {response.status}. Fallos consecutivos: {consecutive_failures}")

        except Exception as e:
            consecutive_failures += 1
            logger.warning(f"Falla de sondeo funcional en {TARGET_URL}: {str(e)}. Fallos consecutivos: {consecutive_failures}/{FAILURE_THRESHOLD}")

        # Evaluar regla de concesión (Lease) del ASR AD-D1
        if consecutive_failures >= FAILURE_THRESHOLD and not failover_triggered:
            tiempo_falla_estimado = consecutive_failures * PROBE_INTERVAL
            logger.error(f"¡UMBRAL EXCEDIDO! Servicio Ventas Principal caído durante {tiempo_falla_estimado}s. DISPARANDO CONMUTACIÓN POR ERROR.")
            publish_failover_event(f"Caída confirmada tras {consecutive_failures} sondeos fallidos ({tiempo_falla_estimado}s)")
            failover_triggered = True

        time.sleep(PROBE_INTERVAL)

if __name__ == "__main__":
    time.sleep(8) # Espera a que los contenedores levanten
    run_monitor()

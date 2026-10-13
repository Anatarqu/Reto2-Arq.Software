"""Configuración común, leída de variables de entorno (ver .env y docker-compose.yml)."""
import os


def _f(name: str, default: str) -> float:
    return float(os.environ.get(name, default))


INSTANCES = os.environ.get("INSTANCES", "ventas-a,ventas-b").split(",")
INSTANCE_ID = os.environ.get("INSTANCE_ID", "ventas-a")

HEARTBEAT_S = _f("HEARTBEAT_S", "1")      # cada cuánto late una instancia
LEASE_TTL_S = int(_f("LEASE_TTL_S", "3"))  # etcd borra el latido si pasan estos segundos
POLL_S = _f("POLL_S", "0.5")              # cada cuánto el promotor revisa etcd
ROUTER_POLL_S = _f("ROUTER_POLL_S", "0.2")
ROUTER_TIMEOUT_S = _f("ROUTER_TIMEOUT_S", "2")

ETCD_ENDPOINTS = os.environ.get(
    "ETCD_ENDPOINTS", "http://etcd1:2379,http://etcd2:2379,http://etcd3:2379"
).split(",")
DB_DSN = os.environ.get("DB_DSN", "postgresql://ccp:ccp@postgres:5432/ccp")
AMQP_URL = os.environ.get("AMQP_URL", "amqp://ccp:ccp@rabbitmq/")
POOL_SIZE = int(_f("POOL_SIZE", "30"))
VENTAS_URL = os.environ.get("VENTAS_URL", "http://{id}:8000")  # cómo llegar a una instancia
ROUTER_URL = os.environ.get("ROUTER_URL", "http://router:8080")
FAULT_HOOKS = os.environ.get("FAULT_HOOKS", "0") == "1"  # permite X-Tx-Hold-Ms (solo experimento)

LEADER_KEY = "/ccp/ventas/leader"


def hb_key(instance_id: str) -> str:
    return f"/ccp/hb/{instance_id}"


DLQ = "pedidos.dlq"
DLQ_RETRY_DELAYS = [1, 2, 4, 8, 16]  # segundos (backoff exponencial)
DLQ_PARKED = "pedidos.dlq.parked"
DLQ_MAX_ATTEMPTS = 10

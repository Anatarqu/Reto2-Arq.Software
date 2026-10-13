import os
import time
import json
import logging
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [UEBA-ENGINE] %(message)s")
logger = logging.getLogger("UEBAEngine")

PORT = int(os.getenv("PORT", 8005))
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", 5432))
DB_NAME = os.getenv("DB_NAME", "ccp_db")
DB_USER = os.getenv("DB_USER", "ccp_app_user")
DB_PASS = os.getenv("DB_PASS", "ccp_secure_pass_2026")

app = FastAPI(title="CCP Risk & UEBA Engine", version="1.0.0")
r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def record_audit(tipo_evento: str, actor_id: str, ip: str, codigo: int, detalle: str):
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO audit_log (tipo_evento, actor_id, ip_origen, codigo_http, descripcion)
            VALUES (%s, %s, %s, %s, %s);
        """, (tipo_evento, actor_id, ip, codigo, detalle))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error escribiendo en audit_log: {e}")

class SecurityEvent(BaseModel):
    token: str
    vendedor_id: int
    ip_origen: str
    pais_origen: str
    pais_habitual: str
    hora_acceso: int  # 0-23
    delta_tiempo_minutos: Optional[int] = 5

@app.post("/events")
def ingest_event(event: SecurityEvent):
    """
    Ingesta y evaluación de streaming de eventos de seguridad (ASR SEC-01).
    Calcula Score de Riesgo (0-100) e invalida el token atómicamente en Redis si Score >= 80.
    """
    t_start = time.time()
    risk_score = 0
    anomalias = []

    # 1. Detección de Viaje Imposible / Salto de País en corto tiempo
    if event.pais_origen.upper() != event.pais_habitual.upper():
        if event.delta_tiempo_minutos and event.delta_tiempo_minutos < 60:
            risk_score += 65
            anomalias.append(f"Viaje imposible: de {event.pais_habitual} a {event.pais_origen} en {event.delta_tiempo_minutos} min")
        else:
            risk_score += 30
            anomalias.append(f"Acceso desde país inusual: {event.pais_origen}")

    # 2. Detección de Horario Inusual (madrugada)
    if event.hora_acceso < 5 or event.hora_acceso > 22:
        risk_score += 25
        anomalias.append(f"Acceso en horario nocturno atípico: {event.hora_acceso}:00 hrs")

    # 3. Detección de IP sospechosa
    if "tor" in event.ip_origen.lower() or event.ip_origen.startswith("198.51.") or event.ip_origen.startswith("203.0."):
        risk_score += 20
        anomalias.append(f"IP proveniente de rango o proxy anónimo: {event.ip_origen}")

    risk_score = min(risk_score, 100)
    eval_time_ms = round((time.time() - t_start) * 1000, 2)

    accion_ejecutada = "NINGUNA"
    if risk_score >= 80:
        # Táctica: Revocar Acceso de forma Inmediata (≤ 10 ms)
        t_rev = time.time()
        r_client.set(f"ccp:blacklist:{event.token}", "REVOKED_BY_UEBA_ENGINE", ex=3600)
        rev_time_ms = round((time.time() - t_rev) * 1000, 2)
        accion_ejecutada = f"TOKEN_REVOCADO_BLACK_LIST_REDIS (Tiempo: {rev_time_ms}ms)"
        logger.error(f"[ALERTA CRÍTICA] Vendedor {event.vendedor_id} suplantado. Score={risk_score}. Token {event.token[:10]}... REVOCADO en Redis en {rev_time_ms}ms")
        record_audit("TOKEN_REVOCATION_ALERT", str(event.vendedor_id), event.ip_origen, 401, f"Score: {risk_score} | {', '.join(anomalias)}")
    elif risk_score >= 50:
        accion_ejecutada = "DESAFIO_ADAPTATIVO_MFA_REQUERIDO"
        logger.warning(f"[RIESGO MEDIO] Vendedor {event.vendedor_id}. Score={risk_score}. Exigiendo Step-Up MFA.")
        record_audit("STEP_UP_MFA_CHALLENGE", str(event.vendedor_id), event.ip_origen, 200, f"Score: {risk_score} | {', '.join(anomalias)}")

    return {
        "vendedor_id": event.vendedor_id,
        "risk_score": risk_score,
        "nivel_amenaza": "ALTO" if risk_score >= 80 else ("MEDIO" if risk_score >= 50 else "BAJO"),
        "anomalias": anomalias,
        "accion": accion_ejecutada,
        "tiempo_evaluacion_ms": eval_time_ms,
        "cumple_slo_1000ms": eval_time_ms < 1000.0
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

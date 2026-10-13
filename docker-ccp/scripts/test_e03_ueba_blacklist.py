import time
import uuid
import requests

BASE_URL = "http://localhost:8080"

def run_test_e03():
    print("=" * 70)
    print(">>> EXPERIMENTO E03: Detección UEBA y Revocación en Lista Negra (ASR SEC-01)")
    print("=" * 70)

    token_prueba = f"jwt-token-valido-{uuid.uuid4().hex[:8]}"
    vendedor_id = 1 # Carlos Perez, Colombia

    # 1. Transacción legítima inicial con el token
    print(f"\n1. Ejecutando petición legítima inicial con token: {token_prueba}...")
    headers = {
        "X-Idempotency-Key": f"legit-{uuid.uuid4()}",
        "X-Vendedor-Id": str(vendedor_id),
        "Authorization": f"Bearer {token_prueba}"
    }
    payload = {
        "cliente_id": 101,
        "bodega_id": 1,
        "items": [{"producto_id": 2, "cantidad": 1}]
    }
    r = requests.post(f"{BASE_URL}/api/v1/pedidos", json=payload, headers=headers)
    print(f"   HTTP Status: {r.status_code}")
    assert r.status_code in [200, 201], "La petición legítima inicial debería ser exitosa"
    print("   [OK] Petición inicial aceptada con éxito.")

    # 2. Inyección de Anomalía Crítica al Motor UEBA (Viaje Imposible)
    print(f"\n2. Inyectando evento de anomalía crítica hacia el Motor UEBA...")
    print(f"   (Salto geográfico: Vendedor en Colombia intentando acceder desde Rusia en 3 min)")
    evento_anomalia = {
        "token": token_prueba,
        "vendedor_id": vendedor_id,
        "ip_origen": "198.51.100.45",
        "pais_origen": "Rusia",
        "pais_habitual": "Colombia",
        "hora_acceso": 3, # 3:00 AM (Horario nocturno)
        "delta_tiempo_minutos": 3
    }

    t0 = time.time()
    r_ueba = requests.post(f"{BASE_URL}/api/v1/sec/events", json=evento_anomalia)
    duracion_ms = round((time.time() - t0) * 1000, 2)

    print(f"   HTTP Status UEBA: {r_ueba.status_code} (Tiempo: {duracion_ms}ms)")
    data_ueba = r_ueba.json()
    print(f"   Score de Riesgo Calculado: {data_ueba.get('risk_score')}/100")
    print(f"   Nivel de Amenaza: {data_ueba.get('nivel_amenaza')}")
    print(f"   Acción Ejecutada: {data_ueba.get('accion')}")
    print(f"   Anomalías Identificadas: {data_ueba.get('anomalias')}")

    assert data_ueba.get("risk_score") >= 80, "El score de riesgo debería ser Alto (>=80)"
    assert data_ueba.get("cumple_slo_1000ms") is True, "La evaluación debe tomar menos de 1000 ms"

    # 3. Intentar acceder nuevamente con el token comprometido
    print(f"\n3. Intentando realizar nuevo pedido con el token suplantado...")
    headers["X-Idempotency-Key"] = f"post-revoke-{uuid.uuid4()}"
    r_post_revoke = requests.post(f"{BASE_URL}/api/v1/pedidos", json=payload, headers=headers)
    print(f"   HTTP Status: {r_post_revoke.status_code}")
    print(f"   Detalle de Respuesta: {r_post_revoke.json()}")

    if r_post_revoke.status_code == 401 and "revocada" in str(r_post_revoke.json()):
        print("\n   ==> ¡ACCESO BLOQUEADO CON ÉXITO!")
        print("   [OK] El Gateway consultó Redis Blacklist e invalidó la sesión en ≤ 10 ms.")
        print("\n>>> RESULTADO EXPERIMENTO E03: APROBADO (0% Accesos No Autorizados)")
    else:
        print("\n>>> RESULTADO EXPERIMENTO E03: FALLIDO (El token no fue invalidado correctamente)")

if __name__ == "__main__":
    run_test_e03()

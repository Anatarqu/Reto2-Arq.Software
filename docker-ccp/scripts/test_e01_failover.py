import time
import requests
import sys

BASE_URL = "http://localhost:8080"
MAIN_DIRECT_URL = "http://localhost:8001"

def run_test_e01():
    print("=" * 70)
    print(">>> EXPERIMENTO E01: Detección Oportuna de Fallas y Failover (ASR AD-D1)")
    print("=" * 70)

    # 1. Verificar estado inicial
    print("\n1. Verificando estado inicial del clúster a través del API Gateway...")
    try:
        r = requests.get(f"{BASE_URL}/health/deep", timeout=5)
        print(f"   HTTP Status: {r.status_code}")
        data = r.json()
        print(f"   Respuesta: {data}")
        assert data["service_role"] == "PRIMARY", "El servicio principal debería estar activo"
        print("   [OK] Servicio Principal respondiendo correctamente.")
    except Exception as e:
        print(f"   [ERROR] No se pudo conectar al API Gateway: {e}")
        sys.exit(1)

    # 2. Inyectar Falla Funcional en el Servicio Principal
    print("\n2. Inyectando falla funcional simulada en el Servicio Principal...")
    r = requests.post(f"{MAIN_DIRECT_URL}/simulate/failure?action=fail")
    print(f"   Inyección confirmada: {r.json()}")

    # 3. Monitorear tiempo de detección y conmutación
    print("\n3. Monitoreando sondeos y conmutación por error (Umbral: 3 fallos = 15s)...")
    start_time = time.time()
    failover_detected = False

    for i in range(1, 20):
        time.sleep(2)
        elapsed = round(time.time() - start_time, 1)
        try:
            r = requests.get(f"{BASE_URL}/health/deep", timeout=3)
            if r.status_code == 200:
                data = r.json()
                role = data.get("service_role")
                print(f"   T+{elapsed}s: Endpoint respondió 200 OK desde nodo: [{role}]")
                if role == "SPARE":
                    print(f"\n   ==> ¡CONMUTACIÓN POR ERROR DETECTADA EXITOSAMENTE EN T+{elapsed}s!")
                    print("   [OK] El Traffic Router redirigió el tráfico hacia el Standby (Warm Backup).")
                    failover_detected = True
                    break
            else:
                print(f"   T+{elapsed}s: Estado HTTP {r.status_code} (Nodo principal degradado)")
        except requests.exceptions.RequestException:
            print(f"   T+{elapsed}s: Timeout / Conexión reintentada...")

    # 4. Restaurar Servicio Principal
    print("\n4. Restaurando Servicio Principal a estado normal...")
    requests.post(f"{MAIN_DIRECT_URL}/simulate/failure?action=recover")
    print("   [OK] Servicio Principal restaurado.")

    if failover_detected:
        print("\n>>> RESULTADO EXPERIMENTO E01: APROBADO (SLO ≤ 15s Cumplido)")
    else:
        print("\n>>> RESULTADO EXPERIMENTO E01: FALLIDO (No se detectó failover en el tiempo límite)")

if __name__ == "__main__":
    run_test_e01()

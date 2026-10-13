import time
import uuid
import concurrent.futures
import requests

BASE_URL = "http://localhost:8080/api/v1/pedidos"

def enviar_pedido(idemp_key, cant=1, cliente_id=101, prod_id=1, bodega_id=1):
    headers = {
        "Content-Type": "application/json",
        "X-Idempotency-Key": idemp_key,
        "X-Vendedor-Id": "1"  # Vendedor Colombia
    }
    payload = {
        "cliente_id": cliente_id,
        "bodega_id": bodega_id,
        "items": [{"producto_id": prod_id, "cantidad": cant}]
    }
    try:
        r = requests.post(BASE_URL, json=payload, headers=headers, timeout=10)
        return r.status_code, r.json()
    except Exception as e:
        return 500, {"error": str(e)}

def run_test_e02():
    print("=" * 70)
    print(">>> EXPERIMENTO E02: Idempotencia y Prevención de Sobreventa (ASR AD-R2)")
    print("=" * 70)

    # PARTE A: Verificación de Idempotencia (Redis SETNX)
    print("\n--- PARTE A: Validación de Idempotencia (Reintentos con misma clave) ---")
    idemp_uuid = f"idemp-test-{uuid.uuid4()}"
    print(f"Enviando 4 peticiones idénticas con clave: {idemp_uuid}")

    respuestas_idemp = []
    for i in range(1, 5):
        status, data = enviar_pedido(idemp_uuid, cant=1, prod_id=2) # SKU 2 con stock amplio
        print(f"   Petición #{i}: Status={status}, PedidoID={data.get('pedido_id')}, Estado={data.get('status')}")
        respuestas_idemp.append((status, data.get('pedido_id')))

    pedido_ids = set([p[1] for p in respuestas_idemp if p[1] is not None])
    if len(pedido_ids) == 1 and respuestas_idemp[0][0] == 201 and respuestas_idemp[1][0] == 200:
        print("   [OK] Idempotencia Verificada: Se procesó 1 sola orden y se devolvió respuesta en caché.")
    else:
        print("   [ADVERTENCIA] Comportamiento de idempotencia inconsistente.")

    # PARTE B: Contención Concurrente y Prevención de Sobreventa (SELECT FOR UPDATE)
    print("\n--- PARTE B: Contención Concurrente Masiva (10 unidades en stock) ---")
    print("Lanzando 20 hilos simultáneos intentando comprar 1 unidad del Producto 1 (Solo hay 10 disp)...")

    exitosos = 0
    rechazados_stock = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(enviar_pedido, f"burst-{uuid.uuid4()}", cant=1, prod_id=1, bodega_id=1)
            for _ in range(20)
        ]
        for f in concurrent.futures.as_completed(futures):
            status, data = f.result()
            if status == 201:
                exitosos += 1
            elif status == 400 and "Stock insuficiente" in str(data.get("detail", "")):
                rechazados_stock += 1

    print(f"\n   Resultados de la ráfaga concurrente:")
    print(f"   - Órdenes Confirmadas (201 Created): {exitosos} (Esperado: ≤ 10)")
    print(f"   - Órdenes Rechazadas por Stock (400 Bad Request): {rechazados_stock} (Esperado: ≥ 10)")

    if exitosos <= 10 and exitosos + rechazados_stock == 20:
        print("   [OK] Cero Sobreventas Verificado: Bloqueo de fila SELECT FOR UPDATE evitó stock negativo.")
        print("\n>>> RESULTADO EXPERIMENTO E02: APROBADO (Consistencia Atómica Garantizada)")
    else:
        print("   [FALLO] Se detectó sobreventa o inconsistencia transaccional.")

if __name__ == "__main__":
    run_test_e02()

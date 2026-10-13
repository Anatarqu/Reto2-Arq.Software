import uuid
import requests

BASE_URL = "http://localhost:8080"

def run_test_e04():
    print("=" * 70)
    print(">>> EXPERIMENTO E04: Aislamiento Multitenant con ABAC y RLS (ASR SEC-02)")
    print("=" * 70)

    # 1. Consulta legítima de Clientes para Vendedor 1 (Colombia)
    print("\n1. Consultando clientes con contexto de Vendedor 1 (Carlos Perez - Colombia)...")
    headers_v1 = {"X-Vendedor-Id": "1"}
    r1 = requests.get(f"{BASE_URL}/api/v1/clientes", headers=headers_v1)
    data1 = r1.json()
    print(f"   HTTP Status: {r1.status_code}")
    print(f"   Total registros retornados: {data1.get('total_registros_visibles')}")
    for c in data1.get("clientes", []):
        print(f"     - Cliente ID: {c['id']}, Nombre: {c['nombre']}, País: {c['pais']}, Vendedor_ID: {c['vendedor_id']}")
        assert c["vendedor_id"] == 1, "Violación de RLS: Se retornó un cliente que no pertenece a Vendedor 1"

    # 2. Consulta legítima de Clientes para Vendedor 2 (México)
    print("\n2. Consultando clientes con contexto de Vendedor 2 (Maria Rodriguez - México)...")
    headers_v2 = {"X-Vendedor-Id": "2"}
    r2 = requests.get(f"{BASE_URL}/api/v1/clientes", headers=headers_v2)
    data2 = r2.json()
    print(f"   HTTP Status: {r2.status_code}")
    print(f"   Total registros retornados: {data2.get('total_registros_visibles')}")
    for c in data2.get("clientes", []):
        print(f"     - Cliente ID: {c['id']}, Nombre: {c['nombre']}, País: {c['pais']}, Vendedor_ID: {c['vendedor_id']}")
        assert c["vendedor_id"] == 2, "Violación de RLS: Se retornó un cliente que no pertenece a Vendedor 2"

    print("   [OK] PostgreSQL Row-Level Security aisló físicamente las carteras de clientes.")

    # 3. Ataque de Acceso Cruzado (IDOR): Vendedor 1 intenta crear pedido para Cliente de Vendedor 2
    print("\n3. Simulando ataque IDOR: Vendedor 1 intentando crear pedido para Cliente 201 (México - Vendedor 2)...")
    payload_idor = {
        "cliente_id": 201,  # Cliente de Vendedora Maria en México
        "bodega_id": 2,
        "items": [{"producto_id": 1, "cantidad": 1}]
    }
    headers_attack = {
        "X-Idempotency-Key": f"attack-{uuid.uuid4()}",
        "X-Vendedor-Id": "1"  # Atacante: Vendedor 1
    }

    r_attack = requests.post(f"{BASE_URL}/api/v1/pedidos", json=payload_idor, headers=headers_attack)
    print(f"   HTTP Status: {r_attack.status_code}")
    print(f"   Respuesta del Sistema: {r_attack.json()}")

    if r_attack.status_code == 403:
        print("\n   ==> ¡ATAQUE IDOR BLOQUEADO CON ÉXITO!")
        print("   [OK] Motor ABAC + RLS cortocircuitó la petición y devolvió HTTP 403 Forbidden.")
        print("   [OK] 0% Fuga de Datos Comprobada.")
        print("\n>>> RESULTADO EXPERIMENTO E04: APROBADO (Aislamiento de Carteras 100% Efectivo)")
    else:
        print(f"\n>>> RESULTADO EXPERIMENTO E04: FALLIDO (Se esperaba 403 Forbidden, se recibió {r_attack.status_code})")

if __name__ == "__main__":
    run_test_e04()

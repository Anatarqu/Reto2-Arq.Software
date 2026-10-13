import time
import sys
import subprocess

def main():
    print("*" * 80)
    print("  COMPAÑÍA COMERCIALIZADORA DE PRODUCTOS (CCP) - SUITE DE EXPERIMENTOS RETO 2")
    print("  Validación de Arquitectura sobre Docker Desktop (4 Escenarios de Calidad)")
    print("*" * 80)
    print("\nIniciando ejecución secuencial de los 4 experimentos de arquitectura...\n")

    tests = [
        ("E01: Detección Oportuna y Failover (ASR AD-D1)", "test_e01_failover.py"),
        ("E02: Idempotencia y Prevención de Sobreventa (ASR AD-R2)", "test_e02_idempotency_concurrency.py"),
        ("E03: Detección UEBA y Revocación en Lista Negra (ASR SEC-01)", "test_e03_ueba_blacklist.py"),
        ("E04: Aislamiento Multitenant con ABAC y RLS (ASR SEC-02)", "test_e04_abac_rls.py"),
    ]

    results = []

    for name, script in tests:
        print(f"\n>>> EJECUTANDO: {name}...")
        t0 = time.time()
        res = subprocess.run([sys.executable, f"scripts/{script}"], capture_output=False)
        dur = round(time.time() - t0, 2)
        status = "PASSED" if res.returncode == 0 else "FAILED"
        results.append((name, status, dur))
        time.sleep(2)

    print("\n" + "=" * 80)
    print("  RESUMEN FINAL DE LA SUITE DE EXPERIMENTOS DE ARQUITECTURA")
    print("=" * 80)
    for name, status, dur in results:
        sym = "✅" if status == "PASSED" else "❌"
        print(f"  {sym}  {name:<60} [{status}] ({dur}s)")
    print("=" * 80)

if __name__ == "__main__":
    main()

"""Deja la BD con datos limpios (50 productos x 3 bodegas, 1.000.000 unidades c/u) y las colas vacías.
Úsalo antes de probar a mano:   docker compose run --rm runner python -m exp.seed
"""
from . import checks

if __name__ == "__main__":
    checks.reset_datos()
    try:
        checks.purgar_colas()
    except Exception as e:  # noqa: BLE001
        print("(no pude purgar colas:", e, ")")
    print("Datos sembrados: 50 productos x 3 bodegas, 1.000.000 unidades cada una.")

"""Orquestador del experimento.   Uso:  python -m exp.run --case C1 --reps 5

Una corrida =  1) dejar el sistema sano y con datos limpios
               2) lanzar carga constante
               3) a los T_INJECT segundos inyectar la falla del caso
               4) esperar a que la DLQ se vacíe
               5) verificar invariantes en la BD y calcular métricas
               6) guardar todo en /results (visible en tu carpeta results\\ de Windows)
"""
import argparse
import asyncio
import csv
import json
import os
import time
import traceback
from pathlib import Path

import pandas as pd

from ccp import config as C

from . import checks, env as envmod
from .loadgen import Load

RESULTS = Path(os.environ.get("RESULTS_DIR", "/results"))
CASES = {
    "C0": "control sin fallas",
    "C1": "caída abrupta del activo (kill)",
    "C2": "activo congelado 20 s (pause) -> zombi",
    "C3": "activo aislado de etcd 20 s -> zombi",
    "C4": "caída a mitad de una transacción",
}
FAULT_S = 20
FIELDS = ["run_id", "caso", "rep", "rate", "duracion_s", "victima", "epoch_antes", "epoch_despues", "t_inject",
          "deteccion_s", "promocion_s", "recuperacion_s", "max_gap_s",
          "pedidos_enviados", "pedidos_db", "ack_ok", "ack_cola", "sin_stock", "no_confirmados",
          "intentos_fallidos", "tx_abiertas_al_matar", "lat_p50_ms", "lat_p95_ms", "lat_p99_ms",
          "perdidos", "reservas_duplicadas", "stock_inconsistente", "pedidos_incompletos",
          "escrituras_epoch_viejo", "zombi_201_epoch_viejo", "zombi_respuestas",
          "falsos_failovers", "dlq_espera_s", "dlq_residual", "error"]


async def progress(load: Load, t_start: float):
    while True:
        await asyncio.sleep(1)
        sec = int(time.time()) - 1
        d = load.per_sec.get(sec, {})
        try:
            ld = await envmod.leader()
        except Exception:  # noqa: BLE001
            ld = {}
        print(f"  t=+{sec - int(t_start):3d}s  líder={ld.get('id')} (epoch {ld.get('epoch')})  "
              f"ok={d.get('ok', 0):4d}  en_cola={d.get('cola', 0):3d}  fallos={d.get('fail', 0):4d}", flush=True)


async def inyectar(env: envmod.Env, case: str, load: Load, t_start: float, t_inj_rel: float, info: dict):
    await asyncio.sleep(max(0.0, t_start + t_inj_rel - time.time()))
    ld = await envmod.leader()
    victim = ld["id"]
    info.update(victim=victim, epoch_before=ld["epoch"])
    victim_url = C.VENTAS_URL.format(id=victim)
    if case == "C4":  # esperar a que haya una transacción realmente abierta y matar en ese instante
        limit, n_tx = time.time() + 10, 0
        while time.time() < limit and not (n_tx := await asyncio.to_thread(checks.transacciones_abiertas)):
            await asyncio.sleep(0.02)
        info["tx_abiertas"] = n_tx
    info["t_inject"] = t = time.time()
    print(f"  >>> INYECCIÓN {case} sobre {victim}", flush=True)
    if case in ("C1", "C4"):
        await env.kill(victim)
        load.hold_ms = 0  # C4: ya no hace falta mantener transacciones abiertas (evita saturar al nuevo líder)
    elif case == "C2":
        await env.pause(victim)
        await asyncio.sleep(FAULT_S)
        await env.unpause(victim)
        print(f"  >>> {victim} descongelado; probando escrituras directas del zombi", flush=True)
        await load.probe_direct(victim_url, "zombi", 10)
    elif case == "C3":
        await env.partition(victim)
        await asyncio.sleep(8)
        await load.probe_direct(victim_url, "zombi8s", 10)
        await asyncio.sleep(FAULT_S - 8)
        await load.probe_direct(victim_url, "zombi20s", 10)
        await env.heal(victim)
        print(f"  >>> {victim} reconectado a etcd", flush=True)
    await asyncio.to_thread(checks.registrar_evento, "runner", "inyeccion", t, json.dumps({"caso": case, "victima": victim}))


def metricas(case, load: Load, info: dict, events: list, t_start: float, epoch_after: int) -> dict:
    df = pd.DataFrame(load.rows)
    m: dict = {}
    t_inj = info.get("t_inject")
    ok = df[df.status.isin([200, 201]) & (df.probe == 0)]
    if t_inj:
        det = [e for e in events if e["tipo"] == "falla_detectada" and e["ts"] >= t_inj - 0.5]
        pub = [e for e in events if e["tipo"] == "lider_publicado" and e["ts"] >= t_inj - 0.5]
        m["deteccion_s"] = round(det[0]["ts"] - t_inj, 3) if det else None
        m["promocion_s"] = round(pub[0]["ts"] - t_inj, 3) if pub else None
        ep = pd.to_numeric(ok.epoch, errors="coerce")
        nuevo = ok[(ok.t_done > t_inj) & (ep > info["epoch_before"])]
        m["recuperacion_s"] = round(nuevo.t_done.min() - t_inj, 3) if len(nuevo) else None
        t = ok[ok.t_done >= t_inj - 2].t_done.sort_values()
        m["max_gap_s"] = round(float(t.diff().dropna().max()), 3) if len(t) > 1 else None
    n_det = len([e for e in events if e["tipo"] == "falla_detectada" and e["ts"] >= t_start])
    m["falsos_failovers"] = n_det if case == "C0" else max(0, n_det - 1)
    m["lat_p50_ms"], m["lat_p95_ms"], m["lat_p99_ms"] = (
        [round(float(ok.lat_ms.quantile(q)), 1) for q in (0.5, 0.95, 0.99)] if len(ok) else [None] * 3)
    m["intentos_fallidos"] = int((~df.status.isin([200, 201, 202, 409]) & (df.probe == 0)).sum())
    z = df[df.probe == 1]
    m["zombi_respuestas"] = json.dumps({str(k): int(v) for k, v in z.status.value_counts().items()}) if len(z) else ""
    ze = pd.to_numeric(z.epoch, errors="coerce") if len(z) else pd.Series(dtype=float)
    m["zombi_201_epoch_viejo"] = int(((z.status == 201) & (ze < epoch_after)).sum()) if len(z) else 0
    return m


async def run_one(env: envmod.Env, case: str, rep: int, a) -> dict:
    run_id = f"{case}-r{rep:02d}-{time.strftime('%H%M%S')}"
    print(f"\n=== {run_id}: {CASES[case]} ===", flush=True)
    await env.ensure_ready()
    await asyncio.to_thread(checks.reset_datos)
    await asyncio.to_thread(checks.purgar_colas)
    duration = a.c0_duration if case == "C0" else a.duration
    load = Load(C.ROUTER_URL, run_id, a.rate, hold_ms=200 if case == "C4" else 0, seed=rep * 10 + int(case[1]))
    info: dict = {}
    t_start = time.time()
    prog = asyncio.create_task(progress(load, t_start))
    inj = None if case == "C0" else asyncio.create_task(inyectar(env, case, load, t_start, a.t_inject, info))
    try:
        await load.run(duration)
        if inj:
            await inj
    finally:
        prog.cancel()
        if inj and not inj.done():
            inj.cancel()
    await env.restore()                                   # revive/reconecta lo que se rompió
    print("  carga terminada; esperando a que la DLQ se vacíe...", flush=True)
    dlq_s, dlq_res = await asyncio.to_thread(checks.esperar_dlq_vacia, a.dlq_wait)
    ld_end = await envmod.leader()
    ack = {k for k, o in load.orders.items() if o["final"] in ("ack_ok", "ack_cola")}
    inv = await asyncio.to_thread(checks.verificar, ack)
    events = await asyncio.to_thread(checks.eventos_desde, t_start - 1)
    m = metricas(case, load, info, events, t_start, ld_end["epoch"])

    finales = [o["final"] for o in load.orders.values()]
    row = {"run_id": run_id, "caso": case, "rep": rep, "rate": a.rate, "duracion_s": duration,
           "victima": info.get("victim"), "epoch_antes": info.get("epoch_before"), "epoch_despues": ld_end["epoch"],
           "t_inject": info.get("t_inject"), "tx_abiertas_al_matar": info.get("tx_abiertas"),
           "pedidos_enviados": load.sent,
           "pedidos_db": inv["pedidos_db"], "ack_ok": finales.count("ack_ok"), "ack_cola": finales.count("ack_cola"),
           "sin_stock": finales.count("sin_stock"), "no_confirmados": finales.count("no_confirmado"),
           "perdidos": inv["perdidos"], "reservas_duplicadas": inv["reservas_duplicadas"],
           "stock_inconsistente": inv["stock_inconsistente"], "pedidos_incompletos": inv["pedidos_incompletos"],
           "escrituras_epoch_viejo": inv["escrituras_epoch_viejo"] + m["zombi_201_epoch_viejo"],
           "dlq_espera_s": dlq_s, "dlq_residual": dlq_res, "error": "", **m}

    d = RESULTS / run_id
    d.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(load.rows).to_csv(d / "requests.csv", index=False)
    pd.DataFrame(list(load.orders.values())).to_csv(d / "orders.csv", index=False)
    pd.DataFrame(events).to_csv(d / "events.csv", index=False)
    (d / "meta.json").write_text(json.dumps({**row, "perdidos_keys": inv["perdidos_keys"], "t_start": t_start,
                                             "t_end": time.time(), "parametros": {
        "heartbeat_s": C.HEARTBEAT_S, "lease_ttl_s": C.LEASE_TTL_S, "poll_s": C.POLL_S}}, indent=2, default=str))
    guardar_resumen(row)
    print(f"  RESULTADO  detección={fmt(row.get('deteccion_s'))}  recuperación={fmt(row.get('recuperacion_s'))}  "
          f"perdidos={row['perdidos']}  dup={row['reservas_duplicadas']}  stock_inc={row['stock_inconsistente']}  "
          f"epoch_viejo={row['escrituras_epoch_viejo']}  falsos_failovers={row['falsos_failovers']}  "
          f"dlq_residual={row['dlq_residual']}", flush=True)
    return row


def fmt(x) -> str:
    return "n/a" if x is None or x != x else f"{x:.2f}s"


def guardar_resumen(row: dict):
    RESULTS.mkdir(parents=True, exist_ok=True)
    f = RESULTS / "summary.csv"
    if f.exists() and f.read_text(encoding="utf-8").splitlines()[0] != ",".join(FIELDS):
        f.rename(RESULTS / f"summary.antiguo-{int(time.time())}.csv")  # el formato cambió: no mezclar
    nuevo = not f.exists()
    with f.open("a", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
        if nuevo:
            w.writeheader()
        w.writerow({k: row.get(k) for k in FIELDS})


def hechas() -> set:
    f = RESULTS / "summary.csv"
    if not f.exists():
        return set()
    df = pd.read_csv(f)
    df = df[df.error.fillna("") == ""]  # las corridas con error se reintentan
    return set(zip(df.caso, df.rep.astype(int))) if len(df) else set()


async def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--case", default="ALL", help="C0, C1, C2, C3, C4 o ALL (control + cuatro fallas)")
    p.add_argument("--reps", type=int, default=30, help="repeticiones por caso de falla")
    p.add_argument("--rate", type=float, default=100, help="pedidos por segundo (100 nominal / 800 pico)")
    p.add_argument("--duration", type=float, default=60, help="duración de cada corrida con falla (s)")
    p.add_argument("--t-inject", type=float, default=15, help="segundo en que se inyecta la falla")
    p.add_argument("--c0-duration", type=float, default=600, help="duración de cada corrida de control (s)")
    p.add_argument("--c0-reps", type=int, default=3)
    p.add_argument("--dlq-wait", type=float, default=300, help="máximo a esperar que la DLQ se vacíe (s)")
    p.add_argument("--quick", action="store_true", help="prueba rápida: 2 reps, 50 s, control de 60 s")
    p.add_argument("--no-resume", action="store_true", help="repetir aunque ya existan en summary.csv")
    a = p.parse_args()
    if a.quick:
        a.reps, a.duration, a.c0_duration, a.c0_reps, a.dlq_wait = 2, 50, 60, 1, 90
    if a.duration < a.t_inject + FAULT_S + 10:
        p.error(f"--duration debe ser >= t-inject + {FAULT_S + 10} para que C2/C3 completen su falla")

    env = envmod.DockerEnv()
    cases = ["C0", "C1", "C2", "C3", "C4"] if a.case == "ALL" else [a.case]
    plan = []  # control primero; luego las fallas intercaladas (rep 1: C1,C2,C3,C4; rep 2: ...)
    if "C0" in cases:
        plan += [("C0", r) for r in range(1, (a.c0_reps if a.case == "ALL" else a.reps) + 1)]
    plan += [(c, r) for r in range(1, a.reps + 1) for c in cases if c != "C0"]
    ya = set() if a.no_resume else hechas()
    plan = [x for x in plan if x not in ya]
    print(f"Plan: {len(plan)} corridas ({len(ya)} ya hechas)  | heartbeat={C.HEARTBEAT_S}s lease={C.LEASE_TTL_S}s "
          f"| {a.rate:g} pedidos/s")
    fallos = 0
    for case, rep in plan:
        try:
            await run_one(env, case, rep, a)
            fallos = 0
        except Exception:  # noqa: BLE001
            traceback.print_exc()
            guardar_resumen({"run_id": f"{case}-r{rep:02d}-ERROR", "caso": case, "rep": rep, "error": "excepción"})
            fallos += 1
            if fallos >= 3:
                raise SystemExit("3 corridas seguidas fallaron: revisa los logs (docker compose logs)")
        await asyncio.sleep(5)
    print("\nTerminado. Siguiente paso:  docker compose run --rm runner python -m exp.analyze")


if __name__ == "__main__":
    asyncio.run(main())

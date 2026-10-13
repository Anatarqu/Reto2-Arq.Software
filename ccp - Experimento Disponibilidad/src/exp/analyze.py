"""Convierte results/ en tablas y gráficas listas para presentar.

Salida (en results/informe/):
  criterios.csv            ¿se cumple cada criterio de aceptación de la hipótesis?
  resumen_por_caso.csv     estadística por caso (mediana, p95, máximo)
  resultados.xlsx          todo lo anterior + una fila por corrida
  fig1_deteccion_recuperacion.png   distribución de tiempos vs. umbrales
  fig2_lineas_de_tiempo.png         qué vio el cliente segundo a segundo en una corrida representativa por caso
  fig3_impacto_cliente.png          intentos fallidos y hueco máximo sin servicio por caso

Uso:  docker compose run --rm runner python -m exp.analyze
"""
import json
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402

RESULTS = Path(os.environ.get("RESULTS_DIR", "/results"))
OUT = RESULTS / "informe"
UMBRAL_DETECCION, UMBRAL_RECUPERACION = 10.8, 30.0
NOMBRES = {"C0": "C0 control", "C1": "C1 kill", "C2": "C2 pause (zombi)", "C3": "C3 partición (zombi)",
           "C4": "C4 kill en transacción"}
AZUL, NARANJA, GRIS, VERDE, ROJO = "#2a6fb0", "#e07b00", "#8a8f98", "#2e8b57", "#c0392b"


def p95(s):
    s = s.dropna()
    return float(np.percentile(s, 95)) if len(s) else np.nan


def cargar() -> pd.DataFrame:
    df = pd.read_csv(RESULTS / "summary.csv")
    return df[df.error.fillna("") == ""].copy()


def resumen_por_caso(df):
    g = df.groupby("caso")
    rows = []
    for caso, d in g:
        rows.append({
            "caso": caso, "descripcion": NOMBRES.get(caso, caso), "corridas": len(d),
            "deteccion_mediana_s": d.deteccion_s.median(), "deteccion_p95_s": p95(d.deteccion_s),
            "deteccion_max_s": d.deteccion_s.max(),
            "recuperacion_mediana_s": d.recuperacion_s.median(), "recuperacion_p95_s": p95(d.recuperacion_s),
            "recuperacion_max_s": d.recuperacion_s.max(), "hueco_max_p95_s": p95(d.max_gap_s),
            "sin_recuperacion": int(d.recuperacion_s.isna().sum()) if caso != "C0" else 0,
            "intentos_fallidos_total": int(d.intentos_fallidos.sum()),
            "no_confirmados_total": int(d.no_confirmados.sum()),
            "lat_p95_mediana_ms": d.lat_p95_ms.median(),
            "perdidos": int(d.perdidos.sum()), "reservas_duplicadas": int(d.reservas_duplicadas.sum()),
            "stock_inconsistente": int(d.stock_inconsistente.sum()),
            "pedidos_incompletos": int(d.pedidos_incompletos.sum()),
            "escrituras_epoch_viejo": int(d.escrituras_epoch_viejo.sum()),
            "falsos_failovers": int(d.falsos_failovers.sum()), "dlq_residual": int(d.dlq_residual.sum()),
        })
    return pd.DataFrame(rows).round(3)


def criterios(df):
    f = df[df.caso != "C0"]
    c0 = df[df.caso == "C0"]
    det, rec = p95(f.deteccion_s), p95(f.recuperacion_s)
    sin_rec = int(f.recuperacion_s.isna().sum())
    rows = [
        ("Detección p95 < 10,8 s", f"{det:.2f} s" if len(f) else "n/d", len(f) > 0 and det < UMBRAL_DETECCION
         and int(f.deteccion_s.isna().sum()) == 0),
        ("Recuperación p95 ≤ 30 s (y todas recuperaron)", f"{rec:.2f} s; sin recuperar: {sin_rec}",
         len(f) > 0 and rec <= UMBRAL_RECUPERACION and sin_rec == 0),
        ("0 pedidos perdidos (confirmados pero ausentes)", int(df.perdidos.sum()), int(df.perdidos.sum()) == 0),
        ("0 reservas duplicadas", int(df.reservas_duplicadas.sum()), int(df.reservas_duplicadas.sum()) == 0),
        ("0 filas de stock inconsistentes", int(df.stock_inconsistente.sum()) + int(df.pedidos_incompletos.sum()),
         int(df.stock_inconsistente.sum()) + int(df.pedidos_incompletos.sum()) == 0),
        ("0 escrituras aceptadas con epoch viejo", int(df.escrituras_epoch_viejo.sum()),
         int(df.escrituras_epoch_viejo.sum()) == 0),
        ("0 failovers falsos en el control (C0)", int(c0.falsos_failovers.sum()) if len(c0) else "sin corridas C0",
         len(c0) > 0 and int(c0.falsos_failovers.sum()) == 0),
        ("0 mensajes sin resolver en la DLQ", int(df.dlq_residual.sum()), int(df.dlq_residual.sum()) == 0),
    ]
    out = pd.DataFrame(rows, columns=["criterio", "valor_observado", "cumple"])
    out["cumple"] = out.cumple.map({True: "SÍ", False: "NO"})
    return out


# ------------------------------------------------------------------------- gráficas
def fig1(df):
    f = df[df.caso != "C0"]
    casos = [c for c in ["C1", "C2", "C3", "C4"] if c in set(f.caso)]
    if not casos:
        return
    fig, axs = plt.subplots(1, 2, figsize=(12, 4.8), sharex=True)
    for ax, col, umbral, titulo in ((axs[0], "deteccion_s", UMBRAL_DETECCION, "Tiempo de detección"),
                                    (axs[1], "recuperacion_s", UMBRAL_RECUPERACION, "Tiempo de recuperación")):
        data = [f[f.caso == c][col].dropna().values for c in casos]
        ax.boxplot(data, tick_labels=[NOMBRES[c] for c in casos], showfliers=True, widths=0.5)
        for i, d in enumerate(data, 1):
            ax.scatter(np.random.normal(i, 0.05, len(d)), d, s=12, alpha=0.5, color=AZUL, zorder=3)
        ax.axhline(umbral, color=ROJO, ls="--", lw=1.4)
        ax.text(0.5, umbral, f"  umbral {umbral:g} s", color=ROJO, va="bottom", fontsize=9)
        ax.set_title(titulo + " (desde la inyección de la falla)")
        ax.set_ylabel("segundos")
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", alpha=.3)
        ax.tick_params(axis="x", labelrotation=15)
    fig.tight_layout()
    fig.savefig(OUT / "fig1_deteccion_recuperacion.png", dpi=160)
    plt.close(fig)


def _timeline(ax, run_id, titulo):
    d = RESULTS / run_id
    req = pd.read_csv(d / "requests.csv")
    ev = pd.read_csv(d / "events.csv") if (d / "events.csv").stat().st_size > 5 else pd.DataFrame(columns=["ts", "tipo"])
    meta = json.loads((d / "meta.json").read_text())
    t0 = meta["t_start"]
    req = req[req.probe == 0]
    s = ((req.t_done - t0).astype(int))
    ok = req[req.status.isin([200, 201])].groupby(s).size()
    cola = req[req.status == 202].groupby(s).size()
    fail = req[~req.status.isin([200, 201, 202, 409])].groupby(s).size()
    idx = range(0, int(req.t_done.max() - t0) + 1)
    ok, cola, fail = (x.reindex(idx, fill_value=0) for x in (ok, cola, fail))
    ax.bar(idx, ok.values, color=AZUL, width=1.0, label="confirmados (200/201)")
    ax.bar(idx, cola.values, bottom=ok.values, color=GRIS, width=1.0, label="en cola DLQ (202)")
    ax.bar(idx, fail.values, bottom=(ok + cola).values, color=NARANJA, width=1.0, label="intentos fallidos")
    marcas = {"inyeccion": ("inyección de la falla", ROJO, "--"),
              "falla_detectada": ("falla detectada", "#7b3fa0", "-."),
              "lider_publicado": ("nuevo líder publicado", VERDE, ":")}
    ymax = max((ok + cola + fail).max(), 1)
    t_inj = meta.get("t_inject") or t0
    for tipo, (nom, col, ls) in marcas.items():
        x = ev[(ev.tipo == tipo) & (ev.ts >= t_inj - 0.5)].ts.min() if len(ev) else np.nan
        if x == x:
            ax.axvline(x - t0, color=col, lw=1.8, ls=ls, label=nom)
    ax.set_title(titulo, fontsize=10)
    ax.set_xlabel("segundos desde el inicio de la carga")
    ax.set_ylabel("respuestas por s")
    ax.set_ylim(0, ymax * 1.25)


def fig2(df):
    f = df[df.caso != "C0"]
    casos = [c for c in ["C1", "C2", "C3", "C4"] if c in set(f.caso)]
    if not casos:
        return
    fig, axs = plt.subplots(len(casos), 1, figsize=(11, 3.1 * len(casos)))
    axs = np.atleast_1d(axs)
    for ax, c in zip(axs, casos):
        d = f[f.caso == c].dropna(subset=["recuperacion_s"])
        if d.empty:
            ax.set_title(f"{NOMBRES[c]}: sin corridas recuperadas")
            continue
        med = d.iloc[(d.recuperacion_s - d.recuperacion_s.median()).abs().argsort().iloc[0]]
        _timeline(ax, med.run_id, f"{NOMBRES[c]} — corrida {med.run_id} (recuperación {med.recuperacion_s:.1f} s, "
                                  f"detección {med.deteccion_s:.1f} s)")
    axs[0].legend(loc="upper right", fontsize=8, ncol=2)
    fig.tight_layout()
    fig.savefig(OUT / "fig2_lineas_de_tiempo.png", dpi=150)
    plt.close(fig)


def fig3(df):
    f = df[df.caso != "C0"]
    casos = [c for c in ["C1", "C2", "C3", "C4"] if c in set(f.caso)]
    if not casos:
        return
    fig, axs = plt.subplots(1, 2, figsize=(11, 4.2))
    axs[0].boxplot([f[f.caso == c].intentos_fallidos.values for c in casos], tick_labels=[NOMBRES[c] for c in casos])
    axs[0].set_title("Intentos HTTP fallidos por corrida (los reintenta el cliente)")
    axs[1].boxplot([f[f.caso == c].max_gap_s.dropna().values for c in casos], tick_labels=[NOMBRES[c] for c in casos])
    axs[1].set_title("Mayor hueco sin ninguna confirmación (s)")
    for ax in axs:
        ax.grid(axis="y", alpha=.3)
        ax.tick_params(axis="x", labelrotation=15)
        ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(OUT / "fig3_impacto_cliente.png", dpi=160)
    plt.close(fig)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    df = cargar()
    if df.empty:
        raise SystemExit("results/summary.csv no tiene corridas válidas todavía")
    res, cri = resumen_por_caso(df), criterios(df)
    res.to_csv(OUT / "resumen_por_caso.csv", index=False)
    cri.to_csv(OUT / "criterios.csv", index=False)
    with pd.ExcelWriter(OUT / "resultados.xlsx") as xw:
        cri.to_excel(xw, sheet_name="criterios", index=False)
        res.to_excel(xw, sheet_name="resumen_por_caso", index=False)
        df.to_excel(xw, sheet_name="corridas", index=False)
    fig1(df)
    fig2(df)
    fig3(df)
    print(cri.to_string(index=False))
    print(f"\nListo. Abre results\\informe\\ ({len(df)} corridas analizadas).")


if __name__ == "__main__":
    main()

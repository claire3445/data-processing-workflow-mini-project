"""Benchmark Pandas vs Polars (eager & lazy) + eksperimen data skew.

Data asli hanya 30 baris valid -> tidak bermakna untuk benchmark. Skrip ini MEMPERBESAR data secara deterministik
(sampling dengan pengembalian dari baris valid, seed tetap, order_id baru) dan menulis CSV ke --workdir.

Pekerjaan yang diukur IDENTIK di semua varian:
  baca CSV -> filter status in (paid, shipped) -> gross_amount, net_amount -> group by product_category, SUM(net_amount)
Waktu yang diukur mencakup pembacaan CSV (end-to-end). Memori tidak diukur.
"""
import argparse
import platform
import statistics
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl

STATUSES = ["paid", "shipped"]

# Windows + pipe (| Tee-Object) memakai cp1252: karakter seperti 'π' di explain() akan crash.
# errors="replace" -> karakter yang tak bisa ditulis diganti '?', program tidak berhenti.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(errors="replace")
    except Exception:
        pass


# ---------- spesifikasi mesin ----------
def ram_gb():
    try:
        if platform.system() == "Windows":
            import ctypes

            class MS(ctypes.Structure):
                _fields_ = [("l", ctypes.c_ulong), ("m", ctypes.c_ulong), ("total", ctypes.c_ulonglong),
                            ("a", ctypes.c_ulonglong), ("b", ctypes.c_ulonglong), ("c", ctypes.c_ulonglong),
                            ("d", ctypes.c_ulonglong), ("e", ctypes.c_ulonglong), ("f", ctypes.c_ulonglong)]
            s = MS(); s.l = ctypes.sizeof(MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(s))
            return s.total / 1e9
        for line in open("/proc/meminfo"):
            if line.startswith("MemTotal"):
                return int(line.split()[1]) / 1e6
    except Exception:
        return float("nan")
    return float("nan")


def machine_info():
    import os
    return (f"OS={platform.platform()} | CPU={platform.processor() or platform.machine()} | cores={os.cpu_count()} | "
            f"RAM={ram_gb():.1f} GB | python={platform.python_version()} | pandas={pd.__version__} | polars={pl.__version__}")


# ---------- pembuatan dataset ----------
def make_dataset(src_parquet, n, seed, path, skew_share=None):
    base = pd.read_parquet(src_parquet)
    base["order_ts"] = pd.to_datetime(base["order_ts"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    rng = np.random.default_rng(seed)
    df = base.iloc[rng.integers(0, len(base), n)].reset_index(drop=True)
    df["order_id"] = [f"B{i:09d}" for i in range(n)]  # order_id baru -> unik
    if skew_share is not None:
        mask = rng.random(n) < skew_share
        df.loc[mask, "customer_id"] = "C_DOMINAN"
    df.to_csv(path, index=False)
    return path


# ---------- tiga varian pekerjaan identik ----------
def job_pandas(path):
    df = pd.read_csv(path)
    df = df[df["status"].isin(STATUSES)]
    df = df.assign(gross_amount=df["quantity"] * df["unit_price"])
    df = df.assign(net_amount=df["gross_amount"] * (1 - df["discount_pct"]))
    return df.groupby("product_category")["net_amount"].sum().sort_index()


def _pl_exprs():
    return [
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        (pl.col("quantity") * pl.col("unit_price") * (1 - pl.col("discount_pct"))).alias("net_amount"),
    ]


def job_polars_eager(path):
    df = pl.read_csv(path)
    df = df.filter(pl.col("status").is_in(STATUSES)).with_columns(_pl_exprs())
    return df.group_by("product_category").agg(pl.col("net_amount").sum()).sort("product_category")


def lazy_query(path):
    return (pl.scan_csv(path)
            .filter(pl.col("status").is_in(STATUSES))
            .with_columns(_pl_exprs())
            .group_by("product_category").agg(pl.col("net_amount").sum()))


def job_polars_lazy(path):
    return lazy_query(path).collect().sort("product_category")


def to_series(res):
    if isinstance(res, pd.Series):
        return res
    return res.to_pandas().set_index("product_category")["net_amount"].sort_index()


def timeit(fn, repeats):
    fn()  # warm-up (tidak dihitung)
    ts = []
    for _ in range(repeats):
        t = time.perf_counter(); fn(); ts.append(time.perf_counter() - t)
    return statistics.median(ts), min(ts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/valid/valid_orders.parquet")
    ap.add_argument("--rows", type=int, default=500_000)
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--skew-share", type=float, default=0.85, help="porsi baris milik customer dominan")
    ap.add_argument("--workdir", default="data/bench")
    ap.add_argument("--results", default="results/benchmark_results.csv")
    a = ap.parse_args()

    Path(a.workdir).mkdir(parents=True, exist_ok=True)
    Path(a.results).parent.mkdir(parents=True, exist_ok=True)
    info = machine_info()
    print("MESIN:", info)

    normal = make_dataset(a.src, a.rows, a.seed, Path(a.workdir) / "orders_big.csv")
    skewed = make_dataset(a.src, a.rows, a.seed, Path(a.workdir) / "orders_skew.csv", a.skew_share)
    mb = Path(normal).stat().st_size / 1e6
    print(f"DATASET: {a.rows:,} baris | {mb:.1f} MB (CSV) | seed={a.seed} | repeats={a.repeats} (+1 warm-up)\n")

    rows = []
    # ---- 1. Pandas vs Polars eager vs lazy ----
    variants = [("pandas", job_pandas), ("polars_eager", job_polars_eager), ("polars_lazy", job_polars_lazy)]
    ref = None
    for name, fn in variants:
        res = to_series(fn(normal))
        if ref is None:
            ref = res
        assert np.allclose(res.values, ref.values) and list(res.index) == list(ref.index), f"hasil {name} berbeda!"
        med, mn = timeit(lambda f=fn: f(normal), a.repeats)
        print(f"{name:14s} median={med:8.4f}s  min={mn:8.4f}s")
        rows.append(dict(experiment="pandas_vs_polars", variant=name, rows=a.rows, file_mb=round(mb, 1),
                         median_s=round(med, 4), min_s=round(mn, 4), repeats=a.repeats))
    print("-> hasil ketiga varian SAMA (assert lolos):", {k: float(v) for k, v in ref.round(2).items()})

    print("\n=== LAZY: rencana query (projection/filter pushdown) ===")
    print(lazy_query(normal).explain())

    # ---- 2. Skew ----
    print("\n=== SKEW: group by customer_id ===")
    for label, path in (("normal", normal), ("skew", skewed)):
        pdf = pd.read_csv(path)
        pldf = pl.read_csv(path)
        sizes = pdf["customer_id"].value_counts()
        print(f"[{label}] customer unik={len(sizes)} | grup terbesar={sizes.iloc[0]:,} baris "
              f"({sizes.iloc[0] / len(pdf):.1%}) | median ukuran grup={int(sizes.median()):,} | "
              f"rasio terbesar/median={sizes.iloc[0] / sizes.median():.1f}x")
        for vname, fn in (("pandas_groupby", lambda d=pdf: d.assign(n=d["quantity"] * d["unit_price"]).groupby("customer_id")["n"].sum()),
                          ("polars_groupby", lambda d=pldf: d.with_columns((pl.col("quantity") * pl.col("unit_price")).alias("n"))
                           .group_by("customer_id").agg(pl.col("n").sum()))):
            med, mn = timeit(fn, a.repeats)
            print(f"   {vname:15s} median={med:8.4f}s  min={mn:8.4f}s")
            rows.append(dict(experiment=f"skew_{label}", variant=vname, rows=a.rows, file_mb=round(mb, 1),
                             median_s=round(med, 4), min_s=round(mn, 4), repeats=a.repeats))
        del pdf, pldf

    pd.DataFrame(rows).assign(machine=info).to_csv(a.results, index=False)
    print(f"\nHasil disimpan: {a.results}")
    print("Catatan: bukan klaim OOM; group_by satu mesin tidak mencerminkan beban tidak merata pada sistem terdistribusi.")


if __name__ == "__main__":
    main()

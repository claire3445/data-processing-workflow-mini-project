"""Muat data valid hasil transformasi ke DuckDB (tabel raw_orders). Rejected TIDAK dimuat.

Catatan: tutup koneksi lain ke warehouse.duckdb (ekstensi VS Code, REPL) sebelum menjalankan ini dan sebelum dbt run.
"""
import argparse
from pathlib import Path

import duckdb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/valid/orders_transformed.parquet")
    ap.add_argument("--db", default="warehouse/warehouse.duckdb")
    a = ap.parse_args()

    Path(a.db).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(a.db)
    con.execute(f"CREATE OR REPLACE TABLE raw_orders AS SELECT * FROM read_parquet('{Path(a.src).as_posix()}')")
    n, nd = con.execute("SELECT count(*), count(DISTINCT order_id) FROM raw_orders").fetchone()
    bad = con.execute("SELECT count(*) FROM raw_orders WHERE order_id IN ('O1031','O1032','O1033','O1034','O1035','O1036')").fetchone()[0]
    print(f"raw_orders: baris={n} unik={nd} | baris rejected yang ikut termuat={bad}")
    print(con.execute("DESCRIBE raw_orders").fetchdf()[["column_name", "column_type"]].to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()

"""Transformasi (Polars): gabung batch + stream, normalisasi timestamp, hitung gross/net amount.

Input : data/valid/valid_orders.parquet   (batch, lolos kontrak)
        data/valid/stream_valid.jsonl     (stream Kafka, lolos kontrak; opsional, dilewati bila belum ada)
Output: data/valid/orders_transformed.parquet
Dedup : order_id (stream menang bila sama) -> replay/duplikat tidak menggandakan baris.
"""
import argparse
import sys
from pathlib import Path

import polars as pl

# Windows + pipe (| Tee-Object) memakai cp1252: karakter kotak tabel Polars akan crash tanpa ini.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(errors="replace")
    except Exception:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/valid/valid_orders.parquet")
    ap.add_argument("--stream", default="data/valid/stream_valid.jsonl")
    ap.add_argument("--out", default="data/valid/orders_transformed.parquet")
    a = ap.parse_args()

    batch = pl.read_parquet(a.src)
    n_batch = batch.height
    n_stream = 0
    df = batch
    sp = Path(a.stream)
    if sp.exists() and sp.stat().st_size > 0:
        stream = pl.read_ndjson(sp).with_columns(
            pl.col("order_ts").str.to_datetime(time_unit="us", time_zone="UTC")
        ).select(batch.columns).cast(dict(batch.schema))
        n_stream = stream.height
        df = pl.concat([batch, stream])
    print(f"batch={n_batch} stream={n_stream} gabungan(sebelum dedup)={df.height}")

    df = df.with_columns(
        # D6: UTC lalu buang zona waktu (naive) agar CAST ke DATE tidak bergantung zona waktu mesin
        pl.col("order_ts").dt.convert_time_zone("UTC").dt.replace_time_zone(None).alias("order_ts"),
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
    ).with_columns(
        # discount_pct 0.10 = diskon 10%
        (pl.col("gross_amount") * (1 - pl.col("discount_pct"))).alias("net_amount"),
    ).unique(subset="order_id", keep="last", maintain_order=True)

    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(a.out)
    print("schema:", dict(df.schema))
    print("shape (setelah dedup):", df.shape)
    print(df.filter(pl.col("order_id").is_in(["O1001", "O1005", "O1009"]))
            .select("order_id", "quantity", "unit_price", "discount_pct", "gross_amount", "net_amount"))


if __name__ == "__main__":
    main()

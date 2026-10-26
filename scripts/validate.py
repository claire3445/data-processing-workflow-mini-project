"""Data contract (Pydantic) + pemisahan valid / rejected.

Dipakai oleh: batch_ingest.py, consumer.py (Kafka), dan bisa dijalankan langsung (CLI).
Input mentah HARUS berupa string apa adanya (dtype=str) agar nilai asli tetap tercatat di payload rejected.
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class Order(BaseModel):
    model_config = ConfigDict(extra="forbid")
    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str = Field(min_length=1)
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str = Field(min_length=1)
    status: Literal["paid", "shipped", "cancelled", "refunded"]


def validate_record(rec: dict):
    """Return (True, dict_valid) atau (False, [daftar_error])."""
    try:
        return True, Order(**rec).model_dump()
    except ValidationError as e:
        return False, [f"{'.'.join(map(str, x['loc']))}: {x['msg']}" for x in e.errors()]


def split_valid_rejected(records):
    valid, rejected = [], []
    for r in records:
        ok, res = validate_record(r)
        if ok:
            valid.append(res)
        else:
            rejected.append({"order_id": r.get("order_id") or None, "payload": r, "errors": res})
    return valid, rejected


def write_outputs(valid, rejected, valid_dir="data/valid", rejected_dir="data/rejected"):
    vd, rd = Path(valid_dir), Path(rejected_dir)
    vd.mkdir(parents=True, exist_ok=True)
    rd.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(valid).to_parquet(vd / "valid_orders.parquet", index=False)
    with open(rd / "rejected_orders.jsonl", "w", encoding="utf-8") as f:
        for r in rejected:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/source/orders.csv", help="CSV sumber atau Parquet raw")
    ap.add_argument("--valid-dir", default="data/valid")
    ap.add_argument("--rejected-dir", default="data/rejected")
    ap.add_argument("--strict", action="store_true", help="fail-fast: hentikan (exit 1) bila ada pelanggaran")
    a = ap.parse_args()

    if a.source.lower().endswith(".parquet"):
        raw = pd.read_parquet(a.source).astype(str)
    else:
        raw = pd.read_csv(a.source, dtype=str, keep_default_na=False)
    valid, rejected = split_valid_rejected(raw.to_dict("records"))

    if a.strict and rejected:
        print(f"[STRICT] {len(rejected)} record melanggar kontrak -> batch DIHENTIKAN, tidak ada yang ditulis.")
        for r in rejected:
            print(f"  {r['order_id']}: {r['errors']}")
        sys.exit(1)

    write_outputs(valid, rejected, a.valid_dir, a.rejected_dir)
    print(f"valid={len(valid)} rejected={len(rejected)}")
    for r in rejected:
        print(f"  {r['order_id']}: {r['errors']} | order_ts asli: {r['payload'].get('order_ts')!r}")


if __name__ == "__main__":
    main()

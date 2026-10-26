"""Batch ingestion: CSV mensimulasikan REST API ber-pagination.

Perubahan dari starter dosen:
 1. CSV dibaca sebagai string apa adanya (dtype=str) -> nilai rusak (mis. 'not-a-date') tidak hilang.
 2. order_ts di-parse HANYA di kolom bantu untuk watermark; baris dengan tanggal rusak tetap lolos
    ke validasi (tidak hilang diam-diam pada run berikutnya).
 3. Retry + exponential backoff benar-benar terpakai; kegagalan disimulasikan lewat --simulate-fail-pages.
 4. Hanya order_id yang BELUM ada di raw yang ditambahkan (idempoten).
 5. Setelah append, seluruh raw divalidasi -> data/valid dan data/rejected ditimpa (deterministik).
 6. Mencetak 4 counter: dibaca, berhasil (valid), ditolak, ditambahkan.
Watermark disimpan SETELAH Parquet berhasil ditulis.
"""
import argparse
import json
import time
from pathlib import Path

import pandas as pd

from validate import split_valid_rejected, write_outputs


def get_page(df, page, page_size, retries=3, backoff_base=1.0, fail_pages=(), _failed=None):
    """Adapter 'API'. Pada API nyata, isi try-block diganti requests.get(...) + cek status HTTP."""
    _failed = _failed if _failed is not None else set()
    for attempt in range(retries):
        try:
            if page in fail_pages and page not in _failed:
                _failed.add(page)
                raise ConnectionError("simulasi 503 Service Unavailable")
            start = page * page_size
            return df.iloc[start:start + page_size].copy()
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = backoff_base * (2 ** attempt)
            print(f"[retry] page={page} percobaan={attempt + 1}/{retries} gagal ({type(e).__name__}: {e}); tunggu {wait:g}s")
            time.sleep(wait)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/source/orders.csv")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--page-size", type=int, default=25)
    ap.add_argument("--checkpoint", default="data/watermark.json")
    ap.add_argument("--simulate-fail-pages", default="", help="mis. '1' atau '0,1': halaman yang gagal pada percobaan pertama")
    ap.add_argument("--backoff-base", type=float, default=1.0)
    ap.add_argument("--valid-dir", default="data/valid")
    ap.add_argument("--rejected-dir", default="data/rejected")
    args = ap.parse_args()

    fail_pages = {int(x) for x in args.simulate_fail_pages.split(",") if x.strip()}
    source = pd.read_csv(args.source, dtype=str, keep_default_na=False)
    source["_ts"] = pd.to_datetime(source["order_ts"], format="ISO8601", errors="coerce", utc=True)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    target = out / "orders.parquet"
    checkpoint = Path(args.checkpoint)
    last_ts = None
    if checkpoint.exists():
        last_ts = json.loads(checkpoint.read_text()).get("watermark")
    print(f"watermark_terakhir={last_ts}")

    # --- ekstraksi per halaman ---
    pages, page, failed = [], 0, set()
    while True:
        chunk = get_page(source, page, args.page_size, backoff_base=args.backoff_base, fail_pages=fail_pages, _failed=failed)
        if chunk.empty:
            break
        print(f"page={page} rows={len(chunk)}")
        pages.append(chunk)
        page += 1
    read_df = pd.concat(pages, ignore_index=True) if pages else source.iloc[0:0].copy()
    n_read = len(read_df)

    # --- incremental: lolos watermark (baris ts rusak ikut, supaya divalidasi) ---
    cand = read_df
    if last_ts:
        wm = pd.Timestamp(last_ts)
        cand = read_df[(read_df["_ts"] > wm) | read_df["_ts"].isna()]
    n_cand = len(cand)

    # --- idempoten: hanya order_id yang belum ada di raw ---
    old = pd.read_parquet(target).astype(str) if target.exists() else None
    known = set(old["order_id"]) if old is not None else set()
    new = cand[~cand["order_id"].isin(known)].drop_duplicates("order_id", keep="last")
    new_raw = new.drop(columns=["_ts"])
    n_added = len(new_raw)

    all_raw = pd.concat([old, new_raw], ignore_index=True) if old is not None else new_raw.reset_index(drop=True)
    all_raw.to_parquet(target, index=False)  # tulis dulu ...

    # --- validasi: baris baru (untuk counter) + seluruh raw (untuk output) ---
    v_new, r_new = split_valid_rejected(new_raw.to_dict("records"))
    v_all, r_all = split_valid_rejected(all_raw.to_dict("records"))
    write_outputs(v_all, r_all, args.valid_dir, args.rejected_dir)

    # ... baru simpan watermark setelah penulisan sukses
    if not all_raw.empty:
        wm_new = pd.to_datetime(all_raw["order_ts"], format="ISO8601", errors="coerce", utc=True).max()
        checkpoint.parent.mkdir(parents=True, exist_ok=True)
        checkpoint.write_text(json.dumps({"watermark": wm_new.isoformat()}, indent=2))

    print(f"[COUNTER] dibaca={n_read} | lolos_watermark={n_cand} | ditambahkan={n_added} | berhasil(valid)={len(v_new)} | ditolak={len(r_new)}")
    print(f"[TOTAL]   raw={len(all_raw)} | valid={len(v_all)} | rejected={len(r_all)} | output={target}")
    print("Catatan: adapter ini memaginasi CSV lokal; ganti get_page dengan requests.get() untuk REST API nyata.")


if __name__ == "__main__":
    main()

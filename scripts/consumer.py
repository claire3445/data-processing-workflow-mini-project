"""Kafka consumer: baca topic 'orders', validasi kontrak, tulis valid/rejected ke file terpisah.

Jaminan pengiriman (nyatakan ini di laporan):
  - enable_auto_commit=False; offset di-commit MANUAL setelah sink (tulis file) sukses  => AT-LEAST-ONCE.
  - Bila proses mati setelah sink dan sebelum commit, pesan dibaca ulang. Efek bisnisnya tidak dobel
    karena sink melakukan UPSERT per order_id (idempoten). Ini BUKAN exactly-once end-to-end.
Berhenti: Ctrl+C untuk berhenti, atau --idle-exit N untuk berhenti sendiri setelah N detik sepi.
"""
import argparse
import json
import os
from datetime import datetime
from pathlib import Path

from validate import validate_record


def _default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(type(o))


class FileSink:
    """Sink idempoten: dict keyed by kunci bisnis, file ditulis ulang secara atomik (tmp -> replace)."""

    def __init__(self, path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.rows = {}
        if self.path.exists():
            for line in self.path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    r = json.loads(line)
                    self.rows[r["_key"]] = r["_row"]
        else:
            self._flush()  # buat file kosong: sink yang belum pernah menerima record tetap punya file

    def upsert(self, key, row):
        existed = key in self.rows
        self.rows[key] = row
        self._flush()
        return existed

    def _flush(self):
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            for k, row in self.rows.items():
                f.write(json.dumps({"_key": k, "_row": row}, ensure_ascii=False, default=_default) + "\n")
        os.replace(tmp, self.path)


def export_plain(sink_path, out_path):
    """Tulis file akhir tanpa pembungkus _key/_row: 1 JSON per baris (dibaca transform.py / audit)."""
    rows = [json.loads(l)["_row"] for l in Path(sink_path).read_text(encoding="utf-8").splitlines() if l.strip()]
    with open(out_path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def handle_message(topic, partition, offset, key, value, valid_sink, rejected_sink):
    """Proses 1 pesan. Return 'valid' | 'rejected' (+ info duplikat). Tidak menyentuh Kafka -> bisa diuji offline."""
    where = f"topic={topic} partition={partition} offset={offset} key={key}"
    try:
        rec = json.loads(value.decode("utf-8"))
        if not isinstance(rec, dict):
            raise ValueError("JSON bukan objek")
    except Exception as e:
        rkey = f"{topic}-{partition}-{offset}"
        dup = rejected_sink.upsert(rkey, {"order_id": None, "payload": value.decode("utf-8", "replace"),
                                          "errors": [f"invalid JSON: {e}"],
                                          "kafka": {"topic": topic, "partition": partition, "offset": offset}})
        print(f"[REJECTED invalid-json] {where} -> {e}")
        return "rejected", dup

    ok, res = validate_record(rec)
    if ok:
        dup = valid_sink.upsert(res["order_id"], res)
        tag = "DUPLIKAT -> di-upsert, tidak menambah baris" if dup else "baru"
        print(f"[VALID] {where} order_id={res['order_id']} ({tag})")
        return "valid", dup

    rkey = rec.get("order_id") or f"{topic}-{partition}-{offset}"
    dup = rejected_sink.upsert(rkey, {"order_id": rec.get("order_id") or None, "payload": rec, "errors": res,
                                      "kafka": {"topic": topic, "partition": partition, "offset": offset}})
    print(f"[REJECTED] {where} order_id={rec.get('order_id')} -> {res}" + (" (duplikat)" if dup else ""))
    return "rejected", dup


def main():
    from kafka import KafkaConsumer  # diimpor di sini agar handle_message bisa diuji tanpa broker

    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--group", default="dpw-consumer")
    ap.add_argument("--valid", default="data/valid/stream_valid.jsonl")
    ap.add_argument("--rejected", default="data/rejected/stream_rejected.jsonl")
    ap.add_argument("--idle-exit", type=int, default=0, help="berhenti setelah N detik tanpa pesan (0 = jalan terus)")
    a = ap.parse_args()

    # sink internal (dengan _key) disimpan di .state; file akhir polos diekspor setiap kali
    v_state, r_state = a.valid + ".state", a.rejected + ".state"
    valid_sink, rejected_sink = FileSink(v_state), FileSink(r_state)

    consumer = KafkaConsumer(
        a.topic, bootstrap_servers=a.bootstrap, group_id=a.group,
        enable_auto_commit=False, auto_offset_reset="earliest",
        consumer_timeout_ms=a.idle_exit * 1000 if a.idle_exit else float("inf"),
    )
    print(f"[start] topic={a.topic} group_id={a.group} bootstrap={a.bootstrap} (commit manual setelah sink)")
    counts = {"valid": 0, "rejected": 0, "duplikat": 0}
    try:
        for msg in consumer:
            kind, dup = handle_message(msg.topic, msg.partition, msg.offset,
                                       msg.key.decode("utf-8", "replace") if msg.key else None,
                                       msg.value, valid_sink, rejected_sink)
            export_plain(v_state, a.valid)
            export_plain(r_state, a.rejected)
            consumer.commit()  # <- SETELAH sink berhasil
            counts[kind] += 1
            counts["duplikat"] += int(dup)
    except KeyboardInterrupt:
        print("\n[stop] dihentikan pengguna")
    finally:
        consumer.close()
        print(f"[ringkasan] diproses={counts['valid'] + counts['rejected']} valid={counts['valid']} "
              f"rejected={counts['rejected']} duplikat={counts['duplikat']} | "
              f"baris unik di sink: valid={len(valid_sink.rows)} rejected={len(rejected_sink.rows)}")


if __name__ == "__main__":
    main()

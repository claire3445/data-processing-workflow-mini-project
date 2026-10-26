"""Kafka producer: kirim baris orders.csv sebagai event JSON ke topic 'orders'.

Key pesan = order_id  -> event dengan order_id sama selalu masuk partisi yang sama (urutan per kunci terjaga).
Jalankan di LAPTOP (bukan di container): broker mengiklankan localhost:9092.
"""
import argparse
import json
import pandas as pd
from kafka import KafkaProducer

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="data/source/orders.csv")
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--only", default="", help="kirim hanya order_id tertentu, mis. O1001,O1002 (untuk demo duplikat)")
    ap.add_argument("--bad-json", action="store_true", help="kirim 1 pesan rusak (bukan JSON) untuk menguji consumer")
    a = ap.parse_args()

    producer = KafkaProducer(bootstrap_servers=a.bootstrap, acks="all", retries=3)

    if a.bad_json:
        md = producer.send(a.topic, key=b"BAD", value=b"{ini bukan json").get(timeout=15)
        print(f"[kirim] pesan RUSAK -> partition={md.partition} offset={md.offset}")
        producer.flush()
        return

    df = pd.read_csv(a.source, dtype=str, keep_default_na=False)  # string apa adanya, validasi tugas consumer
    if a.only:
        wanted = {x.strip() for x in a.only.split(",") if x.strip()}
        df = df[df["order_id"].isin(wanted)]

    n = 0
    for rec in df.to_dict("records"):
        md = producer.send(
            a.topic,
            key=rec["order_id"].encode("utf-8"),
            value=json.dumps(rec, ensure_ascii=False).encode("utf-8"),
        ).get(timeout=15)
        print(f"[kirim] order_id={rec['order_id']} -> partition={md.partition} offset={md.offset}")
        n += 1
    producer.flush()
    print(f"[selesai] {n} event dikirim ke topic '{a.topic}'")

if __name__ == "__main__":
    main()

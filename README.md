# Mini Project Data Processing Workflow — Event-to-Warehouse Pipeline E-Commerce

Pipeline data end-to-end untuk analisis transaksi e-commerce (data **sintetis**): ingestion batch/API → streaming Kafka → data contract Pydantic → transformasi Polars + benchmark → DuckDB → dbt (model dimensional, tes, dokumentasi).

Brief tugas lengkap ada di [Mini_Project.md](Mini_Project.md). Laporan akhir (diagram pipeline, keputusan desain, hasil pengujian, keterbatasan, pembagian pekerjaan) ada di folder [docs/](docs).

- **Mode:** individu
- **Tools:** Python 3.11, Pandas, PyArrow, Polars, Pydantic v2, kafka-python, Docker Compose (Apache Kafka 3.9.0, KRaft single-node), DuckDB, dbt-core 1.12.6 + dbt-duckdb 1.11.0

---

## 1. Arsitektur

```text
data/source/orders.csv  (simulasi REST API ber-pagination: page, page_size)
        |
        v
scripts/batch_ingest.py -- retry + exponential backoff + watermark (order_ts) + dedup order_id
        |
        v
data/raw/orders.parquet ──> scripts/validate.py (Pydantic) ──┬─> data/valid/valid_orders.parquet
                                                              └─> data/rejected/rejected_orders.jsonl (+ alasan)

scripts/producer.py ──> Kafka topic `orders` (3 partisi) ──> scripts/consumer.py (commit manual, sink idempoten)
                                                                  ├─> data/valid/stream_valid.jsonl
                                                                  └─> data/rejected/stream_rejected.jsonl

valid_orders.parquet + stream_valid.jsonl ──> scripts/transform.py (Polars: gross/net, UTC, dedup order_id)
        |                                          └─> data/valid/orders_transformed.parquet
        v
scripts/load_duckdb.py ──> warehouse/warehouse.duckdb (raw_orders)
        |
        v
dbt_project: stg_orders (view) → dim_customer, dim_product_category, fct_orders → mart_daily_sales (table)
        |
        v
dbt test (9 tes) + dbt docs generate        scripts/run_queries.py → jawaban 5 pertanyaan analitik
```

---

## 2. Struktur repositori

```text
.
├── Mini_Project.md            # brief & rubrik dari dosen
├── README.md                  # dokumen ini
├── docs/                      # laporan akhir (deliverable #8)
├── docker-compose.yml         # Kafka single-node (apache/kafka:3.9.0, 3 partisi default)
├── requirements.txt
├── data/
│   ├── source/orders.csv      # sumber sintetis: 36 baris (30 valid + 6 sengaja invalid)
│   ├── raw/orders.parquet     # hasil ingestion batch (mentah, semua kolom string)
│   ├── raw_retry_demo/        # output demo retry/backoff (terpisah dari raw utama)
│   ├── watermark.json         # checkpoint incremental run utama
│   ├── wm_retry.json          # checkpoint demo retry
│   ├── valid/                 # valid_orders.parquet, stream_valid.jsonl, orders_transformed.parquet
│   └── rejected/              # rejected_orders.jsonl, stream_rejected.jsonl (+ alasan penolakan)
├── scripts/
│   ├── batch_ingest.py        # [A] pagination, retry/backoff, watermark, Parquet, 4 counter
│   ├── validate.py            # [C] data contract Pydantic, mode --strict (fail-fast)
│   ├── producer.py            # [B] Kafka producer (key = order_id)
│   ├── consumer.py            # [B] Kafka consumer, commit manual, sink idempoten
│   ├── transform.py           # [D] gross/net amount, normalisasi timestamp, dedup
│   ├── benchmark.py           # [D] Pandas vs Polars (eager/lazy) + eksperimen data skew
│   ├── load_duckdb.py         # [E] muat data valid ke DuckDB (raw_orders)
│   └── run_queries.py         # [E] 5 pertanyaan analitik
├── dbt_project/               # [E] proyek dbt (staging, marts, schema.yml: tes + deskripsi)
├── warehouse/warehouse.duckdb # file DuckDB hasil akhir (deliverable #6)
├── results/benchmark_results.csv
├── logs/                      # keluaran terminal tiap tahap (A_*, B_*, C_*, D_*, E_*)
└── screenshoots/              # bukti tangkapan layar tiap tahap (nama berawalan A1…E9)
```

`data/bench/` (±72 MB), `dbt_project/target/`, `.venv/` tidak di-commit (lihat [.gitignore](.gitignore)); semuanya dibangkitkan ulang oleh perintah di bagian 3.

---

## 3. Cara menjalankan (Windows PowerShell, dari root repo)

### 3.0 Persiapan

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Pastikan Docker Desktop aktif (hanya untuk tahap B).

### 3.A Batch / API ingestion

```powershell
# Run 1: ambil semua halaman (page_size=25 → 2 halaman), tulis raw Parquet, simpan watermark
python scripts/batch_ingest.py --source data/source/orders.csv --out data/raw

# Run 2: bukti idempotensi — tidak ada baris tambahan
python scripts/batch_ingest.py --source data/source/orders.csv --out data/raw

# Demo retry + exponential backoff (halaman 1 gagal pada percobaan pertama, output & checkpoint terpisah)
python scripts/batch_ingest.py --source data/source/orders.csv --out data/raw_retry_demo --checkpoint data/wm_retry.json --simulate-fail-pages 1
```

### 3.B Streaming Kafka

```powershell
docker compose up -d
docker compose ps                      # tunggu status healthy

docker exec dpw-kafka /opt/kafka/bin/kafka-topics.sh --create --topic orders --partitions 3 --bootstrap-server localhost:9092
docker exec dpw-kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092

# Terminal 1: consumer (berhenti sendiri setelah 15 detik sepi)
python scripts/consumer.py --idle-exit 15

# Terminal 2: producer — 36 event dari CSV
python scripts/producer.py --source data/source/orders.csv

# Demo duplikasi: kirim ulang O1001 dan O1002
python scripts/producer.py --source data/source/orders.csv --only O1001,O1002

# Demo pesan rusak (bukan JSON)
python scripts/producer.py --bad-json

# Cek consumer group / offset / lag
docker exec dpw-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group dpw-consumer
```

### 3.C Data contract

```powershell
python scripts/validate.py --source data/source/orders.csv            # pisahkan valid / rejected
python scripts/validate.py --source data/source/orders.csv --strict   # fail-fast: exit 1 bila ada pelanggaran
```

### 3.D Transformasi dan benchmark

```powershell
python scripts/transform.py                  # → data/valid/orders_transformed.parquet
python scripts/benchmark.py                  # 500.000 baris, seed 42, 5 repeats → results/benchmark_results.csv
```

### 3.E Warehouse dan dbt

```powershell
python scripts/load_duckdb.py                # → warehouse/warehouse.duckdb (tabel raw_orders)

cd dbt_project
dbt run  --profiles-dir .
dbt test --profiles-dir .
dbt docs generate --profiles-dir .
dbt docs serve    --profiles-dir .          # buka http://localhost:8080
cd ..

python scripts/run_queries.py                # jawaban 5 pertanyaan analitik
```

> Tutup koneksi lain ke `warehouse.duckdb` (REPL, ekstensi VS Code) sebelum `load_duckdb.py` / `dbt run`; DuckDB hanya mengizinkan satu penulis.

---

## 4. Peta rubrik → bukti (untuk penilaian)

Setiap butir rubrik di [Mini_Project.md](Mini_Project.md) §9 dipetakan ke kode, keluaran, dan tangkapan layar. Semua angka di tabel berasal dari berkas di repo ini.

| Aspek (bobot) | Persyaratan | Kode | Bukti |
|---|---|---|---|
| **A. Batch/API ingestion (20)** | Pagination `page`/`page_size` | `get_page()` + loop di [batch_ingest.py](scripts/batch_ingest.py) | [A_run1.txt](logs/A_run1.txt): `page=0 rows=25`, `page=1 rows=11`; `A1_run1.png` |
| | Retry + exponential backoff | `get_page()` — tunggu `backoff_base * 2^attempt`; kegagalan disimulasikan via `--simulate-fail-pages` | [A_retry.txt](logs/A_retry.txt): `[retry] page=1 percobaan=1/3 … tunggu 1s`; `A3_retry_backoff.png` |
| | Simpan Parquet | `all_raw.to_parquet(...)` | [data/raw/orders.parquet](data/raw/orders.parquet); `A4_parquet_watermark.png` |
| | Watermark + bukti run ke-2 tidak menggandakan | checkpoint `order_ts` + filter `order_id` yang sudah ada | Run 1: `dibaca=36 … ditambahkan=36`. Run 2: `dibaca=36 … ditambahkan=0`, total raw tetap 36 ([A_run2.txt](logs/A_run2.txt)); [watermark.json](data/watermark.json); `A2_run2_idempoten.png` |
| | Counter dibaca/berhasil/ditolak/ditambahkan | baris `[COUNTER]` | Run 1: `dibaca=36 \| berhasil(valid)=30 \| ditolak=6 \| ditambahkan=36` |
| **B. Kafka (15)** | Kafka via Docker Compose | [docker-compose.yml](docker-compose.yml) | `B1_compose_healthy.png` |
| | Topic `orders`, producer ≥ 20 event | [producer.py](scripts/producer.py) — 36 event, key = `order_id` | `B2_topic_3_partisi.png`, [B_producer.txt](logs/B_producer.txt) (36 event; sebaran partisi 0/1/2) |
| | Consumer validasi kontrak, tulis valid/invalid terpisah | [consumer.py](scripts/consumer.py) | [stream_valid.jsonl](data/valid/stream_valid.jsonl) (30 baris), [stream_rejected.jsonl](data/rejected/stream_rejected.jsonl) (6 pelanggaran kontrak + 2 pesan bukan-JSON); `B3_consumer_log.png` |
| | Consumer group, partition, offset | `group_id=dpw-consumer`, `enable_auto_commit=False` | `B5_consumer_group.png`: 3 partisi, LAG=0 |
| | Demo duplikasi + idempotensi sink | `FileSink.upsert()` keyed `order_id` | `B4_duplikat.png` (kirim ulang O1001/O1002) → `B7_warehouse_tetap_30.png` (warehouse tetap 30 baris, 30 unik) |
| | At-least-once vs exactly-once, batas jaminan | docstring [consumer.py](scripts/consumer.py) (`enable_auto_commit=False`, commit setelah sink, upsert per `order_id`) | penjelasan lengkap di laporan ([docs/](docs)) |
| **C. Data contract (15)** | 6 aturan minimal | model `Order` di [validate.py](scripts/validate.py) | 6 contoh pelanggaran O1031–O1036 (quantity, unit_price, discount_pct, order_ts, status, customer_id). Aturan `order_id` wajib diisi ditegakkan (`min_length=1`) tetapi **tidak ada baris di data sumber yang melanggarnya** |
| | Fail-fast | `validate.py --strict` → exit 1, tidak menulis apa pun | [C_validate_strict.txt](logs/C_validate_strict.txt); `C3_validate_strict.png` |
| | Pisahkan valid/rejected + alasan | `write_outputs()` | [rejected_orders.jsonl](data/rejected/rejected_orders.jsonl) (payload asli + `errors`); `C1_validate_karantina.png`, `C2_rejected_jsonl.png` |
| | Rejected tidak masuk warehouse | [load_duckdb.py](scripts/load_duckdb.py) mencetak jumlah baris rejected yang ikut termuat | `D2_warehouse_30_dan_rejected_0.png` |
| **D. Transformasi (15)** | `gross_amount`, `net_amount`, normalisasi tipe/timestamp | [transform.py](scripts/transform.py) | `D1_transform_schema.png`: gabungan batch 30 + stream 30 = 60 → **30 baris** setelah dedup `order_id`; contoh O1005: 2 × 39,0 → gross 78,0, net 62,4 (diskon 0,2); O1001: net 237,5; O1009: net 474,05 |
| | Pandas vs Polars (ukuran, waktu, keterbatasan) | [benchmark.py](scripts/benchmark.py) | [benchmark_results.csv](results/benchmark_results.csv), [D_benchmark.txt](logs/D_benchmark.txt); `D3_benchmark_tabel.png` |
| | Lazy query Polars | `lazy_query()` — `scan_csv` → `collect` + `explain()` | `D4_lazy_explain.png` (PROJECT 5/9 kolom, SELECTION di-pushdown ke scan) |
| | Eksperimen data skew | `make_dataset(..., skew_share=0.85)` | `D5_skew_distribusi.png`; rasio grup terbesar/median 4,0× (normal) vs 170,8× (skew) |
| **E. DuckDB + dbt (25)** | DuckDB warehouse | [warehouse.duckdb](warehouse/warehouse.duckdb) | tabel: `raw_orders`, `stg_orders`, `dim_customer`, `dim_product_category`, `fct_orders`, `mart_daily_sales` |
| | Staging konsisten | [stg_orders.sql](dbt_project/models/staging/stg_orders.sql) | cast eksplisit tiap kolom |
| | 1 fact + 2 dimensi + agregat harian | [models/marts/](dbt_project/models/marts) | [E_dbt_run.txt](logs/E_dbt_run.txt): `PASS=5 ERROR=0`; `E1_dbt_run.png` |
| | Tes `unique`, `not_null`, `relationships` | [schema.yml](dbt_project/models/marts/schema.yml) | [E_dbt_test.txt](logs/E_dbt_test.txt): `PASS=9 ERROR=0`; `E2_dbt_test.png` |
| | `dbt docs generate` | — | `E3_dbt_docs_model.png`, `E4_dbt_lineage.png` (folder `target/` tidak di-commit; regenerasi dengan `dbt docs generate`) |
| | Star vs snowflake, kapan incremental | — | penjelasan di laporan ([docs/](docs)) |
| **Reproducibility, laporan, demo (10)** | README, requirements, compose | dokumen ini | `logs/pip_freeze.txt` (versi persis), `00_versi_tools.png` |

### Jawaban 5 pertanyaan analitik (dari [E_queries.txt](logs/E_queries.txt), dijalankan oleh `scripts/run_queries.py`)

| # | Pertanyaan | Jawaban |
|---|---|---|
| Q1 | Penjualan bersih per hari (paid + shipped) | 10 hari (2026-09-01 … 09-10); tertinggi **2026-09-06 = 1.022,60**, terendah 09-03 = 110,00 — `E5_q1.png` |
| Q2 | Kategori dengan penjualan bersih tertinggi | **Electronics 1.605,12**, Home 996,98, Fashion 573,20, Books 236,73 — `E6_q2.png` |
| Q3 | Jumlah & nilai pesanan per status | paid 20 (2.443,85); shipped 8 (968,18); cancelled 1 (62,40); refunded 1 (474,05) — `E7_q3.png` |
| Q4 | Metode pembayaran terbanyak | **card 14**, ewallet 8, transfer 8 — `E8_q4.png` |
| Q5 | Record ditolak & penyebab terbesar | **6 record**, masing-masing 1 pelanggaran berbeda (customer_id, discount_pct, order_ts, quantity, status, unit_price) → **tidak ada penyebab dominan** (seri 1-1-1-1-1-1) — `E9_q5.png` |

### Ringkasan benchmark (500.000 baris, 35,2 MB CSV, median 5 pengulangan + 1 warm-up)

Mesin: Windows 11, AMD 12 core, RAM 16,4 GB, Python 3.11.9, pandas 3.1.0, polars 2.0.0.

| Percobaan | Varian | Median (s) | Min (s) |
|---|---|---:|---:|
| baca CSV → filter → hitung → group by kategori | pandas | 0,2626 | 0,2427 |
| | polars eager | 0,1366 | 0,1212 |
| | polars lazy | 0,1004 | 0,0982 |
| group by `customer_id`, data normal | pandas | 0,0366 | 0,0341 |
| | polars | 0,0095 | 0,0089 |
| group by `customer_id`, data skew (85 % satu customer) | pandas | 0,0314 | 0,0301 |
| | polars | 0,0363 | 0,0079 |

Hasil ketiga varian pada benchmark pertama **identik** (di-assert di kode). Keterbatasan: satu mesin, satu dataset sintetis hasil resampling 30 baris, memori tidak diukur, waktu mencakup pembacaan CSV. Pada skew, median Polars (0,0363 s) jauh di atas min-nya (0,0079 s) → variansi tinggi, sehingga **tidak ada klaim bahwa skew memperlambat/mempercepat group_by**; yang terbukti hanya distribusi grup menjadi sangat timpang (170,8× vs 4,0×). Tidak ada klaim OOM.

---

## 5. Hasil utama

| Tahap | Hasil |
|---|---|
| Batch ingestion | 36 dibaca → 30 valid, 6 ditolak, 36 ditambahkan (run 1); run 2: 0 ditambahkan |
| Streaming | 36 event + duplikat + 2 pesan rusak → sink: 30 valid unik, 6 rejected unik (+2 pesan non-JSON), lag 0 |
| Warehouse | `raw_orders` 30 baris / 30 `order_id` unik; `fct_orders` 30; `dim_customer` 25; `dim_product_category` 4; `mart_daily_sales` 28 |
| dbt | 5 model sukses, 9 tes lulus (`unique`, `not_null`, `relationships`) |

## 6. Batasan yang perlu diketahui penilai

- **Sumber "API" adalah CSV lokal**. `get_page()` di [batch_ingest.py](scripts/batch_ingest.py) adalah adapter; untuk REST API nyata isi blok `try` diganti `requests.get(url, params={"page":…, "page_size":…})` + `raise_for_status()` (dijelaskan di laporan).
- **Kafka: at-least-once, bukan exactly-once end-to-end.** Duplikasi bisnis dicegah oleh sink idempoten (upsert per `order_id`), bukan oleh broker. Detail di laporan.
- **Tes `relationships` pada dbt bersifat lemah**: `dim_customer` dan `dim_product_category` diturunkan (`select distinct`) dari tabel yang sama dengan `fct_orders`, sehingga FK pasti terpenuhi. Tes ini membuktikan integritas model, bukan kualitas sumber; kualitas sumber dijaga di lapisan data contract.
- **Model incremental dbt tidak diimplementasikan** (semua model `view`/`table`); hanya dijelaskan kapan dibutuhkan (di laporan). Incremental loading dibuktikan di lapisan ingestion (watermark).
- Berkas log di [logs/](logs) hasil `Tee-Object` PowerShell 5.1 berenkode **UTF-16**; buka dengan editor yang mendukung atau gunakan *View raw*. [logs/B_consumer.txt](logs/B_consumer.txt) merekam **percobaan awal consumer yang crash** (`FileNotFoundError` pada file `.state` yang belum ada); bug ini sudah diperbaiki di `FileSink.__init__` ([consumer.py](scripts/consumer.py)). Bukti consumer berhasil: `B5_consumer_group.png` (LAG=0), `B7_warehouse_tetap_30.png`, serta isi [stream_valid.jsonl](data/valid/stream_valid.jsonl) / [stream_rejected.jsonl](data/rejected/stream_rejected.jsonl).
- Data sintetis; tidak ada data pelanggan nyata.

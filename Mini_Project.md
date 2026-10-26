# Mini Project Praktikum Data Processing Workflow
## End-to-End Event-to-Warehouse Pipeline: Analisis Transaksi E-Commerce

**Durasi maksimal:** 1 Minggu (Demo) 
**Mode:** individu  
**Tools utama:** Python, Requests, Pandas/PyArrow, Docker Compose, Apache Kafka, Pydantic, Polars, DuckDB, dbt  
**Dataset:** Data sintetis yang disertakan dalam paket pendukung

## 1. Latar Belakang

Sebuah platform e-commerce ingin membangun pipeline data yang menggabungkan data transaksi historis dari REST API dan kejadian transaksi baru yang dikirim secara streaming. Pipeline harus dapat diulang tanpa menggandakan data, menolak data yang melanggar kontrak, memproses dataset dengan efisien, lalu menyajikan model analitik yang dapat diuji dan didokumentasikan.

Mahasiswa bertindak sebagai **Data Engineer** yang merancang dan membuktikan pipeline dari sumber data sampai data warehouse.

## 2. Tujuan dan cakupan

Proyek wajib mengintegrasikan lima topik praktikum pada dokumen *Praktikum Data Processing Workflow*:

1. **Data Ingestion – Batch & API:** ekstraksi bertahap, pagination, retry/backoff, watermark/incremental loading, dan penyimpanan Parquet.
2. **Data Ingestion – Event-Driven & Streaming:** menjalankan broker melalui Docker Compose, membuat producer dan consumer, serta menjelaskan consumer group, partition, offset, dan semantik pengiriman.
3. **Automated Data Contracts & Assertions:** mendefinisikan kontrak data, validasi fail-fast, dan memisahkan data valid dari data yang ditolak.
4. **Batch Data Transformation & Optimasi Performa:** membandingkan transformasi Pandas dengan Polars; menguji lazy evaluation dan menangani data skew secara terukur.
5. **Analytics Engineering: SQL & dbt:** membangun model dimensional, menjalankan pengujian `unique`, `not_null`, dan `relationships`, serta menghasilkan dokumentasi dbt.

## 3. Skenario bisnis

Data sumber memuat pesanan dengan atribut `order_id`, `customer_id`, `order_ts`, `product_category`, `quantity`, `unit_price`, `discount_pct`, `payment_method`, dan `status`. Data historis masuk melalui proses batch/API. Pesanan baru dikirim melalui Kafka. Data yang lolos validasi diproses dan dimuat ke DuckDB. Lapisan analitik menyediakan ringkasan penjualan menurut tanggal, kategori produk, dan metode pembayaran.

Data dalam paket ini adalah **data sintetis untuk pembelajaran**, bukan data pelanggan nyata. Agar praktikum tidak bergantung pada layanan eksternal, file CSV berperan sebagai sumber API lokal yang dapat dipaginasi oleh skrip. Dalam laporan, jelaskan bagaimana adapter tersebut dapat diganti dengan REST API sungguhan.

## 4. Arsitektur target

```text
CSV sumber (simulasi REST API dengan pagination)
        |
        v
Python batch extractor -- retry/backoff + watermark
        |
        v
Raw Parquet ----> Data contract (Pydantic) ----> rejected records
        |                         |
        |                         v
        |                    valid records
        |                         ^
        |                         |
Kafka producer --> Kafka topic --> consumer
                                  |
                                  v
                         valid/invalid event records
                                  |
                                  v
                   Polars transform + skew experiment
                                  |
                                  v
                            DuckDB database
                                  |
                                  v
                      dbt staging & marts models
                                  |
                                  v
                  dbt tests + generated documentation
```

## 5. Tugas wajib

### A. Batch/API ingestion
- Implementasikan ekstraksi halaman demi halaman dengan parameter `page` dan `page_size`.
- Tambahkan retry dengan exponential backoff untuk error sementara. Untuk demo lokal, simulasi kegagalan boleh dilakukan melalui parameter konfigurasi.
- Simpan hasil mentah ke Parquet.
- Implementasikan watermark berbasis `order_ts` atau ID terakhir yang diproses. Jalankan proses dua kali dan buktikan bahwa proses kedua tidak menggandakan data.
- Catat jumlah record yang dibaca, berhasil, ditolak, dan ditambahkan.

### B. Event-driven streaming
- Jalankan Kafka dengan Docker Compose.
- Buat producer yang menerbitkan minimal 20 event pesanan dari CSV ke topic `orders`.
- Buat consumer yang membaca pesan, memvalidasi kontrak, dan menulis record valid/invalid ke file terpisah.
- Jelaskan fungsi partition, consumer group, offset, dan perbedaan **at-least-once** dengan **exactly-once**.
- Lakukan demonstrasi duplikasi pesan (misalnya kirim event dengan `order_id` sama dua kali), lalu jelaskan bagaimana idempotensi pada sink mencegah duplikasi bisnis.
- Tidak diwajibkan membuktikan exactly-once end-to-end. Mahasiswa harus menyatakan batas jaminan yang benar dari implementasinya.

### C. Data contract dan assertions
Terapkan aturan minimal:
- `order_id` dan `customer_id` wajib diisi.
- `quantity` harus bilangan bulat positif.
- `unit_price` tidak boleh negatif.
- `discount_pct` berada pada rentang 0 sampai 1.
- `order_ts` harus dapat diparse menjadi timestamp.
- `status` harus salah satu dari `paid`, `shipped`, `cancelled`, atau `refunded`.

Gunakan Pydantic atau validasi setara. Record tidak valid harus ditolak sebelum masuk ke tabel warehouse. Simpan alasan penolakan agar dapat diaudit.

### D. Transformasi dan optimasi
- Hitung `gross_amount = quantity * unit_price`.
- Hitung `net_amount = gross_amount * (1 - discount_pct)`.
- Normalisasi timestamp dan tipe data.
- Bandingkan waktu Pandas dan Polars pada dataset yang sama; laporkan ukuran dataset, waktu eksekusi, dan keterbatasan benchmark.
- Tunjukkan penggunaan lazy query Polars (`scan_csv`/`scan_parquet` dan `collect`) atau jelaskan perbedaannya dengan eager execution.
- Buat eksperimen data skew: gandakan baris pada satu `customer_id` dominan, lalu bandingkan distribusi grup dan/atau waktu `group_by`. Jangan mengklaim telah menguji OOM kecuali benar-benar terjadi dan tercatat. Jelaskan strategi mitigasi seperti pemrosesan bertahap, proyeksi kolom, agregasi parsial, dan menghindari materialisasi yang tidak perlu.

### E. Analytics engineering dan dbt
- Gunakan DuckDB sebagai warehouse lokal.
- Buat model staging dengan nama dan tipe data konsisten.
- Buat minimal satu fact table dan dua dimension tables (misalnya `fct_orders`, `dim_customer`, `dim_product_category`).
- Tambahkan model agregat penjualan harian.
- Tambahkan tes `unique`, `not_null`, dan `relationships` yang relevan.
- Jalankan `dbt docs generate`; sertakan bukti atau tangkapan layar dokumentasi.
- Jelaskan perbedaan star schema dan snowflake schema serta kapan incremental model dibutuhkan.

## 6. Deliverables

1. Source code modular dan `README.md` cara menjalankan.
2. `raw/orders.parquet` atau instruksi reproducible untuk membuatnya.
3. File valid dan rejected beserta alasan penolakan.
4. Producer dan consumer Kafka serta konfigurasi Docker Compose.
5. Hasil benchmark Pandas vs Polars dan eksperimen skew.
6. File DuckDB atau skrip SQL untuk membangunnya.
7. Proyek dbt dengan model, tests, dan dokumentasi.
8. Laporan ringkas 4–8 halaman atau Markdown, termasuk diagram pipeline, keputusan desain, hasil pengujian, keterbatasan, dan pembagian pekerjaan.
9. Demo 5–8 menit.

## 7. Kriteria keberhasilan

- Pipeline dapat dijalankan ulang dengan instruksi yang jelas.
- Pagination dan incremental loading dibuktikan dengan log/hasil uji.
- Data cacat tidak masuk ke warehouse dan alasan penolakannya tersimpan.
- Streaming berjalan melalui broker yang dikontainerisasi.
- Duplikasi ditangani pada level bisnis/sink atau keterbatasannya dijelaskan.
- Transformasi menghasilkan nilai perhitungan yang benar.
- Perbandingan performa dapat direproduksi dan tidak mengandung klaim tanpa pengukuran.
- dbt models berhasil dijalankan; tes dan dokumentasi dibuat.
- Hasil analitik dapat menjawab pertanyaan bisnis.

## 8. Pertanyaan analitik minimal

- Berapa nilai penjualan bersih per hari?
- Kategori produk mana yang memiliki penjualan bersih tertinggi?
- Bagaimana jumlah dan nilai pesanan menurut status?
- Metode pembayaran mana yang paling sering digunakan?
- Berapa banyak record yang ditolak oleh data contract, dan apa penyebab terbesarnya?

## 9. Rubrik penilaian (100 poin)

| Aspek | Bobot |
|---|---:|
| Batch/API ingestion: pagination, retry, watermark, Parquet | 20 |
| Kafka producer-consumer, konfigurasi, semantik pesan | 15 |
| Data contract, fail-fast, audit rejected records | 15 |
| Transformasi, benchmark, lazy/eager, data skew | 15 |
| DuckDB, dimensional modeling, dbt tests/docs | 25 |
| Reproducibility, laporan, demo dan refleksi teknis | 10 |


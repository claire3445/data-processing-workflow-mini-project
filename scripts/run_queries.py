"""Jalankan 5 pertanyaan analitik (Q1-Q5) ke DuckDB dan cetak hasilnya.

Jalankan dari ROOT proyek (path rejected_orders.jsonl relatif terhadap root).
Koneksi read-only: aman dijalankan walau dbt docs serve sedang terbuka, tetapi jangan saat dbt run berjalan.
"""
import argparse

import duckdb

QUERIES = {
    "Q1 Penjualan bersih per hari (paid+shipped)": """
        SELECT order_date, round(SUM(net_sales),2) AS net_sales
        FROM mart_daily_sales GROUP BY 1 ORDER BY 1""",
    "Q2 Kategori dengan penjualan bersih tertinggi": """
        SELECT product_category, round(SUM(net_sales),2) AS net_sales
        FROM mart_daily_sales GROUP BY 1 ORDER BY net_sales DESC""",
    "Q3 Jumlah dan nilai pesanan menurut status": """
        SELECT status, COUNT(*) AS order_count, round(SUM(net_amount),2) AS net_value
        FROM fct_orders GROUP BY 1 ORDER BY order_count DESC""",
    "Q4 Metode pembayaran paling sering digunakan": """
        SELECT payment_method, COUNT(*) AS order_count
        FROM fct_orders GROUP BY 1 ORDER BY order_count DESC""",
    "Q5a Record ditolak per alasan": """
        SELECT split_part(e, ':', 1) AS alasan, count(*) AS n
        FROM (SELECT unnest(errors) AS e FROM read_json_auto('{rej}'))
        GROUP BY 1 ORDER BY n DESC, alasan""",
    "Q5b Nilai kotor pesanan yang ditolak (dari payload)": """
        SELECT order_id, errors[1] AS alasan,
               try_cast(payload.quantity AS DOUBLE) * try_cast(payload.unit_price AS DOUBLE) AS nilai_kotor
        FROM read_json_auto('{rej}') ORDER BY order_id""",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="warehouse/warehouse.duckdb")
    ap.add_argument("--rejected", default="data/rejected/rejected_orders.jsonl")
    ap.add_argument("--only", default="", help="mis. Q1 atau Q5 (awalan judul)")
    a = ap.parse_args()

    con = duckdb.connect(a.db, read_only=True)
    for title, sql in QUERIES.items():
        if a.only and not title.startswith(a.only):
            continue
        sql = sql.format(rej=a.rejected.replace("\\", "/"))
        print(f"\n### {title}\n{sql.strip()}\n")
        print(con.execute(sql).fetchdf().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()

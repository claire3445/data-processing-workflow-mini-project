select
    order_id, customer_id, product_category, order_ts, order_date,
    quantity, unit_price, discount_pct, payment_method, status,
    gross_amount, net_amount
from {{ ref('stg_orders') }}

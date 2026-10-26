select distinct customer_id from {{ ref('stg_orders') }}

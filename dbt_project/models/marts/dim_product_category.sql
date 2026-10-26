select distinct product_category from {{ ref('stg_orders') }}

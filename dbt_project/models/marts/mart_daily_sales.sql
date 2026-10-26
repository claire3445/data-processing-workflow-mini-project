select
    order_date,
    product_category,
    payment_method,
    count(*)        as order_count,
    sum(net_amount) as net_sales
from {{ ref('fct_orders') }}
where status in ('paid', 'shipped')
group by 1, 2, 3

with orders as (
    select * from {{ ref('stg_orders') }}
    where ORDER_STATUS in ('shipped','approved','delivered')
),
customers as (
    select * from {{ ref('stg_customers') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state
from orders o
join customers c on o.customer_id = c.customer_id
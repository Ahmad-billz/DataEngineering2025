{{ config(
    materialized='table'
) }}

select 
    substr(order_purchase_timestamp,1,4) as order_purchase_year,
    order_status,
    count(distinct order_id) cnt_distinct_order,
    count(distinct customer_id) cnt_distinct_customer
from {{ ref('int_orders_clean') }}
group by 1,2
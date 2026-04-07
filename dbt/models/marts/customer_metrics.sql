-- SQL transformation: Customer aggregations
{{ config(materialized='table') }}

select
    customer_id,
    count(distinct order_id) as total_orders,
    sum(total_amount) as total_revenue,
    avg(total_amount) as avg_order_value,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date
from {{ ref('stg_sales') }}
group by customer_id

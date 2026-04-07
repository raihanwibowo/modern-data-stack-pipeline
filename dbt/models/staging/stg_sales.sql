-- Staging model: Clean and standardize raw sales data
{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    quantity * price as total_amount,
    cast(order_date as date) as order_date
from {{ ref('raw_sales') }}
where quantity > 0

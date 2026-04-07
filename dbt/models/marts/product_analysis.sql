-- Product analysis using SQL
{{ config(materialized='table') }}

with product_stats as (
    select
        product_id,
        count(distinct order_id) as order_count,
        sum(quantity) as total_quantity_sold,
        sum(total_amount) as total_revenue,
        avg(total_amount) as avg_order_value,
        stddev(total_amount) as revenue_std
    from {{ ref('stg_sales') }}
    group by product_id
),

ranked_products as (
    select
        *,
        percent_rank() over (order by total_revenue) * 100 as revenue_percentile
    from product_stats
)

select
    product_id,
    order_count,
    total_quantity_sold,
    total_revenue,
    avg_order_value,
    revenue_std,
    revenue_percentile,
    
    -- Product tier classification
    case
        when revenue_percentile >= 66 then 'Gold'
        when revenue_percentile >= 33 then 'Silver'
        else 'Bronze'
    end as product_tier
    
from ranked_products

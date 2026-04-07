-- Customer segmentation using SQL
{{ config(materialized='table') }}

select
    customer_id,
    total_orders,
    total_revenue,
    avg_order_value,
    first_order_date,
    last_order_date,
    
    -- Segmentation logic
    case
        when total_revenue >= 100 then 'High Value'
        when total_revenue >= 50 then 'Medium Value'
        else 'Low Value'
    end as customer_segment,
    
    -- Days since last order
    current_date - last_order_date as days_since_last_order,
    
    -- Active customer flag
    case
        when current_date - last_order_date <= 30 then true
        else false
    end as is_active
    
from {{ ref('customer_metrics') }}

-- Staging: delivery orders from Phase 2
-- Source table already enriched with Power BI columns (region, performance_flag, etc.)
select
    order_id,
    customer_state,
    state_name,
    country,
    region,
    delivery_days::double       as delivery_days,
    days_vs_estimate::double    as days_vs_estimate,
    is_late::boolean            as is_late,
    is_early::boolean           as is_early,
    days_late::double           as days_late,
    delivery_bucket,
    estimate_label,
    performance_flag,
    purchase_month,
    month_name,
    month_sort::integer         as month_sort

from {{ source('apex', 'delivery') }}

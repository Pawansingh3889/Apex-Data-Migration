-- Mart: delivery performance aggregated by Brazilian state
-- Replaces the groupby in Phase 2 / Notebook 2 — now a versioned SQL artifact
select
    customer_state,
    state_name,
    region,
    count(*)                                                        as total_orders,
    round(avg(delivery_days), 1)                                    as avg_delivery_days,
    round(median(delivery_days), 1)                                 as median_delivery_days,
    round(avg(days_vs_estimate), 1)                                 as avg_days_vs_estimate,
    round(sum(is_late::integer) * 100.0 / count(*), 1)             as late_rate_pct,
    round(sum(is_early::integer) * 100.0 / count(*), 1)            as early_rate_pct,

    -- Performance tier breakdown per state
    round(sum(case when performance_flag = 'Fast'     then 1 else 0 end) * 100.0 / count(*), 1) as pct_fast,
    round(sum(case when performance_flag = 'Normal'   then 1 else 0 end) * 100.0 / count(*), 1) as pct_normal,
    round(sum(case when performance_flag = 'Slow'     then 1 else 0 end) * 100.0 / count(*), 1) as pct_slow,
    round(sum(case when performance_flag = 'Critical' then 1 else 0 end) * 100.0 / count(*), 1) as pct_critical

from {{ ref('stg_delivery_orders') }}
group by customer_state, state_name, region
having count(*) >= 50    -- exclude states with negligible volume
order by avg_delivery_days desc

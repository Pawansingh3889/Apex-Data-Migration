-- Mart: server health summary by risk level
-- Business logic lives here, not in staging
select
    cl.risk_level,
    count(*)                                                        as query_count,
    round(count(*) * 100.0 / sum(count(*)) over (), 2)             as pct_of_total,
    round(avg(cl.execution_time_ms), 1)                            as avg_exec_ms,
    round(max(cl.execution_time_ms), 1)                            as max_exec_ms,
    round(avg(cl.server_cpu_load), 1)                              as avg_cpu_pct,
    sum(case when cl.status = 'TIMEOUT' then 1 else 0 end)         as timeout_count,
    round(
        sum(case when cl.status = 'TIMEOUT' then 1 else 0 end)
        * 100.0 / count(*), 2
    )                                                               as timeout_rate_pct,
    sum(case when hour(cl.logged_at) between 18 and 22
        then 1 else 0 end)                                         as peak_hour_queries,
    sum(case when cl.target_table = 'orders'
        then 1 else 0 end)                                         as orders_table_queries

from {{ ref('stg_clean_logs') }} cl

group by cl.risk_level
order by
    case cl.risk_level
        when 'CRITICAL' then 1
        when 'WARNING'  then 2
        when 'OK'       then 3
    end
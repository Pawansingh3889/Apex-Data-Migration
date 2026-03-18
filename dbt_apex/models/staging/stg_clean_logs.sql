-- Staging: anomaly-enriched logs from Phase 2 (Isolation Forest output)
-- Types and renames only — business flags stay in the mart
select
    timestamp::timestamp        as logged_at,
    query_type,
    target_table,
    execution_time_ms::double   as execution_time_ms,
    server_cpu_load::double     as server_cpu_load,
    status,
    anomaly_score::double       as anomaly_score,
    is_anomaly::boolean         as is_anomaly,
    risk_level

from {{ source('apex', 'clean_logs') }}
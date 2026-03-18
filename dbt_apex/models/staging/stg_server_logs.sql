-- Staging: raw server logs from Phase 1
-- Types and renames only — no business logic
select
    timestamp::timestamp        as logged_at,
    query_type,
    target_table,
    execution_time_ms::double   as execution_time_ms,
    server_cpu_load::double     as server_cpu_load,
    status

from {{ source('apex', 'raw_logs') }}
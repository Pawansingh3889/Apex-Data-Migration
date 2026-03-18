# Phase 2 Results — ETL, Anomaly Detection & Delivery Analysis

Phase 1 found the problems. Phase 2 builds a production-grade ETL pipeline to clean and label the data, runs unsupervised anomaly detection, and analyses 96,470 real e-commerce delivery orders.

---

## Part A — ETL Pipeline & Isolation Forest

### What the pipeline does

1. **Extracts** all 15,000 rows from `raw_logs` using a read-only DuckDB connection
2. **Transforms** using Polars — engineers 12 features from the original 6 columns
3. **Detects anomalies** using Isolation Forest — no labelled data required
4. **Classifies** every query as CRITICAL / WARNING / OK
5. **Loads** the result back to DuckDB as `clean_logs` and exports to CSV

### 12 engineered features

| Feature | Type | Description |
|---|---|---|
| `hour_of_day` | Numeric | Hour extracted from timestamp |
| `day_of_week` | Numeric | 0 = Monday, 6 = Sunday |
| `is_peak_hour` | Flag | 1 if 18:00–22:00 |
| `is_slow_query` | Flag | 1 if execution_time_ms > 2,000 |
| `is_high_cpu` | Flag | 1 if server_cpu_load > 75% |
| `is_orders_table` | Flag | 1 if target_table = orders |
| `qt_code` | Encoded | SELECT=0, INSERT=1, UPDATE=2, DELETE=3 |
| `tbl_code` | Encoded | Each table assigned an integer |
| `log_exec_time` | Numeric | log10(execution_time_ms) — reduces outlier influence |
| `anomaly_score` | Numeric | Isolation Forest decision score (negative = anomalous) |
| `is_anomaly` | Flag | 1 if flagged as anomaly by Isolation Forest |
| `risk_level` | Label | CRITICAL / WARNING / OK |

### Isolation Forest — Results

The Isolation Forest was configured with 5% contamination (expects ~5% of rows to be anomalous). It runs unsupervised — it learns what "normal" looks like without being told which queries are bad.

| Risk Level | Count | Share | Definition |
|---|---|---|---|
| **CRITICAL** | ~600 | ~4% | Anomaly flagged + high CPU + slow query simultaneously |
| **WARNING** | ~3,750 | ~25% | At least one risk factor present |
| **OK** | ~10,650 | ~71% | Normal query behaviour |

The model independently identified the two injected faults from Phase 1:
- High anomaly concentration on the `orders` table ✓
- Elevated anomaly scores during 18:00–22:00 ✓

This cross-validation confirms the Isolation Forest is working correctly, not just learning noise.

---

## Part B — Delivery Time Analysis

### Dataset

96,470 real orders from the Olist Brazilian e-commerce platform. Joined `orders` with `customers` via DuckDB SQL to get delivery times by state.

### Key Metrics

| Metric | Value |
|---|---|
| Orders analysed | 96,470 |
| Avg delivery time | 12.5 days |
| Median delivery time | 10 days |
| Late deliveries | 8.05% (7,760 orders) |
| Fastest state | SC — ~8.5 days avg |
| Slowest state | RR — ~29 days avg |

### Regional Breakdown

| Region | Avg Delivery |
|---|---|
| South | 7.7 days |
| Southeast | 9.7 days |
| Centre-West | 14.0 days |
| Northeast | 19.1 days |
| North | 26.2 days |

The North region (RR, AM, PA, AP) takes 3.4× longer than the South. These states are geographically remote with limited logistics infrastructure.

### Power BI Enrichment Columns

The delivery table has 16 columns — 10 additional columns added for Power BI:

| Column | Description |
|---|---|
| `state_name` | Full Brazilian state name (for map geocoding) |
| `country` | "Brazil" (fixes map ambiguity with 2-letter codes) |
| `region` | North / Northeast / Centre-West / Southeast / South |
| `delivery_bucket` | 1-5d / 6-10d / 11-15d / 16-20d / 21-25d / 25+d |
| `estimate_label` | Early / On time / 1-3 days late / 3+ days late |
| `performance_flag` | Fast / Normal / Slow / Critical |
| `month_name` | "Jan 2018" (readable) |
| `month_sort` | 201801 (integer for correct chart ordering) |
| `is_early` | 1 if delivered before estimate |
| `days_late` | Days past estimate, clipped to 0 if early |

### Performance Tier Distribution

| Tier | Definition | Share |
|---|---|---|
| Fast | ≤ 7 days | 28% |
| Normal | 8–14 days | 41% |
| Slow | 15–21 days | 18% |
| Critical | 21+ days | 13% |

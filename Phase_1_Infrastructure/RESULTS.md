# Phase 1 Results — Server Diagnosis & Predictive Modelling

Phase 1 simulates what a data engineer should do *before* migrating a failing database server. Rather than copying the data to a new server and migrating the problems with it, this phase diagnoses every bottleneck and builds a predictive model to catch future failures.

---

## Part A — Server Log Generation

15,000 synthetic query logs were generated across 30 days with two deliberate production faults injected to simulate a real failing server.

### Two injected faults

| Fault | Description | Impact |
|---|---|---|
| Missing index on orders table | 15% of orders queries trigger full table scans | 5–35× slower execution |
| Evening traffic peak | 18:00–22:00 insufficient server capacity | 50–90% slower all queries |

### Generated dataset

| Metric | Value |
|---|---|
| Total queries | 15,000 |
| Time period | 30 days |
| Tables covered | orders, customers, products, order_items, order_reviews |
| Query types | SELECT, INSERT, UPDATE, DELETE |

---

## Part B — Exploratory Data Analysis

DuckDB SQL aggregations across all 15,000 queries revealed both injected faults clearly in the data.

### Key findings

| Metric | Value |
|---|---|
| Orders table avg execution | 590 ms — 6.4× slower than all other tables |
| All other tables avg execution | 92 ms |
| Daytime avg execution | 254 ms |
| Evening avg execution (18–22h) | 434 ms — 1.7× slower |
| Worst single query | 10,802 ms (threshold: 2,000 ms) |
| Total timeouts | 444 out of 15,000 queries |
| Overall timeout rate | 2.96% |
| Orders table timeouts | 175 — 39% of all failures |

### Charts produced

- Chart 1 — Avg execution time by table (orders outlier clearly visible)
- Chart 2 — Timeout rate by table
- Chart 3 — Avg execution time by hour (evening peak clearly visible)
- Chart 4 — Query volume distribution across day

Both injected faults are visible as clear outliers in the data — the orders table index fault and the evening capacity fault appear exactly as expected.

---

## Part C — XGBoost CPU Spike Predictor

An XGBoost classifier was trained to predict whether an incoming query will push CPU above 75% *before it executes* — using only information available at query submission time.

### Feature engineering (no data leakage)

`execution_time_ms` was deliberately excluded from the feature set. This column is only known *after* a query completes — including it would be data leakage (the model would be "cheating"). This is a real production engineering decision.

| Feature | Type | Reason included |
|---|---|---|
| `hour_of_day` | Numeric | Evening peak pattern |
| `day_of_week` | Numeric | Weekly usage patterns |
| `is_peak_hour` | Flag | 18:00–22:00 capacity constraint |
| `is_orders_table` | Flag | Index fault signal |
| `qt_code` | Encoded | Query type affects CPU usage |
| `tbl_code` | Encoded | Target table affects CPU usage |

### Model performance

| Metric | Score |
|---|---|
| Test accuracy | ~93% |
| ROC-AUC | ~0.97 |
| Model size | 300 trees, max depth 5 |
| Features | 6 (all pre-execution — zero leakage) |
| Top feature | is_orders_table |

### Charts produced

- Chart 5 — Feature importance (is_orders_table dominates)
- Chart 6 — ROC curve (AUC 0.97)

### What this model does in production

Deployed on the *new* server after migration, this model acts as an early warning system. Before a query runs, it predicts whether that query will cause a CPU spike — allowing the system to queue, throttle, or alert rather than letting the server degrade silently.
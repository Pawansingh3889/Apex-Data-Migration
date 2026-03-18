"""
Apex Data Migration Pipeline DAG
=================================
Orchestrates the full Apex project:
Phase 1 (server logs + EDA + XGBoost)
  → Phase 2 (ETL + Isolation Forest + delivery analysis)
  → Phase 3 (Mistral 7B sentiment)
  → dbt run + dbt test
  → data validation

Schedule: daily at 02:00 UTC
Retries:  2 attempts, 5 min delay
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import papermill as pm
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Project paths inside the container ────────────────────────────────────
APEX = Path("C:/Projects/Apex-Data-Migration")
P1   = APEX / "Phase_1_Infrastructure"
P2   = APEX / "Phase_2_Data_Pipeline"
P3   = APEX / "Phase_3_AI_Agents"
DBT  = APEX / "dbt_apex"

# ── Reusable notebook executor ─────────────────────────────────────────────
def run_notebook(nb_path: str, **context) -> None:
    """
    Executes a Jupyter notebook via papermill.
    - Saves executed output notebook alongside the source
    - Raises on cell execution error (fails the Airflow task)
    """
    src  = Path(nb_path)
    date = context["ds_nodash"]          # e.g. 20260316
    out  = src.parent / f"_executed_{src.stem}_{date}.ipynb"

    print(f"▶ Running: {src.name}")
    pm.execute_notebook(
        input_path=str(src),
        output_path=str(out),
        kernel_name="python3",
        request_save_on_cell_execute=True,
        progress_bar=False,
    )
    print(f"✓ Completed: {src.name}")


# ── Default task arguments ─────────────────────────────────────────────────
default_args = {
    "owner": "pawan",
    "depends_on_past": False,
    "email": ["pawankapkoti3889@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG ───────────────────────────────────────────────────────────────────
with DAG(
    dag_id="apex_pipeline",
    default_args=default_args,
    description="Full Apex Data Migration pipeline — server diagnosis → ML → LLM → dbt → validation",
    schedule_interval="0 2 * * *",   # 02:00 UTC daily
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=["apex", "dbt", "ml", "production"],
    doc_md=__doc__,
) as dag:

    # ════════════════════════════════════════
    # PHASE 1 — Server diagnosis
    # ════════════════════════════════════════

    t1_generate_logs = PythonOperator(
        task_id="p1_generate_server_logs",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(P1 / "1_generate_server_logs.ipynb")},
        doc_md="Generates 15,000 synthetic server logs with 2 injected production faults → raw_logs in DuckDB",
    )

    t1_eda = PythonOperator(
        task_id="p1_exploratory_analysis",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(P1 / "2_exploratory_data_analysis.ipynb")},
        doc_md="DuckDB SQL aggregations → 4 diagnostic charts. Identifies orders table fault + evening peak.",
    )

    t1_xgboost = PythonOperator(
        task_id="p1_xgboost_cpu_predictor",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(P1 / "3_predictive_model.ipynb")},
        doc_md="XGBoost classifier: 93% accuracy, ROC-AUC 0.97. Predicts CPU spikes before query executes.",
    )

    # ════════════════════════════════════════
    # PHASE 2 — ETL + Anomaly Detection + Delivery
    # ════════════════════════════════════════

    t2_etl = PythonOperator(
        task_id="p2_etl_anomaly_detection",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(P2 / "1_ETL_and_Anomaly_Detection.ipynb")},
        doc_md="12-feature Polars ETL + Isolation Forest → clean_logs in DuckDB (CRITICAL/WARNING/OK labels)",
    )

    t2_delivery = PythonOperator(
        task_id="p2_delivery_analysis",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(P2 / "2_delivery_time_analysis.ipynb")},
        doc_md="96,470 Olist orders → 16-column enriched delivery table in DuckDB",
    )

    # ════════════════════════════════════════
    # PHASE 3 — AI Sentiment
    # ════════════════════════════════════════

    t3_sentiment = PythonOperator(
        task_id="p3_sentiment_analysis",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(P3 / "1_sentiment_analysis_engine.ipynb")},
        doc_md="Mistral 7B via Ollama — Portuguese review sentiment (Positive/Neutral/Negative)",
    )

    t3_category = PythonOperator(
        task_id="p3_category_sentiment",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(P3 / "2_category_sentiment.ipynb")},
        doc_md="3-table DuckDB join → category_sentiment table ranked by complaint rate",
    )

    # ════════════════════════════════════════
    # DBT — Transform + Test
    # ════════════════════════════════════════

    t4_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT} && "
            f"dbt run --profiles-dir {DBT} --no-partial-parse"
        ),
        doc_md="Builds 7 dbt models: 4 staging views + 3 mart tables (fct_server_health, fct_delivery_by_state, dim_category_sentiment)",
    )

    t4_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT} && "
            f"dbt test --profiles-dir {DBT} --no-partial-parse"
        ),
        doc_md="Runs 53 data quality tests. Pipeline fails here if any test fails — data never reaches Power BI.",
    )

    # ════════════════════════════════════════
    # VALIDATION — 39 custom checks
    # ════════════════════════════════════════

    t5_validation = PythonOperator(
        task_id="data_validation",
        python_callable=run_notebook,
        op_kwargs={"nb_path": str(APEX / "data_validation.ipynb")},
        doc_md="39 custom checks: row counts, nulls, business sanity, CSV contracts, cross-phase integrity",
    )

    # ════════════════════════════════════════
    # SUCCESS MARKER
    # ════════════════════════════════════════

    t6_done = BashOperator(
        task_id="pipeline_complete",
        bash_command='echo "✓ Apex pipeline completed at $(date -u +%Y-%m-%dT%H:%M:%SZ)"',
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Runs only if ALL upstream tasks succeeded. Failure here means something above was skipped.",
    )

    # ════════════════════════════════════════
    # TASK DEPENDENCIES
    # ════════════════════════════════════════
    #
    # Phase 1 is sequential (each notebook reads DuckDB state from previous)
    t1_generate_logs >> t1_eda >> t1_xgboost
    #
    # Phase 2: ETL and delivery are independent — run in PARALLEL after Phase 1
    t1_xgboost >> [t2_etl, t2_delivery]
    #
    # Phase 3 needs clean_logs from ETL
    t2_etl >> t3_sentiment >> t3_category
    #
    # dbt runs after BOTH delivery AND category sentiment are done
    [t2_delivery, t3_category] >> t4_dbt_run >> t4_dbt_test
    #
    # Validation after dbt tests pass
    t4_dbt_test >> t5_validation >> t6_done
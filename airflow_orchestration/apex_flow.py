"""
Apex Data Migration Pipeline — Prefect Orchestration
"""

from __future__ import annotations

import os
import subprocess
from datetime import timedelta
from pathlib import Path

import papermill as pm
from prefect import flow, task, get_run_logger

import os
from pathlib import Path

if os.path.exists("/apex"):
    APEX = Path("/apex")
else:
    APEX = Path("C:/Projects/Apex-Data-Migration")

P1 = APEX / "Phase_1_Infrastructure"
P2 = APEX / "Phase_2_Data_Pipeline"
P3 = APEX / "Phase_3_AI_Agents"
DBT = APEX / "dbt_apex"


@task(name="Run Notebook", retries=1, retry_delay_seconds=30)
def run_notebook(nb_path: str, task_name: str) -> str:
    logger = get_run_logger()
    src = Path(nb_path)
    out = src.parent / f"_executed_{src.stem}.ipynb"
    logger.info(f"▶ Starting: {task_name}")

    original_dir = os.getcwd()
    os.chdir(str(src.parent))
    try:
        pm.execute_notebook(
            input_path=str(src),
            output_path=str(out),
            kernel_name="python3",
            request_save_on_cell_execute=True,
            progress_bar=False,
        )
    finally:
        os.chdir(original_dir)

    logger.info(f"✓ Completed: {task_name}")
    return str(out)


@task(name="dbt Run", retries=1, retry_delay_seconds=30)
def dbt_run() -> str:
    logger = get_run_logger()
    logger.info("▶ Running dbt models...")
    result = subprocess.run(
        ["dbt", "run", "--no-partial-parse", "--profiles-dir", str(DBT)],
        cwd=str(DBT), capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt run failed:\n{result.stderr}")
    logger.info("✓ dbt run complete — 7 models built")
    return result.stdout


@task(name="dbt Test", retries=1, retry_delay_seconds=30)
def dbt_test() -> str:
    logger = get_run_logger()
    logger.info("▶ Running dbt tests...")
    result = subprocess.run(
        ["dbt", "test", "--no-partial-parse", "--profiles-dir", str(DBT)],
        cwd=str(DBT), capture_output=True, text=True,
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"dbt test failed:\n{result.stderr}")
    logger.info("✓ dbt test complete — 53 tests passed")
    return result.stdout


@flow(name="apex-pipeline", log_prints=True)
def apex_pipeline():
    logger = get_run_logger()
    logger.info("=" * 50)
    logger.info("APEX PIPELINE STARTING")
    logger.info("=" * 50)

    # Phase 1 — sequential
    logger.info("── PHASE 1: Server Diagnosis ──")
    p1_logs = run_notebook(str(P1 / "1_generate_server_logs.ipynb"), "P1: Generate server logs")
    p1_eda  = run_notebook(str(P1 / "2_exploratory_data_analysis.ipynb"), "P1: EDA", wait_for=[p1_logs])
    p1_xgb  = run_notebook(str(P1 / "3_predictive_model.ipynb"), "P1: XGBoost CPU predictor", wait_for=[p1_eda])

    # Phase 2 — parallel
    logger.info("── PHASE 2: ETL + Delivery (parallel) ──")
    p2_etl      = run_notebook(str(P2 / "1_ETL_and_Anomaly_Detection.ipynb"), "P2: ETL + Isolation Forest", wait_for=[p1_xgb])
    p2_delivery = run_notebook(str(P2 / "2_delivery_time_analysis.ipynb"), "P2: Delivery analysis", wait_for=[p1_xgb])

    # Phase 3 — after ETL
    logger.info("── PHASE 3: AI Sentiment ──")
    p3_sent = run_notebook(str(P3 / "1_sentiment_analysis_engine.ipynb"), "P3: Mistral 7B sentiment", wait_for=[p2_etl])
    p3_cat  = run_notebook(str(P3 / "2_category_sentiment.ipynb"), "P3: Category sentiment", wait_for=[p3_sent])

    # dbt — after BOTH parallel branches done
    logger.info("── DBT: Transform + Test ──")
    dbt_r = dbt_run(wait_for=[p2_delivery, p3_cat])
    dbt_t = dbt_test(wait_for=[dbt_r])

    # Validation — last
    logger.info("── VALIDATION: 39 custom checks ──")
    val = run_notebook(str(APEX / "data_validation.ipynb"), "Data validation — 39 checks", wait_for=[dbt_t])

    logger.info("=" * 50)
    logger.info("✓ APEX PIPELINE COMPLETE")
    logger.info("=" * 50)
    return str(val)


if __name__ == "__main__":
    apex_pipeline()
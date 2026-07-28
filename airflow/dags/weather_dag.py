# This is the production-orchestration reference implementation of the
# PulseFlow weather ETL pipeline (extract -> validate -> transform -> load),
# wired up as an hourly Airflow DAG. The live hosted version of this pipeline
# runs on GitHub Actions instead (.github/workflows/pipeline.yml, every 30
# minutes); both call into the same extract/fetch_weather.py and
# extract/transform_load.py logic.

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import duckdb
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

REPO_ROOT = Path(__file__).resolve().parents[2]
EXTRACT_DIR = REPO_ROOT / "extract"
if str(EXTRACT_DIR) not in sys.path:
    sys.path.insert(0, str(EXTRACT_DIR))

import fetch_weather  # noqa: E402
import transform_load  # noqa: E402

logger = logging.getLogger(__name__)


def run_extract(**context) -> None:
    """Fetch current + 24hr forecast for all cities and save raw JSON to disk."""
    fetch_weather.main()


def run_validate(**context) -> None:
    """Validate the latest raw JSON per city; fail the task if nothing is usable."""
    latest_files = transform_load.find_latest_raw_files()
    if not latest_files:
        raise ValueError("No raw weather files found to validate.")

    total_valid = 0
    total_failures = 0
    for city, path in sorted(latest_files.items()):
        data = json.loads(path.read_text())
        rows, failures = transform_load.flatten_city_data(city, data)
        total_valid += len(rows)
        total_failures += failures

    logger.info(
        "Validation summary: %d valid row(s), %d failure(s) across %d city file(s)",
        total_valid, total_failures, len(latest_files),
    )

    if total_valid == 0:
        raise ValueError("Validation failed: no valid rows found in any raw file.")


def run_transform(**context) -> list[dict]:
    """Flatten the latest raw JSON per city into tabular rows."""
    latest_files = transform_load.find_latest_raw_files()

    all_rows: list[dict] = []
    for city, path in sorted(latest_files.items()):
        data = json.loads(path.read_text())
        rows, _ = transform_load.flatten_city_data(city, data)
        all_rows.extend(rows)

    logger.info("Transformed %d row(s) total.", len(all_rows))
    return all_rows


def run_load(**context) -> None:
    """Upsert transformed rows into the weather_raw table in DuckDB."""
    ti = context["ti"]
    rows = ti.xcom_pull(task_ids="transform")

    if not rows:
        logger.warning("No rows to load.")
        return

    con = duckdb.connect(str(transform_load.DB_PATH))
    try:
        transform_load.ensure_table(con)
        transform_load.upsert_rows(con, rows)
    finally:
        con.close()

    logger.info("Loaded %d row(s) into %s", len(rows), transform_load.DB_PATH)


default_args = {
    "owner": "pulseflow",
    "retries": 1,
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="Extract, validate, transform, and load Open-Meteo weather data into DuckDB.",
    default_args=default_args,
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["pulseflow", "weather", "etl"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )

    validate = PythonOperator(
        task_id="validate",
        python_callable=run_validate,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    extract >> validate >> transform >> load

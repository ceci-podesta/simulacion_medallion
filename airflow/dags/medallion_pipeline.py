"""Airflow DAG que orquesta pandas + dbt siguiendo la arquitectura medallion."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from dbt.cli.main import dbtRunner

BASE_DIR = Path(__file__).resolve().parents[2]
RAW_CSV = BASE_DIR / "data" / "raw" / "raw_orders.csv"
SILVER_ROOT = BASE_DIR / "data" / "silver"
DBT_DIR = BASE_DIR / "dbt_medallion"
PROFILES_DIR = BASE_DIR
REPORTS_DIR = BASE_DIR / "reports"

DEFAULT_ARGS = {
    "owner": "cecilia",
    "depends_on_past": False,
}


def _ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_silver(**context) -> str:
    """Ejecuta el script de pandas con la fecha del DAG y devuelve el CSV Silver."""
    ds = context["ds"]
    output_dir = _ensure_dir(SILVER_ROOT / ds)
    script_path = BASE_DIR / "scripts" / "create_silver_orders.py"
    cmd = [
        sys.executable,
        str(script_path),
        "--execution-date",
        ds,
        "--raw-path",
        str(RAW_CSV),
        "--output-dir",
        str(output_dir),
    ]
    subprocess.run(cmd, check=True)
    csv_path = output_dir / f"orders_clean_{ds}.csv"
    context["ti"].xcom_push(key="silver_csv_path", value=str(csv_path))
    return str(csv_path)


def _invoke_dbt(subcommand: str, csv_path: str):
    runner = dbtRunner()
    args = [
        subcommand,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(PROFILES_DIR),
        "--vars",
        f"{{silver_path: '{csv_path}'}}",
    ]
    result = runner.invoke(args)
    if not result.success:
        raise RuntimeError(f"dbt {subcommand} failed")
    return result


def run_dbt_models(**context):
    csv_path = context["ti"].xcom_pull(task_ids="generate_silver", key="silver_csv_path")
    _invoke_dbt("run", csv_path)


def test_and_report(**context):
    ds = context["ds"]
    csv_path = context["ti"].xcom_pull(task_ids="generate_silver", key="silver_csv_path")
    result = _invoke_dbt("test", csv_path)

    report = {
        "run_date": ds,
        "csv_path": csv_path,
        "passed": result.success,
        "tests": [
            {
                "name": r.node.name,
                "status": r.status,
                "execution_time": getattr(r, "execution_time", None),
            }
            for r in result.result
        ],
    }
    _ensure_dir(REPORTS_DIR)
    report_path = REPORTS_DIR / f"dq_status_{ds}.json"
    report_path.write_text(json.dumps(report, indent=2))
    context["ti"].xcom_push(key="dq_report_path", value=str(report_path))


with DAG(
    dag_id="medallion_pipeline",
    schedule="@daily",
    start_date=datetime(2018, 1, 1),
    catchup=True,
    default_args=DEFAULT_ARGS,
    tags=["medallion", "dbt"],
) as dag:
    generate_silver_task = PythonOperator(
        task_id="generate_silver",
        python_callable=generate_silver,
    )

    dbt_run_task = PythonOperator(
        task_id="dbt_run",
        python_callable=run_dbt_models,
    )

    dbt_test_task = PythonOperator(
        task_id="dbt_test",
        python_callable=test_and_report,
    )

    generate_silver_task >> dbt_run_task >> dbt_test_task

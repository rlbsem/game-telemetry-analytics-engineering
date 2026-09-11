"""Daily receipt processing on a shared local volume, using Airflow 2.10.5."""

import os
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from orchestration.runtime import execute_step, stage_interval, write_failure


def _stage(**context):
    return stage_interval(
        landing=os.environ.get("TELEMETRY_LANDING", "/opt/airflow/telemetry/landing"),
        state=os.environ.get("TELEMETRY_STATE", "/opt/airflow/telemetry/state"),
        run_id=context["run_id"],
        interval_start=context["data_interval_start"],
        interval_end=context["data_interval_end"],
    )


def _execute(step, **context):
    execute_step(step, context["ti"].xcom_pull(task_ids="stage_receipt_interval"))


def _failure(context):
    write_failure(
        state=os.environ.get("TELEMETRY_STATE", "/opt/airflow/telemetry/state"), context=context
    )


with DAG(
    dag_id="game_telemetry_daily",
    description="Validate a closed receipt day, build a candidate, then publish tested analytics",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    end_date=datetime(2026, 1, 23, tzinfo=UTC),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "game-analytics-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
        "execution_timeout": timedelta(minutes=35),
        "on_failure_callback": _failure,
    },
    tags=["synthetic", "telemetry", "dbt"],
    doc_md="""
    One UTC receipt day per run. The sample has January 2026 history; bootstrap
    with a forward backfill starting January 1. A fresh state cannot start in
    the middle of history. Failed gates keep the previous generation visible.
    See docs/operations.md for replay and failure-recovery procedures.
    """,
) as dag:
    stage = PythonOperator(task_id="stage_receipt_interval", python_callable=_stage)
    validate = PythonOperator(task_id="validate_contract", python_callable=_execute, op_kwargs={"step": "validate"})
    load = PythonOperator(task_id="load_warehouse", python_callable=_execute, op_kwargs={"step": "load"})
    build = PythonOperator(task_id="dbt_build_and_test", python_callable=_execute, op_kwargs={"step": "build"})
    monitor = PythonOperator(task_id="monitor_launch_health", python_callable=_execute, op_kwargs={"step": "monitor"})
    publish = PythonOperator(task_id="publish_generation", python_callable=_execute, op_kwargs={"step": "publish"})
    stage >> validate >> load >> build >> monitor >> publish

"""Run with the pinned Airflow Python on Linux, independent of telemetry deps."""

from pathlib import Path

from airflow.models import DagBag


def main() -> None:
    bag = DagBag(dag_folder=str(Path(__file__).parent / "dags"), include_examples=False)
    assert not bag.import_errors, bag.import_errors

    # Use the DAG already parsed into DagBag instead of DagBag.get_dag(), which
    # consults Airflow's metadata database. CI only needs to validate the DAG
    # definition and dependency graph, so no metadata DB initialization is required.
    dag = bag.dags.get("game_telemetry_daily")
    assert dag is not None

    chain = [
        "stage_receipt_interval",
        "validate_contract",
        "load_warehouse",
        "dbt_build_and_test",
        "monitor_launch_health",
        "publish_generation",
    ]
    assert set(dag.task_ids) == set(chain)
    assert dag.max_active_runs == 1

    for index, task_id in enumerate(chain):
        task = dag.get_task(task_id)
        expected = {chain[index + 1]} if index + 1 < len(chain) else set()
        assert task.downstream_task_ids == expected
        assert task.trigger_rule == "all_success"
        assert callable(task.python_callable)
        assert task.on_failure_callback is not None
        assert task.retries == 2

    print("DagBag import and all-success publication dependency checks passed")


if __name__ == "__main__":
    main()

"""Executable failure-boundary tests; these do not pretend to run a scheduler."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from orchestration.runtime import execute_step, stage_interval, write_failure


def _landing(tmp_path):
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "expectations.csv").write_text(
        "receipt_date,title_id,expected_events,expected_sessions\n"
        "2026-01-01,atlas,1,1\n2026-01-02,atlas,1,1\n", encoding="utf-8"
    )
    (landing / "2026-01-01.jsonl").write_text(
        '{"received_at":"2026-01-01T04:00:00Z"}\n', encoding="utf-8"
    )
    return landing


def _stage(tmp_path, **kwargs):
    return Path(stage_interval(
        landing=tmp_path / "landing", state=tmp_path / "state", run_id="scheduled:run/1",
        interval_start="2026-01-01T00:00:00Z", interval_end="2026-01-02T00:00:00Z", **kwargs
    ))


def test_staging_is_immutable_and_retries_keep_candidate(tmp_path):
    landing = _landing(tmp_path)
    run = _stage(tmp_path)
    (run / "candidate.duckdb").write_bytes(b"candidate after load")
    (landing / "2026-01-01.jsonl").write_text("changed upstream", encoding="utf-8")
    assert _stage(tmp_path) == run
    assert (run / "candidate.duckdb").read_bytes() == b"candidate after load"
    assert "2026-01-02" not in (run / "input" / "expectations.csv").read_text()
    (run / "input" / "2026-01-01.jsonl").write_text("tampered", encoding="utf-8")
    with pytest.raises(ValueError, match="Immutable staged input changed"):
        _stage(tmp_path)


def test_receipt_upper_bound_is_exclusive(tmp_path):
    landing = _landing(tmp_path)
    (landing / "2026-01-01.jsonl").write_text(
        '{"received_at":"2026-01-02T00:00:00Z"}\n', encoding="utf-8"
    )
    with pytest.raises(ValueError, match="Receipt outside interval"):
        _stage(tmp_path)
    assert not list((tmp_path / "state" / "runs").iterdir())


def test_unmanifested_file_cannot_enter_warehouse(tmp_path):
    _landing(tmp_path)
    run = _stage(tmp_path)
    (run / "input" / "unexpected.jsonl").write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="input inventory changed"):
        execute_step("load", run)


def test_missing_delivery_preserves_nonzero_expectations(tmp_path):
    landing = _landing(tmp_path)
    (landing / "2026-01-01.jsonl").unlink()
    run = _stage(tmp_path)
    assert (run / "input" / "2026-01-01.jsonl").read_bytes() == b""
    assert "atlas,1,1" in (run / "input" / "expectations.csv").read_text()


def test_unknown_expectations_are_not_treated_as_zero(tmp_path):
    landing = _landing(tmp_path)
    (landing / "expectations.csv").write_text(
        "receipt_date,title_id,expected_events,expected_sessions\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="No independent delivery expectations"):
        _stage(tmp_path)


def test_history_gaps_and_backwards_runs_are_rejected(tmp_path):
    _landing(tmp_path)
    with pytest.raises(ValueError, match="Expected next receipt day"):
        stage_interval(
            landing=tmp_path / "landing", state=tmp_path / "state", run_id="gap",
            interval_start="2026-01-02T00:00:00Z", interval_end="2026-01-03T00:00:00Z",
        )
    published = tmp_path / "state" / "published"
    published.mkdir(parents=True)
    (published / "current.json").write_text('{"as_of":"2026-01-03T00:00:00Z"}')
    with pytest.raises(ValueError, match="Expected next receipt day"):
        _stage(tmp_path)


def test_failure_propagates_and_writes_log(tmp_path, monkeypatch):
    _landing(tmp_path)
    run = _stage(tmp_path)

    def failed_command(args, **kwargs):
        assert args[-2:] == ["--input", str(run / "input")]
        assert isinstance(args, list)
        assert "shell" not in kwargs
        kwargs["stdout"].write("contract rejected\n")
        return SimpleNamespace(returncode=2)

    monkeypatch.setattr("orchestration.runtime.subprocess.run", failed_command)
    with pytest.raises(RuntimeError, match="validate failed with exit code 2"):
        execute_step("validate", run)
    assert "contract rejected" in (run / "validate.log").read_text()
    assert not (tmp_path / "state" / "published" / "current.json").exists()


def test_publish_rejects_stale_candidate_and_recovers_lost_ack(tmp_path, monkeypatch):
    _landing(tmp_path)
    run = _stage(tmp_path)
    (run / "candidate.duckdb").write_bytes(b"valid candidate")
    published = tmp_path / "state" / "published"
    generation = published / "generations" / "test"
    generation.mkdir(parents=True)
    published_db = generation / "warehouse.duckdb"
    published_db.write_bytes(b"other candidate")
    pointer = {"as_of": "2026-01-02T00:00:00Z", "database": "generations/test/warehouse.duckdb"}
    (published / "current.json").write_text(json.dumps(pointer))
    with pytest.raises(ValueError, match="Published generation changed"):
        execute_step("publish", run)

    def unexpected_command(*args, **kwargs):
        pytest.fail("Acknowledgement recovery must not republish")

    monkeypatch.setattr("orchestration.runtime.subprocess.run", unexpected_command)
    published_db.write_bytes(b"valid candidate")
    execute_step("publish", run)
    assert json.loads((run / "publication.json").read_text()) == pointer


def test_failure_receipt_contains_operator_context(tmp_path):
    path = Path(write_failure(state=tmp_path, context={
        "run_id": "run-1", "task_instance": SimpleNamespace(task_id="dbt_build_and_test", try_number=3),
        "exception": RuntimeError("relationship test failed"),
    }))
    receipt = json.loads(path.read_text())
    assert receipt["task_id"] == "dbt_build_and_test"
    assert receipt["try_number"] == 3
    assert "relationship test failed" in receipt["exception"]

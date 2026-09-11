"""Execute Airflow's task functions against real CLI subprocesses over two intervals."""

import json
from pathlib import Path

import duckdb
import pytest

from orchestration.runtime import execute_step, stage_interval


@pytest.mark.integration
def test_two_receipt_intervals_publish_a_late_terminal_without_losing_history(tmp_path):
    landing = tmp_path / "landing"; landing.mkdir()
    base = dict(title_id="atlas", player_id="P1", session_id="S1", build_id="1.0.0", platform="console_current", country_code="CA", schema_version=1, metadata={"instrumentation_revision": "rev1"})
    start = dict(base, event_id="E0", event_name="session_start", event_sequence=0, event_ts="2026-01-01T12:00:00Z", received_at="2026-01-01T12:00:01Z")
    end = dict(base, event_id="E1", event_name="session_end", event_sequence=1, play_time_ms=30000, event_ts="2026-01-01T12:01:00Z", received_at="2026-01-02T03:00:00Z")
    (landing / "2026-01-01.jsonl").write_text(json.dumps(start) + "\n"); (landing / "2026-01-02.jsonl").write_text(json.dumps(end) + "\n")
    (landing / "expectations.csv").write_text("receipt_date,title_id,expected_events,expected_sessions\n2026-01-01,atlas,1,1\n2026-01-02,atlas,1,1\n")
    state = tmp_path / "state"; first_pointer = None
    for day in (1, 2):
        run = Path(stage_interval(landing=landing, state=state, run_id=f"test-day-{day}", interval_start=f"2026-01-0{day}T00:00:00Z", interval_end=f"2026-01-0{day + 1}T00:00:00Z"))
        for step in ("validate", "load", "build", "monitor", "publish"): execute_step(step, run)
        pointer = json.loads((state / "published/current.json").read_text())
        with duckdb.connect(str(state / "published" / pointer["database"]), read_only=True) as con:
            assert con.execute("select is_partial from analytics.fct_sessions").fetchone()[0] == (day == 1)
            assert con.execute("select count(*) from raw.events").fetchone()[0] == day
        if day == 1: first_pointer = pointer
        else:
            assert pointer["generation"] != first_pointer["generation"]
            assert (state / "published" / first_pointer["database"]).exists()
        execute_step("publish", run)

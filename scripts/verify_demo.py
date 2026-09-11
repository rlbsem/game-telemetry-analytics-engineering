"""Executable release evidence: full sample signals and replay equivalence."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import time
from pathlib import Path

import duckdb

from telemetry.pipeline import DEFAULT_AS_OF, DEFAULT_COMPLETE_THROUGH, atomic_json, demo, replay


def mart_hashes(database: Path) -> dict:
    result = {}
    with duckdb.connect(str(database), read_only=True) as con:
        tables = [r[0] for r in con.execute("select table_name from information_schema.tables where table_schema='analytics' and table_name like 'mart_%' order by 1").fetchall()]
        for name in tables:
            rows = con.execute(f"select * from analytics.{name} order by all").fetchall()
            result[name] = hashlib.sha256(json.dumps(rows, default=str).encode()).hexdigest()
    return result


def verify(workspace: Path) -> dict:
    workspace = workspace.resolve()
    started = time.perf_counter()
    pointer = demo(workspace / "demo")
    published = workspace / "demo/published"
    report = json.loads((published / pointer["report"]).read_text())
    codes = {(a["code"], a["title_id"]) for a in report["alerts"]}
    expected = {("crash_rate_regression", "atlas"), ("progression_bottleneck", "atlas"),
                ("delivery_volume_loss", "atlas"), ("heartbeat_instrumentation_loss", "atlas")}
    assert expected <= codes, (expected, codes)
    assert ("crash_rate_regression", "echo") not in codes
    health = {(r["title_id"], r["build_id"]): r for r in report["build_health"]}
    assert health["atlas", "1.1.0"]["crash_rate"] > 3 * health["atlas", "1.0.0"]["crash_rate"]
    database = published / pointer["database"]
    with duckdb.connect(str(database), read_only=True) as con:
        progression = con.execute("select completion_rate,death_per_attempt from analytics.mart_progression where title_id='atlas' and build_id='1.1.0' and mission_id='M02'").fetchone()
        assert progression[0] < 0.5 and progression[1] >= 4
        assert con.execute("select bool_and(has_delivery_gap) from analytics.mart_retention where title_id='atlas'").fetchone()[0]
        assert not con.execute("select bool_or(has_delivery_gap) from analytics.mart_retention where title_id='echo'").fetchone()[0]
        rates = con.execute("""select cohort_build_id,sum(d1_players)::double/sum(cohort_players)
            from analytics.mart_retention where title_id='echo' and d1_is_mature
            group by 1 order by 1""").fetchall()
        assert rates[0][1] > rates[1][1], rates
        assert con.execute("select count(*) from analytics.fct_sessions where is_partial").fetchone()[0] > 0
        assert con.execute("select count(*) from raw.events where received_at-event_ts > interval 24 hour").fetchone()[0] > 0
    before = mart_hashes(database)
    replay_pointer = replay(workspace / "demo/landing", workspace / "replay",
                            as_of=DEFAULT_AS_OF, complete_through=DEFAULT_COMPLETE_THROUGH)
    after = mart_hashes(workspace / "replay/published" / replay_pointer["database"])
    assert before == after, "analyst-facing marts differ after clean historical replay"
    evidence = dict(status="passed", python=platform.python_version(), operating_system=platform.system(),
                    duckdb=duckdb.__version__, elapsed_seconds=round(time.perf_counter() - started, 2),
                    summary=report["summary"], dbt_models=pointer["model_count"],
                    dbt_tests=pointer["test_count"], expected_alerts=sorted(expected),
                    clean_replay_equal=True, mart_sha256=before,
                    limits="Executed Python/dbt/DuckDB locally; this does not assert AWS deployment or an Airflow scheduler run.")
    atomic_json(workspace / "verification.json", evidence)
    return evidence


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True, help="fresh directory for the verification run")
    print(json.dumps(verify(parser.parse_args().workspace), indent=2))

"""Hand-computable warehouse cases; exercise actual dbt, not SQL-shaped mocks."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pytest

from telemetry.ingest import load_file, load_path
from telemetry.pipeline import build, publish, validation_receipt

pytestmark = pytest.mark.integration


def event(session, player, when, sequence, name, *, title="atlas", received=None, **extra):
    version = 1 if when < "2026-01-11" else 2
    result = dict(event_id=f"{session}-{sequence}", title_id=title, player_id=player,
                  session_id=session, build_id="1.0.0" if version == 1 else "1.1.0",
                  platform="console_current", country_code="CA", event_name=name,
                  schema_version=version, event_ts=when, received_at=received or when,
                  event_sequence=sequence, metadata={"instrumentation_revision": f"rev{version}"})
    if name in {"session_end", "heartbeat", "crash"}:
        value = extra.pop("seconds", 60)
        result["play_time_ms" if version == 1 else "active_seconds"] = value * 1000 if version == 1 else value
    result.update(extra)
    return result


def session(session_id, player, when, *, title="atlas"):
    start = datetime.fromisoformat(when).replace(tzinfo=UTC)
    return [event(session_id, player, start.isoformat(), 0, "session_start", title=title),
            event(session_id, player, (start + timedelta(minutes=2)).isoformat(), 1, "session_end", title=title)]


def batch(path: Path, events):
    path.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
    return path


def business_snapshot(database):
    with duckdb.connect(str(database), read_only=True) as con:
        names = [r[0] for r in con.execute("select table_name from information_schema.tables where table_schema='analytics' and table_name not in ('stg_events','fct_events','int_event_activity') and table_name not like '%__dbt_%' order by 1").fetchall()]
        result = {}
        for name in names:
            columns = [r[0] for r in con.execute(f"describe analytics.{name}").fetchall() if r[0] != "source_ingest_seq"]
            result[name] = con.execute(f"select {','.join(columns)} from analytics.{name} order by all").fetchall()
        return result


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    root = tmp_path_factory.mktemp("warehouse")
    base = [
        event("overnight", "P1", "2026-01-01T23:55:00Z", 0, "session_start"),
        event("overnight", "P1", "2026-01-02T00:05:00Z", 1, "heartbeat", seconds=240),
        event("overnight", "P1", "2026-01-02T00:06:00Z", 2, "mission_start", mission_id="M01", attempt_id="A1"),
        event("overnight", "P1", "2026-01-02T00:07:00Z", 3, "mission_complete", mission_id="M01", attempt_id="A1"),
        event("overnight", "P1", "2026-01-02T00:08:00Z", 4, "mission_start", mission_id="M01", attempt_id="A2"),
        event("overnight", "P1", "2026-01-02T00:09:00Z", 5, "player_death", mission_id="M01", attempt_id="A2"),
        *session("return1", "P1", "2026-01-02T18:00:00"),
        *session("return7", "P1", "2026-01-08T14:00:00"),
        *session("recent", "P2", "2026-01-15T14:00:00"),
        *session("first-observed", "P3", "2026-01-11T14:00:00"),
        *session("overnight", "P1", "2026-01-01T18:00:00", title="echo"),
    ]
    late = [event("overnight", "P1", "2026-01-02T00:10:00Z", 6, "session_end", seconds=600,
                  received="2026-01-20T00:00:00Z"),
            *session("earlier", "P3", "2026-01-03T14:00:00")]
    for e in late[1:]:
        e["received_at"] = "2026-01-20T00:00:00Z"
    first = batch(root / "first.jsonl", list(reversed(base)))
    delayed = batch(root / "late.jsonl", late)
    database = root / "warehouse.duckdb"
    load_file(first, database)
    build(database, generate_docs=False)
    return root, database, first, delayed


def test_partial_sessions_and_hand_computed_metrics(warehouse):
    _, database, _, _ = warehouse
    with duckdb.connect(str(database), read_only=True) as con:
        row = con.execute("select is_partial,session_terminal_at,session_duration_seconds,active_seconds from analytics.fct_sessions where title_id='atlas' and session_id='overnight'").fetchone()
        assert row == (True, None, None, 240)
        assert con.execute("select dau,sessions_started from analytics.mart_daily_title where title_id='atlas' and activity_date='2026-01-02'").fetchone() == (1, 1)
        assert con.execute("select started_attempts,completed_attempts,deaths,completion_rate from analytics.mart_progression where title_id='atlas' and build_id='1.0.0' and mission_id='M01'").fetchone() == (2, 1, 1, 0.5)
        assert con.execute("select d1_retention,d7_retention from analytics.mart_retention where title_id='atlas' and cohort_date='2026-01-01'").fetchone() == (1.0, 1.0)
        assert con.execute("select d7_is_mature,d7_players,d7_retention from analytics.mart_retention where title_id='atlas' and cohort_date='2026-01-15'").fetchone() == (False, None, None)
        assert con.execute("select count(*) from analytics.dim_player").fetchone()[0] == 4


def test_incremental_late_events_repair_old_sessions_and_cohorts_then_match_replay(warehouse, tmp_path):
    _, original, first, delayed = warehouse
    root = tmp_path; database = root / original.name; shutil.copyfile(original, database)
    load_file(delayed, database); build(database, generate_docs=False)
    with duckdb.connect(str(database), read_only=True) as con:
        assert con.execute("select is_complete,active_seconds,session_duration_seconds from analytics.fct_sessions where title_id='atlas' and session_id='overnight'").fetchone() == (True, 600, 900)
        assert con.execute("select cast(cohort_date as varchar),cohort_build_id from analytics.dim_player where title_id='atlas' and player_id='P3'").fetchone() == ("2026-01-03", "1.0.0")
    incremental = business_snapshot(database)
    load_file(delayed, database); build(database, generate_docs=False); assert business_snapshot(database) == incremental
    build(database, full_refresh=True, generate_docs=False); assert business_snapshot(database) == incremental
    rebuilt = root / "replayed.duckdb"; load_file(delayed, rebuilt); load_file(first, rebuilt); build(rebuilt, full_refresh=True, generate_docs=False)
    assert business_snapshot(rebuilt) == incremental


def test_publication_is_idempotent_and_refuses_stale_validation(warehouse, tmp_path):
    _, original, _, _ = warehouse; root = tmp_path; database = root / original.name; shutil.copyfile(original, database)
    first = publish(database, root / "published"); assert publish(database, root / "published") == first
    pointer = (root / "published/current.json").read_bytes()
    with duckdb.connect(str(database)) as con: con.execute("update analytics.mart_build_health set sessions=sessions+100 where title_id='atlas'")
    with pytest.raises(ValueError, match="changed after validation"): publish(database, root / "published")
    assert (root / "published/current.json").read_bytes() == pointer
    build(database, generate_docs=False)


@pytest.mark.parametrize("case", ["session_build_change", "duplicate_sequence", "time_inversion", "decreasing_counter", "orphan_completion", "schema_build_mismatch", "instrumentation_revision"])
def test_semantic_violations_block_dbt_and_publication(tmp_path, case):
    events = session("broken", "P1", "2026-01-11T12:00:00")
    if case == "session_build_change": events[0].update(build_id="1.0.0", schema_version=1, metadata={"instrumentation_revision": "rev1"})
    elif case == "duplicate_sequence": events[1]["event_sequence"] = 0
    elif case == "time_inversion": events[1]["event_ts"] = "2026-01-11T11:59:00Z"
    elif case == "decreasing_counter":
        events[1]["event_sequence"] = 2; events[1]["event_id"] = "broken-2"; events.insert(1, event("broken", "P1", "2026-01-11T12:01:00Z", 1, "heartbeat", seconds=100))
    elif case == "orphan_completion":
        events[1]["event_sequence"] = 2; events[1]["event_id"] = "broken-2"; events.insert(1, event("broken", "P1", "2026-01-11T12:01:00Z", 1, "mission_complete", mission_id="M01", attempt_id="A1"))
    elif case == "schema_build_mismatch":
        events[1]["schema_version"] = 1; events[1]["play_time_ms"] = events[1].pop("active_seconds") * 1000
    else: events[1]["metadata"]["instrumentation_revision"] = "unexpected"
    database = tmp_path / "invalid.duckdb"; load_file(batch(tmp_path / "invalid.jsonl", events), database)
    with pytest.raises(RuntimeError, match="dbt build failed"): build(database, generate_docs=False)
    with pytest.raises(ValueError, match="no successful"): validation_receipt(database)


def test_zero_delivery_manifest_day_survives_and_raises_alert(tmp_path):
    from telemetry.monitor import monitor
    (tmp_path / "empty.jsonl").write_text(""); (tmp_path / "expectations.csv").write_text("receipt_date,title_id,expected_events,expected_sessions\n2026-01-01,atlas,100,10\n")
    database = tmp_path / "empty.duckdb"; load_path(tmp_path, database); build(database, generate_docs=False); report = monitor(database, tmp_path / "report")
    assert any(a["code"] == "delivery_volume_loss" and a["received_events"] == 0 for a in report["alerts"])


def test_in_place_raw_change_requires_full_refresh(warehouse, tmp_path):
    _, original, _, _ = warehouse; database = tmp_path / original.name; shutil.copyfile(original, database)
    with duckdb.connect(str(database)) as con: con.execute("update raw.events set active_seconds=61 where session_id='return1' and event_name='session_end'")
    with pytest.raises(RuntimeError, match="dbt build failed"): build(database, generate_docs=False)
    log = (tmp_path / "warehouse-dbt/build.log").read_text(encoding="utf-8"); assert "assert_raw_fidelity" in log
    build(database, full_refresh=True, generate_docs=False)
    with duckdb.connect(str(database), read_only=True) as con: assert con.execute("select active_seconds from analytics.fct_sessions where session_id='return1'").fetchone()[0] == 61


def test_pending_attempt_becomes_valid_after_missing_start_arrives(tmp_path):
    rows = session("pending", "P1", "2026-01-22T12:00:00"); rows[1].update(event_id="pending-3", event_sequence=3)
    rows.insert(1, event("pending", "P1", "2026-01-22T12:01:00Z", 2, "mission_complete", mission_id="M01", attempt_id="A1"))
    database = tmp_path / "pending.duckdb"; load_file(batch(tmp_path / "first.jsonl", rows), database); build(database, generate_docs=False)
    with duckdb.connect(str(database), read_only=True) as con: assert con.execute("select pending_attempts,completed_attempts,completion_rate from analytics.mart_progression where title_id='atlas' and build_id='1.1.0' and mission_id='M01'").fetchone() == (1, 0, None)
    late = event("pending", "P1", "2026-01-22T12:00:30Z", 1, "mission_start", mission_id="M01", attempt_id="A1", received="2026-01-22T23:00:00Z")
    load_file(batch(tmp_path / "late.jsonl", [late]), database); build(database, generate_docs=False)
    with duckdb.connect(str(database), read_only=True) as con: assert con.execute("select pending_attempts,completed_attempts,completion_rate from analytics.mart_progression where title_id='atlas' and build_id='1.1.0' and mission_id='M01'").fetchone() == (0, 1, 1.0)

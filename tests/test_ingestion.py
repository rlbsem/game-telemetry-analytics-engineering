"""Real DuckDB transactions: replay, duplicate delivery, and quarantine boundaries."""

import json

import duckdb
import pytest

from telemetry.ingest import BatchRejected, initialize, load_expectations, load_file


def event(event_id="event-001", **overrides):
    payload = {"event_id": event_id, "title_id": "atlas", "player_id": "player-001", "session_id": "session-001", "build_id": "1.1.0", "platform": "console_current", "country_code": "CA", "event_name": "heartbeat", "schema_version": 2, "event_ts": "2026-01-12T12:01:00Z", "received_at": "2026-01-12T12:01:05Z", "event_sequence": 10, "active_seconds": 45.5, "metadata": {"mode": "campaign"}}
    payload.update(overrides); return payload


def landing(tmp_path, name, *events):
    path = tmp_path / name; path.write_text("".join(json.dumps(row) + "\n" for row in events), encoding="utf-8"); return path


def rows(database, query):
    with duckdb.connect(str(database), read_only=True) as connection: return connection.execute(query).fetchall()


def test_initialize_is_repeatable(tmp_path):
    database = tmp_path / "warehouse.duckdb"; initialize(database); initialize(database)
    assert rows(database, "select count(*) from raw.events") == [(0,)]
    assert rows(database, "select count(*) from raw.batches") == [(0,)]


def test_identical_file_replay_has_one_physical_event_and_one_batch(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = landing(tmp_path, "day01.jsonl", event())
    first = load_file(path, database); second = load_file(path, database)
    assert isinstance(first, dict) and isinstance(second, dict)
    assert rows(database, "select count(*) from raw.events") == [(1,)]
    assert rows(database, "select count(*) from raw.batches") == [(1,)]


def test_batch_identity_is_content_based_not_filename_based(tmp_path):
    database = tmp_path / "warehouse.duckdb"; first = landing(tmp_path, "day01.jsonl", event()); second = landing(tmp_path, "copied-day01.jsonl", event())
    load_file(first, database); load_file(second, database)
    assert rows(database, "select count(*) from raw.batches") == [(1,)]


def test_retry_delivery_with_new_receipt_time_deduplicates_and_is_audited(tmp_path):
    database = tmp_path / "warehouse.duckdb"; first = landing(tmp_path, "first.jsonl", event()); retry = landing(tmp_path, "retry.jsonl", event(received_at="2026-01-13T00:00:00Z"))
    load_file(first, database); load_file(retry, database)
    assert rows(database, "select count(*) from raw.events") == [(1,)]
    assert rows(database, "select sum(accepted_count), sum(duplicate_count) from raw.batches") == [(1, 1)]


def test_duplicates_inside_one_file_do_not_inflate_accepted_events(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = landing(tmp_path, "duplicates.jsonl", event(), event(), event()); load_file(path, database)
    assert rows(database, "select count(*) from raw.events") == [(1,)]
    assert rows(database, "select row_count, accepted_count, duplicate_count from raw.batches") == [(3, 1, 2)]


@pytest.mark.parametrize("conflict_inside_batch", [False, True])
def test_conflicting_event_identity_rejects_entire_batch(tmp_path, conflict_inside_batch):
    database = tmp_path / "warehouse.duckdb"; initialize(database)
    if not conflict_inside_batch: load_file(landing(tmp_path, "original.jsonl", event()), database)
    before = rows(database, "select event_id, payload_hash, ingest_seq from raw.events order by ingest_seq")
    pending = [event("new-valid-event", event_sequence=11)]
    if conflict_inside_batch: pending.append(event())
    pending.append(event(active_seconds=999))
    with pytest.raises(BatchRejected) as raised: load_file(landing(tmp_path, "conflict.jsonl", *pending), database)
    assert raised.value.report["status"] == "rejected"
    assert rows(database, "select event_id, payload_hash, ingest_seq from raw.events order by ingest_seq") == before
    assert rows(database, "select count(*) from raw.batches where status = 'rejected'") == [(1,)]


def test_one_malformed_row_quarantines_valid_rows_and_repeated_attempt_still_fails(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = landing(tmp_path, "invalid.jsonl", event(), event("invalid", schema_version=99))
    for _ in range(2):
        with pytest.raises(BatchRejected): load_file(path, database)
    assert rows(database, "select count(*) from raw.events") == [(0,)]
    assert rows(database, "select status, accepted_count, invalid_count from raw.batches") == [("rejected", 0, 1)]


def test_broken_json_is_reported_as_a_batch_failure_with_no_partial_load(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = landing(tmp_path, "broken.jsonl", event())
    with path.open("a", encoding="utf-8") as stream: stream.write('{"event_id": "unfinished"\n')
    with pytest.raises(BatchRejected): load_file(path, database)
    assert rows(database, "select count(*) from raw.events") == [(0,)]
    assert rows(database, "select status, invalid_count from raw.batches") == [("rejected", 1)]


def test_event_ids_are_namespaced_by_title(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = landing(tmp_path, "two-titles.jsonl", event(), event(title_id="echo")); load_file(path, database)
    assert rows(database, "select title_id, event_id from raw.events order by title_id") == [("atlas", "event-001"), ("echo", "event-001")]


def test_out_of_order_delivery_preserves_event_time_and_monotonic_ingest_watermark(tmp_path):
    database = tmp_path / "warehouse.duckdb"; later = landing(tmp_path, "first-delivered.jsonl", event("later", event_sequence=20)); earlier = landing(tmp_path, "late-arrival.jsonl", event("earlier", event_ts="2026-01-12T12:00:00Z", event_sequence=2, active_seconds=5, received_at="2026-01-20T00:00:00Z"))
    load_file(later, database); load_file(earlier, database)
    accepted = rows(database, "select event_id, ingest_seq from raw.events order by ingest_seq")
    assert [row[0] for row in accepted] == ["later", "earlier"] and accepted[1][1] > accepted[0][1]
    assert rows(database, "select event_id from raw.events order by event_ts") == [("earlier",), ("later",)]


def test_rejected_batch_does_not_advance_accepted_high_watermark(tmp_path):
    database = tmp_path / "warehouse.duckdb"; load_file(landing(tmp_path, "first.jsonl", event()), database); watermark = rows(database, "select max(ingest_seq) from raw.events")[0][0]
    with pytest.raises(BatchRejected): load_file(landing(tmp_path, "bad.jsonl", event("pending"), event(active_seconds=999)), database)
    assert rows(database, "select max(ingest_seq) from raw.events")[0][0] == watermark
    load_file(landing(tmp_path, "next.jsonl", event("next")), database)
    assert rows(database, "select max(ingest_seq) from raw.events")[0][0] == watermark + 1


def test_duplicate_json_object_keys_are_rejected_instead_of_last_key_winning(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = tmp_path / "ambiguous.jsonl"; body = json.dumps(event()); path.write_text('{"schema_version": 99,' + body[1:] + "\n", encoding="utf-8")
    with pytest.raises(BatchRejected): load_file(path, database)
    assert rows(database, "select count(*) from raw.events") == [(0,)]


def test_empty_receipt_file_is_valid_and_audited_for_zero_delivery_day(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = landing(tmp_path, "zero-delivery.jsonl"); load_file(path, database)
    assert rows(database, "select count(*) from raw.events") == [(0,)]
    assert rows(database, "select status, row_count, accepted_count from raw.batches") == [("loaded", 0, 0)]


def manifest(tmp_path, name, content):
    path = tmp_path / name; path.write_text("receipt_date,title_id,expected_events,expected_sessions\n" + content, encoding="utf-8"); return path


def test_independent_delivery_manifest_replay_is_idempotent(tmp_path):
    database = tmp_path / "warehouse.duckdb"; path = manifest(tmp_path, "expected.csv", "2026-01-12,atlas,100,8\n"); load_expectations(path, database); load_expectations(path, database)
    assert rows(database, "select expected_events,expected_sessions from raw.delivery_expectations") == [(100, 8)]


def test_changed_delivery_manifest_is_rejected_and_preserves_prior_counts(tmp_path):
    database = tmp_path / "warehouse.duckdb"; load_expectations(manifest(tmp_path, "first.csv", "2026-01-12,atlas,100,8\n"), database); changed = manifest(tmp_path, "changed.csv", "2026-01-12,atlas,80,8\n")
    with pytest.raises(ValueError, match="manifest changed"): load_expectations(changed, database)
    assert rows(database, "select expected_events,expected_sessions from raw.delivery_expectations") == [(100, 8)]


@pytest.mark.parametrize("bad_counts", ["-1,0", "10,11", "10,-1"])
def test_invalid_manifest_rolls_back_preceding_new_rows(tmp_path, bad_counts):
    database = tmp_path / "warehouse.duckdb"; path = manifest(tmp_path, "invalid.csv", "2026-01-12,atlas,100,8\n" + f"2026-01-13,atlas,{bad_counts}\n")
    with pytest.raises(ValueError): load_expectations(path, database)
    assert rows(database, "select count(*) from raw.delivery_expectations") == [(0,)]

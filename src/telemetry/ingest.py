"""Content-addressed batch admission with an immutable event identity registry.

The local adapter deliberately loads bounded receipt-day files in memory. At production
volume replace this adapter with partitioned staging/COPY, keeping the same invariants.
"""

from __future__ import annotations

import csv
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from telemetry.contract import ContractError, normalize_event

COLUMNS = (
    "event_id", "title_id", "player_id", "session_id", "build_id", "platform", "country_code",
    "event_name", "schema_version", "event_ts", "received_at", "event_sequence", "mission_id",
    "attempt_id", "checkpoint_id", "active_seconds", "error_code", "metadata_json", "payload_hash",
    "batch_id", "ingest_seq", "source_line",
)

DDL = """
create schema if not exists raw;
create schema if not exists ops;
create table if not exists raw.events (
 event_id varchar not null, title_id varchar not null, player_id varchar not null,
 session_id varchar not null, build_id varchar not null, platform varchar not null,
 country_code varchar not null, event_name varchar not null, schema_version integer not null,
 event_ts timestamp not null, received_at timestamp not null, event_sequence bigint not null,
 mission_id varchar, attempt_id varchar, checkpoint_id varchar, active_seconds double,
 error_code varchar, metadata_json varchar not null, payload_hash varchar not null,
 batch_id varchar not null, ingest_seq bigint not null unique, source_line bigint not null,
 primary key (title_id, event_id)
);
create table if not exists raw.batches (
 batch_id varchar primary key, source_name varchar, received_min timestamp, received_max timestamp,
 row_count bigint, accepted_count bigint, duplicate_count bigint, invalid_count bigint,
 status varchar, loaded_at timestamp
);
create table if not exists raw.delivery_expectations (
 receipt_date date, title_id varchar, expected_events bigint, expected_sessions bigint,
 primary key(receipt_date, title_id)
);
create table if not exists ops.rejections (
 batch_id varchar, source_line bigint, reason varchar, primary key(batch_id, source_line)
);
"""


class BatchRejected(ValueError):
    def __init__(self, report: dict):
        self.report = report
        super().__init__(f"batch rejected: {report['invalid_count']} invalid/conflicting row(s)")


def unique_object(pairs: list[tuple]) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def initialize(database: Path) -> None:
    database = Path(database)
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as connection:
        connection.execute(DDL)


def read_batch(path: Path) -> tuple[str, list[tuple[int, dict]], list[dict], int]:
    path = Path(path)
    if path.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("local adapter accepts files up to 64 MiB; split larger batches")
    content = path.read_bytes()
    batch_id = hashlib.sha256(content).hexdigest()
    rows, issues = [], []
    row_count = 0
    for line_number, line in enumerate(content.splitlines(), 1):
        row_count += 1
        try:
            if row_count > 100_000:
                raise ValueError("local adapter accepts at most 100,000 rows per file")
            row = normalize_event(json.loads(line, object_pairs_hook=unique_object))
            rows.append((line_number, row))
        except (ContractError, ValueError, TypeError, UnicodeError) as exc:
            issues.append({"source_line": line_number, "reason": str(exc)})
    return batch_id, rows, issues, row_count


def validate_file(path: Path) -> dict:
    batch_id, rows, issues, count = read_batch(path)
    identities = {}
    duplicates = 0
    for line_number, row in rows:
        key = (row["title_id"], row["event_id"])
        if key in identities:
            duplicates += 1
            if identities[key] != row["payload_hash"]:
                issues.append({"source_line": line_number, "reason": "conflicting duplicate event identity"})
        identities[key] = row["payload_hash"]
    return {"batch_id": batch_id, "row_count": count, "invalid_count": len(issues),
            "duplicate_count": duplicates, "issues": issues[:50]}


def load_file(path: Path, database: Path) -> dict:
    path, database = Path(path), Path(database)
    initialize(database)
    batch_id, rows, issues, row_count = read_batch(path)
    with duckdb.connect(str(database)) as connection:
        connection.execute("begin transaction")
        previous = connection.execute("select status,accepted_count,duplicate_count,invalid_count from raw.batches where batch_id=?", [batch_id]).fetchone()
        if previous:
            connection.execute("rollback")
            report = dict(batch_id=batch_id, status=previous[0], accepted_count=previous[1],
                          duplicate_count=previous[2], invalid_count=previous[3], reused=True)
            if previous[0] == "rejected":
                raise BatchRejected(report)
            return report

        keys = sorted({(row["title_id"], row["event_id"]) for _, row in rows})
        key_json = json.dumps([dict(title_id=t, event_id=e) for t, e in keys])
        connection.execute("""create temp table incoming_keys as
            select item.* from (select unnest(from_json(?, ?)) as item)""",
            [key_json, '[{"title_id":"VARCHAR","event_id":"VARCHAR"}]'])
        known = {(t, e): h for t, e, h in connection.execute("select e.title_id,e.event_id,e.payload_hash from raw.events e join incoming_keys k using(title_id,event_id)").fetchall()}
        accepted, duplicate_count = [], 0
        for line_number, row in rows:
            key = (row["title_id"], row["event_id"])
            if key in known:
                if known[key] == row["payload_hash"]:
                    duplicate_count += 1
                else:
                    issues.append({"source_line": line_number, "reason": "conflicting duplicate event identity"})
            else:
                known[key] = row["payload_hash"]
                accepted.append((line_number, row))

        received = [r["received_at"] for _, r in rows]
        status = "rejected" if issues else "loaded"
        report = dict(batch_id=batch_id, status=status, row_count=row_count,
                      accepted_count=0 if issues else len(accepted), duplicate_count=duplicate_count,
                      invalid_count=len(issues), reused=False)
        connection.execute("insert into raw.batches values (?,?,?,?,?,?,?,?,?,?)", [
            batch_id, path.name, min(received, default=None), max(received, default=None), row_count,
            report["accepted_count"], duplicate_count, len(issues), status,
            datetime.now(UTC).replace(tzinfo=None),
        ])
        if issues:
            for issue in {i["source_line"]: i for i in issues}.values():
                connection.execute("insert into ops.rejections values (?,?,?)", [batch_id, issue["source_line"], issue["reason"]])
        else:
            seq = connection.execute("select coalesce(max(ingest_seq),0) from raw.events").fetchone()[0]
            records = []
            for offset, (line_number, row) in enumerate(accepted, 1):
                row.update(batch_id=batch_id, ingest_seq=seq + offset, source_line=line_number)
                records.append([row[c] for c in COLUMNS])
            if records:
                types = [column[1] for column in connection.execute("describe raw.events").fetchall()]
                typed_schema = json.dumps([dict(zip(COLUMNS, types, strict=True))])
                payload = json.dumps([dict(zip(COLUMNS, record, strict=True)) for record in records], default=str, allow_nan=False)
                connection.execute("""insert into raw.events
                    select item.* from (select unnest(from_json(?, ?)) as item)""",
                    [payload, typed_schema])
        connection.execute("commit")
    if issues:
        report["issues"] = issues[:50]
        raise BatchRejected(report)
    return report


def load_expectations(path: Path, database: Path) -> None:
    """Immutable independent delivery counts: a changed manifest is a hard error."""
    initialize(database)
    with Path(path).open(newline="", encoding="utf-8") as handle:
        records = list(csv.DictReader(handle))
    with duckdb.connect(str(database)) as connection:
        connection.execute("begin")
        for row in records:
            key = [row["receipt_date"], row["title_id"]]
            values = [int(row["expected_events"]), int(row["expected_sessions"])]
            if min(values) < 0 or values[1] > values[0]:
                raise ValueError("invalid expected delivery counts")
            previous = connection.execute("select expected_events,expected_sessions from raw.delivery_expectations where receipt_date=? and title_id=?", key).fetchone()
            if previous and list(previous) != values:
                raise ValueError(f"manifest changed for {key}")
            if not previous:
                connection.execute("insert into raw.delivery_expectations values (?,?,?,?)", key + values)
        connection.execute("commit")


def load_path(path: Path, database: Path) -> list[dict]:
    path = Path(path)
    if path.is_dir():
        verify_manifest(path)
    paths = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
    if not paths:
        raise ValueError("no .jsonl files found")
    reports = [load_file(p, database) for p in paths]
    expectations = (path if path.is_dir() else path.parent) / "expectations.csv"
    if expectations.exists():
        load_expectations(expectations, database)
    return reports


def verify_manifest(directory: Path) -> None:
    """When a producer seals an inventory, honor both its hashes and exact membership."""
    manifest_path = Path(directory) / "manifest.json"
    if not manifest_path.exists():
        return
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"), object_pairs_hook=unique_object)
    actual = {p.name for p in Path(directory).glob("*.jsonl")}
    items = manifest["files"]
    expected = {item["path"] for item in items}
    if actual != expected or len(expected) != len(items):
        raise ValueError("sealed manifest inventory does not match .jsonl files")
    for item in items:
        name = item["path"]
        if Path(name).name != name or not name.endswith(".jsonl"):
            raise ValueError("invalid manifest file name")
        data = (Path(directory) / name).read_bytes()
        if hashlib.sha256(data).hexdigest() != item["sha256"]:
            raise ValueError(f"manifest hash mismatch: {name}")
    if hashlib.sha256((Path(directory) / "expectations.csv").read_bytes()).hexdigest() != manifest["expectations_sha256"]:
        raise ValueError("expectation manifest hash mismatch")

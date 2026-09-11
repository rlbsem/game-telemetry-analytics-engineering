"""Real dbt quality gates and immutable, validated warehouse publication."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
import uuid
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb

from telemetry.contract import utc_timestamp
from telemetry.ingest import initialize

PROJECT = Path(__file__).resolve().parents[2]
DEFAULT_AS_OF = "2026-01-23T00:00:00Z"
DEFAULT_COMPLETE_THROUGH = "2026-01-21"


def atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def content_fingerprint(connection: duckdb.DuckDBPyConnection) -> str:
    """Seal actual raw/model contents and types. Full scans are affordable locally.

    At scale use immutable partition manifests plus verified table-version identifiers
    in the publication transaction, instead of rescanning the full warehouse.
    """
    digest = hashlib.sha256()
    relations = connection.execute("""
        select table_schema, table_name from information_schema.tables
        where table_schema in ('raw','analytics') order by 1,2
    """).fetchall()
    for schema, name in relations:
        quoted = f'"{schema}"."{name}"'
        digest.update(quoted.encode())
        cursor = connection.execute(f"select * from {quoted} order by all")
        digest.update(str(cursor.description).encode())
        while rows := cursor.fetchmany(1000):
            for row in rows:
                digest.update(json.dumps(row, default=str, separators=(",", ":"), allow_nan=False).encode())
                digest.update(b"\n")
    return digest.hexdigest()


def validation_receipt(database: Path) -> dict:
    with duckdb.connect(str(database), read_only=True) as connection:
        try:
            cursor = connection.execute("select * exclude(id) from ops.validation where id=1")
            row = cursor.fetchone()
        except duckdb.Error as exc:
            raise ValueError("run a successful build before monitoring or publication") from exc
        if row is None:
            raise ValueError("no successful quality-gate receipt")
        receipt = dict(zip([c[0] for c in cursor.description], row, strict=True))
        if content_fingerprint(connection) != receipt["fingerprint"]:
            raise ValueError("warehouse changed after validation; rerun build")
        return receipt


def build(
    database: Path, *, as_of: str = DEFAULT_AS_OF,
    complete_through: str = DEFAULT_COMPLETE_THROUGH, full_refresh: bool = False,
    generate_docs: bool = True,
) -> dict:
    database = Path(database).resolve()
    initialize(database)
    with duckdb.connect(str(database)) as connection:
        connection.execute("""create table if not exists ops.validation (
            id integer primary key, fingerprint varchar, as_of varchar, complete_through varchar,
            dbt_results_sha256 varchar, model_count integer, test_count integer,
            validated_at varchar
        )""")
        connection.execute("delete from ops.validation")
    cutoff = utc_timestamp(as_of)
    if cutoff.time().isoformat() != "00:00:00":
        raise ValueError("as_of must be midnight UTC: this pipeline publishes closed receipt days")
    complete = date.fromisoformat(complete_through)
    if complete >= cutoff.date():
        raise ValueError("complete_through must precede the as_of date")
    with duckdb.connect(str(database), read_only=True) as connection:
        future = connection.execute("select count(*) from raw.events where event_ts >= ? or received_at >= ?", [cutoff, cutoff]).fetchone()[0]
        if future:
            raise ValueError(f"{future} events are outside as_of; build a separate historical candidate")
        if connection.execute("select count(*) from raw.delivery_expectations where receipt_date >= ?", [cutoff.date()]).fetchone()[0]:
            raise ValueError("delivery manifest contains dates outside as_of")
    artifacts = database.parent / f"{database.stem}-dbt"
    artifacts.mkdir(parents=True, exist_ok=True)
    environment = os.environ.copy()
    environment.update(TELEMETRY_DB=str(database), DBT_SEND_ANONYMOUS_USAGE_STATS="false",
                       DBT_USE_COLORS="false", PYTHONIOENCODING="utf-8")
    executable = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    common = ["--project-dir", str(PROJECT / "dbt"), "--profiles-dir", str(PROJECT / "dbt"),
              "--target-path", str(artifacts), "--log-path", str(artifacts / "logs"),
              "--vars", json.dumps({"as_of": as_of, "complete_through": complete_through})]
    command = [str(executable), "build", *common]
    if full_refresh:
        command.append("--full-refresh")
    with (artifacts / "build.log").open("w", encoding="utf-8") as log:
        result = subprocess.run(command, env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=900)
    if result.returncode:
        raise RuntimeError(f"dbt build failed; inspect {artifacts / 'build.log'}")
    run_results_bytes = (artifacts / "run_results.json").read_bytes()
    results = json.loads(run_results_bytes)["results"]
    if any(r["status"] not in {"success", "pass"} for r in results):
        raise RuntimeError("dbt warning/skipped result; publication requires every gate to pass")
    manifest = json.loads((artifacts / "manifest.json").read_text(encoding="utf-8"))
    models = [r for r in results if manifest["nodes"][r["unique_id"]]["resource_type"] == "model"]
    tests = [r for r in results if manifest["nodes"][r["unique_id"]]["resource_type"] == "test"]
    if not models or not tests:
        raise RuntimeError("expected executable models and tests in dbt run")
    with duckdb.connect(str(database)) as connection:
        for result in models:
            node = manifest["nodes"][result["unique_id"]]
            if node["config"]["materialized"] == "incremental":
                name = node["alias"].replace('"', '""') + "__dbt_backup"
                connection.execute(f'drop table if exists analytics."{name}"')
    if generate_docs:
        with (artifacts / "docs.log").open("w", encoding="utf-8") as log:
            subprocess.run([str(executable), "docs", "generate", "--no-compile", *common],
                           env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=900, check=True)
    with duckdb.connect(str(database)) as connection:
        receipt = dict(fingerprint=content_fingerprint(connection), as_of=as_of,
                       complete_through=complete_through,
                       dbt_results_sha256=hashlib.sha256(run_results_bytes).hexdigest(),
                       model_count=len(models), test_count=len(tests),
                       validated_at=datetime.now(UTC).isoformat())
        connection.execute("insert into ops.validation values (1,?,?,?,?,?,?,?)", list(receipt.values()))
    atomic_json(artifacts / "validation.json", receipt)
    return receipt


def publish(database: Path, output: Path) -> dict:
    """One local writer; atomic pointer replacement preserves the prior generation."""
    from telemetry.monitor import monitor

    database, output = Path(database).resolve(), Path(output).resolve()
    receipt = validation_receipt(database)
    output.mkdir(parents=True, exist_ok=True)
    lock = output / ".publish.lock"
    with lock.open("x", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))
    try:
        pointer = output / "current.json"
        previous = json.loads(pointer.read_text()) if pointer.exists() else None
        if previous and utc_timestamp(previous["as_of"]) > utc_timestamp(receipt["as_of"]):
            raise ValueError("refusing to replace a newer publication; use a separate replay output")
        if previous and previous.get("fingerprint") == receipt["fingerprint"] and previous["as_of"] == receipt["as_of"]:
            return previous
        generation = Path("generations") / uuid.uuid4().hex
        destination = output / generation
        destination.mkdir(parents=True)
        frozen = destination / database.name
        shutil.copyfile(database, frozen)
        frozen_receipt = validation_receipt(frozen)
        monitor(frozen, destination)
        published = {**frozen_receipt, "generation": generation.as_posix(),
                     "database": (generation / database.name).as_posix(),
                     "report": (generation / "report.json").as_posix()}
        atomic_json(pointer, published)
        return published
    finally:
        lock.unlink(missing_ok=True)


def demo(workspace: Path, *, players: int = 160) -> dict:
    from telemetry.generate import generate
    from telemetry.ingest import load_path

    workspace = Path(workspace).resolve()
    generate(workspace / "landing", players)
    database = workspace / "candidate.duckdb"
    load_path(workspace / "landing", database)
    build(database)
    return publish(database, workspace / "published")


def replay(path: Path, workspace: Path, *, as_of: str, complete_through: str) -> dict:
    from telemetry.ingest import load_path

    workspace = Path(workspace).resolve()
    database = workspace / "candidate.duckdb"
    if database.exists():
        raise ValueError("replay needs a fresh workspace; existing data will not be destroyed")
    load_path(Path(path), database)
    build(database, as_of=as_of, complete_through=complete_through, full_refresh=True)
    return publish(database, workspace / "published")

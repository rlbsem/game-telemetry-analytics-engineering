"""Airflow task boundaries with immutable inputs and a private candidate warehouse.

Only small paths go through XCom. Data, command logs, and failure receipts live on
the shared volume. All subprocesses use argument lists and propagate failures.
"""

from __future__ import annotations

import csv
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)


def _utc(value: str | datetime) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    if parsed.tzinfo is None:
        raise ValueError("Receipt interval bounds must include a UTC offset")
    return parsed.astimezone(UTC)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _published_database(root: Path, pointer: dict[str, Any]) -> Path:
    database = (root / pointer["database"]).resolve()
    if not database.is_relative_to(root.resolve()) or not database.is_file():
        raise ValueError("Publication pointer must identify an existing database below published/")
    return database


def _verify_inputs(run: Path, manifest: dict[str, Any]) -> None:
    expected = {item["name"] for item in manifest["files"]}
    actual = {item.name for item in (run / "input").iterdir()}
    if actual != expected or len(expected) != len(manifest["files"]):
        raise ValueError("Immutable staged input inventory changed")
    for item in manifest["files"]:
        staged_path = run / "input" / item["name"]
        if staged_path.is_symlink():
            raise ValueError("Staged receipt input cannot be a symbolic link")
        candidate = staged_path.resolve()
        if not candidate.is_relative_to((run / "input").resolve()):
            raise ValueError("Invalid receipt manifest path")
        if not candidate.is_file() or _sha256(candidate) != item["sha256"]:
            raise ValueError(f"Immutable staged input changed: {item['name']}")


def stage_interval(
    *,
    landing: str | Path,
    state: str | Path,
    run_id: str,
    interval_start: str | datetime,
    interval_end: str | datetime,
) -> str:
    """Freeze one closed UTC receipt day, then copy the last published database.

    A day with expectations and no delivered file is legitimate missing telemetry.
    A day without an expectation manifest is an unknown input, so fails closed.
    Continuous intervals are mandatory; replay uses a separate state directory.
    """
    start, end = _utc(interval_start), _utc(interval_end)
    if end - start != timedelta(days=1) or start.time().isoformat() != "00:00:00":
        raise ValueError("This DAG accepts exactly one midnight-to-midnight UTC receipt day")
    landing, state = Path(landing).resolve(), Path(state).resolve()
    if not landing.is_dir():
        raise FileNotFoundError(f"Missing landing directory: {landing}")
    safe_id = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:20]
    run = state / "runs" / f"{start.date().isoformat()}-{safe_id}"
    existing = _read_json(run / "interval.json")
    if existing is not None:
        if (existing["run_id"], existing["interval_start"], existing["interval_end"]) != (
            run_id, start.isoformat(), end.isoformat()
        ):
            raise ValueError("Existing run receipt has a different identity")
        _verify_inputs(run, existing)
        return str(run)

    published = state / "published"
    predecessor = _read_json(published / "current.json")
    expected_start = _utc(predecessor["as_of"]) if predecessor else _utc(
        os.environ.get("TELEMETRY_HISTORY_START", "2026-01-01T00:00:00+00:00")
    )
    if start != expected_start:
        raise ValueError(
            f"Expected next receipt day at {expected_start.isoformat()}, got {start.isoformat()}; "
            "run missing days in order or use a separate replay state directory"
        )

    expectation_path = landing / "expectations.csv"
    with expectation_path.open(newline="", encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        fieldnames = reader.fieldnames
        expectations = [row for row in reader if row["receipt_date"] == start.date().isoformat()]
    if not expectations:
        raise ValueError(f"No independent delivery expectations for {start.date()}")

    run.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{safe_id}-", dir=run.parent))
    try:
        inputs = temporary / "input"
        inputs.mkdir()
        source = landing / f"{start.date().isoformat()}.jsonl"
        destination = inputs / source.name
        if source.exists():
            shutil.copyfile(source, destination)
            # Prevent a mispartitioned object from loading future receipts.
            with destination.open(encoding="utf-8") as stream:
                for number, line in enumerate(stream, start=1):
                    if line.strip():
                        receipt = _utc(json.loads(line)["received_at"])
                        if not start <= receipt < end:
                            raise ValueError(f"Receipt outside interval in {source.name}:{number}")
        else:
            destination.write_text("", encoding="utf-8")
        with (inputs / "expectations.csv").open("w", newline="", encoding="utf-8") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(expectations)
        if predecessor:
            shutil.copyfile(_published_database(published, predecessor), temporary / "candidate.duckdb")
        manifest = {
            "run_id": run_id,
            "interval_start": start.isoformat(),
            "interval_end": end.isoformat(),
            # Inclusive day closure: wait 48h after the end of that event day.
            "complete_through": (end - timedelta(days=3)).date().isoformat(),
            "predecessor": predecessor,
            "published_root": str(published),
            "files": [{"name": p.name, "sha256": _sha256(p)} for p in sorted(inputs.iterdir())],
        }
        _write_json(temporary / "interval.json", manifest)
        os.replace(temporary, run)
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)
    return str(run)


def execute_step(step: str, run_dir: str | Path) -> None:
    """Run a real CLI boundary, retaining stdout/stderr even on failure."""
    run = Path(run_dir).resolve()
    manifest = _read_json(run / "interval.json")
    if manifest is None:
        raise ValueError("Stage the receipt interval before executing tasks")
    _verify_inputs(run, manifest)
    database = str(run / "candidate.duckdb")
    commands = {
        "validate": ["validate", "--input", str(run / "input")],
        "load": ["load", "--input", str(run / "input"), "--database", database],
        "build": [
            "build", "--database", database, "--as-of", manifest["interval_end"],
            "--complete-through", manifest["complete_through"],
        ],
        "monitor": ["monitor", "--database", database, "--output", str(run / "monitor")],
        "publish": ["publish", "--database", database, "--output", manifest["published_root"]],
    }
    if step not in commands:
        raise ValueError(f"Unknown pipeline step: {step}")
    if step == "publish":
        current = _read_json(Path(manifest["published_root"]) / "current.json")
        if current != manifest["predecessor"]:
            # Recovery if publication completed but its task acknowledgement was lost.
            if current and _utc(current["as_of"]) == _utc(manifest["interval_end"]) and (
                _sha256(_published_database(Path(manifest["published_root"]), current))
                == _sha256(Path(database))
            ):
                _write_json(run / "publication.json", current)
                return
            raise ValueError("Published generation changed since staging; refuse stale candidate")

    interpreter = os.environ.get("TELEMETRY_PYTHON", sys.executable)
    log_path = run / f"{step}.log"
    with log_path.open("a", encoding="utf-8") as log:
        log.write(f"\nStarted {datetime.now(UTC).isoformat()}\n")
        log.flush()
        result = subprocess.run(
            [interpreter, "-m", "telemetry", *commands[step]],
            stdout=log, stderr=subprocess.STDOUT, check=False, timeout=1800,
        )
    if result.returncode:
        raise RuntimeError(f"{step} failed with exit code {result.returncode}; see {log_path}")
    if step == "publish":
        _write_json(run / "publication.json", _read_json(Path(manifest["published_root"]) / "current.json"))


def write_failure(*, state: str | Path, context: dict[str, Any]) -> str:
    """Emit an actionable JSON receipt and log line; no external pager is assumed."""
    task_instance = context.get("task_instance")
    run_id = str(context.get("run_id", "unknown"))
    task_id = str(getattr(task_instance, "task_id", "unknown"))
    key = hashlib.sha256(f"{run_id}:{task_id}".encode()).hexdigest()[:20]
    path = Path(state) / "alerts" / f"failure-{key}.json"
    receipt = {
        "kind": "pipeline_task_failure",
        "severity": "error",
        "observed_at": datetime.now(UTC).isoformat(),
        "run_id": run_id,
        "task_id": task_id,
        "try_number": getattr(task_instance, "try_number", None),
        "interval_start": str(context.get("data_interval_start", "")),
        "interval_end": str(context.get("data_interval_end", "")),
        "exception": str(context.get("exception", "unknown error")),
        "action": "Inspect run logs and failed rows; repair or replay, then clear failed task and downstream tasks.",
    }
    _write_json(path, receipt)
    LOGGER.error("telemetry_alert %s", json.dumps(receipt, sort_keys=True))
    return str(path)

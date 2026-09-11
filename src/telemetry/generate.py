"""Deterministic fictional launches, including known gameplay and transport regressions."""
# ruff: noqa: B023
# emit is called synchronously inside its owning iteration and never escapes it.

from __future__ import annotations

import csv
import hashlib
import json
import random
import uuid
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

START = datetime(2026, 1, 1, tzinfo=UTC)
SEED = 17


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def stable_id(value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"synthetic-game-telemetry/{value}"))


def generate(output: Path, players: int = 160) -> dict:
    """players is per title; production scale is deliberately not simulated."""
    if not 20 <= players <= 2000:
        raise ValueError("players per title must be between 20 and 2000")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        from telemetry.ingest import verify_manifest
        if not (output / "manifest.json").exists() or not (output / "scenario.json").exists():
            raise ValueError("generate requires an empty directory or its own unchanged prior output")
        verify_manifest(output)
        previous = json.loads((output / "scenario.json").read_text(encoding="utf-8"))
        if previous["players_per_title"] != players or previous["seed"] != SEED:
            raise ValueError("different generator settings need a fresh output directory")
    rng = random.Random(SEED)
    events = []
    session_count = 0
    for title in ("atlas", "echo"):
        for player_number in range(players):
            player = f"P{player_number:05d}"  # Deliberately reused across title namespaces.
            cohort = player_number % 15
            new_cohort = cohort >= 10
            for day in range(cohort, 21):
                age = day - cohort
                return_probability = (
                    (0.72 if not new_cohort else 0.42) if age == 1 else
                    (0.45 if not new_cohort else 0.22) if age == 7 else
                    (0.36 if not new_cohort else 0.24)
                )
                if age and rng.random() > return_probability:
                    continue
                session_count += 1
                session = f"{player}-D{day:02d}"
                version = 1 if day < 10 else 2
                build = "1.0.0" if version == 1 else "1.1.0"
                platform = "console_current" if player_number % 4 else "pc"
                start = START + timedelta(days=day, hours=rng.randrange(8, 23), minutes=rng.randrange(60))
                # Some sessions straddle midnight to exercise event-day vs session-start-day.
                if session_count % 31 == 0:
                    start = START + timedelta(days=day, hours=23, minutes=52)
                elapsed = 0
                active = 0
                sequence = 0
                buffer = []

                def emit(name: str, *, step: int = 0, **extra) -> None:
                    nonlocal elapsed, active, sequence
                    elapsed += step
                    active += int(step * 0.8)  # Pause/menu time is not active play.
                    timestamp = start + timedelta(seconds=elapsed)
                    delay = rng.randrange(5, 180)
                    if name in {"session_end", "crash"} and session_count % 37 == 0:
                        delay += 86400 * (2 if day < 19 else 1)
                    delay = min(delay, 172800)
                    if timestamp.date() >= (START + timedelta(days=21)).date():
                        delay = min(delay, 180)
                    row = dict(
                        event_id=stable_id(f"{title}/{session}/{sequence}"), title_id=title,
                        player_id=player, session_id=session, build_id=build,
                        platform=platform, country_code=("CA", "US", "GB", "JP")[player_number % 4],
                        event_name=name, schema_version=version, event_ts=iso(timestamp),
                        received_at=iso(timestamp + timedelta(seconds=delay)), event_sequence=sequence,
                        metadata={"instrumentation_revision": f"rev{version}", "difficulty": "normal"},
                    )
                    if name in {"heartbeat", "session_end", "crash"}:
                        row["play_time_ms" if version == 1 else "active_seconds"] = active * 1000 if version == 1 else active
                    row.update(extra)
                    buffer.append(row)
                    sequence += 1

                emit("session_start")
                crashed = rng.random() < (0.14 if title == "atlas" and version == 2 else 0.025)
                for mission_number in range(1, 4):
                    mission = f"M{mission_number:02d}"
                    attempt = f"A{mission_number}"
                    context = dict(mission_id=mission, attempt_id=attempt)
                    emit("mission_start", step=90, **context)
                    emit("checkpoint_reached", step=60, checkpoint_id=f"{mission}-C01", **context)
                    bottleneck = title == "atlas" and version == 2 and mission == "M02"
                    deaths = rng.randrange(4, 8) if bottleneck else rng.randrange(0, 3)
                    for _ in range(deaths):
                        emit("player_death", step=45, **context)
                    # Controlled instrumentation regression, separate from transport loss.
                    if not (title == "atlas" and version == 2 and player_number % 4 != 0):
                        emit("heartbeat", step=120)
                    else:
                        elapsed += 120
                        active += 96
                    if crashed and mission_number == 1:
                        emit("crash", step=60, error_code="GPU_DEVICE_LOST")
                        break
                    completion_probability = 0.35 if bottleneck else 0.90
                    if rng.random() < completion_probability:
                        emit("mission_complete", step=120, **context)
                    else:
                        break
                if not crashed:
                    if session_count % 23 == 0:
                        emit("error", step=1, error_code="MATCHMAKING_RETRY")
                    emit("session_end", step=90)

                # Missing boundaries are explicit partials; no missing mission starts are
                # injected into the healthy run because that would fail the publication gate.
                if session_count % 53 == 0:
                    buffer = [r for r in buffer if r["event_name"] != "session_start"]
                if session_count % 47 == 0:
                    buffer = [r for r in buffer if r["event_name"] not in {"session_end", "crash"}]
                events.extend(buffer)

    expected = defaultdict(lambda: {"events": 0, "sessions": set()})
    delivered = defaultdict(list)
    lost_events = 0
    for row in events:
        day = row["received_at"][:10]
        key = (day, row["title_id"])
        expected[key]["events"] += 1
        expected[key]["sessions"].add(row["session_id"])
        # Independent collector manifest knows emitted identities before simulated loss.
        lost = (row["title_id"] == "atlas" and day == "2026-01-16"
                and int(row["player_id"][1:]) % 3 == 0)
        if lost:
            lost_events += 1
            continue
        delivered[day].append(row)
        if int(uuid.UUID(row["event_id"])) % 97 == 0:
            retry = dict(row)
            # Kept in its original receipt partition; different receiver time is tested elsewhere.
            delivered[day].append(retry)

    manifest_files = []
    for day in sorted({d for d, _ in expected}):
        rows = delivered[day]
        rng.shuffle(rows)  # Physical order intentionally differs from event order.
        content = "".join(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n" for row in rows)
        path = output / f"{day}.jsonl"
        path.write_text(content, encoding="utf-8", newline="\n")
        manifest_files.append({"path": path.name, "sha256": hashlib.sha256(content.encode()).hexdigest(), "rows": len(rows)})
    with (output / "expectations.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(["receipt_date", "title_id", "expected_events", "expected_sessions"])
        for (day, title), values in sorted(expected.items()):
            writer.writerow([day, title, values["events"], len(values["sessions"])])
    scenario = {
        "seed": SEED, "players_per_title": players, "titles": ["atlas", "echo"],
        "activity_start": "2026-01-01", "activity_end": "2026-01-21",
        "as_of": "2026-01-23T00:00:00Z", "complete_through": "2026-01-21",
        "emitted_events": len(events), "transport_lost_events": lost_events,
        "delivered_rows_including_retries": sum(len(r) for r in delivered.values()),
        "sessions_generated": session_count,
        "signals": {
            "crashes": "Atlas 1.1.0 crash probability 14%; baseline and Echo 2.5%.",
            "progression": "Atlas 1.1.0 M02: 4-7 deaths per reached attempt and 35% completion probability (other missions 90%).",
            "instrumentation": "Atlas 1.1.0 omits heartbeats for 75% of player IDs; terminal counters remain valid.",
            "delivery": "Atlas on receipt date Jan 16 loses events for player IDs divisible by 3; manifest remains complete.",
            "retention": "Acquisition cohorts from Jan 11 have lower return probabilities; build association is not causal proof.",
            "boundaries": "Every 53rd session loses start; every 47th loses terminal; every 37th terminal delayed 1-2 days.",
        },
    }
    (output / "scenario.json").write_text(json.dumps(scenario, indent=2) + "\n", encoding="utf-8")
    manifest = {"synthetic": True, "format_version": 1, "files": manifest_files,
                "expectations_sha256": hashlib.sha256((output / "expectations.csv").read_bytes()).hexdigest()}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return scenario

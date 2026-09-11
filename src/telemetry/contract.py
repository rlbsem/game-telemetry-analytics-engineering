"""Validate wire envelopes before canonicalizing units; never coerce invalid types."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from importlib.resources import files
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA = json.loads(files("telemetry").joinpath("event.schema.json").read_text(encoding="utf-8"))
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())


class ContractError(ValueError):
    """Wire event cannot be admitted to the typed raw layer."""


def utc_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise ValueError("UTC offset required")
        return parsed.astimezone(UTC).replace(tzinfo=None)
    except (TypeError, ValueError, AttributeError) as exc:
        raise ContractError(f"invalid UTC timestamp: {value!r}") from exc


def normalize_event(event: dict[str, Any]) -> dict[str, Any]:
    # allow_nan=False also rejects non-finite values inside extension metadata.
    try:
        serialized = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (ValueError, TypeError) as exc:
        raise ContractError("event must contain finite JSON values") from exc
    if len(serialized.encode("utf-8")) > 16_384:
        raise ContractError("event exceeds 16 KiB contract limit")
    errors = sorted(VALIDATOR.iter_errors(event), key=lambda e: str(list(e.path)))
    if errors:
        raise ContractError("; ".join(f"{'.'.join(map(str, e.path)) or 'event'}: {e.message}" for e in errors[:5]))
    # JSON Schema treats 1.0 as integer. The wire contract requires integer tokens.
    for name in ("schema_version", "event_sequence", "play_time_ms"):
        if name in event and type(event[name]) is not int:
            raise ContractError(f"{name} must be an integer")
    counter_events = {"session_start", "heartbeat", "session_end", "crash"}
    if event["event_name"] not in counter_events and ("active_seconds" in event or "play_time_ms" in event):
        raise ContractError("active counters only belong to session boundaries and heartbeats")
    if "active_seconds" in event:
        milliseconds = Decimal(str(event["active_seconds"])) * 1000
        if milliseconds != milliseconds.to_integral_value():
            raise ContractError("active_seconds must have millisecond resolution (at most 3 decimals)")
    mission_events = {"mission_start", "mission_complete", "checkpoint_reached", "player_death"}
    if event["event_name"] not in mission_events and any(k in event for k in ("mission_id", "attempt_id", "checkpoint_id")):
        raise ContractError("attempt context only belongs to mission events")
    event_ts = utc_timestamp(event["event_ts"])
    received_at = utc_timestamp(event["received_at"])
    if event_ts > received_at + timedelta(minutes=5):
        raise ContractError("event_ts exceeds allowed five-minute client clock skew")
    if event["event_name"] == "session_start" and event["event_sequence"] != 0:
        raise ContractError("session_start must have event_sequence 0")
    if event["event_name"] == "session_start" and event.get("active_seconds", event.get("play_time_ms", 0)) != 0:
        raise ContractError("session_start active counter must be zero")
    row = {k: event[k] for k in (
        "event_id", "title_id", "player_id", "session_id", "build_id", "platform",
        "country_code", "event_name", "schema_version", "event_sequence"
    )}
    row.update(event_ts=event_ts, received_at=received_at)
    row.update({k: event.get(k) for k in ("mission_id", "attempt_id", "checkpoint_id", "error_code")})
    counter = event.get("active_seconds")
    if event["schema_version"] == 1 and "play_time_ms" in event:
        counter = event["play_time_ms"] / 1000.0
    row["active_seconds"] = 0.0 if event["event_name"] == "session_start" else counter
    row["metadata_json"] = json.dumps(event["metadata"], sort_keys=True, separators=(",", ":"), allow_nan=False)
    # Collector receipt time can change on retransmission; all client fields are immutable.
    identity = {k: v for k, v in event.items() if k != "received_at"}
    identity["event_ts"] = event_ts.isoformat()
    row["payload_hash"] = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
    return row

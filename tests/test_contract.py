"""Boundary tests for the game-client wire contract, independent of warehouse SQL."""

from copy import deepcopy
from datetime import datetime

import pytest

from telemetry.contract import ContractError, normalize_event


def wire_event(**overrides):
    """A complete v2 heartbeat from one synthetic client session."""
    event = {
        "event_id": "event-001", "title_id": "atlas", "player_id": "player-001",
        "session_id": "session-001", "build_id": "1.1.0", "platform": "console_current",
        "country_code": "CA", "event_name": "heartbeat", "schema_version": 2,
        "event_ts": "2026-01-12T12:01:00Z", "received_at": "2026-01-12T12:01:05Z",
        "event_sequence": 10, "active_seconds": 45.5,
        "metadata": {"difficulty": "normal", "mode": "campaign"},
    }
    event.update(overrides)
    return event


def test_v2_preserves_fractional_gameplay_counter_and_normalizes_utc():
    result = normalize_event(wire_event())
    assert result["active_seconds"] == 45.5
    assert result["event_ts"] == datetime(2026, 1, 12, 12, 1)
    assert result["received_at"] == datetime(2026, 1, 12, 12, 1, 5)
    assert result["schema_version"] == 2


def test_v1_converts_milliseconds_without_truncation():
    event = wire_event(schema_version=1, build_id="1.0.0", play_time_ms=45_500)
    event.pop("active_seconds")
    assert normalize_event(event)["active_seconds"] == 45.5


def test_normalization_does_not_mutate_caller_payload():
    event = wire_event(); original = deepcopy(event); normalize_event(event); assert event == original


@pytest.mark.parametrize("event_name", ["session_start", "session_end", "heartbeat", "mission_start", "checkpoint_reached", "player_death", "mission_complete", "crash", "error"])
def test_every_documented_event_type_has_a_valid_payload(event_name):
    event = wire_event(event_name=event_name)
    if event_name not in {"heartbeat", "session_end", "crash"}: event.pop("active_seconds")
    if event_name == "session_start": event["event_sequence"] = 0
    if event_name in {"mission_start", "checkpoint_reached", "player_death", "mission_complete"}: event.update(mission_id="M01", attempt_id="attempt-001")
    if event_name == "checkpoint_reached": event["checkpoint_id"] = "C01"
    if event_name in {"crash", "error"}: event["error_code"] = "synthetic-001"
    assert normalize_event(event)["event_name"] == event_name


def test_payload_hash_ignores_transport_retry_time_and_metadata_key_order():
    original = wire_event(); retried = wire_event(received_at="2026-01-13T12:01:05Z", metadata={"mode": "campaign", "difficulty": "normal"})
    assert normalize_event(original)["payload_hash"] == normalize_event(retried)["payload_hash"]
    assert normalize_event(original)["payload_hash"] != normalize_event(wire_event(active_seconds=44))["payload_hash"]


@pytest.mark.parametrize("field", ["event_id", "title_id", "player_id", "session_id", "build_id", "event_ts", "received_at", "event_sequence", "schema_version", "event_name", "metadata"])
def test_missing_envelope_fields_fail(field):
    event = wire_event(); event.pop(field)
    with pytest.raises(ContractError): normalize_event(event)


@pytest.mark.parametrize("overrides", [{"schema_version": 3}, {"schema_version": True}, {"schema_version": "2"}, {"event_name": "level_completed"}, {"event_id": ""}, {"event_id": "white space"}, {"event_id": "évent"}, {"event_id": "x" * 129}, {"player_id": 123}, {"event_sequence": -1}, {"event_sequence": True}, {"event_sequence": 1.5}, {"platform": "unknown"}, {"country_code": "canada"}, {"metadata": []}, {"metadata": {"quality": float("nan")}}, {"active_seconds": -1}, {"active_seconds": True}, {"active_seconds": "45"}, {"active_seconds": float("nan")}, {"active_seconds": float("inf")}, {"active_seconds": 86_401}, {"event_ts": "2026-01-12T12:01:00"}, {"received_at": "not-a-timestamp"}, {"event_ts": "2026-01-12T12:07:00Z"}, {"undocumented_attribute": "silently losing this would hide schema drift"}])
def test_invalid_types_ranges_schema_and_clock_skew_fail(overrides):
    with pytest.raises(ContractError): normalize_event(wire_event(**overrides))


def test_offline_client_late_arrival_has_no_small_lookback_cutoff():
    event = wire_event(received_at="2026-02-20T00:00:00Z")
    assert normalize_event(event)["event_ts"] == datetime(2026, 1, 12, 12, 1)


def test_small_client_clock_skew_is_tolerated(): assert normalize_event(wire_event(event_ts="2026-01-12T12:03:00Z"))

def test_session_start_must_anchor_client_sequence_at_zero():
    event = wire_event(event_name="session_start", event_sequence=1); event.pop("active_seconds")
    with pytest.raises(ContractError): normalize_event(event)

@pytest.mark.parametrize("event_name", ["heartbeat", "session_end", "crash"])
def test_counter_is_required_for_counter_emitting_events(event_name):
    event = wire_event(event_name=event_name, error_code="fatal-001"); event.pop("active_seconds")
    with pytest.raises(ContractError): normalize_event(event)

@pytest.mark.parametrize("event_name", ["mission_start", "mission_complete", "checkpoint_reached", "player_death"])
@pytest.mark.parametrize("missing", ["mission_id", "attempt_id"])
def test_mission_activity_requires_explicit_attempt_identity(event_name, missing):
    event = wire_event(event_name=event_name, mission_id="M02", attempt_id="attempt-001", checkpoint_id="C01"); event.pop(missing)
    with pytest.raises(ContractError): normalize_event(event)


def test_checkpoint_event_requires_checkpoint():
    with pytest.raises(ContractError): normalize_event(wire_event(event_name="checkpoint_reached", mission_id="M01", attempt_id="a1"))

@pytest.mark.parametrize("event_name", ["crash", "error"])
def test_error_events_require_an_error_code(event_name):
    with pytest.raises(ContractError): normalize_event(wire_event(event_name=event_name))

@pytest.mark.parametrize("counter", [-1, True, 0.5, "1000", 86_400_001])
def test_v1_milliseconds_are_bounded_integers(counter):
    event = wire_event(schema_version=1, build_id="1.0.0", play_time_ms=counter); event.pop("active_seconds")
    with pytest.raises(ContractError): normalize_event(event)


def test_version_two_rejects_legacy_counter_to_surface_instrumentation_drift():
    with pytest.raises(ContractError): normalize_event(wire_event(play_time_ms=45_500))

@pytest.mark.parametrize("payload", [None, [], "text", 42])
def test_non_object_wire_payload_is_a_contract_error(payload):
    with pytest.raises(ContractError): normalize_event(payload)

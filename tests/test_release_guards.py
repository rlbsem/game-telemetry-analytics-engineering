import json

import pytest

from telemetry.contract import ContractError, normalize_event
from telemetry.generate import generate
from telemetry.ingest import load_path, verify_manifest


def wire(**extra):
    row = dict(event_id="E1", title_id="atlas", player_id="P1", session_id="S1",
               build_id="1.1.0", platform="console_current", country_code="CA",
               event_name="heartbeat", schema_version=2, event_sequence=1,
               event_ts="2026-01-11T12:01:00Z", received_at="2026-01-11T12:01:01Z",
               active_seconds=1.001, metadata={"instrumentation_revision": "rev2"})
    row.update(extra)
    return row


def test_counter_precision_and_event_specific_fields():
    assert normalize_event(wire())["active_seconds"] == 1.001
    with pytest.raises(ContractError, match="millisecond resolution"): normalize_event(wire(active_seconds=1.0001))
    with pytest.raises(ContractError, match="attempt context"): normalize_event(wire(mission_id="M01", attempt_id="A1"))
    with pytest.raises(ContractError, match="active counters"): normalize_event(wire(event_name="mission_start", mission_id="M01", attempt_id="A1"))


@pytest.mark.parametrize("mutation", ["extra_file", "changed_payload", "changed_expectations"])
def test_sealed_input_cannot_silently_change(tmp_path, mutation):
    generate(tmp_path / "landing", 20); landing = tmp_path / "landing"
    if mutation == "extra_file": (landing / "extra.jsonl").write_text(json.dumps(wire()) + "\n")
    elif mutation == "changed_payload": next(landing.glob("*.jsonl")).write_text(json.dumps(wire()) + "\n")
    else:
        with (landing / "expectations.csv").open("a") as handle: handle.write("2026-01-23,atlas,100,10\n")
    with pytest.raises(ValueError, match="manifest"): verify_manifest(landing)
    with pytest.raises(ValueError, match="manifest"): load_path(landing, tmp_path / "candidate.duckdb")
    assert not (tmp_path / "candidate.duckdb").exists()


def test_regeneration_requires_same_settings_and_unmodified_inventory(tmp_path):
    generate(tmp_path / "landing", 20)
    with pytest.raises(ValueError, match="different generator settings"): generate(tmp_path / "landing", 21)
    (tmp_path / "foreign").mkdir(); (tmp_path / "foreign/user-data.jsonl").write_text("keep me")
    with pytest.raises(ValueError, match="empty directory"): generate(tmp_path / "foreign", 20)
    assert (tmp_path / "foreign/user-data.jsonl").read_text() == "keep me"

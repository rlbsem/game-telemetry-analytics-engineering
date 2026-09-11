"""Synthetic launch fixtures must be reproducible, valid, and honestly bounded."""

import csv
import hashlib
import json
from collections import Counter

import pytest

from telemetry.contract import normalize_event, utc_timestamp
from telemetry.generate import generate


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    directory = tmp_path_factory.mktemp("generated")
    scenario = generate(directory, players=20)
    return directory, scenario


def payloads(directory):
    for path in sorted(directory.glob("*.jsonl")):
        for line in path.read_text(encoding="utf-8").splitlines():
            yield path, json.loads(line)


def test_seed_recreates_identical_receipt_files_and_manifests(generated, tmp_path):
    first, scenario = generated
    assert generate(tmp_path, players=20) == scenario
    assert {p.name: p.read_bytes() for p in first.iterdir()} == {p.name: p.read_bytes() for p in tmp_path.iterdir()}


def test_all_generated_wire_events_conform_and_are_below_publication_cutoff(generated):
    directory, scenario = generated
    as_of = utc_timestamp(scenario["as_of"])
    versions = set(); delayed = 0
    for path, event in payloads(directory):
        row = normalize_event(event)
        assert path.stem == row["received_at"].date().isoformat()
        assert row["event_ts"] < as_of and row["received_at"] < as_of
        delayed += (row["received_at"] - row["event_ts"]).total_seconds() > 86_400
        versions.add(row["schema_version"])
    assert versions == {1, 2}; assert delayed > 0


def test_manifest_hashes_match_files_and_includes_every_receipt_partition(generated):
    directory, _ = generated
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["synthetic"] is True
    assert {item["path"] for item in manifest["files"]} == {p.name for p in directory.glob("*.jsonl")}
    for item in manifest["files"]:
        contents = (directory / item["path"]).read_bytes()
        assert hashlib.sha256(contents).hexdigest() == item["sha256"]
        assert len(contents.splitlines()) == item["rows"]
    assert hashlib.sha256((directory / "expectations.csv").read_bytes()).hexdigest() == manifest["expectations_sha256"]


def test_independent_manifest_exposes_intentional_loss_without_counting_retries(generated):
    directory, scenario = generated
    delivered_ids = {}
    for _, row in payloads(directory): delivered_ids[(row["title_id"], row["event_id"])] = row
    assert len(delivered_ids) < scenario["delivered_rows_including_retries"]
    assert scenario["emitted_events"] - len(delivered_ids) == scenario["transport_lost_events"] > 0
    delivered_counts = Counter((row["received_at"][:10], row["title_id"]) for row in delivered_ids.values())
    with (directory / "expectations.csv").open(newline="", encoding="utf-8") as stream:
        expected = {(row["receipt_date"], row["title_id"]): int(row["expected_events"]) for row in csv.DictReader(stream)}
    deficits = {key: count - delivered_counts[key] for key, count in expected.items() if count > delivered_counts[key]}
    assert deficits == {("2026-01-16", "atlas"): scenario["transport_lost_events"]}


@pytest.mark.parametrize("players", [0, 19, 2001])
def test_generator_refuses_unsupported_sample_size(tmp_path, players):
    with pytest.raises(ValueError): generate(tmp_path, players=players)

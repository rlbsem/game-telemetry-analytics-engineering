-- Reference Redshift DDL, not part of the tested local DuckDB execution path.
-- Apply with a migration identity; ingestion and analyst identities are separate.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS raw.events (
    event_id VARCHAR(128) NOT NULL,
    title_id VARCHAR(128) NOT NULL,
    player_id VARCHAR(128) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    build_id VARCHAR(128) NOT NULL,
    platform VARCHAR(32) NOT NULL,
    country_code CHAR(2) NOT NULL,
    event_name VARCHAR(32) NOT NULL,
    schema_version INTEGER NOT NULL,
    event_ts TIMESTAMP NOT NULL,
    received_at TIMESTAMP NOT NULL,
    event_sequence BIGINT NOT NULL,
    mission_id VARCHAR(128),
    attempt_id VARCHAR(128),
    checkpoint_id VARCHAR(128),
    active_seconds DOUBLE PRECISION,
    error_code VARCHAR(128),
    metadata_json VARCHAR(65535) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    batch_id CHAR(64) NOT NULL,
    ingest_seq BIGINT NOT NULL,
    source_line BIGINT NOT NULL
)
DISTSTYLE AUTO
COMPOUND SORTKEY(received_at, title_id)
ENCODE AUTO;

CREATE TABLE IF NOT EXISTS raw.batches (
    batch_id CHAR(64) NOT NULL,
    source_name VARCHAR(2048) NOT NULL,
    received_min TIMESTAMP,
    received_max TIMESTAMP,
    row_count BIGINT NOT NULL,
    accepted_count BIGINT NOT NULL,
    duplicate_count BIGINT NOT NULL,
    invalid_count BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL,
    loaded_at TIMESTAMP NOT NULL
)
DISTSTYLE ALL
SORTKEY(loaded_at)
ENCODE AUTO;

-- Deliberately no PRIMARY KEY declaration: Redshift would not enforce it.
-- The loader serializes acceptance; dbt checks the physical key invariants.

CREATE TABLE IF NOT EXISTS raw.delivery_expectations (
    receipt_date DATE NOT NULL,
    title_id VARCHAR(128) NOT NULL,
    expected_events BIGINT NOT NULL,
    expected_sessions BIGINT NOT NULL
)
DISTSTYLE AUTO
SORTKEY(receipt_date, title_id)
ENCODE AUTO;

-- Atomic reference load procedure. Requires 001_raw.sql and prevalidated,
-- canonical NDJSON objects plus an S3 COPY manifest. Not wire v1 input.
-- No COMMIT/TRUNCATE/exception swallowing: a raised error rolls back acceptance.
-- The caller records failed attempts in an external immutable audit log after
-- rollback, so a rejected load cannot accidentally leave partial raw events.
CREATE OR REPLACE PROCEDURE ops.sp_accept_event_batch(
    p_batch_id VARCHAR(64),
    p_manifest_uri VARCHAR(2048),
    p_iam_role_arn VARCHAR(512)
)
AS $$
DECLARE
    n_invalid BIGINT;
    n_rows BIGINT;
    n_accepted BIGINT;
    v_last_seq BIGINT;
BEGIN
    IF p_batch_id IS NULL OR LEN(p_batch_id) <> 64
       OR p_manifest_uri IS NULL OR p_manifest_uri NOT LIKE 's3://%'
       OR p_iam_role_arn IS NULL OR p_iam_role_arn NOT LIKE 'arn:aws:iam::%:role/%' THEN
        RAISE EXCEPTION 'Require a content SHA256, an immutable S3 manifest and an IAM role ARN';
    END IF;

    LOCK TABLE raw.events, raw.batches;
    SELECT COUNT(*) INTO n_rows FROM raw.batches WHERE batch_id = p_batch_id AND status = 'loaded';
    IF n_rows > 0 THEN
        RETURN;
    END IF;

    CREATE TEMP TABLE stage_telemetry AS
    SELECT event_id, title_id, player_id, session_id, build_id, platform,
        country_code, event_name, schema_version, event_ts, received_at,
        event_sequence, mission_id, attempt_id, checkpoint_id, active_seconds,
        error_code, metadata_json, payload_hash, source_line
    FROM raw.events WHERE 1 = 0;

    EXECUTE 'COPY stage_telemetry FROM ' || QUOTE_LITERAL(p_manifest_uri)
        || ' IAM_ROLE ' || QUOTE_LITERAL(p_iam_role_arn)
        || ' MANIFEST FORMAT AS JSON ''auto'' TIMEFORMAT ''auto''';

    SELECT COUNT(*) INTO n_invalid FROM stage_telemetry
    WHERE event_id IS NULL OR title_id IS NULL OR player_id IS NULL OR session_id IS NULL
        OR build_id IS NULL OR platform IS NULL OR country_code IS NULL
        OR event_name IS NULL OR event_name NOT IN (
            'session_start', 'session_end', 'heartbeat', 'mission_start',
            'checkpoint_reached', 'player_death', 'mission_complete', 'crash', 'error')
        OR schema_version IS NULL OR schema_version NOT IN (1, 2)
        OR event_ts IS NULL OR received_at IS NULL OR event_ts > DATEADD(minute, 5, received_at)
        OR event_sequence IS NULL OR event_sequence < 0
        OR source_line IS NULL OR source_line < 1
        OR metadata_json IS NULL OR payload_hash IS NULL OR LEN(payload_hash) <> 64
        OR active_seconds < 0;
    IF n_invalid > 0 THEN
        RAISE EXCEPTION 'Canonical staging contains % invalid rows', n_invalid;
    END IF;

    SELECT COUNT(*) INTO n_invalid FROM (
        SELECT title_id, event_id FROM stage_telemetry
        GROUP BY title_id, event_id HAVING COUNT(DISTINCT payload_hash) > 1
    ) collisions;
    IF n_invalid > 0 THEN
        RAISE EXCEPTION 'Conflicting event identities inside batch: %', n_invalid;
    END IF;

    SELECT COUNT(*) INTO n_invalid
    FROM stage_telemetry s JOIN raw.events r
        ON s.title_id = r.title_id AND s.event_id = r.event_id
    WHERE s.payload_hash <> r.payload_hash;
    IF n_invalid > 0 THEN
        RAISE EXCEPTION 'Conflicting event identities against warehouse: %', n_invalid;
    END IF;

    SELECT COALESCE(MAX(ingest_seq), 0) INTO v_last_seq FROM raw.events;
    CREATE TEMP TABLE new_telemetry AS
    SELECT s.* FROM (
        SELECT stage_telemetry.*,
            ROW_NUMBER() OVER (PARTITION BY title_id, event_id ORDER BY source_line) AS copy_number
        FROM stage_telemetry
    ) s
    WHERE copy_number = 1 AND NOT EXISTS (
        SELECT 1 FROM raw.events r WHERE r.title_id = s.title_id AND r.event_id = s.event_id
    );

    INSERT INTO raw.events (
        event_id, title_id, player_id, session_id, build_id, platform,
        country_code, event_name, schema_version, event_ts, received_at,
        event_sequence, mission_id, attempt_id, checkpoint_id, active_seconds,
        error_code, metadata_json, payload_hash, batch_id, ingest_seq, source_line
    )
    SELECT event_id, title_id, player_id, session_id, build_id, platform,
        country_code, event_name, schema_version, event_ts, received_at,
        event_sequence, mission_id, attempt_id, checkpoint_id, active_seconds,
        error_code, metadata_json, payload_hash, p_batch_id,
        v_last_seq + ROW_NUMBER() OVER (ORDER BY title_id, event_id), source_line
    FROM new_telemetry;

    SELECT COUNT(*) INTO n_rows FROM stage_telemetry;
    SELECT COUNT(*) INTO n_accepted FROM new_telemetry;
    INSERT INTO raw.batches
    SELECT p_batch_id, p_manifest_uri, MIN(received_at), MAX(received_at),
        n_rows, n_accepted, n_rows - n_accepted, 0, 'loaded', GETDATE()
    FROM stage_telemetry;
    DROP TABLE new_telemetry;
    DROP TABLE stage_telemetry;
END;
$$ LANGUAGE plpgsql SECURITY INVOKER;

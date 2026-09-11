select
    event_id, title_id, player_id, session_id, build_id, platform, country_code,
    event_name, schema_version, event_ts, received_at, event_sequence,
    mission_id, attempt_id, checkpoint_id, active_seconds, error_code,
    metadata_json, payload_hash, batch_id, ingest_seq, source_line,
    cast(event_ts as date) as event_date,
    {{ telemetry_key(['title_id', 'event_id']) }} as event_key,
    {{ telemetry_key(['title_id', 'session_id']) }} as session_key,
    {{ telemetry_key(['title_id', 'player_id']) }} as player_key,
    {{ telemetry_key(['title_id', 'build_id']) }} as build_key,
    case when attempt_id is not null
        then {{ telemetry_key(['title_id', 'session_id', 'attempt_id']) }} end as attempt_key
from {{ source('raw', 'events') }}

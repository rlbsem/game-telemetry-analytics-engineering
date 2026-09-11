select * from {{ source('raw', 'events') }}
except all
select event_id, title_id, player_id, session_id, build_id, platform, country_code,
       event_name, schema_version, event_ts, received_at, event_sequence, mission_id,
       attempt_id, checkpoint_id, active_seconds, error_code, metadata_json, payload_hash,
       batch_id, ingest_seq, source_line
from {{ ref('fct_events') }}

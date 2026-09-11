select e.event_key
from {{ ref('fct_events') }} e
left join {{ ref('dim_build') }} b on e.build_key = b.build_key
where e.event_ts >= cast('{{ var("as_of") }}' as timestamp)
   or e.received_at >= cast('{{ var("as_of") }}' as timestamp)
   or e.event_ts > e.received_at + interval 5 minute
   or e.event_ts < b.released_at
   or e.schema_version <> b.schema_version
   or coalesce(json_extract_string(e.metadata_json, '$.instrumentation_revision'), '') <> b.instrumentation_revision
   or e.active_seconds < 0
   or e.event_sequence < 0
   or e.ingest_seq <= 0
   or (e.event_name in ('mission_start', 'mission_complete', 'player_death', 'checkpoint_reached')
       and (e.mission_id is null or e.attempt_id is null))
   or (e.event_name = 'checkpoint_reached' and e.checkpoint_id is null)

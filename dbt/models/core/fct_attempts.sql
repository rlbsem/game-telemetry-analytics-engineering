-- An explicit client attempt ID distinguishes retries and revisits to one mission.
with attempts as (
select
    attempt_key, min(title_id) as title_id, min(session_id) as session_id,
    min(session_key) as session_key, min(player_id) as player_id,
    min(player_key) as player_key, min(build_id) as build_id,
    min(build_key) as build_key, min(attempt_id) as attempt_id,
    min(mission_id) as mission_id,
    min(event_ts) as first_observed_event_at,
    min(case when event_name = 'mission_start' then event_ts end) as started_at,
    max(case when event_name = 'mission_complete' then event_ts end) as completed_at,
    sum(case when event_name = 'mission_start' then 1 else 0 end) as start_count,
    sum(case when event_name = 'mission_complete' then 1 else 0 end) as completion_count,
    sum(case when event_name = 'player_death' then 1 else 0 end) as deaths,
    count(distinct case when event_name = 'checkpoint_reached' then checkpoint_id end) as checkpoints_reached,
    max(ingest_seq) as source_ingest_seq
from {{ ref('fct_events') }}
where attempt_key is not null
  and event_name in ('mission_start', 'mission_complete', 'player_death', 'checkpoint_reached')
group by attempt_key
)
select *, start_count = 0 as is_pending,
       start_count = 1 and completion_count = 1 as is_completed
from attempts

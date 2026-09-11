select
    {{ telemetry_key(['title_id', 'player_id', 'event_date']) }} as player_day_key,
    title_id, player_id, player_key, event_date as activity_date,
    count(*) as event_count, count(distinct session_key) as observed_sessions,
    sum(active_seconds_delta) as active_seconds,
    sum(case when event_name = 'session_start' then 1 else 0 end) as sessions_started,
    sum(case when event_name = 'player_death' then 1 else 0 end) as deaths,
    sum(case when event_name = 'mission_complete' then 1 else 0 end) as completions
from {{ ref('int_event_activity') }}
group by title_id, player_id, player_key, event_date

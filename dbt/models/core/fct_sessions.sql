{{ config(materialized='incremental', unique_key='session_key',
          incremental_strategy='delete+insert', on_schema_change='fail') }}

with impacted as (
    select distinct session_key
    from {{ ref('fct_events') }}
    {% if is_incremental() %}
    where ingest_seq > (select coalesce(max(source_ingest_seq), 0) from {{ this }})
    {% endif %}
), session_events as (
    -- Re-read full history of every impacted client session, including old starts.
    select e.*
    from {{ ref('fct_events') }} e
    inner join impacted i using (session_key)
), aggregated as (
    select
        session_key, min(title_id) as title_id, min(session_id) as session_id,
        min(player_id) as player_id, min(player_key) as player_key,
        min(build_id) as build_id, min(build_key) as build_key,
        min(platform) as platform, min(country_code) as country_code,
        min(event_ts) as first_event_at, max(event_ts) as last_event_at,
        min(case when event_name = 'session_start' then event_ts end) as session_start_at,
        max(case when event_name in ('session_end', 'crash') then event_ts end) as session_terminal_at,
        count(*) as event_count,
        sum(case when event_name = 'session_start' then 1 else 0 end) as start_count,
        sum(case when event_name in ('session_end', 'crash') then 1 else 0 end) as terminal_count,
        sum(case when event_name = 'player_death' then 1 else 0 end) as death_count,
        sum(case when event_name = 'error' then 1 else 0 end) as error_count,
        max(case when event_name = 'crash' then 1 else 0 end) = 1 as is_crashed,
        coalesce(max(active_seconds), 0.0) as active_seconds,
        max(ingest_seq) as source_ingest_seq
    from session_events
    group by session_key
)
select *,
    cast(coalesce(session_start_at, first_event_at) as date) as session_date,
    start_count = 1 and terminal_count = 1 as is_complete,
    not (start_count = 1 and terminal_count = 1) as is_partial,
    case when start_count = 1 and terminal_count = 1
        then date_diff('second', session_start_at, session_terminal_at) end as session_duration_seconds
from aggregated

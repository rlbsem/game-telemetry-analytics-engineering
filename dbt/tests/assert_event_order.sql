with ordered as (
    select event_key, event_ts,
           lag(event_ts) over (partition by session_key order by event_sequence, event_id) as prior_event_ts
    from {{ ref('fct_events') }}
)
select event_key from ordered where event_ts < prior_event_ts
union all
select event_key from {{ ref('int_event_activity') }} where active_seconds_delta < 0

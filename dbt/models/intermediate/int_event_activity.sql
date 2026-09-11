with counters as (
    select event_key, cast(active_seconds as decimal(18,3)) - coalesce(lag(cast(active_seconds as decimal(18,3))) over (
        partition by session_key order by event_sequence, event_id
    ), 0) as active_seconds_delta
    from {{ ref('fct_events') }}
    where active_seconds is not null
)
select e.*, coalesce(c.active_seconds_delta, 0.0) as active_seconds_delta
from {{ ref('fct_events') }} e
left join counters c using (event_key)

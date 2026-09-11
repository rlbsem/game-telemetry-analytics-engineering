with ranked as (
    select *, row_number() over (
        partition by title_id, player_id
        order by event_ts, event_sequence, event_id
    ) as first_event_rank
    from {{ ref('fct_events') }}
), last_seen as (
    select player_key, max(event_ts) as last_seen_at
    from ranked group by player_key
)
select r.player_key, r.title_id, r.player_id,
       r.event_ts as first_seen_at, r.event_date as cohort_date,
       r.build_id as cohort_build_id, r.build_key as cohort_build_key,
       l.last_seen_at
from ranked r
inner join last_seen l using (player_key)
where r.first_event_rank = 1

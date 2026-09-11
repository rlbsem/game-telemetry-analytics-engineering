with boundaries as (
    select attempt_key,
           count(distinct mission_id) as missions,
           min(event_sequence) as first_sequence, max(event_sequence) as last_sequence,
           min(case when event_name = 'mission_start' then event_sequence end) as start_sequence,
           max(case when event_name = 'mission_complete' then event_sequence end) as completion_sequence
    from {{ ref('fct_events') }}
    where attempt_key is not null
    group by attempt_key
)
select a.attempt_key
from {{ ref('fct_attempts') }} a
inner join boundaries b using (attempt_key)
left join {{ ref('missions') }} m on a.title_id = m.title_id and a.mission_id = m.mission_id
where a.start_count > 1 or a.completion_count > 1
   or (a.start_count = 0 and a.first_observed_event_at < cast('{{ var("as_of") }}' as timestamp) - interval 48 hour)
   or b.missions <> 1 or m.mission_id is null
   or a.completed_at < a.started_at
   or b.first_sequence <> b.start_sequence
   or b.last_sequence <> b.completion_sequence

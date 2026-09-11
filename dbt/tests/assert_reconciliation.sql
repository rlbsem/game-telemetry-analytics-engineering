with counts as (
    select
      (select count(*) from {{ source('raw', 'events') }}) as raw_events,
      (select count(*) from {{ ref('fct_events') }}) as canonical_events,
      (select coalesce(sum(event_count), 0) from {{ ref('fct_sessions') }}) as session_events,
      (select coalesce(sum(event_count), 0) from {{ ref('fct_player_day') }}) as player_day_events,
      (select coalesce(sum(active_seconds), 0) from {{ ref('fct_sessions') }}) as session_seconds,
      (select coalesce(sum(active_seconds), 0) from {{ ref('fct_player_day') }}) as day_seconds,
      (select count(*) from {{ ref('fct_events') }} e
       left join {{ ref('calendar') }} c on e.event_date = c.date_day
       where c.date_day is null) as events_outside_calendar
)
select * from counts
where raw_events <> canonical_events or canonical_events <> session_events
   or canonical_events <> player_day_events or abs(session_seconds - day_seconds) > 0.000001
   or events_outside_calendar <> 0
   or cast('{{ var("as_of") }}' as timestamp)::date >
      (select max(date_day) + interval 1 day from {{ ref('calendar') }})
   or cast('{{ var("complete_through") }}' as date) >= cast('{{ var("as_of") }}' as timestamp)::date

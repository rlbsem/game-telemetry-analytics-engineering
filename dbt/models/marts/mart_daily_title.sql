with activity as (
    select title_id, activity_date, count(*) as dau,
           sum(active_seconds) as active_seconds, sum(deaths) as deaths,
           sum(completions) as completions
    from {{ ref('fct_player_day') }} group by title_id, activity_date
), sessions as (
    select title_id, session_date as activity_date,
           sum(case when session_start_at is not null then 1 else 0 end) as sessions_started,
           sum(case when is_complete then 1 else 0 end) as complete_sessions,
           sum(case when is_partial then 1 else 0 end) as partial_sessions,
           sum(case when is_crashed then 1 else 0 end) as crashed_sessions,
           count(*) as observed_sessions,
           sum(case when is_complete then cast(active_seconds as decimal(18,3)) end)
               / nullif(sum(case when is_complete then 1 else 0 end),0) / 60.0 as avg_session_active_minutes
    from {{ ref('fct_sessions') }} group by title_id, session_date
), date_spine as (
    select t.title_id, c.date_day as activity_date
    from {{ ref('dim_title') }} t
    cross join {{ ref('calendar') }} c
    where c.date_day >= t.launch_date
      and c.date_day < cast('{{ var("as_of") }}' as timestamp)::date
)
select d.title_id, d.activity_date,
       coalesce(a.dau, 0) as dau, coalesce(s.sessions_started, 0) as sessions_started,
       coalesce(s.complete_sessions, 0) as complete_sessions,
       coalesce(s.partial_sessions, 0) as partial_sessions,
       coalesce(s.crashed_sessions, 0) as crashed_sessions,
       coalesce(s.observed_sessions, 0) as observed_sessions,
       coalesce(a.active_seconds, 0.0) as active_seconds,
       coalesce(a.deaths, 0) as deaths, coalesce(a.completions, 0) as completions,
       s.avg_session_active_minutes,
       s.crashed_sessions::double / nullif(s.observed_sessions, 0) as crash_rate,
       d.activity_date <= cast('{{ var("complete_through") }}' as date) as is_date_closed
from date_spine d
left join activity a using (title_id, activity_date)
left join sessions s using (title_id, activity_date)

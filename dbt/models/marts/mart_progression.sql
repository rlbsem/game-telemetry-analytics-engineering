with aggregated as (
    select title_id, build_id, mission_id,
           count(*) as attempts,
           sum(case when start_count = 1 then 1 else 0 end) as started_attempts,
           sum(case when is_pending then 1 else 0 end) as pending_attempts,
           sum(case when is_completed then 1 else 0 end) as completed_attempts,
           count(distinct case when start_count = 1 then player_key end) as players_started,
           count(distinct case when is_completed then player_key end) as players_completed,
           sum(case when start_count = 1 then deaths else 0 end) as deaths
    from {{ ref('fct_attempts') }}
    group by title_id, build_id, mission_id
)
select b.title_id, b.build_id, m.mission_id, m.mission_order, m.mission_name,
       coalesce(a.attempts, 0) as attempts,
       coalesce(a.started_attempts, 0) as started_attempts,
       coalesce(a.pending_attempts, 0) as pending_attempts,
       coalesce(a.completed_attempts, 0) as completed_attempts,
       coalesce(a.players_started, 0) as players_started,
       coalesce(a.players_completed, 0) as players_completed,
       coalesce(a.deaths, 0) as deaths,
       a.completed_attempts::double / nullif(a.started_attempts, 0) as completion_rate,
       a.deaths::double / nullif(a.started_attempts, 0) as death_per_attempt,
       (a.started_attempts - a.completed_attempts)::double / nullif(a.started_attempts, 0) as unfinished_rate
from {{ ref('dim_build') }} b
inner join {{ ref('missions') }} m using (title_id)
left join aggregated a using (title_id, build_id, mission_id)

select b.title_id, b.build_id, b.build_key, b.released_at,
       b.schema_version, b.instrumentation_revision,
       count(s.session_key) as sessions,
       count(distinct s.player_key) as players,
       coalesce(sum(s.event_count), 0) as event_count,
       coalesce(sum(case when s.is_complete then 1 else 0 end), 0) as complete_sessions,
       coalesce(sum(case when s.is_partial then 1 else 0 end), 0) as partial_sessions,
       coalesce(sum(case when s.is_crashed then 1 else 0 end), 0) as crashed_sessions,
       sum(case when s.is_crashed then 1.0 else 0 end) / nullif(count(s.session_key), 0) as crash_rate,
       sum(case when s.is_complete then cast(s.active_seconds as decimal(18,3)) end)
           / nullif(sum(case when s.is_complete then 1 else 0 end),0) / 60.0 as avg_session_active_minutes,
       sum(case when s.is_complete then s.session_duration_seconds end)
           / nullif(sum(case when s.is_complete then 1 else 0 end),0) / 60.0 as avg_session_wall_minutes,
       coalesce(sum(cast(s.active_seconds as decimal(18,3))), 0.0) as active_seconds,
       coalesce(sum(s.death_count), 0) as deaths,
       coalesce(sum(s.error_count), 0) as errors
from {{ ref('dim_build') }} b
left join {{ ref('fct_sessions') }} s using (build_key)
group by b.title_id, b.build_id, b.build_key, b.released_at,
         b.schema_version, b.instrumentation_revision

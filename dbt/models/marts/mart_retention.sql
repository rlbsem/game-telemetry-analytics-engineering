with player_retention as (
    select p.title_id, p.player_id, p.cohort_date, p.cohort_build_id,
           max(case when d.activity_date = p.cohort_date + interval 1 day then 1 else 0 end) as d1_active,
           max(case when d.activity_date = p.cohort_date + interval 7 day then 1 else 0 end) as d7_active
    from {{ ref('dim_player') }} p
    left join {{ ref('fct_player_day') }} d
      on p.player_key = d.player_key
     and d.activity_date in (p.cohort_date + interval 1 day, p.cohort_date + interval 7 day)
    group by p.title_id, p.player_id, p.cohort_date, p.cohort_build_id
), cohorts as (
    select title_id, cohort_date, cohort_build_id,
           count(*) as cohort_players, sum(d1_active) as observed_d1_players,
           sum(d7_active) as observed_d7_players,
           cohort_date + interval 1 day <= cast('{{ var("complete_through") }}' as date) as d1_is_mature,
           cohort_date + interval 7 day <= cast('{{ var("complete_through") }}' as date) as d7_is_mature
    from player_retention group by title_id, cohort_date, cohort_build_id
), delivery_quality as (
    select title_id, bool_or(missing_manifest or received_events <> expected_events) as has_delivery_gap
    from {{ ref('mart_launch_health') }} group by title_id
)
select c.title_id, cohort_date, cohort_build_id, cohort_players, d1_is_mature, d7_is_mature,
       coalesce(q.has_delivery_gap, true) as has_delivery_gap,
       case when d1_is_mature then observed_d1_players end as d1_players,
       case when d7_is_mature then observed_d7_players end as d7_players,
       case when d1_is_mature then observed_d1_players::double / cohort_players end as d1_retention,
       case when d7_is_mature then observed_d7_players::double / cohort_players end as d7_retention
from cohorts c
left join delivery_quality q on c.title_id = q.title_id

select * from {{ ref('mart_progression') }}
where completed_attempts > started_attempts or players_completed > players_started
   or attempts <> started_attempts + pending_attempts or deaths < 0
   or (started_attempts = 0 and (completion_rate is not null or unfinished_rate is not null))
   or (started_attempts > 0 and abs(completion_rate + unfinished_rate - 1.0) > 0.000000001)

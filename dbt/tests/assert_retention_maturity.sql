select * from {{ ref('mart_retention') }}
where cohort_players <= 0
   or (d1_is_mature and (d1_players is null or d1_retention is null))
   or (not d1_is_mature and (d1_players is not null or d1_retention is not null))
   or (d7_is_mature and (d7_players is null or d7_retention is null))
   or (not d7_is_mature and (d7_players is not null or d7_retention is not null))
   or d1_players > cohort_players or d7_players > cohort_players
   or d1_players < 0 or d7_players < 0
   or abs(d1_retention - d1_players::double / cohort_players) > 0.000000001
   or abs(d7_retention - d7_players::double / cohort_players) > 0.000000001

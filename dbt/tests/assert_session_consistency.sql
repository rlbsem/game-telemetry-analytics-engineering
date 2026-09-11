with context as (
    select session_key,
           count(distinct player_id) as players, count(distinct build_id) as builds,
           count(distinct platform) as platforms, count(distinct country_code) as countries,
           count(*) - count(distinct event_sequence) as duplicate_sequences,
           min(event_sequence) as first_sequence, max(event_sequence) as last_sequence,
           min(case when event_name = 'session_start' then event_sequence end) as start_sequence,
           max(case when event_name in ('session_end', 'crash') then event_sequence end) as terminal_sequence
    from {{ ref('fct_events') }} group by session_key
)
select s.session_key
from {{ ref('fct_sessions') }} s
inner join context c using (session_key)
where c.players <> 1 or c.builds <> 1 or c.platforms <> 1 or c.countries <> 1
   or c.duplicate_sequences <> 0
   or s.start_count > 1 or s.terminal_count > 1
   or c.start_sequence <> c.first_sequence
   or c.terminal_sequence <> c.last_sequence
   or s.session_terminal_at < s.session_start_at
   or s.first_event_at < s.session_start_at
   or s.last_event_at > s.session_terminal_at
   or (s.is_complete and s.active_seconds > s.session_duration_seconds)
   or (s.is_partial and s.session_duration_seconds is not null)

with delivered as (
    select cast(received_at as date) as receipt_date, title_id,
           count(*) as received_events, count(distinct session_key) as received_sessions,
           count(distinct build_key) as observed_builds,
           sum(case when event_name = 'checkpoint_reached' then 1 else 0 end) as checkpoint_events,
           sum(case when event_name = 'session_start' then 1 else 0 end) as session_start_events,
           sum(case when event_name = 'crash' then 1 else 0 end) as crash_events,
           max(date_diff('second', event_ts, received_at)) as max_delivery_delay_seconds
    from {{ ref('fct_events') }}
    group by cast(received_at as date), title_id
), expected as (
    select * from {{ source('raw', 'delivery_expectations') }}
    where receipt_date < cast('{{ var("as_of") }}' as timestamp)::date
)
select coalesce(e.receipt_date, d.receipt_date) as receipt_date,
       coalesce(e.title_id, d.title_id) as title_id,
       e.expected_events, e.expected_sessions,
       coalesce(d.received_events, 0) as received_events,
       coalesce(d.received_sessions, 0) as received_sessions,
       coalesce(d.observed_builds, 0) as observed_builds,
       coalesce(d.checkpoint_events, 0) as checkpoint_events,
       coalesce(d.session_start_events, 0) as session_start_events,
       coalesce(d.crash_events, 0) as crash_events,
       d.max_delivery_delay_seconds,
       coalesce(d.received_events, 0)::double / nullif(e.expected_events, 0) as event_delivery_ratio,
       coalesce(d.received_sessions, 0)::double / nullif(e.expected_sessions, 0) as session_delivery_ratio,
       e.title_id is null as missing_manifest
from expected e
full outer join delivered d using (receipt_date, title_id)

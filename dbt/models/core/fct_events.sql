{{ config(materialized='incremental', unique_key='event_key',
          incremental_strategy='delete+insert', on_schema_change='fail') }}

-- No event-time lookback: arbitrarily old arrivals receive a new ingest_seq.
select * from {{ ref('stg_events') }}
{% if is_incremental() %}
where ingest_seq > (select coalesce(max(ingest_seq), 0) from {{ this }})
{% endif %}

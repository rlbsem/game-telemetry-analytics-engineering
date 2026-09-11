-- Redshift diagnostic queries; reviewed against AWS docs, not measured here.

SELECT query_id, query_label, status, start_time, end_time,
    elapsed_time / 1000000.0 AS elapsed_seconds,
    queue_time / 1000000.0 AS queue_seconds,
    execution_time / 1000000.0 AS execution_seconds,
    returned_rows, returned_bytes
FROM sys_query_history
WHERE start_time >= DATEADD(day, -1, GETDATE())
ORDER BY start_time DESC;

SELECT "schema", "table", diststyle, sortkey1, size AS size_mb,
    tbl_rows, skew_rows, unsorted, stats_off
FROM svv_table_info
WHERE "schema" IN ('raw', 'analytics')
ORDER BY size DESC;

SELECT title_id, event_id, COUNT(*) AS copies
FROM raw.events
GROUP BY title_id, event_id
HAVING COUNT(*) > 1;

select {{ telemetry_key(['title_id', 'build_id']) }} as build_key,
       title_id, build_id, released_at, schema_version, instrumentation_revision
from {{ ref('builds') }}

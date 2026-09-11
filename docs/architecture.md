# Architecture and decision record

The implementation was substantially rebuilt around an executable local path: deterministic synthetic telemetry, strict admission, dbt models/tests, launch monitoring, immutable publication, and a documented scale-out path. The game-analytics scope, independent synthetic framing and MIT license are retained. No commit history is fabricated.

## Data boundaries

1. The generator writes deterministic, unordered NDJSON partitioned by **trusted receipt date**. Its sealed inventory records object hashes. A separate collector expectation table is generated before simulated transport loss.
2. Contract validation handles shape, strict types, UTC timestamps, event-specific requirements and v1/v2 normalization. The raw adapter admits a bounded batch transactionally. Raw means typed accepted telemetry here; original wire bytes remain in landing for replay.
3. dbt staging adds stable, length-prefixed composite keys. Canonical events retain batch ID, physical source line, immutable payload hash and ingestion sequence. Session facts recompute affected entities; smaller downstream concepts rebuild.
4. Models/tests operate on a private candidate. The publication seal covers every raw and analytical relation's columns and contents. Only a validated frozen copy is exported, and an atomic pointer switches readers to it. Run results and the dbt manifest provide execution and transformation lineage.

The ledger gives **idempotent admission**, not a claim of end-to-end exactly-once networking. An event retransmission can arrive with a different receipt timestamp; the first admitted copy supplies the canonical receipt date. Counters and other client fields must remain identical. Malformed or conflicting batches preserve rejection reasons and load no events.

## Why this incremental algorithm

An event-date window can silently lose arbitrarily late terminal events. The append-only arrival sequence identifies affected session keys without discarding older history. `fct_sessions` joins those keys against **all** canonical events and lets dbt perform transactional `delete+insert`. A failed model cannot leave a pre-hook deletion gap.

Dimensions, attempts, player days and marts intentionally rebuild in this sample. Correctly propagating changed first-seen events through cohort membership is more valuable here than maintaining several fragile watermarks. At scale, propagate affected player/date/cohort keys and test partition replacement against full-history results.

## Failure and observation semantics

Transport delays are different from client event order. The pipeline preserves both timestamps and validates monotone client sequence/order. A missing session boundary produces a partial fact, not a made-up complete session. Missing attempt starts receive a 48-hour event-time grace, after which a missing transition blocks publication.

`as_of` is an exclusive midnight UTC receipt boundary. `complete_through` is the inclusive event-date observation cutoff used for retention maturity. That cutoff does not repair or conceal loss. Retention exposes a conservative title-wide `has_delivery_gap`; launch-delivery rows identify affected dates.

## Operational constraints

DuckDB is an embedded analytical database selected so the SQL, dbt tests and replay path can run locally without cloud credentials. There is one candidate writer. Airflow uses a shared local volume and serial intervals, not a distributed executor sharing a database file. Publication snapshots consume disk and require retention management.

See [metric contracts](metrics.md), [event contract](telemetry_contract.md), [recovery procedures](operations.md) and [Redshift scale-out design](redshift.md).

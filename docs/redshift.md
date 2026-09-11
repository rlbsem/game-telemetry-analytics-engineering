# Redshift and production-volume design

**Boundary:** the executed warehouse is DuckDB. The SQL under [warehouse/redshift](../warehouse/redshift/) is a deployment reference, not a tested Redshift target or a deployed AWS service. The dbt project contains DuckDB syntax. A Redshift adapter/dispatch pass, CI warehouse and result-equivalence tests are required before claiming portability. No assumptions are made about any specific game studio's internal systems.

## Ingestion boundary

Keep immutable wire objects in S3 partitioned by trusted receipt date/hour and producer/title, with checksums, schema version and collector manifest. Avoid event-time-only landing partitions because delayed offline clients change past analytical dates. Give ingestion, model transformation and analyst identities separate permissions; use encrypted storage and managed IAM role credentials rather than embedded keys.

Normalize and validate into bounded canonical batches. Load balanced objects with Redshift `COPY` into staging, then accept atomically. [002_accept_batch.sql](../warehouse/redshift/002_accept_batch.sql) demonstrates a serialized admission procedure: ledger check, staging load, within-batch/conflicting-identity checks, deterministic row numbering for new ingestion sequence values, insert and receipt in one transaction.

AWS Glue has a useful optional role when many small client objects need distributed validation, v1/v2 normalization and compaction before COPY. A bounded Glue job would read only the sealed receipt inventory, apply the same golden contract fixtures, quarantine failures with object/line provenance, write normalized Parquet or canonical JSON into a new run prefix, and publish a success manifest only when every output completes. Preserve payload hashes and event IDs through compaction. This is an architecture pattern; no Glue job or AWS execution is claimed or required locally.

## Physical layout follows measured workload

[001_raw.sql](../warehouse/redshift/001_raw.sql) starts raw events with `DISTSTYLE AUTO` and a compound receipt-time/title sort key for the ingestion workload. That is a starting hypothesis, not a universal fastest layout. Monitor skew and query plans under representative title sizes and peak launch concurrency.

| Relation / query | Candidate design to benchmark | Reason / risk |
|---|---|---|
| Raw acceptance and recent receipt scans | Receipt-time leading sort; AUTO distribution | Prunes recent arrival ranges; event-time replay is a separate access pattern |
| Canonical events and affected sessions | Candidate session-hash distribution, compatible session-fact distribution | Session joins can be colocated, but skew must be measured |
| Player-day and retention | Candidate player-hash distribution on both sides; title/date sort | Avoid redistributing large player joins |
| Small title/build/mission dimensions | AUTO or ALL after size/churn review | Replication is useful only while dimensions remain small |
| Analyst marts | Compact grain, date-leading sort and explicit columns | Scan reduced reusable facts rather than raw event JSON |

Do not distribute large facts on a two-valued title or platform key. Do not recommend a sort/distribution key solely because it is present in a join. Use workload evidence.

## Propagating late data efficiently

The local model proves semantics but scans history for downstream rebuilds. At scale, materialize newly accepted event/session keys, then propagate changed player/date/attempt/cohort keys. Recompute the **whole entity or whole affected partition**, including its history, and replace it transactionally. An earlier first-seen event can move both the old and new cohort denominator; rebuilding only the new date is wrong.

A bounded recent window can be an optimization only with an exception lane for older arrivals. Track late-arrival distributions and scan amplification, then route out-of-window keys to explicit backfill. Keep event-time correction ranges separate from receipt-time ingestion checkpoints. Validate incremental partitions against full rebuilds on sampled titles and hand-computable boundary fixtures before rollout.

Keep transformation and analyst workloads isolated with suitable WLM queues or workgroups and query limits. Publish tested model versions or a transactional schema pointer; do not expose partially refreshed dependent marts.

## Performance evidence to collect

[monitor_queries.sql](../warehouse/redshift/monitor_queries.sql) provides query-history, table-health and physical-duplicate checks. Establish a repeatable benchmark: same data volume, title skew, receipt/event range, concurrency and cache conditions; compare p50/p95 elapsed/queue/execution time, rows and bytes read, spill and join redistribution.

The repository makes no throughput, Redshift cost or production performance claim from its 27,000-event sample. Git-versioned raw DDL is sufficient for this local project. Liquibase or another migration system would become justified when raw schemas have an independent release lifecycle, requiring explicit producer compatibility checks and rollback plans.

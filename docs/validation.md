# Engineering validation and known limits

This document records the failure modes examined during implementation, the corrections made, the executed validation boundary, and the remaining limits. It evaluates the repository as an analytics-engineering system rather than claiming production-scale deployment or any company-specific architecture.

## Defects found and corrected

| Failure mode examined | Implemented correction | Evidence |
|---|---|---|
| SQL files that only resemble dbt | Added an executable profile, ingestion boundary, seeds, sources, graph, models and actual dbt tests | Full build and generated manifest/run results |
| Cosmetic orchestration | Replaced echo steps with durable interval staging, validation, transactional load, dbt, monitor and gated publication | Real task functions executed across two receipt days; runtime failure/retry tests |
| Sessions fabricated from min/max events | Retained actual start/terminal boundaries, explicit partial facts and cumulative active counters | Hand-computed overnight/missing-end fixture |
| Incorrect completion denominator / repeated visits collapsed | Client attempt IDs and started/completed/pending semantics; contradictions fail tests | Two attempts at the same mission plus missing-start/grace fixtures |
| Late data ignored by a date lookback | Arrival sequence frontier and whole-session recomputation | An 18-day-late terminal repairs a historical session |
| Stale cohort membership | Rebuild first-observed player/cohort attribution, including first build | Earlier late activity moves a player from v2 to a v1 cohort |
| False zero retention for recent cohorts | Null immature numerators/rates; separate delivery-gap qualifier | Mature D1/D7 and immature D7 fixtures; intentional transport loss |
| Replay changes numerical outputs | Sum exact fixed-precision active totals and integer wall totals before division | All five full-size mart hashes equal after a clean replay |
| Malformed data partially admitted | Strict JSON/version/identifier/time validation and whole-batch rejection | Mixed good/bad batch rollback, conflicting identities and rejected retry tests |
| Ambiguous JSON or stale landing files | Reject duplicate JSON keys; verify sealed inventory and hashes | Duplicate-key, changed-file, extra-file and altered-expectation tests |
| Python row binding dominates a tiny OLAP load | Native typed-JSON batch conversion instead of per-cell conversion probes | Local profile isolated the overhead; full regression and replay rerun after change |
| Validated models changed before publication | Content seal checks actual raw/model contents; private candidate and atomic published pointer | Mart mutation is refused and previous pointer stays unchanged |
| In-place raw correction silently bypasses incremental facts | Raw-to-canonical fidelity test; explicit full-refresh/replay path | Incremental build fails on changed raw content, full refresh repairs it |
| Copied DuckDB views point to a missing catalog | Retain database basename in the immutable generation | Publication and two-day predecessor-copy tests |
| Full-refresh adapter backups leak into the delivered schema | Clean exact manifest-owned incremental backups without CASCADE | Full refresh/replay comparison and final catalog inspection |
| Fresh installation depends on the working environment | Locked runtime/build dependencies and editable-install instructions | Separate fresh Python environment, dependency check and complete test suite |
| Cloud capabilities overstated | Runnable local target clearly separated from unexecuted Redshift/Glue patterns | README and deployment boundary notes |

## Engineering evidence

The SQL demonstrates reusable OLAP grains, composite joins, incremental entity repair, exact-day cohorts, ratio denominators and model consistency. Business questions map to event contracts and shared marts instead of asking analysts to repeat core logic. The source/build/instrumentation registry and example launch agreement show how analytics engineering can collaborate with studio instrumentation owners and analysts. The synthetic launch produces independently testable crash, progression, telemetry and retention signals.

Repeatability is demonstrated through immutable inputs, a content-addressed batch ledger, deterministic generation, negative tests, rebuild/replay equality and isolated publication. Source control is represented by a clean repository layout, dependency lock and CI workflow; no artificial commit history is provided. Python and dbt are implemented. Airflow operators call real tested workflow functions. Redshift/AWS concerns are supported by reference SQL and explicit scale-out tradeoffs rather than an invented deployment.

## Validation boundary

The final local run passed **131 Python/integration/task-runtime tests** and **95 dbt data tests across 14 models and four seeds**, using Python 3.12 on Windows, dbt-core 1.9.8, dbt-duckdb 1.9.6 and DuckDB 1.3.2. The full sample plus clean replay check completed in 34.06 seconds on this validation host; this is local execution evidence, not a production throughput benchmark. See [verification.json](example_outputs/verification.json) for sample counts, environment and exact replay hashes. The generated [launch report](example_outputs/report.md) comes from the tested warehouse.

Docker and a working Linux execution environment were unavailable on this host. Therefore the Airflow scheduler, DagBag import and Compose deployment are **not claimed as locally executed**. Linux CI contains the actual DagBag check, while local integration tests execute its task runtime through real CLI processes. No CI success is claimed before this repository is pushed and the workflow runs. Redshift, Glue and S3 were not provisioned or benchmarked.

## Known limits and tradeoffs

- This is a small, single-writer correctness implementation. Full downstream rebuilds, historical identity joins and full-content publication seals need partition/key-based designs at production scale.
- The contract supports one client session context and cumulative active counters. Account/device identity reconciliation, offline clock repair, cross-play linkage and real privacy/erasure policies are intentionally outside this synthetic example.
- First-observed cohorts depend on available history. Receipt loss can bias player populations even when an observation window is mature. Threshold monitors are descriptive triage, not significance tests or causal release attribution.
- Reporting-event-day allocation conserves active time but does not locate gameplay exactly across midnight between sparse counter samples. This limitation is explicit and testable.
- The raw ingestion reference has a single serialized writer. A distributed design needs a committed batch frontier, partition ownership and external publication/version semantics; a global maximum sequence is insufficient under arbitrary concurrent commits.
- Liquibase, streaming infrastructure, Kubernetes and additional services were omitted because they do not improve the demonstrated contract/model/replay problem. Glue is an optional compaction/normalization pattern, not fabricated project experience.

The repository demonstrates the implemented local architecture, tests, replay behavior, data-quality controls and documented scale-out considerations. It does not claim production-scale deployment, cloud execution, or affiliation with a game studio.

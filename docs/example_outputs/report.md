# Synthetic launch report

As of **2026-01-23T00:00:00Z**; retention mature through **2026-01-21**.

27,079 accepted events · 1,857 observed sessions · 320 title-scoped players.

Signals are intentionally injected; rates are descriptive, not causal. Missing deliveries can bias results even after the observation window has matured.

| Title | Build | Sessions | Crashed | Crash rate | Avg active minutes |
|---|---|---:|---:|---:|---:|
| atlas | 1.0.0 | 345 | 7 | 2.0% | 16.59 |
| atlas | 1.1.0 | 586 | 71 | 12.1% | 14.08 |
| echo | 1.0.0 | 318 | 9 | 2.8% | 15.84 |
| echo | 1.1.0 | 608 | 13 | 2.1% | 16.21 |

## Triage

- **crash_rate_regression** — atlas 1.1.0 . Gameplay/platform engineer: stratify platform and error_code, inspect release change; association is not causal proof.
- **progression_bottleneck** — atlas 1.1.0 M02. Mission designer + analyst: inspect deaths and open-attempt share; unfinished attempts are not proven abandonment.
- **delivery_volume_loss** — atlas 2026-01-16 . Ingestion owner: compare sealed collector manifest and receipt partitions; recover missing objects before trusting volume trends.
- **heartbeat_instrumentation_loss** — atlas 1.1.0 . Client telemetry owner: compare instrumentation revisions and emission by build; terminal counters preserve totals but interval coverage degrades.
- **cohort_retention_shift** — echo 1.1.0 . Game analyst: inspect first-observed cohort mix and acquisition windows; a cohort-build association does not establish release causality.

Analyst-ready marts are generated alongside this report during a demo run.

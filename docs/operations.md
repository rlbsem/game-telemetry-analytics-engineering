# Running, replaying and recovering the pipeline

## Local workflow

Run from the repository root after the README installation. The sample cutoff is historical and explicit; no metrics use the wall clock to guess which dates are complete.

```bash
python -m telemetry generate --output .local/landing
python -m telemetry validate --input .local/landing
python -m telemetry load --input .local/landing --database .local/candidate.duckdb
python -m telemetry build --database .local/candidate.duckdb
python -m telemetry monitor --database .local/candidate.duckdb --output .local/triage
python -m telemetry publish --database .local/candidate.duckdb --output .local/published
```

Validation rejects malformed/conflicting input. Identical retransmissions are expected and counted. Loading each file is atomic; a directory may contain earlier successfully loaded files when a later file fails. Retrying resumes through the content-addressed ledger. The private candidate is never automatically exposed to readers.

`build` invalidates any prior success receipt before it starts. It runs dbt models and tests, refuses skipped/warning/failed results, generates docs, then records a content seal. An unchanged dbt model graph is not sufficient: mutation of actual raw or mart data after validation makes publication fail. The seal is a correctness check within a trusted workspace, not a security signature against an operator who can rewrite the database and its ledger.

`publish` keeps the database basename, checks the frozen copy, exports its marts, then atomically replaces `current.json`. Readers resolve paths relative to the publication directory. An unchanged candidate/observation boundary reuses its generation. A failed export or quality check leaves the old pointer intact. One publisher owns the local lock; do not use this adapter as a multi-host warehouse.

## Airflow execution

The actual DAG is [game_telemetry.py](../orchestration/dags/game_telemetry.py). Each PythonOperator calls tested [runtime.py](../orchestration/runtime.py) functions. Those functions execute the same CLI with subprocess argument lists, preserve task logs, and pass only paths through XCom.

| Task | Durable boundary |
|---|---|
| Stage receipt interval | Freeze exactly one midnight-to-midnight UTC receipt day and its expectation inventory; copy last published database into a private candidate |
| Validate contract | Strict file validation before loading |
| Load warehouse | Canonical normalization, immutable identity registry and transactional batch ledger |
| dbt build and test | Entire dependency graph and quality gates |
| Monitor launch health | Shared mart queries, exported threshold report with ownership/action |
| Publish generation | Recheck predecessor and validation; atomically advance reader pointer |

All edges use `all_success`; publication is the final leaf. The DAG permits one active run/task, two retries with exponential delay, task timeouts and a two-hour run deadline. Failure callbacks write a structured alert under `state/alerts/` and an error log. No external notification service is configured or claimed. Production would route that event to the studio's existing incident system with title/build/run identifiers.

The DAG accepts receipt intervals, not event-time slices; a Jan 2 file may correct a Jan 1 session. The first interval starts Jan 1, 2026. A gap or backward interval is refused, and subsequent publication must still have the predecessor captured at staging. This is why replay uses isolated state. Airflow defines logical intervals and supports historical runs separately from the execution clock; see its 2.10.5 interval and backfill documentation.

The container configuration pins Airflow 2.10.5 and separates its dependencies from dbt in a second virtual environment. It uses LocalExecutor with PostgreSQL for orchestration metadata and a shared local volume for telemetry. These are local demo credentials and a loopback-only web port. The task runtime was exercised here, but Docker, the scheduler and AWS were not executed on the Windows validation host. Linux CI includes a real DagBag import and dependency-graph check.

With Docker available, run the optional container workflow from the repository root:

```bash
docker compose -f orchestration/compose.yaml build
docker compose -f orchestration/compose.yaml run --rm init
docker compose -f orchestration/compose.yaml run --rm cli /opt/airflow/telemetry-venv/bin/python -m telemetry generate --output /opt/airflow/telemetry/landing
docker compose -f orchestration/compose.yaml run --rm cli python -m orchestration.check_dag
docker compose -f orchestration/compose.yaml up -d scheduler webserver
docker compose -f orchestration/compose.yaml run --rm cli dags backfill --start-date 2026-01-01 --end-date 2026-01-22 game_telemetry_daily
```

The DAG is paused on creation and bounds the synthetic history. Use the forward backfill instead of triggering today's date. Open the local web UI at `http://localhost:8080` with the configured demo credentials to inspect the graph. The included Compose file is not a hardened production deployment.

## Freshness and closure

For daily intervals, the task runtime sets inclusive `complete_through = interval_end date - 3 days`, waiting 48 hours after an event day's end. The generator caps delayed delivery at 48 hours and explicitly provides its own observation cutoff for the complete demo. Counters and event dates may cross midnight. Session starts run through Jan 21; some events occur Jan 22.

This cutoff expresses observation maturity, not proof that missing telemetry exists nowhere. Delivery manifests and `has_delivery_gap` remain visible. A real feed needs producer closure attestations; if a partition is still expected, postpone closure or hold publication. Late events beyond the normal budget are admitted, audited and allowed to restate history rather than silently discarded.

## Recovery runbook

| Failure | Recovery |
|---|---|
| Bad envelope / unknown version | Inspect task log or `ops.rejections`; obtain a corrected producer batch and reviewed schema mapping. Do not bypass validation. |
| Conflicting event identity | Compare immutable client payload hashes and source files; repair source identity semantics. |
| Missing attempt start beyond grace | Recover the corresponding start; an old orphan fails immediately until repaired. |
| Unknown build or instrumentation revision | Review release catalog and contract fixture, then rebuild. |
| dbt failure | Inspect `<database-stem>-dbt/build.log` and `run_results.json`; repair the model or source issue and rerun build. Prior published data stays readable. |
| Task retry after load | Reuse staged files and content IDs; the ledger prevents duplicate admission. |
| Publication completed but acknowledgement lost | Rerun publish; matching interval/database content recovers the existing pointer. |
| Staged input changed | Restore exact immutable staged input or create a fresh run/candidate. |
| Stale candidate / history gap | Finish missing intervals in order or use separate replay state. |
| Process died holding `.publish.lock` | Confirm no publisher is alive, then remove only that stale lock and retry. |

Semantic model changes can be rebuilt with `build --full-refresh`. To test corrected historical logic against a fresh warehouse:

```bash
python -m telemetry demo --workspace .local/rebuild-01
```

Replay refuses an existing candidate path. Compare its CSV marts and business definitions with the prior publication before using its separate pointer. A historical as-of replay needs receipt files and expectations strictly before that boundary; supplying future files fails explicitly rather than silently filtering them.

Destructible development means building in disposable `.local/` state and reproducing from immutable inputs, not deleting someone else's database. Retain published generations according to an explicit recovery policy.

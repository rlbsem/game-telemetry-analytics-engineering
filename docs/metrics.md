# Metric contracts

These definitions are implemented once in dbt and consumed by the report and monitor.
All identities and data are synthetic. Title IDs scope players, builds, sessions and
missions; a player ID repeated across titles is **not** evidence of the same person.
The small release and mission catalogs are checked-in seeds. They deliberately reuse
build and mission IDs across titles to exercise composite joins.

| Model | Grain | Purpose / owning discipline |
|---|---|---|
| `stg_events` | Title, event ID | Canonical envelope and stable keys; analytics engineering |
| `fct_events` | Title, event ID | Append-only event history and lineage to ingestion receipt |
| `dim_title` | Title | Title catalog and example studio liaison |
| `dim_build` | Title, build | Release time, schema version, instrumentation revision; studio instrumentation owner |
| `dim_player` | Title, player | First/last observed activity and deterministic cohort attribution |
| `fct_sessions` | Title, client session ID | One reusable session definition |
| `fct_attempts` | Title, session, client attempt ID | Mission visit, including retries and revisits |
| `fct_player_day` | Title, player, UTC event date | Active population and allocated active playtime |
| `mart_daily_title` | Title, UTC date | Calendar-complete daily activity and session metrics |
| `mart_build_health` | Title, build | Session, stability and duration comparison |
| `mart_progression` | Title, build, mission | Attempt completion, unfinished attempts and deaths |
| `mart_retention` | Title, cohort date, first observed build | Exact D1/D7 retention with maturity |
| `mart_launch_health` | Title, UTC receipt date | Delivery completeness against an independent manifest |

Analytics engineering owns key/grain/incremental correctness. A studio instrumentation
owner approves event semantics and release mappings. A game analyst approves business
denominators, release comparison eligibility and interpretation. Those are illustrative
responsibilities, not claims about any real studio organization.

## Sessions and time

A session is the client-issued ID for one title. The warehouse does not silently make
new sessions after inactivity. Player, build, platform and country must stay constant
within it. IDs may repeat across titles. A complete session has exactly one real start
and one real terminal (`session_end` or `crash`). Multiple terminals, events after a
terminal, events before the start, sequence inversion and decreasing active counters
fail publication. A crash terminates a session and is counted once per session.

Missing start or terminal means `is_partial = true`; timestamps are not invented.
`session_date` is the actual UTC start date, or first observed date for missing-start
partials. `sessions_started` counts only real starts. `observed_sessions` includes
partials. A partial session can become complete after late delivery.

`active_seconds` is the maximum observed cumulative active gameplay counter. It is an
observed lower bound for incomplete telemetry. Wall-clock duration is a different
quantity: terminal minus start, null for partial sessions. Complete-session averages
exclude partial sessions and include crash-terminated sessions; inspect the partial
share beside the average because changing telemetry completeness can bias it. A
complete session has a required terminal counter; missing counter samples fail admission.

Daily active time is the sum of counter increments, assigned to the UTC date of the
event reporting the increment. This conserves the session counter total. It does not
claim exact midnight allocation: a cross-midnight reporting interval belongs entirely
to its reporting date. A more demanding use case would require interval boundaries or
more frequent counter sampling. This explicit approximation avoids inventing uniform
gameplay between sparse heartbeats. Counter resets require a new session.

Counters have millisecond resolution. SQL computes their deltas and aggregates with
fixed-precision decimals, and divides summed totals only after aggregation. This avoids
floating-point reduction-order differences in replayed duration averages.

## Daily activity and build health

**DAU:** distinct observed players per title and UTC event-occurrence date, including
players with only partial telemetry. This measures observed instrumented activity,
not all installed accounts. `fct_player_day` has exactly one row per active player/day.
Session counts use session attribution date and do not replace the DAU definition.
A session crossing midnight can contribute to two DAUs and only one session-start day.

**Crash rate:** distinct observed crashed sessions / all observed sessions. It is
null when the denominator is zero. Do not divide crash event count by player count or
average daily percentages. Missing crash telemetry biases this estimate downward;
delivery and partial-session indicators must be evaluated with it.

The date spine creates zero-activity rows from title launch until the last full UTC
date before `as_of`. `is_date_closed` uses `complete_through`; provisional days remain
visible. The CLI requires midnight UTC `as_of`. Schema versions are checked against
the title/build release catalog so a version change cannot silently disappear into
aggregate counts. The raw contract normalizes v1 milliseconds and v2 seconds into
the same semantic active counter before these models run.

Build comparisons are descriptive. Release windows have different exposure lengths,
player mixes and progression histories. Cohort, platform, geography and sufficient
sample sizes belong in an analyst's comparison design; a before/after difference is
not evidence that the build caused it. This sample does not claim experiment results.

## Progression

An attempt is one visit to a mission, with a stable client attempt ID scoped by
title/session. A death does not implicitly start another attempt. A retry with a new
mission start receives a new attempt ID. Completion requires the same attempt's start;
start must precede mission activity in sequence and completion must be its last event.
Changing mission ID inside one attempt fails. Mission catalog order is display order,
not an assumed universal rule that all games must progress linearly.

Missing attempt starts have a 48-hour event-time grace, measured from the earliest
observed event in the attempt to `as_of`. They remain visible as `pending_attempts`;
their completions and deaths do not enter rate numerators. Once the grace expires,
publication fails until the start is recovered or the input is explicitly repaired.
Contradictory observed transitions fail immediately. A newly delivered old orphan
does not receive a new grace period simply because it arrived late.

| Metric | Numerator | Denominator |
|---|---|---|
| `completion_rate` | Started attempts with observed completion | Started attempts |
| `unfinished_rate` | Started attempts without observed completion | Started attempts |
| `death_per_attempt` | Death events in started attempts | Started attempts |
| `players_completed` | Distinct players with a completed attempt | Count, not a rate |

`unfinished_rate` is a practical bottleneck indicator, **not confirmed abandonment**.
It includes attempts still in progress, crashes, exits and missing completion events.
Repeated-player attempts remain separate; a player can fail several visits before
completing. Inspection of attempt counts, death frequency and delivery health prevents
mistaking an instrumentation failure for a difficulty spike. Zero-start rows have
null rates, not zero percent completion.

## Retention and closure

A cohort begins at a player's first **observed** event date within a title. This is
not registration, acquisition or proven first-ever play. Cohort build is selected by
`event_ts`, then `event_sequence`, then `event_id`; ingestion order cannot change a tie.
Earlier late-arriving activity can legitimately move a player to an earlier cohort or
build, so the small player dimension and retention mart are rebuilt from history.

D1 and D7 are exact calendar-day returns on cohort date +1 and +7 in UTC. A return
on D2 is not D1 retention. The denominator is all observed players in the cohort.
The numerator and rate are null until the corresponding return date is at or before
`complete_through`. Maturity never uses the latest observed event, which would hide
silent missing days. `complete_through` is an explicit coverage assertion; it is not
inferred proof that delivery is complete. The separate `has_delivery_gap` flag marks
a title with any manifest loss or unmanifested delivery in the observed history. It is
deliberately conservative rather than attributing a receipt gap to an uncertain event date.
Mature values remain observed-population estimates; exclude flagged titles for a clean
comparison or explicitly report the limitation. Operational closure requires producer
attestation, and an unresolved late feed needs a lower maturity cutoff.
Late data can restate already reported cohorts; published snapshots retain evidence.
Pool retained players and eligible cohort players when combining cohorts; do not
average their percentages. Synthetic transport loss can still bias observed retention.

## Launch delivery health

The independent synthetic server manifest records expected distinct events and
distinct sessions **before deliberate transport loss**, grouped by trusted UTC
receipt date. The mart compares that with accepted, deduplicated received events on
the same date. Event-time dates are deliberately not used for this denominator.
Exact duplicate retransmissions do not inflate delivery counts. A session delivered
on multiple dates counts on each relevant date; it is a receipt coverage measure.

Expected rows drive the join, retaining days with zero delivered telemetry. A full
outer join also exposes unmanifested deliveries through `missing_manifest`. Ratios
can exceed one and should surface unexpected spikes; zero expectations produce null
ratios. Batch rejection counts live in ingestion audit receipts and the monitor;
they are not arbitrarily assigned to a title when a rejected file spans titles.

## Incremental correctness and rebuilds

`fct_events` reads new `ingest_seq` values, not a timestamp lookback. An arbitrarily
old accepted event therefore enters the next build. `fct_sessions` finds session keys
with event sequences above its prior `source_ingest_seq` maximum, rereads **all**
events for those keys, and replaces those complete aggregates with dbt's
`delete+insert` materialization. No destructive pre-hook creates a deletion gap.
The batch loader supplies immutable accepted events and a globally monotone sequence;
in-place raw corrections are unsupported. Reset/replay a candidate or run a reviewed
full refresh for corrections and semantic model changes.

The small dimensions, attempt fact, player-day fact and marts rebuild by design.
This keeps historical cohort corrections and cross-midnight counter changes correct
without a second incremental state machine. It is not a claim that a full-history
daily rebuild is economical at production event volume. See [Redshift design](redshift.md)
for the separate scale-out pattern and its testing requirements. The runnable target
is DuckDB; dbt-duckdb provides the incremental materialization used here.

The dbt graph includes source, uniqueness, relationship and domain tests; custom
gates cover temporal/context contradictions, attempt transitions, retention maturity,
calendar coverage, row reconciliation and conservation of active seconds. Generic
`unique_key` configuration alone does not validate data grain. The generated
`manifest.json`, `run_results.json` and dbt docs preserve the model/exposure lineage.

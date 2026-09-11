# Telemetry contract and studio collaboration

Machine-readable shape: [event.schema.json](../src/telemetry/event.schema.json). Semantic checks and canonicalization: [contract.py](../src/telemetry/contract.py). These are synthetic game events, not a platform SDK specification.

## Envelope

| Field | Meaning / rule |
|---|---|
| `title_id`, `event_id` | Immutable event identity; event IDs are scoped to title |
| `player_id` | Pseudonymous title-local gameplay identity; no cross-title/account inference |
| `session_id` | Client-issued session identity; new session after counter reset/restart/build change |
| `build_id` | Released title/build catalog entry; stable inside a session |
| `platform`, `country_code` | Controlled sample platform domain and coarse two-letter synthetic geography |
| `event_name`, `schema_version` | Controlled domain and strict integer version 1 or 2 |
| `event_ts` | UTC client occurrence time; `Z` or zero UTC offset required |
| `received_at` | Trusted collector receipt time, not client-controlled in the proposed production path |
| `event_sequence` | Monotone integer within a session; start is sequence 0, gaps allowed |
| `mission_id`, `attempt_id`, `checkpoint_id` | Context for mission telemetry; attempts distinguish repeated visits |
| `play_time_ms` / `active_seconds` | v1 integer milliseconds / v2 finite seconds with at most three decimal places, cumulative **active** gameplay counter |
| `error_code` | Required on crash/error; example diagnostic classification |
| `metadata` | Bounded primitive extension fields, including release instrumentation revision in this sample |

Identifiers use a restricted ASCII alphabet and a 128-character limit. Unknown top-level fields, unknown versions, duplicate JSON keys, non-finite numbers and malformed JSON fail admission. Metadata does not drive core game metrics. All sample build mappings require `instrumentation_revision` to match the release seed; an unexpected revision fails dbt's contract gate.

Timestamps can arrive arbitrarily late; client time may be at most five minutes ahead of receipt time. Clock skew beyond that is quarantined. Event ordering uses sequence with timestamp consistency checks, not physical file order. The sample contract caps counters at 24 hours per session; a longer-lived game would need a reviewed wider counter/session policy.

## Event semantics

| Events | Required context and behavior |
|---|---|
| `session_start` | Sequence 0; active counter absent or zero |
| `heartbeat` | Cumulative active counter |
| `session_end` | Cumulative active counter; closes the session |
| `crash` | Counter and error code; terminal, normally no later session_end |
| `error` | Error code; recoverable and nonterminal |
| `mission_start` | Mission and new attempt ID |
| `checkpoint_reached` | Mission, attempt and checkpoint ID |
| `player_death` | Mission and attempt; a death alone does not create a new attempt |
| `mission_complete` | Same mission/attempt as the corresponding start |

Only boundary/heartbeat events carry active counters; only mission events carry attempt context. The loader handles row-level validation. dbt handles cross-event rules: immutable session context, sequence uniqueness, timestamps/counters moving forward, one start/terminal, no post-terminal events, and legal attempt boundaries. This division lets a studio distinguish a bad envelope from a semantically inconsistent instrumentation sequence.

## Evolution and idempotency

The v1-to-v2 change replaces milliseconds with seconds without changing the meaning of active gameplay. The loader normalizes both before SQL. Each released build records its schema version and instrumentation revision. New optional metadata is compatible; new envelope fields, changed units, changed event meaning or new events require a reviewed schema/version and test fixture before producer rollout. Never silently reinterpret an existing field for one build.

Exact duplicate client payloads are normal retransmissions. They are skipped and counted, while changed content under the same `(title_id,event_id)` rejects the whole batch. Payload hashing ignores only receiver time and canonicalizes equivalent UTC timestamp spellings. It does not erase version changes. The batch ID hashes original file bytes, so a repeat of the same immutable file cannot produce a second load.

Keep wire partitions and manifests. For a corrected normalizer or model, replay them into a new candidate and compare outputs before publication. Do not edit accepted raw rows in place. An intentional corrected event must have reviewed correction semantics; arbitrary event-ID replacement is not an automatic fix for inconsistent telemetry.

## Example launch agreement

Before a new title/build release, the studio instrumentation owner, analyst and analytics engineer agree on:

1. The decision: compare crash-session rate, mission completion attempts and active minutes by build, with an explicit minimum sample size and platform stratification.
2. The telemetry: session lifecycle, attempt identity, counters, error taxonomy, version/build mapping and trusted collector receipt metadata.
3. Acceptance fixtures: normal completion, death/retry, crash without clean shutdown, missing boundaries, cross-midnight play, pause/menu time, retransmission, delayed terminal, and a schema conversion.
4. Closure and action: producer manifest ownership, lateness expectation, threshold triage owner, quarantine recovery and rollback criteria.

Analytics engineering owns grain, joins, lineage, loading and model tests. The studio owns game semantics and instrumentation. The analyst owns interpretation and denominator approval. These are example responsibilities for this project, not claims about a particular studio's organization. Launch review uses the generated report and dbt exposure; decisions should cite the published generation so later backfills can explain restatements.

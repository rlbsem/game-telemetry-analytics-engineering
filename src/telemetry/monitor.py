"""Launch triage from shared marts, with counts, denominators and bounded claims."""

from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from telemetry.pipeline import atomic_json, validation_receipt

MARTS = ("mart_daily_title", "mart_build_health", "mart_progression", "mart_retention", "mart_launch_health")


def records(connection: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cursor = connection.execute(sql)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


def monitor(database: Path, output: Path) -> dict:
    validation = validation_receipt(database)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database), read_only=True) as connection:
        exported = {}
        for name in MARTS:
            cursor = connection.execute(f"select * from analytics.{name} order by all")
            columns = [column[0] for column in cursor.description]
            rows = cursor.fetchall()
            with (output / f"{name}.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle, lineterminator="\n")
                writer.writerow(columns)
                writer.writerows(rows)
            exported[name] = [dict(zip(columns, row, strict=True)) for row in rows]
        summary = dict(zip(["events", "sessions", "players", "attempts"], [
            connection.execute(f"select count(*) from analytics.{name}").fetchone()[0]
            for name in ("fct_events", "fct_sessions", "dim_player", "fct_attempts")
        ], strict=True))
        summary.update(dict(zip(["loaded_batches", "duplicate_deliveries", "rejected_batches"], connection.execute("select count(*) filter(where status='loaded'),coalesce(sum(duplicate_count),0),count(*) filter(where status='rejected') from raw.batches").fetchone(), strict=True)))
        schema = records(connection, "select title_id,build_id,schema_version,count(*) as events from analytics.fct_events group by 1,2,3 order by 1,2,3")
        event_mix = records(connection, """
            select title_id,build_id,
                count(*) filter(where event_name='heartbeat')*1.0 /
                  nullif(count(distinct session_id),0) as heartbeats_per_session,
                count(distinct session_id) as sessions
            from analytics.fct_events group by 1,2 order by 1,2
        """)
        retention = records(connection, """
            select title_id,cohort_build_id, bool_or(has_delivery_gap) as has_delivery_gap,
                sum(case when d1_is_mature then cohort_players else 0 end) as d1_eligible,
                sum(d1_players) as d1_returned,
                sum(d1_players)*1.0/nullif(sum(case when d1_is_mature then cohort_players else 0 end),0) as d1_rate,
                sum(case when d7_is_mature then cohort_players else 0 end) as d7_eligible,
                sum(d7_players) as d7_returned,
                sum(d7_players)*1.0/nullif(sum(case when d7_is_mature then cohort_players else 0 end),0) as d7_rate
            from analytics.mart_retention group by 1,2 order by 1,2
        """)
    alerts = []

    def alert(code: str, title: str, context: dict, action: str) -> None:
        alerts.append(dict(code=code, severity="review", title_id=title, **context, action=action))

    builds = {(r["title_id"], r["build_id"]): r for r in exported["mart_build_health"]}
    for (title, build_id), row in builds.items():
        baseline = builds.get((title, "1.0.0"))
        if build_id == "1.0.0" or not baseline or min(row["sessions"], baseline["sessions"]) < 30:
            continue
        if row["crash_rate"] > baseline["crash_rate"] + 0.03:
            alert("crash_rate_regression", title,
                  {"build_id": build_id, "observed_rate": row["crash_rate"], "baseline_rate": baseline["crash_rate"], "sessions": row["sessions"], "threshold_absolute_increase": 0.03},
                  "Gameplay/platform engineer: stratify platform and error_code, inspect release change; association is not causal proof.")
        previous_duration = baseline["avg_session_active_minutes"]
        if previous_duration and abs(row["avg_session_active_minutes"] / previous_duration - 1) > 0.25:
            alert("session_duration_shift", title,
                  {"build_id": build_id, "observed_minutes": row["avg_session_active_minutes"], "baseline_minutes": previous_duration, "threshold_relative_change": 0.25},
                  "Game analyst: compare partial-session share and player mix before interpreting engagement.")
    for row in exported["mart_progression"]:
        if row["started_attempts"] >= 30 and row["completion_rate"] < 0.6:
            alert("progression_bottleneck", row["title_id"],
                  {"build_id": row["build_id"], "mission_id": row["mission_id"], "completion_rate": row["completion_rate"], "started_attempts": row["started_attempts"], "deaths_per_attempt": row["death_per_attempt"]},
                  "Mission designer + analyst: inspect deaths and open-attempt share; unfinished attempts are not proven abandonment.")
    for row in exported["mart_launch_health"]:
        if row["missing_manifest"]:
            alert("missing_delivery_manifest", row["title_id"], {"receipt_date": row["receipt_date"]},
                  "Ingestion owner: supply independent delivery expectations; observed volume alone cannot prove completeness.")
        elif row["expected_events"] and row["event_delivery_ratio"] < 0.95:
            alert("delivery_volume_loss", row["title_id"],
                  {"receipt_date": row["receipt_date"], "received_events": row["received_events"], "expected_events": row["expected_events"], "delivery_ratio": row["event_delivery_ratio"], "threshold_ratio": 0.95},
                  "Ingestion owner: compare sealed collector manifest and receipt partitions; recover missing objects before trusting volume trends.")
        elif row["expected_events"] and row["event_delivery_ratio"] > 1.05:
            alert("delivery_volume_spike", row["title_id"],
                  {"receipt_date": row["receipt_date"], "delivery_ratio": row["event_delivery_ratio"], "threshold_ratio": 1.05},
                  "Ingestion owner: inspect unexpected producers and manifest scope before interpreting increased activity.")
    mix = {(r["title_id"], r["build_id"]): r for r in event_mix}
    for (title, build_id), row in mix.items():
        baseline = mix.get((title, "1.0.0"))
        if build_id != "1.0.0" and baseline and row["sessions"] >= 30 and baseline["sessions"] >= 30:
            if row["heartbeats_per_session"] < baseline["heartbeats_per_session"] * 0.5:
                alert("heartbeat_instrumentation_loss", title,
                      {"build_id": build_id, "observed_per_session": row["heartbeats_per_session"], "baseline_per_session": baseline["heartbeats_per_session"], "threshold_ratio": 0.5},
                      "Client telemetry owner: compare instrumentation revisions and emission by build; terminal counters preserve totals but interval coverage degrades.")
    cohorts = {(r["title_id"], r["cohort_build_id"]): r for r in retention}
    for (title, build_id), row in cohorts.items():
        baseline = cohorts.get((title, "1.0.0"))
        if build_id == "1.0.0" or not baseline or row["has_delivery_gap"] or baseline["has_delivery_gap"]:
            continue
        if min(row["d1_eligible"], baseline["d1_eligible"]) >= 30 and row["d1_rate"] < baseline["d1_rate"] - 0.15:
            alert("cohort_retention_shift", title,
                  {"build_id": build_id, "observed_d1_rate": row["d1_rate"], "baseline_d1_rate": baseline["d1_rate"], "eligible_players": row["d1_eligible"], "threshold_absolute_drop": 0.15},
                  "Game analyst: inspect first-observed cohort mix and acquisition windows; a cohort-build association does not establish release causality.")
    report = dict(synthetic=True, as_of=validation["as_of"], complete_through=validation["complete_through"],
                  summary=summary, validation=validation, alerts=alerts,
                  build_health=exported["mart_build_health"], schema_coverage=schema,
                  event_mix=event_mix, retention_summary=retention,
                  interpretation="Threshold alerts are triage, not significance tests or causal estimates. Delivery gaps and observed-only populations can bias metrics. CSVs contain denominators; docs/metrics.md defines semantics.")
    atomic_json(output / "report.json", report)
    lines = ["# Synthetic launch report", "", f"As of **{validation['as_of']}**; retention mature through **{validation['complete_through']}**.", "",
             f"{summary['events']:,} accepted events · {summary['sessions']:,} observed sessions · {summary['players']:,} title-scoped players.", "",
             "Signals are intentionally injected; rates are descriptive, not causal. Missing deliveries can bias results even after the observation window has matured.", "",
             "| Title | Build | Sessions | Crashed | Crash rate | Avg active minutes |", "|---|---|---:|---:|---:|---:|"]
    for row in exported["mart_build_health"]:
        rate = f"{row['crash_rate']:.1%}" if row["crash_rate"] is not None else "—"
        minutes = f"{row['avg_session_active_minutes']:.2f}" if row["avg_session_active_minutes"] is not None else "—"
        lines.append(f"| {row['title_id']} | {row['build_id']} | {row['sessions']} | {row['crashed_sessions']} | {rate} | {minutes} |")
    lines.extend(["", "## Triage", ""])
    lines += [f"- **{a['code']}** — {a['title_id']} {a.get('build_id', a.get('receipt_date', ''))} {a.get('mission_id', '')}. {a['action']}" for a in alerts]
    if not alerts:
        lines.append("No configured threshold crossed; check denominators before treating this as healthy.")
    lines.extend(["", "Full values and thresholds: [report.json](report.json). Analyst-ready marts are exported alongside this report.", ""])
    (output / "report.md").write_text("\n".join(lines), encoding="utf-8")
    return report

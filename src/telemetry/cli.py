from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from telemetry.generate import generate
from telemetry.ingest import BatchRejected, load_path, validate_file, verify_manifest
from telemetry.pipeline import DEFAULT_AS_OF, DEFAULT_COMPLETE_THROUGH, build, demo, publish, replay


def main() -> int:
    parser = argparse.ArgumentParser(description="Synthetic game telemetry analytics")
    sub = parser.add_subparsers(dest="command", required=True)
    command = sub.add_parser("generate", help="Write deterministic receipt-partitioned telemetry")
    command.add_argument("--output", type=Path, required=True)
    command.add_argument("--players", type=int, default=160, help="players per title")
    for name in ("validate", "load"):
        command = sub.add_parser(name)
        command.add_argument("--input", type=Path, required=True)
        if name == "load":
            command.add_argument("--database", type=Path, required=True)
    command = sub.add_parser("build")
    command.add_argument("--database", type=Path, required=True)
    command.add_argument("--full-refresh", action="store_true")
    command.add_argument("--as-of", default=DEFAULT_AS_OF)
    command.add_argument("--complete-through", default=DEFAULT_COMPLETE_THROUGH)
    for name in ("monitor", "publish"):
        command = sub.add_parser(name)
        command.add_argument("--database", type=Path, required=True)
        command.add_argument("--output", type=Path, required=True)
    command = sub.add_parser("demo")
    command.add_argument("--workspace", type=Path, default=Path(".local/demo"))
    command.add_argument("--players", type=int, default=160)
    command = sub.add_parser("replay")
    command.add_argument("--input", type=Path, required=True)
    command.add_argument("--workspace", type=Path, required=True)
    command.add_argument("--as-of", default=DEFAULT_AS_OF)
    command.add_argument("--complete-through", default=DEFAULT_COMPLETE_THROUGH)
    args = vars(parser.parse_args())
    name = args.pop("command")
    try:
        if name == "validate":
            path = args["input"]
            if path.is_dir():
                verify_manifest(path)
            paths = sorted(path.glob("*.jsonl")) if path.is_dir() else [path]
            if not paths:
                raise ValueError("no .jsonl files found")
            result = [validate_file(p) for p in paths]
            print(json.dumps(result, indent=2, default=str))
            return int(any(r["invalid_count"] for r in result))
        if name == "load":
            result = load_path(args["input"], args["database"])
        elif name == "generate":
            result = generate(**args)
        elif name == "build":
            result = build(**args)
        elif name == "publish":
            result = publish(**args)
        elif name == "demo":
            result = demo(**args)
        elif name == "replay":
            args["path"] = args.pop("input")
            result = replay(**args)
        else:
            from telemetry.monitor import monitor
            result = monitor(**args)
        print(json.dumps(result, indent=2, default=str))
        return 0
    except (ValueError, RuntimeError, OSError) as exc:
        if isinstance(exc, BatchRejected):
            print(json.dumps(exc.report, indent=2, default=str), file=sys.stderr)
        else:
            print(str(exc), file=sys.stderr)
        return 1

#!/bin/bash
# Print every local orchestrator task and its status.
set -euo pipefail
cd "$(dirname "$0")"

TOKEN=$(cat data/orchestrator/control-token)
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
curl -sS --max-time 5 -H "Authorization: Bearer $TOKEN" \
  -o "$tmp" http://127.0.0.1:8781/v1/orchestrator/tasks

python3 - "$tmp" <<'PY'
import json, sys
from collections import Counter
from datetime import datetime, timezone, timedelta

data = json.load(open(sys.argv[1]))
tasks = data.get("tasks") or []
tz = timezone(timedelta(hours=8))

def kind_of(task):
    return ((task.get("spec") or {}).get("kind") or "-")

def when(ts):
    if not ts:
        return "-"
    ts = ts.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return ts[:19]
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(tz).strftime("%-I:%M:%S %p")

def error_of(task):
    result = task.get("result") or {}
    if isinstance(result, dict):
        for key in ("error", "error_message", "error_code"):
            if result.get(key):
                return str(result[key])
        outputs = result.get("outputs") or {}
        if isinstance(outputs, dict):
            for key, value in outputs.items():
                if "error" in key.lower() and value:
                    return f"{key}={value}"
    return ""

states = Counter(task.get("state") or "?" for task in tasks)
kinds = Counter(kind_of(task) for task in tasks)
standard = sum(1 for task in tasks if kind_of(task).startswith("transfer.") and task.get("state") == "completed")
room = sum(1 for task in tasks if kind_of(task).startswith("room.") and task.get("state") == "completed")
failed = sum(1 for task in tasks if task.get("state") in ("failed", "cancelled", "rejected"))

print(f"TASKS  {len(tasks)} total")
if tasks:
    print("  " + "   ".join(f"{state} {count}" for state, count in sorted(states.items())))
    print("  " + "   ".join(f"{kind} {count}" for kind, count in sorted(kinds.items())))
else:
    print("  none")
print(f"  failed {failed}")
print()
print("QUALIFY  completed local tasks")
print(f"  standard  {standard:3d} / 120")
print(f"  room      {room:3d} / 40")
print()
print(f"{'KIND':<22} {'STATE':<12} {'CREATED':<14} {'UPDATED':<14} {'ERROR'}")
print("-" * 88)
if not tasks:
    print("(none yet)")
else:
    for task in sorted(tasks, key=lambda item: item.get("created_at") or ""):
        print(
            f"{kind_of(task):<22} {task.get('state') or '-':<12} "
            f"{when(task.get('created_at')):<14} {when(task.get('updated_at')):<14} {error_of(task)}"
        )
PY

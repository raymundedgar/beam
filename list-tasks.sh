#!/bin/bash
# Print every local orchestrator task and write cancelled/failed/rejected
# to data/analytics/unsuccessful.md for the editor.
set -euo pipefail
cd "$(dirname "$0")"

TOKEN=$(cat data/orchestrator/control-token)
mkdir -p data/analytics
OUT=data/analytics/unsuccessful.md
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
curl -sS --max-time 5 -H "Authorization: Bearer $TOKEN" \
    -o "$tmp" http://127.0.0.1:8781/v1/orchestrator/tasks

python3 - "$tmp" "$OUT" <<'PY'
import json, re, sys
from collections import Counter
from datetime import datetime, timezone, timedelta

data = json.load(open(sys.argv[1]))
out_path = sys.argv[2]
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
    bits = []
    if task.get("upstream_error"):
        bits.append(str(task["upstream_error"]))
    if isinstance(result, dict):
        for key in ("error_code", "error_message", "error"):
            if result.get(key):
                bits.append(str(result[key]))
        outputs = result.get("outputs") or {}
        if isinstance(outputs, dict):
            for key, value in outputs.items():
                if "error" in key.lower() and value:
                    bits.append(f"{key}={value}")
    text = " | ".join(bits)
    text = re.sub(r"https?://[^ \n\"]+", lambda m: m.group(0).split("?", 1)[0], text)
    text = " ".join(text.split())
    if len(text) > 280:
        text = text[:277] + "..."
    return text

def md_cell(value):
    return str(value).replace("|", "/").replace("\n", " ")

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

bad = [task for task in tasks if task.get("state") in ("failed", "cancelled", "rejected")]
now = datetime.now(tz).strftime("%-I:%M:%S %p")
lines = [
    "# Unsuccessful tasks",
    "",
    f"Updated {now} (UTC+8). {len(bad)} of {len(tasks)} local tasks.",
    "",
    f"- cancelled: {states.get('cancelled', 0)}",
    f"- failed: {states.get('failed', 0)}",
    f"- rejected: {states.get('rejected', 0)}",
    "",
]
if not bad:
    lines.append("None right now.")
else:
    lines += [
        "| KIND | STATE | CREATED | UPDATED | REASON |",
        "| --- | --- | --- | --- | --- |",
    ]
    for task in sorted(bad, key=lambda item: item.get("updated_at") or "", reverse=True):
        lines.append(
            "| {kind} | {state} | {created} | {updated} | {reason} |".format(
                kind=md_cell(kind_of(task)),
                state=md_cell(task.get("state") or "-"),
                created=md_cell(when(task.get("created_at"))),
                updated=md_cell(when(task.get("updated_at"))),
                reason=md_cell(error_of(task) or "-"),
            )
        )
    lines.append("")
open(out_path, "w", encoding="utf-8").write("\n".join(lines) + "\n")
print()
print(f"Wrote {out_path}  ({len(bad)} unsuccessful)")
PY

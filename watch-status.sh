#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"

TOKEN=$(cat data/orchestrator/control-token)
ID=$(jq -r .orchestrator.OrchestratorID data/orchestrator/registry.json)

orch=$(curl -s --max-time 2 http://127.0.0.1:8781/healthz | jq -r '.status // "down"' 2>/dev/null || echo down)
worker=$(curl -s --max-time 2 http://127.0.0.1:8780/healthz | jq -r '.status // "down"' 2>/dev/null || echo down)

core_json=$(curl -s --max-time 5 "https://beamcore.b1m.ai/orchestrators/$ID" 2>/dev/null || echo '{}')
core_status=$(echo "$core_json" | jq -r '.status // "unknown"')
core_ready=$(echo "$core_json" | jq -r 'if .ready == true then "yes" elif .ready == false then "no" else "?" end')
core_seen=$(echo "$core_json" | jq -r '.last_seen // empty')

ago="—"
if [ -n "$core_seen" ]; then
  seen_ts=$(date -d "$core_seen" +%s 2>/dev/null || true)
  now_ts=$(date +%s)
  if [ -n "${seen_ts:-}" ]; then
    diff=$((now_ts - seen_ts))
    if [ "$diff" -lt 0 ]; then ago="just now"
    elif [ "$diff" -lt 60 ]; then ago="${diff}s ago"
    elif [ "$diff" -lt 3600 ]; then ago="$((diff / 60))m ago"
    else ago="$((diff / 3600))h ago"
    fi
  else
    ago="$core_seen"
  fi
fi

connected=$(curl -s --max-time 2 -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8781/v1/orchestrator/manifest | jq -r 'if ((.Capabilities // []) | length) > 0 then "yes" else "no" end' 2>/dev/null || echo no)

tasks_json=$(curl -s --max-time 2 -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8781/v1/orchestrator/tasks 2>/dev/null || echo '{"tasks":[]}')
count=$(echo "$tasks_json" | jq '((.tasks // []) | length)')
by_state=$(echo "$tasks_json" | jq -r '
  ((.tasks // []) | group_by(.state) | map("\(.[0].state) \(length)") | join("   "))
  // "none"
')
[ -z "$by_state" ] && by_state="none"

read -r standard_done room_done <<<"$(echo "$tasks_json" | jq -r '
  (.tasks // []) as $t
  | [
      ($t | map(select(.state == "completed" and ((.spec.kind // "") | startswith("transfer.")))) | length),
      ($t | map(select(.state == "completed" and ((.spec.kind // "") | startswith("room.")))) | length)
    ]
  | @tsv
')"
standard_done=${standard_done:-0}
room_done=${room_done:-0}
standard_target=120
room_target=40

qualify_line() {
  local label="$1" done="$2" target="$3"
  local width=20 filled=0 pct=0
  if [ "$target" -gt 0 ]; then
    filled=$((done * width / target))
    pct=$((done * 100 / target))
    [ "$done" -gt 0 ] && [ "$filled" -eq 0 ] && filled=1
    [ "$filled" -gt "$width" ] && filled=$width
    [ "$pct" -gt 100 ] && pct=100
  fi
  local bar
  bar=$(printf '%*s' "$filled" '' | tr ' ' '#')
  bar+=$(printf '%*s' $((width - filled)) '' | tr ' ' '-')
  printf '  %-10s %3d / %-3d  [%s]  %3d%%\n' "$label" "$done" "$target" "$bar" "$pct"
}

recent_rows=$(echo "$tasks_json" | jq -r '
  [((.tasks // []) | sort_by(.updated_at) | reverse[])][0:5]
  | .[]
  | [(.spec.kind // "-"), (.state // "-"), (.updated_at // "")]
  | @tsv
')
if [ -z "$recent_rows" ]; then
  recent="  (none yet)"
else
  recent=$'KIND\tSTATE\tUPDATED'
  while IFS=$'\t' read -r kind state updated; do
    [ -z "$kind" ] && continue
    local_ts="—"
    if [ -n "$updated" ]; then
      local_ts=$(TZ=Asia/Shanghai date -d "$updated" '+%-I:%M:%S %p' 2>/dev/null || echo "$updated")
    fi
    recent+=$'\n'"$kind"$'\t'"$state"$'\t'"$local_ts"
  done <<< "$recent_rows"
fi

printf 'STATUS\n'
printf '  %-16s %s\n' orchestrator "$orch"
printf '  %-16s %s\n' worker "$worker"
printf '  %-16s %s\n' beamcore "$core_status"
printf '  %-16s %s\n' ready "$core_ready"
printf '  %-16s %s\n' last_seen "$ago"
printf '  %-16s %s\n' worker_linked "$connected"
printf '\nTASKS  %s total\n' "$count"
printf '  %s\n' "$by_state"
printf '\nQUALIFY  completed local tasks toward BeamCore targets\n'
qualify_line standard "$standard_done" "$standard_target"
qualify_line room "$room_done" "$room_target"
printf '\nRECENT\n'
echo "$recent" | column -t -s $'\t' | sed 's/^/  /'
printf '\n'

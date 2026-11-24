#!/bin/bash
# Task size, duration, and throughput for the last wave, plus subnet 105 stake.
#
#   ./throughput.sh          last wave
#   ./throughput.sh watch    refresh every 2s; stake is read at most every 5 minutes
#   ./throughput.sh all      every local task from the past 24 hours
#   ./throughput.sh 4:40PM   every task from 4:40 PM local time onward
set -euo pipefail
cd "$(dirname "$0")"

MODE="${1:-snapshot}"
SINCE=""
case "$MODE" in
  snapshot|watch|all) ;;
  *) SINCE="$MODE"; MODE="since" ;;
esac

TOKEN=$(cat data/orchestrator/control-token)
mkdir -p data/analytics

clear 2>/dev/null || true

python3 - "$MODE" "$TOKEN" "$SINCE" <<'PY'
import hashlib, ipaddress, json, os, re, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

MODE, TOKEN = sys.argv[1], sys.argv[2]
SINCE_TEXT = sys.argv[3] if len(sys.argv) > 3 else ""
TZ = timezone(timedelta(hours=8))
MIB = 1024 * 1024
EMISSION_EVERY = 5 * 60
EMISSION_PATH = "data/analytics/emission.json"
BTCLI = os.environ.get("BTCLI", "/root/beam/.venv/bin/btcli")
IN_FLIGHT = {"received", "offered", "reserved", "committed", "running", "starting"}
# Local pool. w1 is this orchestrator's own worker.
WORKERS = {
    "b0f59a79-8cce-4137-af3a-84c16a1b61fc": ("w1", "198.58.113.189"),
    "3278bea7-6dc2-4b46-b84c-10516be0e4ac": ("w2", "173.255.192.120"),
    "ed8b526a-9f7a-4660-913f-17268e8cd337": ("w3", "45.33.120.60"),
    "861d023f-16c0-42f0-9e70-5db1b2d459d5": ("w4", "104.200.20.75"),
    "a5979750-9f01-419b-9b4e-42eac2336089": ("w5", "72.14.185.75"),
    "7487a1e8-aa29-4b3a-867c-5424b97f5d09": ("w6", "69.164.198.44"),
}

def worker_of(task):
    raw = task.get("worker_id") or ""
    if not raw:
        identity = (task.get("spec") or {}).get("identity") or {}
        raw = identity.get("worker_id") or ""
    known = WORKERS.get(raw)
    if known:
        return known[0]
    return raw[:8] if raw else "—"

def fetch_tasks():
    req = urllib.request.Request(
        "http://127.0.0.1:8781/v1/orchestrator/tasks",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    with urllib.request.urlopen(req, timeout=8) as resp:
        return json.loads(resp.read().decode()).get("tasks") or []

def parse_time(value):
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def local_clock(dt):
    if dt is None:
        return "—"
    return dt.astimezone(TZ).strftime("%-I:%M:%S %p")

def payload_of(task):
    payload = (task.get("spec") or {}).get("payload")
    if isinstance(payload, str):
        try:
            return json.loads(payload)
        except json.JSONDecodeError:
            return {}
    return payload if isinstance(payload, dict) else {}

def offered_bytes(task):
    payload = payload_of(task)
    parts = payload.get("parts")
    if isinstance(parts, list):
        total = 0
        found = False
        for part in parts:
            if isinstance(part, dict) and part.get("length"):
                total += int(part["length"])
                found = True
        if found:
            return total
    for key in ("file_size_bytes", "size_bytes", "size"):
        if payload.get(key):
            return int(payload[key])
    return 0

def task_bytes(task):
    result = task.get("result") or {}
    processed = result.get("bytes_processed") if isinstance(result, dict) else None
    if processed:
        return int(processed)
    return offered_bytes(task)

def task_window(task):
    result = task.get("result") if isinstance(task.get("result"), dict) else {}
    start = parse_time((result or {}).get("started_at")) or parse_time(task.get("created_at"))
    state = task.get("state") or ""
    if state in IN_FLIGHT:
        end = datetime.now(timezone.utc)
    else:
        end = parse_time((result or {}).get("completed_at")) or parse_time(task.get("updated_at"))
    return start, end

def fmt_mib(nbytes):
    if not nbytes:
        return "—"
    value = nbytes / MIB
    if value >= 1024:
        return f"{value / 1024:.2f} GiB"
    if value >= 10:
        return f"{value:.1f} MiB"
    return f"{value:.2f} MiB"

def fmt_secs(seconds):
    if seconds is None or seconds < 0:
        return "—"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, rem = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m{int(rem):02d}s"
    hours, minutes = divmod(int(minutes), 60)
    return f"{hours}h{minutes:02d}m"

def fmt_rate(mib_s):
    if mib_s is None:
        return "—"
    return f"{mib_s:.1f} MiB/s"

def fmt_dl_ul(dl, ul):
    if dl is None and ul is None:
        return "—"
    dl_s = f"{dl:.1f}" if dl is not None else "—"
    ul_s = f"{ul:.1f}" if ul is not None else "—"
    return f"{dl_s} / {ul_s}"

def parse_dur_sec(s):
    if not s or s == "-":
        return 0.0
    s = s.strip()
    if s.endswith("ms"):
        return float(s[:-2]) / 1000.0
    if s.endswith("us"):
        return float(s[:-2]) / 1000000.0
    if s.endswith("s"):
        return float(s[:-1])
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0

def fmt_sec_val(s):
    if s is None or s < 0:
        return "—"
    return f"{s:.2f}s"

def fmt_stall_val(s):
    if s is None or s < 0.05:
        return "—"
    return f"{s:.1f}s"

def fmt_rate_val(r):
    if r is None:
        return "—"
    return f"{r:.1f}"

def format_stop(state, stop, in_flight, starved=0.0):
    starve_note = ""
    if starved and starved >= 1.0:
        starve_note = f"starved {starved:.1f}s"

    if state == "completed":
        return starve_note or "—"
    state_s = (state or "—").upper()
    if in_flight:
        return state_s
    stop_text = f"{state_s} - {stop}" if (stop and stop != "—") else state_s
    if starve_note:
        return f"{stop_text} ({starve_note})"
    return stop_text

def host_of(url):
    if not url or not isinstance(url, str):
        return None
    try:
        return urllib.parse.urlparse(url).hostname
    except ValueError:
        return None

def endpoint_url(value):
    if isinstance(value, str):
        return value
    if not isinstance(value, dict):
        return None
    for key in ("url", "advertise_url", "http_url"):
        if value.get(key):
            return value[key]
    endpoint = value.get("endpoint")
    if isinstance(endpoint, dict):
        return endpoint.get("url")
    if isinstance(endpoint, str):
        return endpoint
    return None

def provider_of(host):
    if not host:
        return None
    name = host.lower().rstrip(".")
    if name.endswith(".invalid") or name.endswith(".local") or name in ("localhost",):
        return "peer"
    if name.endswith(".r2.cloudflarestorage.com") or name.endswith(".r2.dev") or name == "r2.cloudflarestorage.com":
        return "r2"
    if "amazonaws.com" in name or ".s3." in name or name.startswith("s3."):
        return "s3"
    if (
        name.endswith(".googleapis.com")
        or name.endswith(".googleusercontent.com")
        or "storage.cloud.google.com" in name
    ):
        return "gcs"
    if name.endswith(".backblazeb2.com"):
        return "b2"
    if "wasabisys.com" in name:
        return "wasabi"
    try:
        ipaddress.ip_address(name)
        return "peer"
    except ValueError:
        pass
    return "http"

def collect_providers(task):
    payload = payload_of(task)
    sources, dests = [], []
    source_keys = ("source", "sources", "src")
    dest_keys = ("destination", "destinations", "dest", "target", "targets", "upload")

    def remember(bucket, provider):
        if provider and provider not in bucket:
            bucket.append(provider)

    def add_url(url, side):
        provider = provider_of(host_of(url if isinstance(url, str) else endpoint_url(url)))
        if side == "dest":
            remember(dests, provider)
        else:
            remember(sources, provider)

    def walk(obj, side):
        if isinstance(obj, dict):
            url = endpoint_url(obj)
            if url:
                add_url(url, side)
            elif isinstance(obj.get("url"), str) and obj["url"].startswith("http"):
                add_url(obj["url"], side)
            for key, value in obj.items():
                lowered = key.lower()
                next_side = side
                if any(token in lowered for token in dest_keys):
                    next_side = "dest"
                elif any(token in lowered for token in source_keys):
                    next_side = "source"
                walk(value, next_side)
        elif isinstance(obj, list):
            for item in obj:
                walk(item, side)
        elif isinstance(obj, str) and obj.startswith("http"):
            add_url(obj, side)

    walk(payload, "source")
    return sources, dests

REGIONS = {"wnam", "enam", "sam", "weur", "eeur", "apac", "oc", "afr", "me"}

def clean_region(value):
    if not isinstance(value, str):
        return ""
    value = value.strip().lower()
    return value if value in REGIONS else ""

def regions_of(task):
    payload = payload_of(task)
    sources, dests = [], []

    def add(bucket, value):
        label = clean_region(value)
        if label and label not in bucket:
            bucket.append(label)

    parts = payload.get("parts")
    if isinstance(parts, list):
        for part in parts:
            if not isinstance(part, dict):
                continue
            source = part.get("source") if isinstance(part.get("source"), dict) else {}
            dest = part.get("destination") if isinstance(part.get("destination"), dict) else {}
            add(sources, source.get("region"))
            add(dests, dest.get("region"))
    source = payload.get("source")
    if isinstance(source, dict):
        add(sources, source.get("region"))
    for key in ("targets", "destinations"):
        items = payload.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                add(dests, item.get("region"))
    src = "+".join(sources)
    dst = "+".join(dests)
    if src and dst:
        return src if src == dst else f"{src}>{dst}"
    return src or dst or "—"

def batch_id_of(task):
    raw = task.get("batch_id")
    if not raw and isinstance(task.get("spec"), dict):
        raw = task["spec"].get("batch_id")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return "—"

def kind_of(task):
    base = ((task.get("spec") or {}).get("kind") or "-")
    sources, dests = collect_providers(task)
    if not sources and not dests:
        return base
    if sources and not dests:
        return f"{base}/{'+'.join(sources)}"
    if dests and not sources:
        return f"{base}/{'+'.join(dests)}"
    src, dst = "+".join(sources), "+".join(dests)
    if src == dst:
        return f"{base}/{src}"
    return f"{base}/{src}>{dst}"

KIND_ABBR = {
    "transfer.multipart": "TM",
    "room.transfer": "RT",
    "transfer.authenticated_part.v1": "AP",
    "transfer.authenticated_part": "AP",
    "transfer.source_digest.v1": "SD",
    "storage.probe.relay.v2": "SP",
    "storage.probe": "SP",
}

def kind_abbr(kind_str):
    if not kind_str or kind_str == "-":
        return "-"
    if "/" in kind_str:
        base, rest = kind_str.split("/", 1)
        abbr = KIND_ABBR.get(base, base)
        return f"{abbr}/{rest}"
    return KIND_ABBR.get(kind_str, kind_str)

def workload_ids(task):
    found = []
    for raw in (task.get("external_id"), task.get("workload_key")):
        if not isinstance(raw, str):
            continue
        for part in raw.split("/"):
            part = part.strip().lower()
            if len(part) >= 8 and part not in found:
                found.append(part)
    result = task.get("result") if isinstance(task.get("result"), dict) else {}
    wid = result.get("workload_id") if isinstance(result, dict) else None
    if isinstance(wid, str) and len(wid) >= 8 and wid.lower() not in found:
        found.append(wid.lower())
    return found

def parse_since(text):
    """A local clock time such as 4:40PM, 4:40:05 PM, or 16:40. Today, or yesterday if that clock time is still ahead."""
    compact = "".join(text.strip().upper().split())
    clock = None
    for fmt in ("%I:%M%p", "%I:%M:%S%p", "%H:%M", "%H:%M:%S"):
        try:
            clock = datetime.strptime(compact, fmt)
            break
        except ValueError:
            continue
    if clock is None:
        raise SystemExit(f"throughput: could not read time {text!r} (example: 4:40PM)")
    now = datetime.now(TZ)
    since = now.replace(hour=clock.hour, minute=clock.minute, second=clock.second, microsecond=0)
    if since > now + timedelta(minutes=2):
        since -= timedelta(days=1)
    return since

SINCE = parse_since(SINCE_TEXT) if MODE == "since" else None

def pace_info(since=None):
    """Last transfer rates, durations, stalls, and starvation seen for each workload, from the worker journal."""
    journal_since = "6 hours ago"
    if since is not None:
        journal_since = since.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    try:
        proc = subprocess.run(
            ["journalctl", "-u", "beam-worker", "--since", journal_since, "-o", "cat", "-g", "transfer (pace|read_frozen|report)"],
            capture_output=True, text=True, timeout=8,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    if proc.returncode != 0:
        return {}
    info = {}
    for line in proc.stdout.splitlines():
        if "transfer pace" in line:
            workload = re.search(r"workload=([0-9a-f-]{8,})", line)
            side = re.search(r"side=(source|destination)", line)
            dur = re.search(r"dur=([0-9.]+(?:ms|s|us)?)", line)
            blk = re.search(r"blocked=([0-9.]+(?:ms|s|us)?)", line)
            rate = re.search(r"rate_mib=([0-9.]+)", line)
            if not workload or not side:
                continue
            key = workload.group(1).lower()
            if key not in info:
                entry = {
                    "dl_rate": None, "ul_rate": None,
                    "dl_time": 0.0, "ul_time": 0.0,
                    "dl_stall": 0.0, "ul_stall": 0.0,
                    "starved": 0.0,
                    "has_dl": False, "has_ul": False,
                }
                info[key] = entry
                info[key[:8]] = entry
            entry = info[key]
            is_dl = side.group(1) == "source"
            d_sec = parse_dur_sec(dur.group(1)) if dur else 0.0
            b_sec = parse_dur_sec(blk.group(1)) if blk else 0.0
            if is_dl:
                entry["has_dl"] = True
                entry["dl_time"] += d_sec
                entry["dl_stall"] += b_sec
                if rate:
                    entry["dl_rate"] = float(rate.group(1))
            else:
                entry["has_ul"] = True
                entry["ul_time"] += d_sec
                entry["ul_stall"] += b_sec
                if rate:
                    entry["ul_rate"] = float(rate.group(1))
        elif "transfer read_frozen" in line:
            workload = re.search(r"workload=([0-9a-f-]{8,})", line)
            idle = re.search(r"idle=([0-9.]+(?:ms|s|us)?)", line)
            if workload and idle:
                key = workload.group(1).lower()
                if key not in info:
                    entry = {
                        "dl_rate": None, "ul_rate": None,
                        "dl_time": 0.0, "ul_time": 0.0,
                        "dl_stall": 0.0, "ul_stall": 0.0,
                        "starved": 0.0,
                        "has_dl": False, "has_ul": False,
                    }
                    info[key] = entry
                    info[key[:8]] = entry
                info[key]["dl_stall"] += parse_dur_sec(idle.group(1))
        elif "transfer report" in line:
            workload = re.search(r"workload=([0-9a-f-]{8,})", line)
            starved = re.search(r"slot_starved=([0-9.]+(?:ms|s|us)?)", line)
            slot_wait = re.search(r"slot_wait=([0-9.]+(?:ms|s|us)?)", line)
            if workload:
                key = workload.group(1).lower()
                if key not in info:
                    entry = {
                        "dl_rate": None, "ul_rate": None,
                        "dl_time": 0.0, "ul_time": 0.0,
                        "dl_stall": 0.0, "ul_stall": 0.0,
                        "starved": 0.0,
                        "has_dl": False, "has_ul": False,
                    }
                    info[key] = entry
                    info[key[:8]] = entry
                total_starve = 0.0
                if starved:
                    total_starve += parse_dur_sec(starved.group(1))
                if slot_wait:
                    total_starve += parse_dur_sec(slot_wait.group(1))
                info[key]["starved"] = total_starve
    return info

def task_metrics(task, pace):
    for key in workload_ids(task):
        entry = pace.get(key) or pace.get(key[:8])
        if entry:
            dl_rate = entry.get("dl_rate")
            ul_rate = entry.get("ul_rate")
            dl_time = entry["dl_time"] if entry.get("has_dl") else None
            ul_time = entry["ul_time"] if entry.get("has_ul") else None
            dl_stall = entry["dl_stall"] if entry.get("has_dl") else None
            ul_stall = entry["ul_stall"] if entry.get("has_ul") else None
            starved = entry.get("starved", 0.0)
            return dl_rate, ul_rate, dl_time, ul_time, dl_stall, ul_stall, starved
    return None, None, None, None, None, None, 0.0

def enrich(task, medians, pace):
    raw_kind = kind_of(task)
    kind = kind_abbr(raw_kind)
    state = task.get("state") or "-"
    nbytes = task_bytes(task)
    start, end = task_window(task)
    seconds = (end - start).total_seconds() if start and end else None
    # A refusal is recorded in a few milliseconds. Offered size over that
    # interval is not a transfer rate.
    if state == "rejected":
        seconds = None
    mib_s = (nbytes / MIB / seconds) if nbytes and seconds and seconds > 0 else None
    dl_mib_s, ul_mib_s, dl_time, ul_time, dl_stall, ul_stall, starved = task_metrics(task, pace)
    return {
        "kind": kind,
        "state": state,
        "bytes": nbytes,
        "seconds": seconds,
        "mib_s": mib_s,
        "dl_mib_s": dl_mib_s,
        "ul_mib_s": ul_mib_s,
        "dl_time": dl_time,
        "ul_time": ul_time,
        "dl_stall": dl_stall,
        "ul_stall": ul_stall,
        "starved": starved,
        "started": start,
        "ended": None if state in IN_FLIGHT else end,
        "updated": parse_time(task.get("updated_at")),
        "created": parse_time(task.get("created_at")),
        "in_flight": state in IN_FLIGHT,
        "task_id": (task.get("external_id") or "")[:8],
        "worker": worker_of(task),
        "stop": stop_reason(task),
        "region": regions_of(task),
        "batch_id": batch_id_of(task),
        "batch": batch_id_of(task),
    }

def medians_of(rows):
    by_kind = defaultdict(list)
    for row in rows:
        if row["mib_s"] and row["state"] == "completed":
            by_kind[row["kind"]].append(row["mib_s"])
    out = {}
    for kind, values in by_kind.items():
        values.sort()
        out[kind] = values[len(values) // 2]
    return out

UNSUCCESSFUL_PATH = "data/analytics/unsuccessful.md"

# BeamCore's reason on task_offer_batch_cancel and failureReason.
STOP_REASONS = (
    ("deadline_expired", "The batch reached the deadline in its offer."),
    ("progress_timeout", "A room lane stopped making progress."),
    ("no_delivery_cut", "The batch had delivered nothing and was taken back before that deadline."),
    ("tail_cut", "The batch was taken back before that deadline, late in the transfer."),
    ("worker_failed", "The participant failed the work."),
    ("storage_failed", "The participant reported a storage failure."),
    ("path_failed", "The participant reported that a room path was refused."),
    ("endpoint_failed", "A room endpoint failed."),
    ("beam_side", "Beam failed."),
    ("superseded", "Another attempt covered the work."),
    ("transfer_ended", "The transfer ended first."),
)

def task_reason(task):
    result = task.get("result") if isinstance(task.get("result"), dict) else {}
    bits = []
    if task.get("upstream_error"):
        bits.append(str(task["upstream_error"]))
    if result:
        for key in ("error_code", "error_message", "error"):
            if result.get(key):
                bits.append(str(result[key]))
    text = " | ".join(bits)
    text = re.sub(r"https?://[^ \n\"]+", lambda match: match.group(0).split("?", 1)[0], text)
    text = " ".join(text.split())
    if len(text) > 280:
        text = text[:277] + "..."
    return text.replace("|", "/")

def stop_reason(task):
    text = task_reason(task).lower()
    for code, _ in STOP_REASONS:
        if code in text:
            return code
    state = task.get("state") or ""
    if state in ("completed",) or state in IN_FLIGHT:
        return ""
    result = task.get("result") if isinstance(task.get("result"), dict) else {}
    delivered = 0
    if isinstance(result, dict) and result.get("bytes_processed"):
        delivered = int(result["bytes_processed"])
    expires = parse_time(task.get("assignment_expires_at"))
    updated = parse_time(task.get("updated_at"))
    at_deadline = False
    if expires is not None and updated is not None:
        try:
            at_deadline = updated >= expires - timedelta(seconds=2)
        except OverflowError:
            at_deadline = False
    if "lease_expired" in text or at_deadline:
        return "deadline_expired"
    if any(token in text for token in ("http 500", "http 502", "http 503", "http 504", "serviceunavailable")):
        return "storage_failed"
    if "admission" in text or "worker rejected" in text:
        return "worker_failed"
    if state == "cancelled":
        if delivered > 0:
            return "tail_cut"
        return "no_delivery_cut"
    if state == "failed":
        return "worker_failed"
    return ""

def write_unsuccessful(tasks):
    bad = [task for task in tasks if task.get("state") in ("failed", "cancelled", "rejected")]
    now = datetime.now(TZ).strftime("%-I:%M:%S %p")
    lines = [
        "# Unsuccessful tasks",
        "",
        f"Updated {now} (UTC+8). {len(bad)} of {len(tasks)} local tasks.",
        "",
        f"- cancelled: {sum(1 for task in bad if task.get('state') == 'cancelled')}",
        f"- failed: {sum(1 for task in bad if task.get('state') == 'failed')}",
        f"- rejected: {sum(1 for task in bad if task.get('state') == 'rejected')}",
        "",
    ]
    if not bad:
        lines.append("None right now.")
    else:
        lines += [
            "| KIND | WORKER | STATE | REGION | BATCH ID | CREATED | UPDATED | STOP | REASON |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for task in sorted(bad, key=lambda item: item.get("updated_at") or "", reverse=True):
            lines.append(
                "| {kind} | {worker} | {state} | {region} | {batch_id} | {created} | {updated} | {stop} | {reason} |".format(
                    kind=kind_of(task),
                    worker=worker_of(task),
                    state=task.get("state") or "-",
                    region=regions_of(task),
                    batch_id=batch_id_of(task),
                    created=local_clock(parse_time(task.get("created_at"))),
                    updated=local_clock(parse_time(task.get("updated_at"))),
                    stop=stop_reason(task) or "—",
                    reason=task_reason(task) or "-",
                )
            )
        lines.append("")
    os.makedirs(os.path.dirname(UNSUCCESSFUL_PATH) or ".", exist_ok=True)
    with open(UNSUCCESSFUL_PATH, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

# Offers in one burst land together. A gap longer than this starts the next wave.
WAVE_GAP = timedelta(minutes=2)

def last_wave(rows):
    stamped = [row for row in rows if row.get("created")]
    if not stamped:
        return rows
    stamped.sort(key=lambda row: row["created"])
    wave = [stamped[-1]]
    for row in reversed(stamped[:-1]):
        if wave[-1]["created"] - row["created"] > WAVE_GAP:
            break
        wave.append(row)
    wave.reverse()
    return wave

def paint(text, code):
    if not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"

def emphasize(text):
    return paint(text, "1;31")

def complete(text):
    return paint(text, "1;32")

def print_rows(rows):
    if not rows:
        print("  (none)")
        return

    print("  " + "   ".join(f"{label} {ip}" for label, ip in WORKERS.values()))
    print(
        f"  {'TASK ID':<8}  {'WORKER':<6}  {'KIND':<7}  "
        f"{'TIME':>7}  {'DL TIME':>8}  {'UL TIME':>8}  "
        f"{'DL STALL':>8}  {'UL STALL':>8}  "
        f"{'RATE':>10}  {'DL RATE':>7}  {'UL RATE':>7}  "
        f"{'STARTED':>11}  {'REGION':<10}  {'STOP'}"
    )
    print("  " + "-" * 136)

    batches = defaultdict(list)
    for row in rows:
        batches[row.get("batch_id") or "—"].append(row)

    for batch_id, batch_rows in batches.items():
        print()
        print(f"BATCH ID  {batch_id}")

        for row in batch_rows:
            stop_str = format_stop(row.get("state"), row.get("stop"), row.get("in_flight"), row.get("starved", 0.0))
            line = (
                f"  {(row.get('task_id') or '—'):<8}  "
                f"{(row.get('worker') or '—'):<6}  "
                f"{row['kind']:<7}  "
                f"{fmt_secs(row['seconds']):>7}  "
                f"{fmt_sec_val(row.get('dl_time')):>8}  "
                f"{fmt_sec_val(row.get('ul_time')):>8}  "
                f"{fmt_stall_val(row.get('dl_stall')):>8}  "
                f"{fmt_stall_val(row.get('ul_stall')):>8}  "
                f"{fmt_rate(row['mib_s']):>10}  "
                f"{fmt_rate_val(row.get('dl_mib_s')):>7}  "
                f"{fmt_rate_val(row.get('ul_mib_s')):>7}  "
                f"{local_clock(row.get('started')):>11}  "
                f"{(row.get('region') or '—'):<10}  "
                f"{stop_str}"
            )

            if row["state"] == "completed":
                line = complete(line)
            elif not row["in_flight"]:
                line = emphasize(line)

            print(line)

def from_time(rows, since):
    chosen = []
    for row in rows:
        stamps = [row.get("created"), row.get("updated"), row.get("started"), row.get("ended")]
        if any(stamp is not None and stamp >= since for stamp in stamps):
            chosen.append(row)
    return chosen

def pct(sorted_vals, q):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = (len(sorted_vals) - 1) * (q / 100.0)
    lo = int(pos)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac

def summarize_group(rows):
    states = defaultdict(int)
    total = 0
    completed_bytes = 0
    completed_secs = 0.0
    rates = []
    durations = []
    dl_rates = []
    ul_rates = []
    dl_times = []
    ul_times = []
    dl_stalls = []
    ul_stalls = []
    ends = []
    for row in rows:
        states[row["state"]] += 1
        total += row["bytes"] or 0
        if row["state"] == "completed" and row["seconds"]:
            completed_secs += row["seconds"]
            completed_bytes += row["bytes"] or 0
            durations.append(row["seconds"])
            if row["mib_s"]:
                rates.append(row["mib_s"])
            if row.get("dl_mib_s") is not None:
                dl_rates.append(row["dl_mib_s"])
            if row.get("ul_mib_s") is not None:
                ul_rates.append(row["ul_mib_s"])
            if row.get("dl_time") is not None:
                dl_times.append(row["dl_time"])
            if row.get("ul_time") is not None:
                ul_times.append(row["ul_time"])
            if row.get("dl_stall") is not None:
                dl_stalls.append(row["dl_stall"])
            if row.get("ul_stall") is not None:
                ul_stalls.append(row["ul_stall"])
        if row.get("updated"):
            ends.append(row["updated"])
    rates.sort()
    durations.sort()
    dl_rates.sort()
    ul_rates.sort()
    dl_times.sort()
    ul_times.sort()
    dl_stalls.sort()
    ul_stalls.sort()
    starved_vals = [row.get("starved", 0.0) for row in rows if row.get("starved", 0.0) >= 0.05]
    starved_vals.sort()
    avg = (completed_bytes / MIB / completed_secs) if completed_secs else None
    rate_stats = {
        "median": pct(rates, 50),
        "mean": (sum(rates) / len(rates)) if rates else None,
        "avg": avg,
        "p90": pct(rates, 90),
        "p95": pct(rates, 95),
        "p99": pct(rates, 99),
        "max": rates[-1] if rates else None,
        "dl_median": pct(dl_rates, 50),
        "ul_median": pct(ul_rates, 50),
    }
    time_stats = {
        "median": pct(durations, 50),
        "mean": (sum(durations) / len(durations)) if durations else None,
        "p90": pct(durations, 90),
        "p95": pct(durations, 95),
        "p99": pct(durations, 99),
        "max": durations[-1] if durations else None,
        "dl_median": pct(dl_times, 50),
        "ul_median": pct(ul_times, 50),
        "dl_stall_median": pct(dl_stalls, 50),
        "ul_stall_median": pct(ul_stalls, 50),
        "starved_median": pct(starved_vals, 50),
        "starved_max": starved_vals[-1] if starved_vals else None,
    }
    counts = "   ".join(
        f"{name} {states[name]}"
        for name in ("completed", "cancelled", "failed", "rejected", "running")
        if states.get(name)
    )
    inflight = sum(1 for row in rows if row["in_flight"])
    if inflight:
        counts = (counts + "   " if counts else "") + f"in flight {inflight}"
    return total, counts, rate_stats, time_stats, ends

def print_stats_lines(indent, counts, rate_stats, time_stats):
    if counts:
        print(f"{indent}{counts}")
    dl_ul_med = ""
    if rate_stats.get("dl_median") is not None or rate_stats.get("ul_median") is not None:
        dl_s = f"{rate_stats['dl_median']:.1f}" if rate_stats.get("dl_median") is not None else "—"
        ul_s = f"{rate_stats['ul_median']:.1f}" if rate_stats.get("ul_median") is not None else "—"
        dl_ul_med = f" (dl {dl_s} / ul {ul_s})"
    print(
        f"{indent}rate   median {fmt_rate(rate_stats['median'])}{dl_ul_med}   "
        f"mean {fmt_rate(rate_stats['mean'])}   "
        f"avg {fmt_rate(rate_stats['avg'])}   "
        f"p90 {fmt_rate(rate_stats['p90'])}   "
        f"p95 {fmt_rate(rate_stats['p95'])}   "
        f"p99 {fmt_rate(rate_stats['p99'])}   "
        f"max {fmt_rate(rate_stats['max'])}"
    )
    dl_ul_time_med = ""
    if time_stats.get("dl_median") is not None or time_stats.get("ul_median") is not None:
        dl_t = f"{time_stats['dl_median']:.2f}s" if time_stats.get("dl_median") is not None else "—"
        ul_t = f"{time_stats['ul_median']:.2f}s" if time_stats.get("ul_median") is not None else "—"
        dl_ul_time_med = f" (dl {dl_t} / ul {ul_t})"
    print(
        f"{indent}time   median {fmt_secs(time_stats['median'])}{dl_ul_time_med}   "
        f"mean {fmt_secs(time_stats['mean'])}   "
        f"p90 {fmt_secs(time_stats['p90'])}   "
        f"p95 {fmt_secs(time_stats['p95'])}   "
        f"p99 {fmt_secs(time_stats['p99'])}   "
        f"max {fmt_secs(time_stats['max'])}"
    )
    stall_parts = []
    if time_stats.get("dl_stall_median") is not None or time_stats.get("ul_stall_median") is not None:
        dl_st = time_stats.get("dl_stall_median") or 0.0
        ul_st = time_stats.get("ul_stall_median") or 0.0
        dl_st_s = f"{dl_st:.2f}s" if dl_st >= 0.01 else "0s"
        ul_st_s = f"{ul_st:.2f}s" if ul_st >= 0.01 else "0s"
        stall_parts.append(f"median dl {dl_st_s} / ul {ul_st_s}")
    if time_stats.get("starved_max") and time_stats["starved_max"] >= 0.5:
        st_med = time_stats.get("starved_median") or 0.0
        st_max = time_stats["starved_max"]
        st_med_s = f"{st_med:.2f}s" if st_med >= 0.01 else "0s"
        stall_parts.append(f"starved median {st_med_s} / max {st_max:.2f}s")
    if stall_parts:
        print(f"{indent}stall  {'   '.join(stall_parts)}")

def print_since(rows, since):
    chosen = from_time(rows, since)
    chosen.sort(key=lambda row: row["updated"] or row["created"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    total, counts, rate_stats, time_stats, _ = summarize_group(chosen)
    print(f"\nFROM   {local_clock(since)}   {len(chosen)} tasks   {fmt_mib(total)}")
    print_stats_lines("       ", counts, rate_stats, time_stats)
    print()
    print_rows(chosen)
    return chosen

def print_all_24h(rows, since):
    chosen = from_time(rows, since)
    chosen.sort(key=lambda row: row["updated"] or row["created"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    total, counts, rate_stats, time_stats, _ = summarize_group(chosen)
    print(f"\nALL TASKS (24H)   from {local_clock(since)}   {len(chosen)} tasks   {fmt_mib(total)}")
    print_stats_lines("                  ", counts, rate_stats, time_stats)
    print()
    print_rows(chosen)
    return chosen

def print_wave(rows):
    wave = last_wave(rows)
    wave.sort(key=lambda row: row["updated"] or row["created"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    if not wave:
        print("\nWAVE   (none)")
        return []
    started = min(row["created"] for row in wave if row.get("created"))
    total, counts, rate_stats, time_stats, ends = summarize_group(wave)
    span = (max(ends) - started).total_seconds() if ends and started else None
    print(f"\nWAVE   {local_clock(started)}   {len(wave)} tasks   {fmt_mib(total)}   span {fmt_secs(span)}")
    print_stats_lines("       ", counts, rate_stats, time_stats)
    print()
    print_rows(wave)
    return wave

def read_hotkey():
    try:
        with open(".env", encoding="utf-8") as handle:
            for line in handle:
                if line.startswith("BEAM_BITTENSOR_HOTKEY="):
                    return line.split("=", 1)[1].strip()
    except OSError:
        return ""
    return ""

def load_emission():
    try:
        with open(EMISSION_PATH, encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}

def save_emission(data):
    os.makedirs(os.path.dirname(EMISSION_PATH), exist_ok=True)
    with open(EMISSION_PATH, "w", encoding="utf-8") as handle:
        json.dump(data, handle)

def tao_price_usd():
    request = urllib.request.Request(
        "https://api.coingecko.com/api/v3/simple/price?ids=bittensor&vs_currencies=usd",
        headers={"Accept": "application/json", "User-Agent": "beam-throughput"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        body = json.loads(response.read().decode())
    return float(body["bittensor"]["usd"])

def query_stake():
    if not os.path.isfile(BTCLI):
        raise RuntimeError(f"btcli not found at {BTCLI}")
    proc = subprocess.run(
        [BTCLI, "stake", "list", "--wallet", "default", "--network", "finney", "--json", "--quiet"],
        capture_output=True, text=True, timeout=25,
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "btcli failed").strip().splitlines()
        raise RuntimeError(detail[-1][:180] if detail else "btcli failed")
    rows = json.loads(proc.stdout)
    hotkey = read_hotkey()
    for row in rows:
        if row.get("netuid") != 105:
            continue
        if hotkey and row.get("hotkey") != hotkey:
            continue
        return row
    return None

def emission():
    now = time.time()
    cached = load_emission()
    if cached.get("fetched") and now - float(cached["fetched"]) < EMISSION_EVERY:
        return cached
    usd = cached.get("usd_price")
    try:
        usd = tao_price_usd()
    except (OSError, urllib.error.URLError, KeyError, TypeError, ValueError):
        pass
    try:
        row = query_stake()
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError, RuntimeError) as err:
        cached["fetched"] = now
        cached["error"] = str(err)
        if usd is not None:
            cached["usd_price"] = usd
        save_emission(cached)
        return cached
    if row is None:
        cached = {"fetched": now, "error": "no subnet 105 stake", "usd_price": usd}
    else:
        tao = float(row.get("value_tao") or 0)
        cached = {
            "fetched": now,
            "alpha": row.get("stake_amount"),
            "tao": tao,
            "usd_price": usd,
            "usd": None if usd is None else tao * float(usd),
            "error": None,
        }
    save_emission(cached)
    return cached

def print_legend():
    print("\nLEGEND")
    print("  KIND    TM: transfer.multipart   RT: room.transfer   AP: authenticated_part   SP: storage.probe")
    print("  RATES   RATE is total in MiB/s; DL RATE (source CDN) and UL RATE (destination storage) are in MiB/s")
    print("  TIMES   TIME is total task duration; DL TIME and UL TIME are download and upload durations")
    print("  STALLS  DL STALL is source freeze time; UL STALL is destination socket/TCP write stall time")
    print("  STARVE  In STOP column, indicates task was concurrency/buffer starved for >= 1s")

def print_stop_legend(rows):
    seen = {row.get("stop") for row in rows if row.get("stop")}
    lines = [(code, meaning) for code, meaning in STOP_REASONS if code in seen]
    if not lines:
        return
    print("\nWHY WORK STOPPED")
    for code, meaning in lines:
        print(f"  {code:<20} {meaning}")
    print()

def print_emission(row):
    if not row or row.get("tao") is None:
        print(f"EMISSION  {row.get('error') or 'subnet 105 stake unavailable'}")
        return
    alpha = row.get("alpha")
    alpha_s = "—" if alpha is None else f"{float(alpha):.6f}"
    usd = row.get("usd")
    usd_s = "—" if usd is None else f"${usd:,.4f}"
    price = row.get("usd_price")
    price_s = "" if price is None else f"   TAO ${float(price):,.2f}"
    print(f"EMISSION  sn105   {alpha_s} alpha   {float(row['tao']):.6f} TAO   {usd_s}{price_s}")

def clear_terminal():
    try:
        subprocess.run(["clear"], check=False)
    except Exception:
        os.system("clear")

def render(show_all):
    tasks = fetch_tasks()
    write_unsuccessful(tasks)
    cutoff_24h = datetime.now(TZ) - timedelta(hours=24) if show_all else None
    pace = pace_info(SINCE or cutoff_24h)
    prelim = [enrich(task, {}, pace) for task in tasks]
    medians = medians_of(prelim)
    rows = [enrich(task, medians, pace) for task in tasks]
    rows.sort(key=lambda row: row["updated"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    stake = emission()

    clear_terminal()
    print_emission(stake)
    if show_all:
        shown = print_all_24h(rows, cutoff_24h)
    elif SINCE is not None:
        shown = print_since(rows, SINCE)
    else:
        shown = print_wave(rows)
    print_legend()
    print_stop_legend(shown)
    print()

def main():
    show_all = MODE == "all"
    while True:
        render(show_all)
        if MODE != "watch":
            return
        time.sleep(2)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as err:
        print(f"throughput: {err}", file=sys.stderr)
        sys.exit(1)
PY
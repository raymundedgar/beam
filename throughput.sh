#!/bin/bash
# Task size, duration, throughput, and stall hints (disk / memory / cpu / network).
#
#   ./throughput.sh          last wave
#   ./throughput.sh watch    refresh every 2s and sample the host during in-flight work
#   ./throughput.sh all      every local task
set -euo pipefail
cd "$(dirname "$0")"

MODE="${1:-snapshot}"
case "$MODE" in
  snapshot|watch|all) ;;
  *) echo "usage: $0 [snapshot|watch|all]" >&2; exit 2 ;;
esac

TOKEN=$(cat data/orchestrator/control-token)
mkdir -p data/analytics
SAMPLES=data/analytics/host-samples.jsonl

python3 - "$MODE" "$TOKEN" "$SAMPLES" <<'PY'
import ipaddress, json, os, re, sys, time, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

MODE, TOKEN, SAMPLE_PATH = sys.argv[1], sys.argv[2], sys.argv[3]
TZ = timezone(timedelta(hours=8))
MIB = 1024 * 1024
SAMPLE_KEEP = timedelta(hours=24)
IN_FLIGHT = {"received", "offered", "reserved", "committed", "running", "starting"}

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

def read_cpu():
    with open("/proc/stat", encoding="utf-8") as fh:
        parts = fh.readline().split()
    nums = [int(x) for x in parts[1:8]]
    return {"total": sum(nums), "idle": nums[3], "iowait": nums[4], "busy": sum(nums) - nums[3] - nums[4]}

def root_disk():
    try:
        with open("/proc/self/mountinfo", encoding="utf-8") as fh:
            for line in fh:
                if " - " not in line:
                    continue
                left, right = line.split(" - ", 1)
                fields = left.split()
                if len(fields) < 5 or fields[4] != "/":
                    continue
                rest = right.split()
                if len(rest) < 2:
                    continue
                return os.path.basename(rest[1])
    except OSError:
        return None
    return None

def read_disk_ticks(name):
    if not name:
        return None
    with open("/proc/diskstats", encoding="utf-8") as fh:
        for line in fh:
            parts = line.split()
            if len(parts) >= 13 and parts[2] == name:
                return int(parts[12])
    # try parent disk if partition (sda1 -> sda)
    trimmed = name.rstrip("0123456789")
    if trimmed and trimmed != name:
        return read_disk_ticks(trimmed)
    return None

def read_mem():
    info = {}
    with open("/proc/meminfo", encoding="utf-8") as fh:
        for line in fh:
            key, value = line.split(":", 1)
            info[key] = int(value.strip().split()[0]) * 1024
    total = info.get("MemTotal", 1)
    avail = info.get("MemAvailable", info.get("MemFree", 0))
    return {
        "total": total,
        "avail": avail,
        "used_pct": 100.0 * (1.0 - avail / total),
        "avail_pct": 100.0 * avail / total,
        "avail_mib": avail / MIB,
    }

def primary_iface():
    try:
        with open("/proc/net/route", encoding="utf-8") as fh:
            next(fh)
            for line in fh:
                parts = line.split()
                if len(parts) >= 2 and parts[1] == "00000000":
                    return parts[0]
    except OSError:
        pass
    return None

def read_net(iface):
    if not iface:
        return None
    with open("/proc/net/dev", encoding="utf-8") as fh:
        for line in fh:
            if ":" not in line:
                continue
            name, rest = line.split(":", 1)
            if name.strip() != iface:
                continue
            nums = [int(x) for x in rest.split()]
            return {"rx": nums[0], "tx": nums[8]}
    return None

def worker_rss():
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/comm", encoding="utf-8") as fh:
                comm = fh.read().strip()
        except OSError:
            continue
        if comm != "beam-worker":
            continue
        try:
            with open(f"/proc/{pid}/status", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1]) / 1024.0
        except OSError:
            continue
    return None

def snapshot_counters():
    disk = root_disk()
    iface = primary_iface()
    return {
        "t": time.time(),
        "cpu": read_cpu(),
        "disk": read_disk_ticks(disk),
        "disk_name": disk,
        "net": read_net(iface),
        "iface": iface,
        "mem": read_mem(),
        "worker_rss_mib": worker_rss(),
    }

def rates(prev, curr):
    dt = max(curr["t"] - prev["t"], 1e-6)
    cpu_dt = max(curr["cpu"]["total"] - prev["cpu"]["total"], 1)
    iowait = 100.0 * (curr["cpu"]["iowait"] - prev["cpu"]["iowait"]) / cpu_dt
    cpu = 100.0 * (curr["cpu"]["busy"] - prev["cpu"]["busy"]) / cpu_dt
    disk = None
    if prev["disk"] is not None and curr["disk"] is not None:
        disk = min(100.0, 100.0 * (curr["disk"] - prev["disk"]) / (dt * 1000.0))
    rx = tx = None
    if prev["net"] and curr["net"]:
        rx = 8.0 * (curr["net"]["rx"] - prev["net"]["rx"]) / dt / 1_000_000
        tx = 8.0 * (curr["net"]["tx"] - prev["net"]["tx"]) / dt / 1_000_000
    mem = curr["mem"]
    return {
        "t": curr["t"],
        "cpu": max(0.0, cpu),
        "iowait": max(0.0, iowait),
        "disk": None if disk is None else max(0.0, disk),
        "mem_used_pct": mem["used_pct"],
        "mem_avail_pct": mem["avail_pct"],
        "mem_avail_mib": mem["avail_mib"],
        "net_rx_mbps": rx,
        "net_tx_mbps": tx,
        "worker_rss_mib": curr["worker_rss_mib"],
        "iface": curr["iface"],
        "disk_name": curr["disk_name"],
    }

def classify_sample(sample):
    if sample["mem_avail_pct"] < 12 or sample["mem_avail_mib"] < 256:
        return "memory"
    disk = sample.get("disk") or 0
    if sample["iowait"] >= 20 or disk >= 70:
        return "disk"
    if sample["cpu"] >= 85:
        return "cpu"
    return None

def load_samples():
    cutoff = time.time() - SAMPLE_KEEP.total_seconds()
    rows = []
    try:
        with open(SAMPLE_PATH, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if row.get("t", 0) >= cutoff:
                    rows.append(row)
    except FileNotFoundError:
        pass
    return rows

def append_sample(sample):
    os.makedirs(os.path.dirname(SAMPLE_PATH) or ".", exist_ok=True)
    with open(SAMPLE_PATH, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(sample) + "\n")
    # rewrite if the file grew too large
    try:
        if os.path.getsize(SAMPLE_PATH) > 2_000_000:
            rows = load_samples()
            with open(SAMPLE_PATH, "w", encoding="utf-8") as fh:
                for row in rows[-4000:]:
                    fh.write(json.dumps(row) + "\n")
    except OSError:
        pass

def stall_for(task, samples, live, slow):
    state = task.get("state") or ""
    start, end = task_window(task)
    window = []
    if start and end:
        start_ts, end_ts = start.timestamp(), end.timestamp()
        window = [row for row in samples if start_ts - 1 <= row.get("t", 0) <= end_ts + 1]
    if state in IN_FLIGHT and live:
        window = window + [live]
    if not window:
        return "—" if not slow else "unsampled"
    votes = defaultdict(int)
    for row in window:
        reason = classify_sample(row)
        if reason:
            votes[reason] += 1
    if votes:
        return max(votes, key=votes.get)
    if slow:
        return "network"
    return "—"

def enrich(task, samples, live, medians):
    kind = kind_of(task)
    state = task.get("state") or "-"
    nbytes = task_bytes(task)
    start, end = task_window(task)
    seconds = (end - start).total_seconds() if start and end else None
    # A refusal is recorded in a few milliseconds. Offered size over that
    # interval is not a transfer rate.
    if state == "rejected":
        seconds = None
    mib_s = (nbytes / MIB / seconds) if nbytes and seconds and seconds > 0 else None
    median = medians.get(kind)
    slow = bool(mib_s is not None and median and mib_s < 0.5 * median and seconds >= 2)
    stall = stall_for(task, samples, live, slow)
    return {
        "kind": kind,
        "state": state,
        "bytes": nbytes,
        "seconds": seconds,
        "mib_s": mib_s,
        "slow": slow,
        "stall": stall,
        "updated": parse_time(task.get("updated_at")),
        "created": parse_time(task.get("created_at")),
        "in_flight": state in IN_FLIGHT,
        "id": (task.get("external_id") or "")[:10],
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

def print_host(sample):
    if not sample:
        print("HOST   sampling…")
        return
    disk = "—" if sample["disk"] is None else f"{sample['disk']:.0f}%"
    rx = "—" if sample["net_rx_mbps"] is None else f"{sample['net_rx_mbps']:.0f}"
    tx = "—" if sample["net_tx_mbps"] is None else f"{sample['net_tx_mbps']:.0f}"
    rss = "—" if sample["worker_rss_mib"] is None else f"{sample['worker_rss_mib']:.0f} MiB"
    bottleneck = classify_sample(sample) or "ok"
    print(
        f"HOST   cpu {sample['cpu']:.0f}%   iowait {sample['iowait']:.0f}%   "
        f"mem {sample['mem_used_pct']:.0f}% used ({sample['mem_avail_mib']:.0f} MiB free)   "
        f"disk {disk}   net rx {rx} / tx {tx} Mbps   worker {rss}   now {bottleneck}"
    )

UNSUCCESSFUL_PATH = os.path.join(os.path.dirname(SAMPLE_PATH) or ".", "unsuccessful.md")

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
            "| KIND | STATE | CREATED | UPDATED | REASON |",
            "| --- | --- | --- | --- | --- |",
        ]
        for task in sorted(bad, key=lambda item: item.get("updated_at") or "", reverse=True):
            lines.append(
                "| {kind} | {state} | {created} | {updated} | {reason} |".format(
                    kind=kind_of(task),
                    state=task.get("state") or "-",
                    created=local_clock(parse_time(task.get("created_at"))),
                    updated=local_clock(parse_time(task.get("updated_at"))),
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

def emphasize(text):
    if not sys.stdout.isatty():
        return text
    return f"\033[1;31m{text}\033[0m"

def print_rows(rows):
    if not rows:
        print("  (none)")
        return
    print(f"  {'KIND':<32} {'STATE':<10} {'SIZE':>10} {'TIME':>8} {'RATE':>10}  {'STALL':<10} {'UPDATED'}")
    print("  " + "-" * 98)
    for row in rows:
        mark = "*" if row["in_flight"] and row["stall"] not in ("—", "ok") else " "
        line = (
            f"{mark} {row['kind']:<32} {row['state']:<10} {fmt_mib(row['bytes']):>10} "
            f"{fmt_secs(row['seconds']):>8} {fmt_rate(row['mib_s']):>10}  "
            f"{row['stall']:<10} {local_clock(row['updated'])}"
        )
        if row["state"] != "completed" and not row["in_flight"]:
            line = emphasize(line)
        print(line)

def print_wave(rows):
    wave = last_wave(rows)
    wave.sort(key=lambda row: row["updated"] or row["created"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    if not wave:
        print("\nWAVE   (none)")
        return
    started = min(row["created"] for row in wave if row.get("created"))
    states = defaultdict(int)
    total = 0
    completed_bytes = 0
    completed_secs = 0.0
    rates = []
    ends = []
    for row in wave:
        states[row["state"]] += 1
        total += row["bytes"] or 0
        if row["state"] == "completed" and row["seconds"]:
            completed_secs += row["seconds"]
            completed_bytes += row["bytes"] or 0
            if row["mib_s"]:
                rates.append(row["mib_s"])
        if row.get("updated"):
            ends.append(row["updated"])
    rates.sort()
    median = rates[len(rates) // 2] if rates else None
    avg = (completed_bytes / MIB / completed_secs) if completed_secs else None
    span = (max(ends) - started).total_seconds() if ends and started else None
    counts = "   ".join(
        f"{name} {states[name]}"
        for name in ("completed", "cancelled", "failed", "rejected", "running")
        if states.get(name)
    )
    inflight = sum(1 for row in wave if row["in_flight"])
    if inflight:
        counts = (counts + "   " if counts else "") + f"in flight {inflight}"
    print(f"\nWAVE   {local_clock(started)}   {len(wave)} tasks   {fmt_mib(total)}   span {fmt_secs(span)}")
    if counts:
        print(f"       {counts}")
    print(f"       completed median {fmt_rate(median)}   avg {fmt_rate(avg)}")
    print()
    print_rows(wave)

def render(live, samples, show_all):
    tasks = fetch_tasks()
    write_unsuccessful(tasks)
    prelim = [enrich(task, samples, None, {}) for task in tasks]
    medians = medians_of(prelim)
    rows = [enrich(task, samples, live, medians) for task in tasks]
    rows.sort(key=lambda row: row["updated"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)

    print_host(live)
    print_wave(rows)
    if show_all:
        print(f"\nALL TASKS  {len(rows)}")
        print_rows(rows)
    print()

def take_live(prev):
    curr = snapshot_counters()
    if prev is None:
        time.sleep(0.25)
        curr2 = snapshot_counters()
        live = rates(curr, curr2)
        return curr2, live
    live = rates(prev, curr)
    return curr, live

def main():
    samples = load_samples()
    prev = None
    show_all = MODE == "all"
    while True:
        prev, live = take_live(prev)
        samples.append(live)
        append_sample(live)
        if MODE == "watch":
            sys.stdout.write("\033[2J\033[H")
        render(live, samples, show_all)
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
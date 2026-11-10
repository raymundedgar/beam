#!/bin/bash
# Task size, duration, and throughput for the last wave, plus subnet 105 stake.
#
#   ./throughput.sh          last wave
#   ./throughput.sh watch    refresh every 2s; stake is read at most every 5 minutes
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

python3 - "$MODE" "$TOKEN" <<'PY'
import hashlib, ipaddress, json, os, re, subprocess, sys, time, urllib.error, urllib.parse, urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

MODE, TOKEN = sys.argv[1], sys.argv[2]
TZ = timezone(timedelta(hours=8))
MIB = 1024 * 1024
EMISSION_EVERY = 5 * 60
EMISSION_PATH = "data/analytics/emission.json"
BTCLI = os.environ.get("BTCLI", "/root/beam/.venv/bin/btcli")
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

def upload_of(task):
    payload = payload_of(task)
    found = []

    def add(url):
        if not isinstance(url, str) or "upload" not in url.lower():
            return
        try:
            query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
        except ValueError:
            return
        uid = (query.get("uploadId") or query.get("upload_id") or [""])[0]
        if not uid:
            return
        short = hashlib.sha256(uid.encode()).hexdigest()[:8]
        if short not in found:
            found.append(short)

    parts = payload.get("parts")
    if isinstance(parts, list):
        for part in parts:
            if isinstance(part, dict):
                dest = part.get("destination") if isinstance(part.get("destination"), dict) else {}
                add(dest.get("url"))
    for key in ("destinations", "targets"):
        items = payload.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                add(item.get("url") or (item.get("endpoint") or {}).get("url") if isinstance(item.get("endpoint"), dict) else item.get("url"))
    return "+".join(found) if found else "—"

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

def enrich(task, medians):
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
    return {
        "kind": kind,
        "state": state,
        "bytes": nbytes,
        "seconds": seconds,
        "mib_s": mib_s,
        "started": start,
        "ended": None if state in IN_FLIGHT else end,
        "updated": parse_time(task.get("updated_at")),
        "created": parse_time(task.get("created_at")),
        "in_flight": state in IN_FLIGHT,
        "id": (task.get("external_id") or "")[:10],
        "stop": stop_reason(task),
        "region": regions_of(task),
        "upload": upload_of(task),
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
            "| KIND | STATE | REGION | UPLOAD | CREATED | UPDATED | STOP | REASON |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for task in sorted(bad, key=lambda item: item.get("updated_at") or "", reverse=True):
            lines.append(
                "| {kind} | {state} | {region} | {upload} | {created} | {updated} | {stop} | {reason} |".format(
                    kind=kind_of(task),
                    state=task.get("state") or "-",
                    region=regions_of(task),
                    upload=upload_of(task),
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

def emphasize(text):
    if not sys.stdout.isatty():
        return text
    return f"\033[1;31m{text}\033[0m"

def print_rows(rows):
    if not rows:
        print("  (none)")
        return
    print(
        f"  {'KIND':<32} {'STATE':<10} {'SIZE':>10} {'TIME':>8} {'RATE':>10}  "
        f"{'REGION':<14} {'UPLOAD':<10} {'STOP':<18} {'START':<12} {'END'}"
    )
    print("  " + "-" * 142)
    for row in rows:
        line = (
            f"  {row['kind']:<32} {row['state']:<10} {fmt_mib(row['bytes']):>10} "
            f"{fmt_secs(row['seconds']):>8} {fmt_rate(row['mib_s']):>10}  "
            f"{(row.get('region') or '—'):<14} {(row.get('upload') or '—'):<10} "
            f"{(row.get('stop') or '—'):<18} {local_clock(row.get('started')):<12} "
            f"{local_clock(row.get('ended'))}"
        )
        if row["state"] != "completed" and not row["in_flight"]:
            line = emphasize(line)
        print(line)

def print_wave(rows):
    wave = last_wave(rows)
    wave.sort(key=lambda row: row["updated"] or row["created"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)
    if not wave:
        print("\nWAVE   (none)")
        return []
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

def render(show_all):
    tasks = fetch_tasks()
    write_unsuccessful(tasks)
    prelim = [enrich(task, {}) for task in tasks]
    medians = medians_of(prelim)
    rows = [enrich(task, medians) for task in tasks]
    rows.sort(key=lambda row: row["updated"] or datetime.fromtimestamp(0, tz=timezone.utc), reverse=True)

    print_emission(emission())
    print_stop_legend(print_wave(rows))
    if show_all:
        print(f"\nALL TASKS  {len(rows)}")
        print_rows(rows)
    print()

def main():
    show_all = MODE == "all"
    while True:
        if MODE == "watch":
            sys.stdout.write("\033[2J\033[H")
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
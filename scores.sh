#!/bin/bash
# Published BeamCore PRISM snapshot for this orchestrator.
# Throughput and verified bandwidth are the same goodput figure.
#
#   ./scores.sh            normal run (makes at most one audit read)
#   ./scores.sh --force    ignore the saved rate-limit hold and the freshness
#                          check for this run and ask BeamCore right now
#
# BEAM_SCORES_WAIT   seconds to wait after a refused read when BeamCore sends no
#                    Retry-After (default 300; doubles on each repeat, max 1h)
# DATAIMPULSE_USER / DATAIMPULSE_PASS in .env send audit reads through
# gw.dataimpulse.com. Leave the password empty to connect directly.
set -euo pipefail
cd "$(dirname "$0")"

python3 - "$@" <<'PY'
import email.utils, json, os, sys, time, urllib.error, urllib.parse, urllib.request
from datetime import datetime, timedelta, timezone

TZ = timezone(timedelta(hours=8))
HEADERS = {"Accept": "application/json", "User-Agent": "beam-scores"}
ORDER = {"standard_transfers": 0, "room_transfers": 1}
API = os.environ.get("BEAM_API", "https://api.b1m.ai").rstrip("/")
FORCE = any(arg in ("-f", "--force") for arg in sys.argv[1:])

class ScoreError(Exception):
    def __init__(self, message, status=None, retry_after=None):
        super().__init__(message)
        self.status = status
        self.retry_after = retry_after

def parse_retry_after(value):
    if not value:
        return None
    value = value.strip()
    try:
        return max(0, int(float(value)))
    except ValueError:
        pass
    try:
        moment = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if moment is None:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return max(0, int((moment - datetime.now(timezone.utc)).total_seconds()))

def dotenv(path=".env"):
    found = {}
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                found[key.strip()] = value.strip().strip('"').strip("'")
    except OSError:
        pass
    return found

def setting(key, found, default=""):
    value = os.environ.get(key)
    if value is not None and value.strip():
        return value.strip()
    return found.get(key, default).strip()

def audit_opener():
    found = dotenv()
    user = setting("DATAIMPULSE_USER", found)
    password = setting("DATAIMPULSE_PASS", found)
    if not user or not password:
        return urllib.request.build_opener(), False
    host = setting("DATAIMPULSE_HOST", found, "gw.dataimpulse.com")
    port = setting("DATAIMPULSE_PORT", found, "823")
    proxy = "http://%s:%s@%s:%s" % (
        urllib.parse.quote(user, safe=""),
        urllib.parse.quote(password, safe=""),
        host,
        port,
    )
    handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})]
    return urllib.request.build_opener(*handlers), True

OPENER, USING_PROXY = audit_opener()

def get_json(url):
    request = urllib.request.Request(url, headers=HEADERS)
    try:
        with OPENER.open(request, timeout=30) as response:
            return json.loads(response.read().decode())
    except urllib.error.HTTPError as err:
        detail = err.read().decode(errors="replace")[:200]
        raise ScoreError(
            f"HTTP {err.code} ({detail.strip() or err.reason})",
            status=err.code,
            retry_after=parse_retry_after(err.headers.get("Retry-After")),
        )
    except (OSError, ValueError) as err:
        # timeouts, DNS failures, dropped connections, bodies that are not JSON
        raise ScoreError(f"request failed: {err}")

def clock(value):
    if not value:
        return "—"
    text = str(value).replace("Z", "+00:00")
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        return str(value)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(TZ).strftime("%-I:%M:%S %p")

def at(timestamp):
    return clock(datetime.fromtimestamp(timestamp, timezone.utc).isoformat())

def num(value, digits):
    if value is None or value == "":
        return "—"
    return f"{float(value):.{digits}f}"

def points(value):
    if value is None or value == "":
        return "—"
    amount = int(float(value))
    if amount > 0:
        return f"+{amount}"
    return str(amount)

def highlight(text):
    if not sys.stdout.isatty():
        return text
    return f"\033[1;33m{text}\033[0m"

GLOSSARY = [
    ("PRISM", "performance × reliability × readiness × penalty. Each workload is scored on its own."),
    ("performance", "Where your 24h duel points sit between the lowest and highest miner in this pool."),
    ("readiness", "Control connection and availability. 1.000 means Beam sees you ready."),
    ("reliability", "Verified successes divided by accountable tasks over 24h, recovery included."),
    ("penalty", "Integrity, fraud, or Sybil cut. 1.000 means none."),
    ("confidence", "Qualification progress. 0.900 is the line for the qualified pool."),
    ("throughput", "Verified goodput of finished assignments. Same figure as verified bandwidth, and not a PRISM factor."),
]
def mbps(value):
    if value is None or value == "":
        return "—"
    return f"{float(value):.1f} Mbps"

with open("data/orchestrator/registry.json", encoding="utf-8") as handle:
    orchestrator = json.load(handle)["orchestrator"]
orchestrator_id = orchestrator["OrchestratorID"]
hotkey = orchestrator["Hotkey"]

def scoring_url(epoch):
    return (
        f"{API}/v1/audit/scoring/"
        f"{epoch}/orchestrator/{urllib.parse.quote(orchestrator_id)}"
    )

def profile_map(snapshot):
    found = {}
    for profile in (snapshot.get("data") or {}).get("profiles") or []:
        found[profile.get("workload")] = profile
    return found

CACHE = "data/analytics/prism-epochs.json"
TRANSFER_CACHE = "data/analytics/prism-transfers.json"

def load_cache():
    try:
        with open(CACHE, encoding="utf-8") as handle:
            cached = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return cached if isinstance(cached, dict) else {}

def save_cache(cached):
    os.makedirs(os.path.dirname(CACHE), exist_ok=True)
    with open(CACHE, "w", encoding="utf-8") as handle:
        json.dump(cached, handle)

def load_json(path):
    try:
        with open(path, encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}

def save_json(path, value):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(value, handle)

def print_history(cached):
    print(f"  {'EPOCH':<8} {'PUBLISHED':<12} {'LEADERBOARD':>12} {'STANDARD':>10} {'ROOM':>8}")
    for key in sorted(cached, key=int, reverse=True)[:8]:
        item = cached[key]
        print(
            f"  {key:<8} {clock(item.get('publishedAt')):<12} "
            f"{num(item.get('leaderboard'), 3):>12} "
            f"{num(item.get('standard'), 3):>10} {num(item.get('room'), 3):>8}"
        )

def row_from(item):
    item_data = item.get("data") or {}
    mapped = profile_map(item)
    return {
        "publishedAt": item.get("publishedAt"),
        "observedAt": item.get("observedAt"),
        "leaderboard": item_data.get("emissionPrismTiebreakScore"),
        "standard": (mapped.get("standard_transfers") or {}).get("prismFinalScore"),
        "room": (mapped.get("room_transfers") or {}).get("prismFinalScore"),
        "snapshot": item,
        "fetchedAt": time.time(),
    }

def change_line(label, profile, penalty, bandwidth):
    if label == "performance":
        return (
            f"performance {num(profile.get('performanceScore'), 3)}  "
            f"({points(profile.get('performancePoints24h'))} pts over 24h, verified {bandwidth})"
        )
    if label == "reliability":
        reliable = profile.get("reliableTaskCount")
        total = profile.get("reliabilityTaskCount")
        detail = num(profile.get("reliabilityMultiplier"), 3)
        if reliable is not None and total:
            missed = int(total) - int(reliable)
            detail = f"{detail}  ({missed} of {int(total)} accountable tasks unverified)"
        return f"reliability {detail}"
    if label == "readiness":
        return f"readiness {num(profile.get('readinessMultiplier'), 3)}"
    if label == "penalty":
        return f"penalty {num(penalty, 3)}"
    return label

def print_snapshot(snapshot, note):
    data = snapshot.get("data") or {}
    epoch = data.get("epoch") or "—"
    profiles = sorted(data.get("profiles") or [], key=lambda profile: ORDER.get(profile.get("workload"), 9))
    print(f"PRISM   epoch {epoch}   published {clock(snapshot.get('publishedAt'))} UTC+8")
    print(f"        observed {clock(snapshot.get('observedAt'))}   pool {data.get('pool') or '—'}   tier {data.get('emissionTier') or '—'}")
    print(f"        leaderboard {num(data.get('emissionPrismTiebreakScore'), 3)}")
    if note:
        print(f"        {note}")
    if USING_PROXY:
        print("        audit via DataImpulse")
    print()
    changes = []
    for profile in profiles:
        name = (profile.get("workload") or "—").replace("_", " ")
        reliable = profile.get("reliableTaskCount")
        total = profile.get("reliabilityTaskCount")
        reliability = num(profile.get("reliabilityMultiplier"), 3)
        if reliable is not None and total:
            reliability = f"{reliability}  ({reliable}/{total})"
        bandwidth = mbps(profile.get("verifiedBandwidthMbps"))
        penalty = profile.get("penaltyMultiplier")
        if penalty is None:
            penalty = data.get("penaltyMultiplier")
        factors = [
            ("performance", profile.get("performanceScore")),
            ("reliability", profile.get("reliabilityMultiplier")),
            ("readiness", profile.get("readinessMultiplier")),
            ("penalty", penalty),
        ]
        numbers = [(name, float(value)) for name, value in factors if value is not None and value != ""]
        bottleneck = "—"
        if numbers:
            lowest = min(value for _, value in numbers)
            factors_low = [label for label, value in numbers if value == lowest]
            bottleneck = f"{' & '.join(factors_low)} {num(lowest, 3)}"
            changes.append((name, [change_line(label, profile, penalty, bandwidth) for label in factors_low]))
        rows = [
            ("bottleneck", bottleneck),
            ("PRISM", num(profile.get("prismFinalScore"), 3)),
            ("performance", f"{num(profile.get('performanceScore'), 3)}  ({points(profile.get('performancePoints24h'))} pts)"),
            ("readiness", num(profile.get("readinessMultiplier"), 3)),
            ("reliability", reliability),
            ("penalty", num(penalty, 3)),
            ("confidence", num(profile.get("confidenceScore"), 3)),
            ("throughput", bandwidth),
            ("verified bandwidth", bandwidth),
        ]
        print(f"{name}   pool {profile.get('pool') or '—'}")
        for label, value in rows:
            line = f"  {label:<20} {value}"
            if label == "bottleneck":
                line = highlight(line)
            print(line)
        print()
    print("LEGEND")
    for label, meaning in GLOSSARY:
        print(f"  {label:<20} {meaning}")
    print()
    if changes:
        print("  change")
        for workload, lines in changes:
            print(f"    {workload}")
            for line in lines:
                print(f"      {line}")
        print()

# ---------------------------------------------------------------------------
# Refresh
#
# A new public snapshot arrives about once an hour. The audit route allows
# about one read per window, so a run makes AT MOST ONE audit read, picked in
# this order:
#   1. download the card of an epoch we already know is live   (_nextEpoch)
#   2. ask which epoch is live, only when the saved card is stale
#   3. fetch one missing failure record (only when 1 and 2 were not needed)
# A refused read (429) holds further reads for the time BeamCore asks for
# (Retry-After), or for a back-off that starts at BEAM_SCORES_WAIT and doubles.
# Any successful read clears the hold. --force skips the hold for one run.
# ---------------------------------------------------------------------------
FRESH = 50 * 60
try:
    BASE_WAIT = max(30, int(os.environ.get("BEAM_SCORES_WAIT", "300")))
except ValueError:
    BASE_WAIT = 300
MAX_WAIT = 60 * 60

def usable(snapshot):
    """A card is only worth caching if it actually carries scores."""
    if not isinstance(snapshot, dict):
        return False
    data = snapshot.get("data")
    return isinstance(data, dict) and bool(data.get("profiles"))

cached = load_cache()
# An empty or partial card saved by an earlier run would otherwise count as
# "already downloaded" forever. Forget it so it is fetched again.
for key in [k for k, row in cached.items()
            if isinstance(row, dict) and row.get("snapshot") is not None and not usable(row.get("snapshot"))]:
    del cached[key]

def has_card(epoch):
    return usable((cached.get(str(epoch)) or {}).get("snapshot"))

newest_key = max(cached, key=int) if cached else None
newest = cached.get(newest_key) if newest_key else None
published = None
if newest and newest.get("publishedAt"):
    try:
        published = datetime.fromisoformat(str(newest.get("publishedAt")).replace("Z", "+00:00"))
        if published.tzinfo is None:
            published = published.replace(tzinfo=timezone.utc)
    except ValueError:
        published = None
fresh = (not FORCE) and published is not None and (datetime.now(timezone.utc) - published).total_seconds() < FRESH
snapshot = (newest or {}).get("snapshot")
audit = load_json(TRANSFER_CACHE)
blocked_until = 0.0 if FORCE else float(audit.get("_blockedUntil") or 0)
next_epoch = str(audit.get("_nextEpoch") or "")
if next_epoch and has_card(next_epoch):
    audit.pop("_nextEpoch", None)          # pointer already satisfied
    next_epoch = ""
note = None
called = False

def audit_ok():
    audit.pop("_blockedUntil", None)
    audit.pop("_blockStrikes", None)

def audit_refused(err):
    strikes = int(audit.get("_blockStrikes") or 0)
    wait = err.retry_after if err.retry_after is not None else BASE_WAIT * (2 ** strikes)
    wait = min(max(wait, 30), MAX_WAIT)
    audit["_blockStrikes"] = strikes + 1
    audit["_blockedUntil"] = time.time() + wait
    return audit["_blockedUntil"]

if time.time() < blocked_until:
    parts = [f"saved epoch {newest_key}"]
    if next_epoch and next_epoch != str(newest_key):
        parts.append(f"live epoch {next_epoch}")
    parts.append(f"audit limit, next try {at(blocked_until)}")
    note = "; ".join(parts)
elif next_epoch:
    called = True
    try:
        fetched = get_json(scoring_url(next_epoch))
        if not usable(fetched):
            raise ScoreError(f"epoch {next_epoch} has no scores yet")
        cached[next_epoch] = row_from(fetched)
        save_cache(cached)
        newest_key = max(cached, key=int)
        snapshot = cached[newest_key].get("snapshot")
        audit.pop("_nextEpoch", None)
        audit_ok()
    except ScoreError as err:
        if err.status == 429:
            until = audit_refused(err)
            note = f"saved epoch {newest_key}; live epoch {next_epoch} refused, next try {at(until)}"
        else:
            # Do not keep retrying a pointer that may be stale: forget it so the
            # next run asks which epoch is live instead.
            audit.pop("_nextEpoch", None)
            note = f"saved epoch {newest_key}; live epoch {next_epoch} read failed ({err}); will re-check"
elif not fresh:
    called = True
    try:
        allowance = get_json(f"{API}/v1/audit/sell-allowance/{urllib.parse.quote(hotkey)}")
        epoch = str((allowance.get("data") or {}).get("epoch") or "")
        if not epoch:
            raise ScoreError("BeamCore did not return an epoch")
        if not epoch.isdigit():
            raise ScoreError(f"unexpected epoch {epoch!r}")
        audit_ok()
        if has_card(epoch):
            note = "saved snapshot"
        else:
            audit["_nextEpoch"] = int(epoch)
            note = f"saved epoch {newest_key}; live epoch {epoch} downloads on the next run"
    except ScoreError as err:
        if err.status == 429:
            until = audit_refused(err)
            note = f"saved epoch {newest_key}; audit limit, next try {at(until)}"
        else:
            note = f"saved epoch {newest_key}; live read failed ({err})"
        if snapshot is None and not cached:
            save_json(TRANSFER_CACHE, audit)
            raise SystemExit(f"scores: BeamCore read failed ({err}), and no saved snapshot yet")
save_json(TRANSFER_CACHE, audit)

FAILURE_SIDES = {
    "worker_failed": "your side",
    "storage_failed": "your side",
    "path_failed": "your side",
    "endpoint_failed": "your side",
    "beam_side": "Beam",
    "deadline_expired": "timeout or cut",
    "progress_timeout": "timeout or cut",
    "no_delivery_cut": "timeout or cut",
    "tail_cut": "timeout or cut",
}

def local_transfer_ids():
    try:
        with open("data/orchestrator/tasks.json", encoding="utf-8") as handle:
            records = json.load(handle).get("records") or {}
    except (OSError, json.JSONDecodeError):
        return []
    found = {}
    for task in records.values():
        if task.get("state") not in ("failed", "cancelled"):
            continue
        transfer_id = task.get("transfer_id")
        if transfer_id:
            found[transfer_id] = max(found.get(transfer_id, ""), task.get("updated_at") or "")
    return [transfer_id for transfer_id, _ in sorted(found.items(), key=lambda item: item[1], reverse=True)]

def walk_attempts(value, found):
    if isinstance(value, dict):
        if value.get("failureReason"):
            found.append(value)
            return
        for child in value.values():
            walk_attempts(child, found)
    elif isinstance(value, list):
        for child in value:
            walk_attempts(child, found)

def print_failures(records):
    print("FAILURES")
    shown = False
    for transfer_id, record in records.items():
        if str(transfer_id).startswith("_"):
            continue
        attempts = []
        walk_attempts(record.get("body"), attempts)
        if not attempts:
            continue
        shown = True
        print(f"  {transfer_id}")
        for attempt in attempts:
            reason = str(attempt.get("failureReason") or "—")
            charge = attempt.get("recoveryChargePoints")
            if charge is None:
                charge = attempt.get("recoveryCharge")
            side = FAILURE_SIDES.get(reason, "")
            suffix = f"  {side}" if side else ""
            print(f"    {reason:<20} recovery {charge if charge is not None else '—'}{suffix}")
    if not shown:
        print("  (none saved yet)")
    print()

if snapshot:
    print_snapshot(snapshot, note)
elif newest:
    print(f"PRISM   epoch {newest_key}   published {clock(newest.get('publishedAt'))} UTC+8")
    print(f"        leaderboard {num(newest.get('leaderboard'), 3)}")
    if note:
        print(f"        {note}")
    if USING_PROXY:
        print("        audit via DataImpulse")
    print()
elif note:
    print(f"PRISM   {note}")
    print()
print_history(cached)

# Failure records share the audit route's one-read budget, so they are only
# fetched on runs that made no other audit read and are not on hold. That keeps
# them from ever delaying the epoch refresh.
transfers = audit

def needs_fetch(transfer_id):
    record = transfers.get(transfer_id)
    if not isinstance(record, dict):
        return True
    if record.get("body") is not None:
        return False
    return float(record.get("retryAt") or 0) <= time.time()

wanted = local_transfer_ids()
missing = [transfer_id for transfer_id in wanted if needs_fetch(transfer_id)]
if not called and missing and time.time() >= float(transfers.get("_blockedUntil") or 0):
    transfer_id = missing[0]
    audit_url = (
        f"{API}/v1/audit/transfers/"
        f"{urllib.parse.quote(transfer_id)}?participantId={urllib.parse.quote(orchestrator_id)}"
    )
    try:
        transfers[transfer_id] = {"body": get_json(audit_url)}
        audit_ok()
    except ScoreError as err:
        if err.status == 429:
            audit_refused(err)
        else:
            # remember the miss so one bad id cannot use up every run's read
            transfers[transfer_id] = {"body": None, "retryAt": time.time() + FRESH}
    save_json(TRANSFER_CACHE, transfers)
print()
print_failures(transfers)
PY
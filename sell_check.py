#!/usr/bin/env python3
"""Check how much SN105 alpha you can sell without lowering your Beam multiplier.

Usage:
  python sell_check.py <hotkey> [--amount ALPHA] [--no-proxy]

Proxy (DataImpulse) - set env vars or put them in a .env file next to this script:
  DATAIMPULSE_USER, DATAIMPULSE_PASS
  DATAIMPULSE_HOST (optional, default gw.dataimpulse.com:823)

Price (alpha -> TAO -> USD):
  TAO/USD  : CoinGecko, direct connection (or set env TAO_USD)
  alpha/TAO: SN105 pool price via `pip install bittensor` (or set env ALPHA_PRICE_TAO)
Core script otherwise uses stdlib only.
"""
import argparse, json, os, urllib.request
from datetime import datetime, timezone
from urllib.parse import quote

RAO = 1_000_000_000
BLOCK_SEC = 12
NETUID = 105
UA = {"User-Agent": "sell-check/2.0"}

def load_dotenv():
    """Minimal .env loader: checks the script's folder, then the current folder.
    Real environment variables win over .env values."""
    from pathlib import Path
    for base in (Path(__file__).resolve().parent, Path.cwd()):
        f = base / ".env"
        if not f.is_file():
            continue
        for line in f.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            if line.startswith("export "):
                line = line[7:]
            k, v = line.split("=", 1)
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
                v = v[1:-1]
            os.environ.setdefault(k.strip(), v)

def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def make_opener(use_proxy=True):
    user, pwd = os.getenv("DATAIMPULSE_USER"), os.getenv("DATAIMPULSE_PASS")
    if not use_proxy:
        return urllib.request.build_opener()
    if not (user and pwd):
        raise SystemExit("Set DATAIMPULSE_USER and DATAIMPULSE_PASS (or use --no-proxy).")
    host = os.getenv("DATAIMPULSE_HOST", "gw.dataimpulse.com:823")
    p = f"http://{quote(user, safe='')}:{quote(pwd, safe='')}@{host}"
    return urllib.request.build_opener(urllib.request.ProxyHandler({"http": p, "https": p}))

def get_json(opener, url):
    with opener.open(urllib.request.Request(url, headers=UA), timeout=30) as r:
        return json.load(r)

def tao_usd(opener=None):
    """TAO/USD from CoinGecko. Direct connection (not via proxy): CoinGecko
    often rate-limits/blocks shared proxy IPs. `opener` is kept for compatibility."""
    if os.getenv("TAO_USD"):
        return float(os.environ["TAO_USD"])
    try:
        request = urllib.request.Request(
            "https://api.coingecko.com/api/v3/simple/price?ids=bittensor&vs_currencies=usd",
            headers={"Accept": "application/json", "User-Agent": "beam-throughput"},
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            body = json.loads(response.read().decode())
        return float(body["bittensor"]["usd"])
    except Exception as e:
        print(f"  (TAO/USD lookup failed: {e}; set TAO_USD env var)")
        return None

def alpha_price_tao():
    """SN105 alpha spot price in TAO (direct connection, not via proxy).
    Tries several SDK APIs since method names differ between bittensor versions."""
    if os.getenv("ALPHA_PRICE_TAO"):
        return float(os.environ["ALPHA_PRICE_TAO"])
    try:
        import bittensor as bt
    except Exception as e:
        print(f"  (bittensor not installed: {e}; run `pip install -U bittensor` "
              "or set ALPHA_PRICE_TAO)")
        return None
    ver = getattr(bt, "__version__", "?")
    try:
        cls = getattr(bt, "Subtensor", None) or getattr(bt, "subtensor")
        sub = cls(network="finney")
    except Exception as e:
        print(f"  (could not connect to finney: {e})")
        return None

    attempts = [
        lambda: sub.subnet(NETUID).price,                      # newer SDKs
        lambda: sub.get_subnet_dynamic_info(NETUID).price,     # some 8.x/9.x SDKs
        lambda: sub.prices.alpha_price(netuid=NETUID),         # newest SDK namespace
    ]
    errors = []
    for fn in attempts:
        try:
            return float(fn())
        except Exception as e:
            errors.append(str(e))
    print(f"  (alpha price lookup failed on bittensor {ver}: {errors[0]}; "
          "run `pip install -U bittensor` or set ALPHA_PRICE_TAO)")
    return None

def predict(s, amount_alpha):
    """Apply the docs' formulas to one planned sale of `amount_alpha`."""
    allowance = int(s["transferableAlphaRao"]) / RAO
    base7d = int(s["baseAlpha7dRao"]) / RAO
    excess = max(0.0, amount_alpha - allowance)
    n = s["sales7d"] + 1
    sell_m = max(0.60, 1 - 2 * excess / base7d) if base7d else 0.60
    freq_m = 1.0 if n <= 1 else max(0.10, 1 / (1 + 1.5 * (n - 1)))
    if int(s["sellDebtAlphaRao"]) > 0:
        sell_m = 0.60
    return excess, sell_m, freq_m, max(0.10, sell_m * freq_m)

def fmt_value(alpha, p_tao, usd):
    tao = f"{alpha * p_tao:.6f} TAO" if p_tao else "n/a TAO"
    dol = f"${alpha * p_tao * usd:,.2f}" if (p_tao and usd) else "n/a USD"
    return tao, dol

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("hotkey", nargs="?")
    ap.add_argument("--amount", type=float, help="planned sale in alpha, to simulate multiplier")
    ap.add_argument("--no-proxy", action="store_true")
    args = ap.parse_args()
    load_dotenv()
    hotkey = args.hotkey or input("Hotkey: ").strip()

    opener = make_opener(not args.no_proxy)
    resp = get_json(opener, f"https://api.b1m.ai/v1/audit/sell-allowance/{hotkey}")
    d, s = resp["data"], resp["data"]["sellAllowance"]
    a = lambda k: int(s[k]) / RAO

    now = datetime.now(timezone.utc)
    est_block = s["nextPenaltyFreeSaleBlock"] + int(
        (now - ts(s["nextPenaltyFreeSaleEstimatedAt"])).total_seconds() / BLOCK_SEC)
    hrs_left = (s["windowEndBlock"] - est_block) * BLOCK_SEC / 3600

    usd, p_tao = tao_usd(opener), alpha_price_tao()
    safe = a("transferableAlphaRao")
    tao_s, usd_s = fmt_value(safe, p_tao, usd)

    print(f"Hotkey / UID        : {d['hotkey']} / {d['uid']}")
    print(f"Epoch               : {d['epoch']}  (published {resp['publishedAt']})")
    print("-" * 60)
    print(f"Multiplier now      : {s['sellAllowanceMultiplier']}"
          f"  (sell {s['sellMultiplier']} x freq {s['frequencyMultiplier']}"
          f" x new-hotkey {s['newHotkeyRamp']})")
    print(f"Sales in last 7d    : {s['sales7d']}  (sold {a('soldAlpha7dRao'):.6f} α)")
    print(f"Oversale debt       : {a('sellDebtAlphaRao'):.6f} α")
    print("-" * 60)
    print(f"SAFE TO SELL (once) : {safe:.6f} α")
    print(f"                    ≈ {tao_s}   ≈ {usd_s}")
    if p_tao:
        print(f"  (alpha price {p_tao:.6f} TAO, TAO ${usd or 0:,.2f}; spot price, "
              "real proceeds are lower after slippage/fees)")

    # ---- safety verdict ----
    amt = args.amount if args.amount is not None else safe
    excess, sm, fm, mult = predict(s, amt)
    print("-" * 60)
    print(f"Simulated sale      : {amt:.6f} α  -> predicted multiplier {mult:.2f} "
          f"(sell {sm:.2f} x freq {fm:.2f}, excess {excess:.6f} α)")

    problems = []
    if s["sales7d"] >= 1:
        problems.append("you already sold in the last 7 days (2nd sale -> frequency 0.40)")
    if int(s["sellDebtAlphaRao"]) > 0:
        problems.append("you have oversale debt")
    if s["newHotkeyRamp"] < 1:
        problems.append(f"new-hotkey ramp is still {s['newHotkeyRamp']}")
    if est_block < s["nextPenaltyFreeSaleBlock"]:
        problems.append("penalty-free sale block not reached yet")
    if hrs_left <= 0:
        problems.append("snapshot window passed; data is stale")
    if excess > 0:
        problems.append("amount exceeds your allowance")
    if problems:
        print("✗ NOT safe: " + "; ".join(problems))
    else:
        print(f"✓ Safe per the docs' formulas (~{hrs_left:.1f}h left in this window). "
              "Sell in ONE transaction.")
    print("Reminder: sales from ANY hotkey on your coldkey (unstake, transfer, "
          "subnet swap) count. Re-run just before selling.")

if __name__ == "__main__":
    main()
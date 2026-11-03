#!/usr/bin/env python3
"""Register or update a BEAM orchestrator with BeamCore.

Signs {hotkey}:{fee_percentage} with the miner hotkey, then POSTs
https://beamcore.b1m.ai/orchestrators/register

Example:
  python scripts/register_orchestrator.py \\
    --wallet-name default --wallet-hotkey default \\
    --url https://YOUR_VPS_HOSTNAME --fee-percentage 10
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


CORE_SERVER_URL = os.environ.get("CORE_SERVER_URL", "https://beamcore.b1m.ai")
REGISTER_PATH = "/orchestrators/register"
USER_AGENT = "Mozilla/5.0 (compatible; beam-orchestrator-register/1.0; +https://beamcore.b1m.ai)"


def sign_registration(wallet_name: str, wallet_hotkey: str, fee_percentage: int, wallet_path: str | None):
    import bittensor as bt

    options = {"name": wallet_name, "hotkey": wallet_hotkey}
    if wallet_path:
        options["path"] = wallet_path
    wallet = bt.Wallet(**options)
    hotkey = str(wallet.hotkey.ss58_address)
    message = f"{hotkey}:{fee_percentage}"
    signature = "0x" + wallet.hotkey.sign(message.encode("utf-8")).hex()
    return hotkey, message, signature


def post_register(payload: dict, api_key: str | None) -> tuple[int, dict | str]:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": USER_AGENT,
    }
    if api_key:
        headers["x-api-key"] = api_key
    request = urllib.request.Request(
        CORE_SERVER_URL.rstrip("/") + REGISTER_PATH,
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return response.status, json.loads(raw)
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8", errors="replace")
        try:
            return err.code, json.loads(raw)
        except json.JSONDecodeError:
            return err.code, raw


def main() -> int:
    parser = argparse.ArgumentParser(description="Register a BEAM orchestrator with BeamCore")
    parser.add_argument("--wallet-name", default=os.environ.get("BT_WALLET_NAME", "default"))
    parser.add_argument("--wallet-hotkey", default=os.environ.get("BT_WALLET_HOTKEY", "default"))
    parser.add_argument("--wallet-path", default=os.environ.get("BT_WALLET_PATH"))
    parser.add_argument("--fee-percentage", type=int, default=10, help="0-100; must match the signed message")
    parser.add_argument("--name", default="my-orchestrator")
    parser.add_argument("--region", default="global")
    parser.add_argument("--url", required=True, help="Public orchestrator URL, e.g. https://your-vps.example.com")
    parser.add_argument("--max-workers", type=int, default=1000)
    parser.add_argument(
        "--api-key",
        default=None,
        help="Explicit API key to update metadata. Not read from the environment so a leftover "
        "BEAMCORE_NATS_PASSWORD cannot turn a signed register into an update.",
    )
    args = parser.parse_args()
    if args.fee_percentage < 0 or args.fee_percentage > 100:
        print("fee_percentage must be 0-100", file=sys.stderr)
        return 2

    payload = {
        "name": args.name,
        "region": args.region,
        "url": args.url,
        "max_workers": args.max_workers,
        "fee_percentage": args.fee_percentage,
    }

    if args.api_key:
        import bittensor as bt

        options = {"name": args.wallet_name, "hotkey": args.wallet_hotkey}
        if args.wallet_path:
            options["path"] = args.wallet_path
        wallet = bt.Wallet(**options)
        payload["hotkey"] = str(wallet.hotkey.ss58_address)
        print(f"Updating orchestrator metadata for {payload['hotkey']}", file=sys.stderr)
        status, data = post_register(payload, args.api_key)
    else:
        hotkey, message, signature = sign_registration(
            args.wallet_name, args.wallet_hotkey, args.fee_percentage, args.wallet_path
        )
        payload["hotkey"] = hotkey
        payload["signature"] = signature
        print(f"Signed message: {message}", file=sys.stderr)
        print(f"POST {CORE_SERVER_URL.rstrip('/')}{REGISTER_PATH}", file=sys.stderr)
        status, data = post_register(payload, None)

    print(f"HTTP {status}")
    print(json.dumps(data, indent=2) if isinstance(data, dict) else data)
    if isinstance(data, dict) and data.get("api_key"):
        print("Save api_key now; BeamCore will not return it again.", file=sys.stderr)
    return 0 if 200 <= status < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())


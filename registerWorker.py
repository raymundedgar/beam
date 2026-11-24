import re
import subprocess
import sys
from pathlib import Path

import httpx
import bittensor as bt

ROOT = "/root/beam"
ORCH_ID = "b0f59a79-8cce-4137-af3a-84c16a1b61fc"

if len(sys.argv) < 2 or sys.argv[1].startswith("-"):
    sys.exit("usage: registerWorker.py WALLET_NAME")

wallet_name = sys.argv[1]
hotkey_name = "default"
wallet = bt.Wallet(name=wallet_name, hotkey=hotkey_name)
hotkey = wallet.hotkey.ss58_address
coldkey = wallet.coldkeypub.ss58_address

addresses = subprocess.check_output(["hostname", "-I"], text=True).split()
ip = next((item for item in addresses if re.fullmatch(r"[0-9]+(\.[0-9]+){3}", item)), "")
if not ip:
    sys.exit("hostname -I did not include an IPv4 address")

port = 9000
message = f"{hotkey}:{ip}:{port}"
signature = "0x" + wallet.hotkey.sign(message.encode("utf-8")).hex()

resp = httpx.post("https://beamcore.b1m.ai/workers/register", json={
    "hotkey": hotkey, "coldkey": coldkey, "ip": ip, "port": port,
    "claimed_bandwidth_mbps": 1000, "signature": signature,
}, timeout=30.0)
print(resp.status_code, file=sys.stderr)
resp.raise_for_status()
body = resp.json()
worker_id = body["worker_id"]
node_id = body.get("node_id") or body.get("NodeID") or ""
if not node_id:
    node_key = Path(ROOT) / "data/worker/node.key"
    node_id = subprocess.check_output(
        [str(Path(ROOT) / "bin/beam-worker"), "node-id", "--node-key", str(node_key)],
        text=True,
    ).strip()

state = Path(ROOT) / "data/worker"
state.mkdir(parents=True, exist_ok=True)
api_key = body.get("api_key") or ""
if api_key:
    key_path = state / "api_key"
    key_path.write_text(api_key + "\n")
    key_path.chmod(0o600)
    print(f"saved api_key to {key_path}", file=sys.stderr)
else:
    print("response had no api_key; left the existing data/worker/api_key in place", file=sys.stderr)

print(f"ROOT={ROOT}")
print(f"ORCH_ID={ORCH_ID}")
print(f"NODE_ID={node_id}")
print(f"WORKER_ID={worker_id}")
print(f"PUBLIC_IP={ip}")

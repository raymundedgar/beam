import httpx, bittensor as bt

wallet = bt.Wallet(name="default", hotkey="default")
hotkey = wallet.hotkey.ss58_address
coldkey = wallet.coldkeypub.ss58_address
ip, port = "198.58.113.189", 9000
message = f"{hotkey}:{ip}:{port}"
signature = "0x" + wallet.hotkey.sign(message.encode("utf-8")).hex()

resp = httpx.post("https://beamcore.b1m.ai/workers/register", json={
    "hotkey": hotkey, "coldkey": coldkey, "ip": ip, "port": port,
    "claimed_bandwidth_mbps": 1000, "signature": signature,
}, timeout=30.0)
print(resp.status_code, resp.text)
resp.raise_for_status()
print(resp.json()["worker_id"], resp.json()["api_key"])

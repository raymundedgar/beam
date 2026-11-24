#!/bin/bash
# Run on a new worker VPS:
#   bash setup-worker.sh
#
# Installs btcli, asks for the wallet name, creates that wallet if it is
# missing, then copies the worker binary, WCP certificate, and
# registerWorker.py from the Dallas orchestrator. Does not start the worker.
# Membership is added on the orchestrator after this script prints the curl.
set -euo pipefail

MAIN=root@198.58.113.189
ROOT=/root/beam
PY=/root/btcli/bin/python
PIP=/root/btcli/bin/pip
BTCLI=/root/btcli/bin/btcli
ORCH_ID=b0f59a79-8cce-4137-af3a-84c16a1b61fc

read -r -p "Wallet name: " WALLET
while [[ -z "${WALLET}" ]]; do
  read -r -p "Wallet name: " WALLET
done
echo "installing python and btcli"
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y python3 python3-venv ufw
python3 -m venv /root/btcli
"$PIP" install --upgrade pip
"$PIP" install 'bittensor==11.3.0'
"$PIP" install httpx
"$BTCLI" --version

if "$PY" -c 'import sys, bittensor as bt; w = bt.Wallet(name=sys.argv[1], hotkey="default"); print(w.hotkey.ss58_address)' "$WALLET"; then
  echo "using existing wallet ${WALLET}/default"
else
  echo "creating wallet ${WALLET}/default"
  echo "btcli will show the mnemonic and ask for a coldkey password. Write the mnemonic down."
  "$BTCLI" wallet create --wallet "$WALLET" --wallet-hotkey default --n-words 12
fi

mkdir -p "$ROOT/bin" "$ROOT/data/tls" "$ROOT/data/worker"
echo "copying worker binary, WCP certificate, and registerWorker.py from ${MAIN}"
scp "$MAIN:/root/beam/bin/beam-worker" "$ROOT/bin/beam-worker"
scp "$MAIN:/root/beam/data/tls/wcp.crt" "$ROOT/data/tls/wcp.crt"
scp "$MAIN:/root/beam/registerWorker.py" "$ROOT/registerWorker.py"
chmod 755 "$ROOT/bin/beam-worker"

ufw allow 22/tcp
ufw allow 9470/tcp
if ! ufw status | grep -q 'Status: active'; then
  ufw --force enable
fi

cd "$ROOT"
eval "$("$PY" registerWorker.py "$WALLET")"
if [[ -z "$WORKER_ID" || -z "$NODE_ID" || -z "$PUBLIC_IP" ]]; then
  echo "registration did not report worker_id, node_id, and public ip" >&2
  exit 1
fi

cat > "$ROOT/.env" <<EOF
CORE_SERVER_URL=https://beamcore.b1m.ai
BEAM_ENV=prod
BEAM_ORCHESTRATOR_ID=${ORCH_ID}
BEAM_WORKER_ID=${WORKER_ID}
BEAM_WCP_ADDRESS=198.58.113.189:8782
BEAM_WCP_CA=${ROOT}/data/tls/wcp.crt
BEAM_WCP_SERVER_NAME=localhost
BEAM_WORKER_CAPABILITIES=transfer.multipart,transfer.multipart.fanout.v1,transfer.authenticated_part.v1,transfer.source_digest.v1,room.transfer,room.transfer.direct.v1,room.transfer.e2ee.v2,storage.probe.relay.v2,room.transfer.lease_renewal.v1,room.transfer.stream_relay.v1
BEAM_ROOM_TRANSFER_ADDR=0.0.0.0:9470
BEAM_ROOM_TRANSFER_ADVERTISE_URL=http://${PUBLIC_IP}:9470
BEAM_WORKER_SUBSYSTEM_MODE=integrated
BEAM_WORKER_MEMORY_BYTES=1073741824
BEAM_WORKER_CPU_MILLIS=1000
BEAM_WORKER_BANDWIDTH_MBPS=10000
BEAM_WORKER_CONNECTIONS=4096
BEAM_WORKER_DL_CONCURRENCY=16
BEAM_WORKER_DL_RESERVE=4
BEAM_WORKER_DL_PROBE_REPLACEMENTS=3
BEAM_WORKER_DL_KEEP_RATE=3
BEAM_WORKER_DL_MAX_DISCARDS=3
BEAM_WORKER_UL_CONCURRENCY=8
BEAM_WORKER_UL_MAX_DISCARDS=3
BEAM_WORKER_UL_HOLD_MAX=10
BEAM_WORKER_UL_PRIORITY=true
BEAM_WORKER_BOTH_HTTP_ATTEMPTS=3
EOF
chmod 600 "$ROOT/.env"

cat > /etc/systemd/system/beam-worker.service <<'EOF'
[Unit]
Description=Beam worker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/root/beam
EnvironmentFile=/root/beam/.env
ExecStart=/root/beam/bin/beam-worker serve --node-key data/worker/node.key --capabilities ${BEAM_WORKER_CAPABILITIES} --room-transfer-addr ${BEAM_ROOM_TRANSFER_ADDR} --room-transfer-advertise-url ${BEAM_ROOM_TRANSFER_ADVERTISE_URL} --subsystem-mode ${BEAM_WORKER_SUBSYSTEM_MODE}
Restart=always
RestartSec=2
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload

cat <<EOF

Worker files are in place. worker_id=${WORKER_ID} node_id=${NODE_ID} ip=${PUBLIC_IP}

On the Dallas orchestrator, allow this server and add its membership:

  ufw allow from ${PUBLIC_IP} to any port 8782 proto tcp
  curl -X POST http://127.0.0.1:8781/v1/orchestrator/memberships \\
    -H "Authorization: Bearer \$(cat /root/beam/data/orchestrator/control-token)" \\
    -H 'Content-Type: application/json' \\
    -d '{"OrchestratorID":"${ORCH_ID}","WorkerID":"${WORKER_ID}","NodeID":"${NODE_ID}","Status":"active"}'

After that returns 201, start the worker on this server:

  systemctl enable --now beam-worker

EOF

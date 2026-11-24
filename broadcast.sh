#!/bin/bash
# Broadcasts .env updates and/or new compiled beam-worker binaries to all 4 remote workers.
#
# Usage:
#   ./broadcast.sh           # Update .env settings and restart workers
#   ./broadcast.sh --bin     # Copy new bin/beam-worker binary, update .env, and restart
#   ./broadcast.sh --restart # Only restart remote workers
set -euo pipefail

ROOT="/root/beam"
LOCAL_BIN="$ROOT/bin/beam-worker"
DEPLOY_BIN=false
ONLY_RESTART=false

case "${1:-}" in
  --bin|-b|--binary)
    DEPLOY_BIN=true
    ;;
  --restart|-r)
    ONLY_RESTART=true
    ;;
  --help|-h)
    echo "Usage: $0 [--bin | --restart]"
    echo "  --bin, -b       Deploy new bin/beam-worker to all workers and restart"
    echo "  --restart, -r   Restart beam-worker service on all remote workers"
    echo "  (no args)       Update .env configurations and restart"
    exit 0
    ;;
esac

WORKERS=(
  "173.255.192.120"
  "45.33.120.60"
  "104.200.20.75"
  "72.14.185.75"
  "69.164.198.44"
)

if [[ "$DEPLOY_BIN" == true ]]; then
  if [[ ! -f "$LOCAL_BIN" ]]; then
    echo "Error: $LOCAL_BIN does not exist. Run './run-miner.sh build' first." >&2
    exit 1
  fi
  BIN_TIME=$(stat -c '%y' "$LOCAL_BIN" 2>/dev/null || date -r "$LOCAL_BIN")
  echo "==> Deploying binary: $LOCAL_BIN (compiled: $BIN_TIME)"
fi

for IP in "${WORKERS[@]}"; do
  echo "--------------------------------------------------------"
  echo "==> Worker: $IP"

  # 1. Transfer new binary if requested (atomic write to .new to prevent 'text file busy')
  if [[ "$DEPLOY_BIN" == true ]]; then
    echo "  [1/3] Uploading beam-worker binary..."
    scp -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
      "$LOCAL_BIN" "root@$IP:/root/beam/bin/beam-worker.new"
    
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "root@$IP" bash -s << 'REMOTE_BIN'
      # Backup previous binary and atomic replace
      cp /root/beam/bin/beam-worker /root/beam/bin/beam-worker.prev 2>/dev/null || true
      chmod 755 /root/beam/bin/beam-worker.new
      mv -f /root/beam/bin/beam-worker.new /root/beam/bin/beam-worker
REMOTE_BIN
    echo "  Binary deployed and swapped in place."
  fi

  # 2. Update .env settings (unless only restarting)
  if [[ "$ONLY_RESTART" == false ]]; then
    echo "  [2/3] Updating .env settings..."
    ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "root@$IP" bash -s << 'REMOTE_ENV'
      set_env() {
        local k="$1" v="$2" f="/root/beam/.env"
        grep -q "^${k}=" "$f" && sed -i "s|^${k}=.*|${k}=${v}|" "$f" || echo "${k}=${v}" >> "$f"
      }
      set_env "BEAM_WORKER_DL_KEEP_RATE" "5"
      set_env "BEAM_WORKER_DL_MAX_DISCARDS" "3"
      set_env "BEAM_WORKER_UL_HOLD_MAX" "3"
      set_env "BEAM_WORKER_UL_REPLACE_REF" "10"
REMOTE_ENV
  fi

  # 3. Restart beam-worker service
  echo "  [3/3] Restarting beam-worker.service..."
  STATUS=$(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "root@$IP" bash -s << 'REMOTE_RESTART'
    systemctl restart beam-worker
    sleep 1
    systemctl is-active beam-worker
REMOTE_RESTART
  )
  echo "  Status on $IP: $STATUS"
done

echo "--------------------------------------------------------"
echo "Done! All remote workers updated successfully."

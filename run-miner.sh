#!/bin/bash
# Build, run, and (optionally) rebuild Beam orchestrator + worker on code change.
#
#   ./run-miner.sh           start miner, then watch Go files and restart
#   ./run-miner.sh up        start miner and return (survives SSH via systemd)
#   ./run-miner.sh watch     watch only (starts miner if it is down)
#   ./run-miner.sh down      stop miner; watcher Ctrl+C does not do this
#   ./run-miner.sh status    show runtime, health, PIDs
#   ./run-miner.sh logs      follow orchestrator + worker logs
#
# Prefers systemd so the miner stays up if this terminal dies.
# Set BEAM_MINER_RUNTIME=tmux to force tmux instead.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"

ENV_FILE="$ROOT/.env"
ORCH_BIN="$ROOT/bin/beam-orchestrator"
WORKER_BIN="$ROOT/bin/beam-worker"
ORCH_UNIT=beam-orchestrator
WORKER_UNIT=beam-worker
TMUX_SESSION=beam
WATCH_DIRS=(cmd internal go.mod go.sum)

WORKER_CAPABILITIES_DEFAULT='transfer.multipart,transfer.multipart.fanout.v1,transfer.authenticated_part.v1,transfer.source_digest.v1,room.transfer,room.transfer.direct.v1,room.transfer.e2ee.v2,storage.probe.relay.v2'
ROOM_TRANSFER_ADDR_DEFAULT='0.0.0.0:9470'
ROOM_TRANSFER_URL_DEFAULT='http://198.58.113.189:9470'
SUBSYSTEM_MODE_DEFAULT='integrated'

log() { printf '[run-miner] %s\n' "$*"; }
die() { printf '[run-miner] %s\n' "$*" >&2; exit 1; }

runtime() {
  if [ "${BEAM_MINER_RUNTIME:-}" = tmux ]; then
    echo tmux
    return
  fi
  if [ "${BEAM_MINER_RUNTIME:-}" = systemd ]; then
    echo systemd
    return
  fi
  if [ "$(id -u)" -eq 0 ] && command -v systemctl >/dev/null 2>&1 && systemctl list-units >/dev/null 2>&1; then
    echo systemd
    return
  fi
  command -v tmux >/dev/null 2>&1 || die "need systemd (as root) or tmux"
  echo tmux
}

ensure_env() {
  if [ -f "$ENV_FILE" ]; then
    chmod 600 "$ENV_FILE" 2>/dev/null || true
    return
  fi
  local pid
  pid="$(pgrep -n -f '[.]/bin/beam-orchestrator serve|[.]/bin/beam-worker serve' || true)"
  if [ -n "$pid" ] && [ -r "/proc/$pid/environ" ]; then
    python3 - "$ENV_FILE" "$pid" "$WORKER_CAPABILITIES_DEFAULT" "$ROOM_TRANSFER_ADDR_DEFAULT" "$ROOM_TRANSFER_URL_DEFAULT" "$SUBSYSTEM_MODE_DEFAULT" <<'PY'
import os, sys
path, pid, caps, addr, url, mode = sys.argv[1:]
raw = open(f"/proc/{pid}/environ", "rb").read().split(b"\0")
env = {}
for item in raw:
    if not item or b"=" not in item:
        continue
    key, value = item.split(b"=", 1)
    env[key.decode()] = value.decode(errors="replace")
keys = [
    "CORE_SERVER_URL", "BEAM_ENV", "BEAM_BITTENSOR_HOTKEY", "BEAMCORE_NATS_URL",
    "BEAMCORE_NATS_USER", "BEAMCORE_NATS_PASSWORD", "BEAM_PUBLIC_API_URL",
    "BEAMCORE_GATEWAY_URL", "BEAM_WCP_LISTEN_ADDR", "BEAM_WCP_TLS_CERT",
    "BEAM_WCP_TLS_KEY", "BEAM_ROOM_TUNNEL_COORDINATOR_URL", "BEAM_ORCHESTRATOR_ID",
    "BEAM_WORKER_ID", "BEAM_WCP_ADDRESS", "BEAM_WCP_CA", "BEAM_WCP_SERVER_NAME",
    "BEAM_WORKER_BANDWIDTH_MBPS", "BEAM_WORKER_MEMORY_BYTES", "BEAM_WORKER_CONNECTIONS",
    "BEAM_WORKER_CPU_MILLIS",
]
lines = ["# generated from a running Beam process; keep this file private", ""]
for key in keys:
    if key in env and env[key] != "":
        lines.append(f"{key}={env[key]}")
extra = {
    "BEAM_WORKER_CAPABILITIES": caps,
    "BEAM_ROOM_TRANSFER_ADDR": addr,
    "BEAM_ROOM_TRANSFER_ADVERTISE_URL": env.get("BEAM_ROOM_TRANSFER_ADVERTISE_URL") or url,
    "BEAM_WORKER_SUBSYSTEM_MODE": env.get("BEAM_WORKER_SUBSYSTEM_MODE") or mode,
}
for key, value in extra.items():
    lines.append(f"{key}={value}")
os.umask(0o077)
with open(path, "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines) + "\n")
os.chmod(path, 0o600)
PY
    log "wrote $ENV_FILE from PID $pid"
    return
  fi
  die "missing $ENV_FILE (copy .env.example and fill NATS/WCP values)"
}

load_env() {
  ensure_env
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
  : "${BEAM_BITTENSOR_HOTKEY:?BEAM_BITTENSOR_HOTKEY is required in .env}"
  : "${BEAM_WORKER_CAPABILITIES:=$WORKER_CAPABILITIES_DEFAULT}"
  : "${BEAM_ROOM_TRANSFER_ADDR:=$ROOM_TRANSFER_ADDR_DEFAULT}"
  : "${BEAM_ROOM_TRANSFER_ADVERTISE_URL:=$ROOM_TRANSFER_URL_DEFAULT}"
  : "${BEAM_WORKER_SUBSYSTEM_MODE:=$SUBSYSTEM_MODE_DEFAULT}"
}

unmanaged_pids() {
  local pid cmd cgroup
  for pid in $(pgrep -f '/beam-(orchestrator|worker)( |$)' || true); do
    cmd="$(ps -o cmd= -p "$pid" 2>/dev/null || true)"
    case "$cmd" in
      *bin/beam-orchestrator*|*bin/beam-worker*) ;;
      *) continue ;;
    esac
    cgroup="$(tr '\0' ' ' < "/proc/$pid/cgroup" 2>/dev/null || true)"
    case "$cgroup" in
      *beam-orchestrator*|*beam-worker*) continue ;;
    esac
    printf '%s\n' "$pid"
  done
}

stop_unmanaged() {
  local pids
  pids="$(unmanaged_pids | tr '\n' ' ')"
  [ -z "${pids// /}" ] && return
  log "stopping unmanaged miner PIDs: $pids"
  # shellcheck disable=SC2086
  kill $pids 2>/dev/null || true
  sleep 1
  pids="$(unmanaged_pids | tr '\n' ' ')"
  [ -z "${pids// /}" ] && return
  # shellcheck disable=SC2086
  kill -9 $pids 2>/dev/null || true
}

install_units() {
  umask 022
  cat > /etc/systemd/system/${ORCH_UNIT}.service <<EOF
[Unit]
Description=Beam orchestrator
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=$ROOT
EnvironmentFile=$ENV_FILE
ExecStart=$ORCH_BIN serve --hotkey \${BEAM_BITTENSOR_HOTKEY} --netuid 105
Restart=always
RestartSec=2
KillSignal=SIGTERM
TimeoutStopSec=20
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
  cat > /etc/systemd/system/${WORKER_UNIT}.service <<EOF
[Unit]
Description=Beam worker
After=network-online.target ${ORCH_UNIT}.service
Wants=network-online.target
Requires=${ORCH_UNIT}.service

[Service]
Type=simple
WorkingDirectory=$ROOT
EnvironmentFile=$ENV_FILE
ExecStart=$WORKER_BIN serve --node-key data/worker/node.key --capabilities \${BEAM_WORKER_CAPABILITIES} --room-transfer-addr \${BEAM_ROOM_TRANSFER_ADDR} --room-transfer-advertise-url \${BEAM_ROOM_TRANSFER_ADVERTISE_URL} --subsystem-mode \${BEAM_WORKER_SUBSYSTEM_MODE}
Restart=always
RestartSec=2
KillSignal=SIGTERM
TimeoutStopSec=20
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
}

wait_port() {
  local spec="$1" tries="${2:-20}"
  local i
  for i in $(seq 1 "$tries"); do
    if python3 - "$spec" <<'PY'
import socket, sys
host, port = sys.argv[1].rsplit(":", 1)
sock = socket.socket()
sock.settimeout(0.4)
try:
    sock.connect((host, int(port)))
except OSError:
    raise SystemExit(1)
finally:
    sock.close()
PY
    then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

build() {
  mkdir -p "$ROOT/bin"
  log "building orchestrator"
  go build -trimpath -o "$ORCH_BIN" ./cmd/beam-orchestrator
  log "building worker"
  local rev dirty ldflags
  rev="$(git -C "$ROOT" rev-parse --short=7 HEAD 2>/dev/null || true)"
  dirty=""
  if [ -n "$rev" ] && { ! git -C "$ROOT" diff --quiet || ! git -C "$ROOT" diff --cached --quiet; }; then
    dirty=dirty
  fi
  ldflags=""
  if [ -n "$rev" ]; then
    ldflags="-X github.com/Beam-Network/beam/internal/workload/handlers/transfer.buildRevision=${rev}"
    if [ -n "$dirty" ]; then
      ldflags="${ldflags} -X github.com/Beam-Network/beam/internal/workload/handlers/transfer.buildDirty=dirty"
    fi
  fi
  go build -trimpath -ldflags "${ldflags}" -o "$WORKER_BIN" ./cmd/beam-worker
  if [ -n "$rev" ]; then
    cp -f "$WORKER_BIN" "${WORKER_BIN}-${rev}"
    log "backed up worker binary to ${WORKER_BIN}-${rev}"
  fi
}

tmux_running() {
  tmux has-session -t "$TMUX_SESSION" 2>/dev/null
}

start_tmux() {
  command -v tmux >/dev/null 2>&1 || die "tmux is not installed"
  if tmux_running; then
    tmux kill-session -t "$TMUX_SESSION"
  fi
  tmux new-session -d -s "$TMUX_SESSION" -n orchestrator
  tmux send-keys -t "$TMUX_SESSION:orchestrator" "cd '$ROOT' && set -a && source '$ENV_FILE' && set +a && while true; do '$ORCH_BIN' serve --hotkey \"\$BEAM_BITTENSOR_HOTKEY\" --netuid 105; echo '[run-miner] orchestrator exited' \$?; sleep 2; done" C-m
  wait_port 127.0.0.1:8781 30 || log "orchestrator 8781 not up yet"
  tmux new-window -t "$TMUX_SESSION" -n worker
  tmux send-keys -t "$TMUX_SESSION:worker" "cd '$ROOT' && set -a && source '$ENV_FILE' && set +a && while true; do '$WORKER_BIN' serve --node-key data/worker/node.key --capabilities \"\$BEAM_WORKER_CAPABILITIES\" --room-transfer-addr \"\$BEAM_ROOM_TRANSFER_ADDR\" --room-transfer-advertise-url \"\$BEAM_ROOM_TRANSFER_ADVERTISE_URL\" --subsystem-mode \"\$BEAM_WORKER_SUBSYSTEM_MODE\"; echo '[run-miner] worker exited' \$?; sleep 2; done" C-m
  log "tmux session '$TMUX_SESSION' started (attach with: tmux attach -t $TMUX_SESSION)"
}

restart_tmux() {
  tmux_running || { start_tmux; return; }
  tmux send-keys -t "$TMUX_SESSION:orchestrator" C-c
  tmux send-keys -t "$TMUX_SESSION:worker" C-c
  sleep 1
}

stop_tmux() {
  tmux_running && tmux kill-session -t "$TMUX_SESSION"
}

start_systemd() {
  install_units
  stop_unmanaged
  systemctl enable "$ORCH_UNIT" "$WORKER_UNIT" >/dev/null
  systemctl restart "$ORCH_UNIT"
  wait_port 127.0.0.1:8781 30 || die "orchestrator did not listen on 127.0.0.1:8781"
  systemctl restart "$WORKER_UNIT"
}

restart_systemd() {
  systemctl restart "$ORCH_UNIT"
  wait_port 127.0.0.1:8781 30 || die "orchestrator did not listen on 127.0.0.1:8781 after restart"
  systemctl restart "$WORKER_UNIT"
}

stop_systemd() {
  systemctl stop "$WORKER_UNIT" "$ORCH_UNIT" 2>/dev/null || true
}

miner_up() {
  load_env
  build
  case "$(runtime)" in
    systemd) start_systemd ;;
    tmux) stop_unmanaged; start_tmux ;;
  esac
  log "miner runtime=$(runtime)"
}

miner_down() {
  case "$(runtime)" in
    systemd) stop_systemd ;;
    tmux) stop_tmux ;;
  esac
  stop_unmanaged
  log "miner stopped"
}

miner_restart() {
  case "$(runtime)" in
    systemd) restart_systemd ;;
    tmux) restart_tmux ;;
  esac
}

status() {
  local rt orch worker
  rt="$(runtime)"
  orch="$(curl -s --max-time 1 http://127.0.0.1:8781/healthz 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status","?"))' 2>/dev/null || echo down)"
  worker="$(curl -s --max-time 1 http://127.0.0.1:8780/healthz 2>/dev/null | python3 -c 'import sys,json; print(json.load(sys.stdin).get("status","?"))' 2>/dev/null || echo down)"
  printf 'runtime     %s\n' "$rt"
  printf 'orchestrator %s\n' "$orch"
  printf 'worker      %s\n' "$worker"
  if [ "$rt" = systemd ]; then
    systemctl --no-pager --full status "$ORCH_UNIT" "$WORKER_UNIT" 2>/dev/null | sed -n '1,24p' || true
  elif tmux_running; then
    tmux list-windows -t "$TMUX_SESSION"
  fi
  pgrep -af 'bin/beam-(orchestrator|worker)' || true
}

logs() {
  case "$(runtime)" in
    systemd)
      journalctl -fu "$ORCH_UNIT" -u "$WORKER_UNIT" -n 50
      ;;
    tmux)
      tmux_running || die "tmux session '$TMUX_SESSION' is not running"
      log "attach to see logs: tmux attach -t $TMUX_SESSION"
      tmux attach -t "$TMUX_SESSION"
      ;;
  esac
}

source_fingerprint() {
  find cmd internal -name '*.go' -print0 2>/dev/null \
    | sort -z \
    | xargs -0 sha256sum 2>/dev/null
  sha256sum go.mod go.sum 2>/dev/null || true
}

ensure_inotify() {
  command -v inotifywait >/dev/null 2>&1 && return
  if command -v apt-get >/dev/null 2>&1 && [ "$(id -u)" -eq 0 ]; then
    log "installing inotify-tools"
    DEBIAN_FRONTEND=noninteractive apt-get install -y inotify-tools >/dev/null
  fi
  command -v inotifywait >/dev/null 2>&1 || log "inotifywait missing; polling every 2s"
}

watch_loop() {
  load_env
  ensure_inotify
  local last now
  last="$(source_fingerprint)"
  log "watching ${WATCH_DIRS[*]}  (Ctrl+C stops watching; miner stays up under $(runtime))"
  trap 'log "watcher stopped"; exit 0' INT TERM
  while true; do
    if command -v inotifywait >/dev/null 2>&1; then
      inotifywait -q -r -e modify,close_write,create,delete,move \
        --exclude '/(\.git|bin|data)/' \
        --include '(\.go$|go\.mod$|go\.sum$)' \
        cmd internal go.mod go.sum >/dev/null 2>&1 || true
      sleep 1
    else
      sleep 2
    fi
    now="$(source_fingerprint)"
    [ "$now" = "$last" ] && continue
    last="$now"
    log "code changed; rebuilding"
    if build; then
      log "restarting miner"
      miner_restart
      log "restart complete"
    else
      log "build failed; leaving current miner running"
    fi
  done
}

usage() {
  sed -n '2,12p' "$0"
}

cmd="${1:-watch}"
case "$cmd" in
  up) miner_up ;;
  down|stop) miner_down ;;
  restart) load_env; miner_restart ;;
  status) status ;;
  logs) logs ;;
  watch)
    load_env
    case "$(runtime)" in
      systemd)
        if ! systemctl is-active --quiet "$ORCH_UNIT" || ! systemctl is-active --quiet "$WORKER_UNIT"; then
          miner_up
        else
          log "miner already under systemd; watching for code changes"
        fi
        ;;
      tmux)
        if tmux_running; then
          log "tmux session '$TMUX_SESSION' already running; watching for code changes"
        else
          miner_up
        fi
        ;;
    esac
    watch_loop
    ;;
  -h|--help|help) usage ;;
  *) die "unknown command: $cmd (try: up | watch | down | status | logs)" ;;
esac

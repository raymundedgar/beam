# Beam Worker Performance Engineering

Performance optimizations to the transfer path of a Beam (Bittensor Subnet 105) miner worker, written in Go on top of the open-source [Beam-Network/beam](https://github.com/Beam-Network/beam) codebase.

> **Attribution:** Beam, its orchestrator, validator, protocols and the original transfer handler were built by the Beam team. This fork documents only my own changes: 60+ commits (Nov 4-24, 2026) and about 30 Go source and test files I created under `internal/workload/handlers/transfer/`. The optimization ideas and the decisions about what to change are mine. I check their feasibility with Claude Sonnet and refine my prompts with ChatGPT, and Grok runs inside Cursor to write the code changes (credited in commit trailers as `Co-authored-by: Cursor`).

## Goal

Miners are ranked on transfer throughput and on completing batches before their deadlines. I read my worker's logs, found where time was lost, and changed the transfer path to fix it. Each change shipped behind an environment flag so I could roll it back.

## What I built

| Improvement | Problem | Approach | Code |
|---|---|---|---|
| **Slow and frozen upload replacement** | A stalled socket could hold a part for 10-22 s, and Go timeouts can't tell slow from wedged | Sample Linux `TCP_INFO` (acks, send window, RTT) and judge the last second's rate, not the average. Replace a socket only when a fresh upload is projected to finish sooner. Zero-window stalls are cut after 2 s | `ack_linux.go`, `put_policy.go`, `hedge.go` |
| **Overlapped download and upload** | The upload started only after the download finished | Start the `PUT` after 1 MiB (or 500 ms) through a shared `followBuffer`. A discarded upload re-reads from memory instead of re-downloading | `overlap.go`, `batch.go` |
| **Cost-aware download restarts** | Slow reads (about 3 MiB/s while peers ran 14-39 MiB/s) and signed `GET`s that can't resume | Compare against a per-host median rate and restart only when a full re-read is faster. Reads at or above 5 MiB/s are kept | `read_median.go` |
| **Batch-fair slot scheduler** | One large batch could take every connection slot | FCFS slots plus reserved slots bound to distinct batches, with direct handoff and no leaks on cancel | `transfer_slot.go` |
| **Upload send-token gate** | Uploads waiting on the download held slots while sending nothing | Release the token after 400 ms of starvation and re-acquire only when 1 MiB is buffered | `upload_slot.go` |
| **Connection warming and dial timeout** | Retries paid for TCP and TLS setup, and the dial timeout was 30 s | Pre-warm connections to each host and cut the dial timeout to 5 s | `warm.go`, `storagehttp/client.go` |
| **Memory budget** | Buffering parts in memory needs a bound | Budgeted semaphore that never deadlocks on an oversized part | `gate.go` |
| **Observability and tooling** | Tuning needs evidence | Per-attempt logs (remote IP, Cloudflare colo, blocked-read gaps), git-revision and policy stamps, and scripts `run-miner.sh`, `throughput.sh`, `scores.sh` | `stall.go`, `build.go`, `*.sh` |

```text
Baseline:  [ download ............ ][ upload ............ ]   ~ S + U
Mine:      [ download ............ ]
                 [ upload, trailing the buffer .......... ]   ~ max(S, U)
           slow upload socket -> discard and re-send from the buffer, no second GET
```

## Key design decisions

- **Acks, not write rate.** I replaced a byte-rate watchdog with kernel ack tracking, because a full send buffer made healthy sockets look slow.
- **Don't over-replace.** Discards need consecutive slow seconds, and the threshold rises after 16 MiB or the halfway point. Short tails stay put, because replacing them took longer than waiting.
- **Thresholds from data.** The 10 MiB/s replacement reference and the 3-window streak come from replaying 24 hours of my logs (96 replacements: median 2.2 s at 22 MiB/s).
- **Experiments off by default.** Multi-IP read racing, read hedging, parallel and late-probe uploads, strict ETag checks and upload priority are implemented but disabled until evidence supports them.

## Testing and validation

About 15 test files cover ack-based decisions, zero-window holds (real loopback sockets), the follow buffer, idle-close recovery, the slot scheduler, token conservation and the memory budget:

```bash
go test ./internal/workload/handlers/transfer/...
```

Thresholds were tuned from my own task logs and checked against later waves. I do not claim leaderboard positions, rewards or controlled benchmarks.

## Upstream

Not my work: the orchestrator, worker framework, validator, BeamCore, NATS and WCP integrations, authenticated and fan-out transfers, digests, and room transfers. My rewrites of `handler.go` build on the original. Setup guides: [orchestrator](docs/orchestrator.md), [worker](docs/worker.md), [validator](docs/validator.md).

```bash
git clone https://github.com/raymundedgar/beam.git && cd beam
go build -o bin/beam-orchestrator ./cmd/beam-orchestrator
go build -o bin/beam-worker ./cmd/beam-worker
```

MIT License, inherited from upstream. See [LICENSE](LICENSE).

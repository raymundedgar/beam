package connectors

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/beamlink/circuit"
	"github.com/Beam-Network/beam/internal/orchestrator/dispatch"
	orchestratordomain "github.com/Beam-Network/beam/internal/orchestrator/domain"
	"github.com/Beam-Network/beam/internal/orchestrator/registry"
	"github.com/Beam-Network/beam/internal/workload/contracts"
	"github.com/Beam-Network/beam/internal/workload/domain"
	workloadruntime "github.com/Beam-Network/beam/internal/workload/runtime"
)

type concurrentControlWCP struct {
	started chan struct{}
	release chan struct{}
	active  atomic.Int32
	peak    atomic.Int32
}

func (c *concurrentControlWCP) Offer(context.Context, string, domain.Spec) (workloadruntime.Decision, error) {
	n := c.active.Add(1)
	for {
		peak := c.peak.Load()
		if n <= peak || c.peak.CompareAndSwap(peak, n) {
			break
		}
	}
	select {
	case c.started <- struct{}{}:
	default:
	}
	<-c.release
	c.active.Add(-1)
	return workloadruntime.Decision{Accepted: true}, nil
}

func (c *concurrentControlWCP) Commit(context.Context, string, domain.Commit) error {
	return nil
}
func (c *concurrentControlWCP) Cancel(context.Context, string, string, string) error {
	return nil
}
func (c *concurrentControlWCP) Connected(string) bool { return true }
func (c *concurrentControlWCP) UpsertCircuit(context.Context, circuit.Plan) error {
	return nil
}
func (c *concurrentControlWCP) RevokeCircuit(context.Context, circuit.Revocation) error {
	return nil
}

func TestTaskOfferBatchDispatchesIndependentOffersTogether(t *testing.T) {
	now := time.Now().UTC()
	orchestratorRegistry, err := registry.New(orchestratordomain.Orchestrator{OrchestratorID: "orch-1", CreatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	if err := orchestratorRegistry.Join(orchestratordomain.Membership{OrchestratorID: "orch-1",
		WorkerID: "worker-1", NodeID: "node-1", Status: "active", JoinedAt: now}, now); err != nil {
		t.Fatal(err)
	}
	resources := domain.Resources{MemoryBytes: 64 << 20, BandwidthMbps: 32, Connections: 32, Streams: 32}
	if err := orchestratorRegistry.UpdateObservation(orchestratordomain.WorkerObservation{
		WorkerID: "worker-1", NodeID: "node-1", Status: "active",
		Capabilities: []string{contracts.TransferMultipartCapability},
		Total: resources, Available: resources, ObservedAt: now,
	}, now); err != nil {
		t.Fatal(err)
	}
	wcp := &concurrentControlWCP{started: make(chan struct{}, 4), release: make(chan struct{})}
	tasks, err := dispatch.NewService(dispatch.Config{OrchestratorID: "orch-1"}, orchestratorRegistry,
		wcp, dispatch.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	control := newRoomControl(NATSConfig{Environment: "dev", Hotkey: "hotkey-1"}, nil, nil, nil, tasks)
	offers := make([]map[string]any, 4)
	for index := range offers {
		offers[index] = map[string]any{
			"offer_id": "offer-" + string(rune('a'+index)),
			"source":   map[string]any{"url": "https://source.invalid/object", "chunk_size": 8},
			"destinations": []map[string]any{{
				"url": "https://dest.invalid/part",
			}},
		}
	}
	batch := beamCorePayload(t, map[string]any{
		"type": "task_offer_batch", "batch_id": "batch-parallel", "assignment_timeout_ms": 30_000,
		"offers": offers,
	})
	done := make(chan error, 1)
	go func() { done <- control.handleTaskOfferBatch(context.Background(), batch) }()
	for range 4 {
		select {
		case <-wcp.started:
		case <-time.After(time.Second):
			t.Fatal("offers did not start together")
		}
	}
	if peak := wcp.peak.Load(); peak < 4 {
		t.Fatalf("worker offers were serialized: peak concurrency %d", peak)
	}
	close(wcp.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("parallel batch dispatch did not finish")
	}
	for _, id := range []string{"offer-a", "offer-b", "offer-c", "offer-d"} {
		if _, ok := tasks.Record(id + "/" + id); !ok {
			t.Fatalf("offer %s was not dispatched", id)
		}
	}
}

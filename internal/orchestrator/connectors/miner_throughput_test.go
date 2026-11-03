package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/beamlink/circuit"
	"github.com/Beam-Network/beam/internal/orchestrator/dispatch"
	orchestratordomain "github.com/Beam-Network/beam/internal/orchestrator/domain"
	"github.com/Beam-Network/beam/internal/orchestrator/registry"
	beamcoreadapter "github.com/Beam-Network/beam/internal/workload/adapters/beamcore"
	"github.com/Beam-Network/beam/internal/workload/contracts"
	"github.com/Beam-Network/beam/internal/workload/domain"
	workloadruntime "github.com/Beam-Network/beam/internal/workload/runtime"
)

const (
	throughputOfferCount = 32
	throughputOfferDelay = 2 * time.Millisecond
)

// delayedControlWCP stands in for a Worker WCP round-trip. Sequential dispatch
// pays this once per offer; parallel dispatch overlaps it.
type delayedControlWCP struct{ delay time.Duration }

func (c delayedControlWCP) Offer(context.Context, string, domain.Spec) (workloadruntime.Decision, error) {
	if c.delay > 0 {
		time.Sleep(c.delay)
	}
	return workloadruntime.Decision{Accepted: true}, nil
}
func (delayedControlWCP) Commit(context.Context, string, domain.Commit) error     { return nil }
func (delayedControlWCP) Cancel(context.Context, string, string, string) error    { return nil }
func (delayedControlWCP) Connected(string) bool                                   { return true }
func (delayedControlWCP) UpsertCircuit(context.Context, circuit.Plan) error       { return nil }
func (delayedControlWCP) RevokeCircuit(context.Context, circuit.Revocation) error { return nil }

func newThroughputControl(tb testing.TB) *roomControl {
	tb.Helper()
	now := time.Now().UTC()
	orchestratorRegistry, err := registry.New(orchestratordomain.Orchestrator{OrchestratorID: "orch-1", CreatedAt: now})
	if err != nil {
		tb.Fatal(err)
	}
	if err := orchestratorRegistry.Join(orchestratordomain.Membership{OrchestratorID: "orch-1",
		WorkerID: "worker-1", NodeID: "node-1", Status: "active", JoinedAt: now}, now); err != nil {
		tb.Fatal(err)
	}
	resources := domain.Resources{MemoryBytes: 1 << 40, BandwidthMbps: 1 << 20, Connections: 1 << 16, Streams: 1 << 16}
	if err := orchestratorRegistry.UpdateObservation(orchestratordomain.WorkerObservation{
		WorkerID: "worker-1", NodeID: "node-1", Status: "active",
		Capabilities: []string{contracts.TransferMultipartCapability},
		Total: resources, Available: resources, ObservedAt: now,
	}, now); err != nil {
		tb.Fatal(err)
	}
	tasks, err := dispatch.NewService(dispatch.Config{OrchestratorID: "orch-1"}, orchestratorRegistry,
		delayedControlWCP{delay: throughputOfferDelay}, dispatch.NewMemoryStore())
	if err != nil {
		tb.Fatal(err)
	}
	return newRoomControl(NATSConfig{Environment: "dev", Hotkey: "hotkey-1"}, nil, nil, nil, tasks)
}

func throughputBatch(tb testing.TB, batchID string, offers int) []byte {
	tb.Helper()
	items := make([]map[string]any, offers)
	for index := range items {
		items[index] = map[string]any{
			"offer_id": fmt.Sprintf("%s-offer-%d", batchID, index),
			"source":   map[string]any{"url": "https://source.invalid/object", "chunk_size": 8},
			"destinations": []map[string]any{{
				"url": "https://dest.invalid/part",
			}},
		}
	}
	return beamCorePayload(tb, map[string]any{
		"type": "task_offer_batch", "batch_id": batchID, "assignment_timeout_ms": 30_000,
		"offers": items,
	})
}

// handleTaskOfferBatchSequential is the pre-change dispatcher: one WCP offer at
// a time, on the caller's goroutine.
func handleTaskOfferBatchSequential(ctx context.Context, control *roomControl, encoded []byte) error {
	payload, err := decodeControlPayload(encoded, "task_offer_batch")
	if err != nil {
		return err
	}
	var batch beamcoreadapter.TaskOfferBatch
	if err := json.Unmarshal(payload, &batch); err != nil {
		return err
	}
	if batch.BatchID == "" || len(batch.Offers) == 0 {
		return errors.New("BeamCore task offer batch is empty")
	}
	receivedAt := time.Now()
	timeout := time.Duration(batch.AssignmentTimeoutMS) * time.Millisecond
	var failures []error
	for _, offer := range batch.Offers {
		spec, err := beamcoreadapter.ToWorkload(offer, timeout, receivedAt)
		if err != nil {
			return err
		}
		if _, err := control.tasks.Dispatch(ctx, dispatch.DispatchRequest{
			Source: dispatch.SourceBeamCore, ExternalID: spec.AttemptID, BatchID: batch.BatchID,
			TransferID: batch.TransferID, Spec: spec,
		}); err != nil {
			failures = append(failures, fmt.Errorf("offer %s: %w", spec.AttemptID, err))
		}
	}
	return errors.Join(failures...)
}

func BenchmarkMinerTaskOfferBatch(b *testing.B) {
	for _, test := range []struct {
		name string
		run  func(context.Context, *roomControl, []byte) error
	}{
		{"old_sequential", handleTaskOfferBatchSequential},
		{"new_parallel", func(ctx context.Context, control *roomControl, encoded []byte) error {
			return control.handleTaskOfferBatch(ctx, encoded)
		}},
	} {
		b.Run(test.name, func(b *testing.B) {
			control := newThroughputControl(b)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := throughputBatch(b, fmt.Sprintf("%s-%d", test.name, i), throughputOfferCount)
				if err := test.run(context.Background(), control, batch); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(throughputOfferCount), "offers/op")
		})
	}
}

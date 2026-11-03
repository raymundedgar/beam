package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Beam-Network/beam/internal/workload/domain"
)

func benchWorkloadRecord(id string) Record {
	now := time.Now().UTC()
	return Record{
		Spec: domain.Spec{
			WorkloadID: id, AttemptID: id,
			Identity: domain.Identity{WorkerID: "worker-1"},
			Kind:     domain.KindTransferMultipart, Class: domain.ClassJob,
			Lease:   domain.Lease{OfferExpiresAt: now.Add(time.Minute)},
			Payload: []byte(`{"parts":[]}`),
		},
		State: domain.StateReserved, CreatedAt: now, UpdatedAt: now,
	}
}

func appendJournal(path string, record Record, fsync bool) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(file).Encode(workloadFrame{Record: record}); err != nil {
		_ = file.Close()
		return err
	}
	if fsync {
		if err := file.Sync(); err != nil {
			_ = file.Close()
			return err
		}
	}
	return file.Close()
}

func BenchmarkWorkloadJournalAppend(b *testing.B) {
	for _, test := range []struct {
		name  string
		fsync bool
	}{
		{"old_fsync_each_write", true},
		{"new_no_fsync", false},
	} {
		b.Run(test.name, func(b *testing.B) {
			dir := b.TempDir()
			path := filepath.Join(dir, "workloads.json.wal")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := appendJournal(path, benchWorkloadRecord(strconv.Itoa(i)), test.fsync); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWorkerFileStoreCreate(b *testing.B) {
	dir := b.TempDir()
	store, err := OpenFileStore(filepath.Join(dir, "workloads.json"))
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := store.Create(benchWorkloadRecord(strconv.Itoa(i))); err != nil {
			b.Fatal(err)
		}
	}
}

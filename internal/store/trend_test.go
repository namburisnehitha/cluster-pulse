package store_test

import (
	"testing"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/store"
)

func makeSnap(cpu, memory string) store.ResourceSnapshot {
	return store.ResourceSnapshot{
		PodName:     "pod-1",
		Namespace:   "default",
		CPUUsage:    cpu,
		MemoryUsage: memory,
		RecordedAt:  time.Now(),
	}
}

func TestComputeTrend(t *testing.T) {

	// Situation 1: empty slice — no data, direction unknown, all zeros
	trend := store.ComputeTrend([]store.ResourceSnapshot{})
	if trend.Direction != "unknown" {
		t.Errorf("empty slice: direction: got %s, want unknown", trend.Direction)
	}
	if trend.SampleCount != 0 {
		t.Errorf("empty slice: sample count: got %d, want 0", trend.SampleCount)
	}
	if trend.AvgMemoryMi != 0 {
		t.Errorf("empty slice: avg memory: got %d, want 0", trend.AvgMemoryMi)
	}
	if trend.AvgCPUMilli != 0 {
		t.Errorf("empty slice: avg cpu: got %d, want 0", trend.AvgCPUMilli)
	}

	// Situation 2: single snapshot — can't determine trend, direction unknown
	trend = store.ComputeTrend([]store.ResourceSnapshot{makeSnap("100m", "200Mi")})
	if trend.Direction != "unknown" {
		t.Errorf("single snapshot: direction: got %s, want unknown", trend.Direction)
	}
	if trend.SampleCount != 1 {
		t.Errorf("single snapshot: sample count: got %d, want 1", trend.SampleCount)
	}
	if trend.AvgMemoryMi != 200 {
		t.Errorf("single snapshot: avg memory: got %d, want 200", trend.AvgMemoryMi)
	}
	if trend.AvgCPUMilli != 100 {
		t.Errorf("single snapshot: avg cpu: got %d, want 100", trend.AvgCPUMilli)
	}

	// Situation 3: increasing memory — recent half higher than older half by >10%
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "500Mi"), // older
		makeSnap("100m", "500Mi"), // older
		makeSnap("100m", "700Mi"), // recent
		makeSnap("100m", "700Mi"), // recent
	})
	if trend.Direction != "increasing" {
		t.Errorf("increasing: direction: got %s, want increasing", trend.Direction)
	}
	if trend.SampleCount != 4 {
		t.Errorf("increasing: sample count: got %d, want 4", trend.SampleCount)
	}
	if trend.AvgMemoryMi != 600 {
		t.Errorf("increasing: avg memory: got %d, want 600", trend.AvgMemoryMi)
	}

	// Situation 4: decreasing memory — recent half lower than older half by >10%
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "700Mi"), // older
		makeSnap("100m", "700Mi"), // older
		makeSnap("100m", "500Mi"), // recent
		makeSnap("100m", "500Mi"), // recent
	})
	if trend.Direction != "decreasing" {
		t.Errorf("decreasing: direction: got %s, want decreasing", trend.Direction)
	}
	if trend.SampleCount != 4 {
		t.Errorf("decreasing: sample count: got %d, want 4", trend.SampleCount)
	}

	// Situation 5: stable — less than 10% difference
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "205Mi"),
		makeSnap("100m", "205Mi"),
	})
	if trend.Direction != "stable" {
		t.Errorf("stable: direction: got %s, want stable", trend.Direction)
	}

	// Situation 6: all zero values — older is 0, guard triggers, direction stable
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("0m", "0Mi"),
		makeSnap("0m", "0Mi"),
		makeSnap("0m", "0Mi"),
		makeSnap("0m", "0Mi"),
	})
	if trend.Direction != "stable" {
		t.Errorf("all zeros: direction: got %s, want stable", trend.Direction)
	}
	if trend.AvgMemoryMi != 0 {
		t.Errorf("all zeros: avg memory: got %d, want 0", trend.AvgMemoryMi)
	}
	if trend.AvgCPUMilli != 0 {
		t.Errorf("all zeros: avg cpu: got %d, want 0", trend.AvgCPUMilli)
	}

	// Situation 7: two snapshots — minimum meaningful input, split works correctly
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "200Mi"), // older
		makeSnap("100m", "400Mi"), // recent
	})
	if trend.Direction != "increasing" {
		t.Errorf("two snapshots: direction: got %s, want increasing", trend.Direction)
	}
	if trend.SampleCount != 2 {
		t.Errorf("two snapshots: sample count: got %d, want 2", trend.SampleCount)
	}

	// Situation 8: exactly at 10% boundary — diff == 0.1, neither > nor < triggers, stable
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "100Mi"),
		makeSnap("100m", "100Mi"),
		makeSnap("100m", "110Mi"),
		makeSnap("100m", "110Mi"),
	})
	if trend.Direction != "stable" {
		t.Errorf("10%% boundary: direction: got %s, want stable", trend.Direction)
	}

	// Situation 9: unparsable quantity strings — parseQuantity returns 0, no panic
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("badcpu", "badmem"),
		makeSnap("badcpu", "badmem"),
		makeSnap("badcpu", "badmem"),
		makeSnap("badcpu", "badmem"),
	})
	if trend.AvgMemoryMi != 0 {
		t.Errorf("unparsable: avg memory: got %d, want 0", trend.AvgMemoryMi)
	}
	if trend.AvgCPUMilli != 0 {
		t.Errorf("unparsable: avg cpu: got %d, want 0", trend.AvgCPUMilli)
	}
	if trend.SampleCount != 4 {
		t.Errorf("unparsable: sample count: got %d, want 4", trend.SampleCount)
	}

	// Situation 10: odd length slice — split handles remainder correctly, no panic
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "400Mi"),
	})
	if trend.SampleCount != 3 {
		t.Errorf("odd length: sample count: got %d, want 3", trend.SampleCount)
	}
	if trend.Direction != "increasing" {
		t.Errorf("odd length: direction: got %s, want increasing", trend.Direction)
	}

	// Situation 11: all identical values — diff is 0, should be stable
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "200Mi"),
	})
	if trend.Direction != "stable" {
		t.Errorf("identical values: direction: got %s, want stable", trend.Direction)
	}
	if trend.AvgMemoryMi != 200 {
		t.Errorf("identical values: avg memory: got %d, want 200", trend.AvgMemoryMi)
	}
	if trend.AvgCPUMilli != 100 {
		t.Errorf("identical values: avg cpu: got %d, want 100", trend.AvgCPUMilli)
	}

	// Situation 12: CPU spiking but memory stable — CPU drives direction since it has larger change
	trend = store.ComputeTrend([]store.ResourceSnapshot{
		makeSnap("100m", "200Mi"),
		makeSnap("100m", "200Mi"),
		makeSnap("900m", "205Mi"),
		makeSnap("900m", "205Mi"),
	})

	if trend.Direction != "increasing" {
		t.Errorf("cpu spike memory stable: direction: got %s, want increasing — CPU drives direction", trend.Direction)
	}
	if trend.AvgCPUMilli != 500 {
		t.Errorf("cpu spike: avg cpu: got %d, want 500", trend.AvgCPUMilli)
	}

	// Situation 13: large slice — 1000 snapshots, no panic, correct sample count
	largeSnaps := make([]store.ResourceSnapshot, 1000)
	for i := range largeSnaps {
		largeSnaps[i] = makeSnap("100m", "200Mi")
	}
	trend = store.ComputeTrend(largeSnaps)
	if trend.SampleCount != 1000 {
		t.Errorf("large slice: sample count: got %d, want 1000", trend.SampleCount)
	}
	if trend.Direction != "stable" {
		t.Errorf("large slice: direction: got %s, want stable", trend.Direction)
	}
}

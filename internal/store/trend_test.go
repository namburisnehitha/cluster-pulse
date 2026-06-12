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
	tests := []struct {
		name              string
		snaps             []store.ResourceSnapshot
		expectedDirection string
		expectedSamples   int
		expectedMemory    int
		expectedCPU       int
	}{
		{
			name:              "empty slice",
			snaps:             []store.ResourceSnapshot{},
			expectedDirection: "unknown",
			expectedSamples:   0,
		},
		{
			name:              "single snapshot",
			snaps:             []store.ResourceSnapshot{makeSnap("100m", "200Mi")},
			expectedDirection: "unknown",
			expectedSamples:   1,
		},
		{
			name: "increasing memory",
			snaps: []store.ResourceSnapshot{
				makeSnap("100m", "500Mi"),
				makeSnap("100m", "500Mi"),
				makeSnap("100m", "700Mi"),
				makeSnap("100m", "700Mi"),
			},
			expectedDirection: "increasing",
			expectedSamples:   4,
		},
		{
			name: "decreasing memory",
			snaps: []store.ResourceSnapshot{
				makeSnap("100m", "700Mi"),
				makeSnap("100m", "700Mi"),
				makeSnap("100m", "500Mi"),
				makeSnap("100m", "500Mi"),
			},
			expectedDirection: "decreasing",
			expectedSamples:   4,
		},
		{
			name: "stable memory",
			snaps: []store.ResourceSnapshot{
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "205Mi"),
				makeSnap("100m", "205Mi"),
			},
			expectedDirection: "stable",
			expectedSamples:   4,
		},
		{
			name: "all zero values",
			snaps: []store.ResourceSnapshot{
				makeSnap("0m", "0Mi"),
				makeSnap("0m", "0Mi"),
				makeSnap("0m", "0Mi"),
				makeSnap("0m", "0Mi"),
			},
			expectedDirection: "stable",
			expectedSamples:   4,
			expectedMemory:    0,
			expectedCPU:       0,
		},
		{
			name: "two snapshots increasing",
			snaps: []store.ResourceSnapshot{
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "400Mi"),
			},
			expectedDirection: "increasing",
			expectedSamples:   2,
		},
		{
			name: "exactly at 10% boundary",
			snaps: []store.ResourceSnapshot{
				makeSnap("100m", "100Mi"),
				makeSnap("100m", "100Mi"),
				makeSnap("100m", "110Mi"),
				makeSnap("100m", "110Mi"),
			},
			expectedDirection: "stable",
			expectedSamples:   4,
		},
		{
			name: "unparsable quantity",
			snaps: []store.ResourceSnapshot{
				makeSnap("badcpu", "badmem"),
				makeSnap("badcpu", "badmem"),
				makeSnap("badcpu", "badmem"),
				makeSnap("badcpu", "badmem"),
			},
			expectedDirection: "stable",
			expectedSamples:   4,
			expectedMemory:    0,
			expectedCPU:       0,
		},
		{
			name: "odd length slice",
			snaps: []store.ResourceSnapshot{
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "400Mi"),
			},
			expectedDirection: "increasing",
			expectedSamples:   3,
		},
		{
			name: "all identical values",
			snaps: []store.ResourceSnapshot{
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "200Mi"),
				makeSnap("100m", "200Mi"),
			},
			expectedDirection: "stable",
			expectedSamples:   4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			trend := store.ComputeTrend(tt.snaps)

			if trend.Direction != tt.expectedDirection {
				t.Errorf("direction: got %s, expected %s", trend.Direction, tt.expectedDirection)
			}
			if trend.SampleCount != tt.expectedSamples {
				t.Errorf("sample count: got %d, expected %d", trend.SampleCount, tt.expectedSamples)
			}
			if tt.expectedMemory != 0 && trend.AvgMemoryMi != tt.expectedMemory {
				t.Errorf("avg memory: got %d, expected %d", trend.AvgMemoryMi, tt.expectedMemory)
			}
			if tt.expectedCPU != 0 && trend.AvgCPUMilli != tt.expectedCPU {
				t.Errorf("avg cpu: got %d, expected %d", trend.AvgCPUMilli, tt.expectedCPU)
			}
		})
	}
}

package store

import (
	"math"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
	"k8s.io/apimachinery/pkg/api/resource"
)

func ComputeTrend(snaps []ResourceSnapshot) ai.ResourceTrend {
	if len(snaps) == 0 {
		return ai.ResourceTrend{Direction: "unknown"}
	}

	memVals := make([]int, len(snaps))
	cpuVals := make([]int, len(snaps))

	for i, s := range snaps {
		memVals[i] = parseQuantity(s.MemoryUsage, false)
		cpuVals[i] = parseQuantity(s.CPUUsage, true)
	}

	if len(snaps) == 1 {
		return ai.ResourceTrend{
			Direction:   "unknown",
			AvgMemoryMi: memVals[0],
			AvgCPUMilli: cpuVals[0],
			SampleCount: 1,
		}
	}

	avgMem := average(memVals)
	avgCPU := average(cpuVals)

	mid := len(memVals) / 2
	olderMem := average(memVals[:mid])
	recentMem := average(memVals[mid:])
	olderCPU := average(cpuVals[:mid])
	recentCPU := average(cpuVals[mid:])

	memDiff := 0.0
	cpuDiff := 0.0

	if olderMem > 0 {
		memDiff = float64(recentMem-olderMem) / float64(olderMem)
	}
	if olderCPU > 0 {
		cpuDiff = float64(recentCPU-olderCPU) / float64(olderCPU)
	}

	diff := memDiff
	if math.Abs(cpuDiff) > math.Abs(memDiff) {
		diff = cpuDiff
	}

	direction := "stable"
	if diff > 0.1 {
		direction = "increasing"
	} else if diff < -0.1 {
		direction = "decreasing"
	}

	return ai.ResourceTrend{
		AvgMemoryMi: avgMem,
		AvgCPUMilli: avgCPU,
		Direction:   direction,
		SampleCount: len(snaps),
	}
}

func average(vals []int) int {
	if len(vals) == 0 {
		return 0
	}
	sum := 0
	for _, v := range vals {
		sum += v
	}
	return sum / len(vals)
}

func parseQuantity(s string, toMilli bool) int {
	q, err := resource.ParseQuantity(s)
	if err != nil {
		return 0
	}
	if toMilli {
		return int(q.MilliValue()) // for CPU, in millicores
	}
	return int(q.Value() / (1024 * 1024)) // for memory, in Mi
}

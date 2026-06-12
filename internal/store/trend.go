package store

import (
	"strconv"
	"strings"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
)

func ComputeTrend(snaps []ResourceSnapshot) ai.ResourceTrend {
	if len(snaps) == 0 {
		return ai.ResourceTrend{Direction: "unknown"}
	}

	memVals := make([]int, len(snaps))
	cpuVals := make([]int, len(snaps))

	for i, s := range snaps {
		memVals[i] = parseQuantity(s.MemoryUsage, "Mi")
		cpuVals[i] = parseQuantity(s.CPUUsage, "m")
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
	older := average(memVals[:mid])
	recent := average(memVals[mid:])

	direction := "stable"
	if older > 0 {
		diff := float64(recent-older) / float64(older)
		if diff > 0.1 {
			direction = "increasing"
		} else if diff < -0.1 {
			direction = "decreasing"
		}
	}

	return ai.ResourceTrend{
		AvgMemoryMi: avgMem,
		AvgCPUMilli: avgCPU,
		Direction:   direction,
		SampleCount: len(snaps),
	}
}

func parseQuantity(s, unit string) int {
	s = strings.TrimSuffix(s, unit)
	val, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return val
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

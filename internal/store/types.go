package store

import "time"

type ResourceSnapshot struct {
	ID          int64
	PodName     string
	Namespace   string
	CPUUsage    string
	MemoryUsage string
	RecordedAt  time.Time
}

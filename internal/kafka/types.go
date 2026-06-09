package kafka

import (
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
)

type PodEvent struct {
	Pod       k8.Pod    `JSON:"Pod"`
	Timestamp time.Time `JSON:"Timestamp"`
}

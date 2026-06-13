package kafka

import (
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

type Consumer struct {
	reader *kafka.Reader
}

type PodEvent struct {
	Pod       k8.Pod    `json:"pod"`
	Timestamp time.Time `json:"timestamp"`
}

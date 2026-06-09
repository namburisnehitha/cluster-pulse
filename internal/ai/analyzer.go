package ai

import (
	"context"

	"github.com/namburisnehitha/cluster-pulse/internal/kafka"
)

type Analyzer interface {
	Analyze(ctx context.Context, event kafka.PodEvent) (Analysis, error)
}

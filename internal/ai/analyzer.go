package ai

import (
	"context"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/kafka"
)

type Analyzer interface {
	Analyze(ctx context.Context, event kafka.PodEvent, trend ResourceTrend, node *k8.Node) (Analysis, error)
}

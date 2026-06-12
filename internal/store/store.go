package store

import (
	"context"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
)

type Store interface {
	SaveAnalysis(ctx context.Context, analysis ai.Analysis) error
	SaveResourceSnapshot(ctx context.Context, snap ResourceSnapshot) error
	GetAnalysis(ctx context.Context, podName, namespace string) (*ai.Analysis, error)
	GetPodHistory(ctx context.Context, podName, namespace string, limit int) ([]ResourceSnapshot, error)
	ListAnalyses(ctx context.Context, cursor string, limit int) ([]ai.Analysis, string, error)
	CallPrune(ctx context.Context, retentionDays int) error
	Close() error
}

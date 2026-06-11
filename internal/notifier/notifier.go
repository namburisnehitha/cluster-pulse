package notifier

import (
	"context"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
)

type notification interface {
	Notify(ctx context.Context, analysis ai.Analysis) error
}

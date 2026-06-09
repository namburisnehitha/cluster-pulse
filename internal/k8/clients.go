package k8

import "context"

type K8 interface {
	WatchPods(ctx context.Context) (<-chan Pod, error)
	GetPodLogs(ctx context.Context, namespace, podName string) (string, error)
	GetRecentEvents(ctx context.Context, namespace, podName string) ([]Event, error)
	GetRecentDeployments(ctx context.Context, namespace string) ([]Deployment, error)
	ListAllPods(ctx context.Context) ([]Pod, error)
}

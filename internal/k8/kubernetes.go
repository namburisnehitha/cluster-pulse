package k8

import (
	"context"
	"io"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv "k8s.io/metrics/pkg/client/clientset/versioned"
)

type Client struct {
	clientset     kubernetes.Interface
	metricsClient *metricsv.Clientset
	tracer        trace.Tracer
}

func NewClient() (*Client, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		// not running in cluster, use kubeconfig
		config, err = clientcmd.BuildConfigFromFlags("", clientcmd.RecommendedHomeFile)
		if err != nil {
			return nil, err
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	metricsClient, err := metricsv.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &Client{clientset: clientset, metricsClient: metricsClient, tracer: otel.Tracer("Watch-Pod")}, nil
}

func (c *Client) WatchPods(ctx context.Context) (<-chan PodResult, error) {
	podCh := make(chan PodResult)

	go func() {
		defer close(podCh)

		var lastResourceVersion string
		backoff := time.Second

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			endReasonSet := false

			watchCtx, watchSpan := getTracer().Start(ctx, "k8s.watch.connection",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String(string(semconv.K8SNamespaceNameKey), ""),
					attribute.String("k8s.resource", "pods"),
					attribute.String("k8s.watch.resource_version", lastResourceVersion),
				),
			)

			opts := metav1.ListOptions{}
			if lastResourceVersion != "" {
				opts.ResourceVersion = lastResourceVersion
			}

			watcher, err := c.clientset.CoreV1().Pods("").Watch(ctx, opts)
			if err != nil {
				watchSpan.RecordError(err)
				watchSpan.SetStatus(codes.Error, "watch failed to start")
				watchSpan.SetAttributes(attribute.String("k8s.watch.end_reason", "start_failed"))
				endReasonSet = true
				watchSpan.End()

				podCh <- PodResult{Err: err}

				_, reconnectSpan := getTracer().Start(ctx, "k8s.watch.reconnect",
					trace.WithAttributes(
						attribute.String("k8s.resource", "pods"),
						attribute.Int64("k8s.watch.backoff_ms", backoff.Milliseconds()),
					),
				)
				select {
				case <-ctx.Done():
					reconnectSpan.End()
					return
				case <-time.After(backoff):
				}
				reconnectSpan.End()

				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
			backoff = time.Second

			var (
				eventCount            int
				filteredCount         int
				logsErrorCount        int
				eventsErrorCount      int
				deploymentsErrorCount int
			)

			watchStart := time.Now()

		eventLoop:
			for {
				select {
				case <-ctx.Done():
					watcher.Stop()
					watchSpan.SetAttributes(
						attribute.String("k8s.watch.end_reason", "context_cancelled"),
						attribute.String("k8s.watch.last_resource_version", lastResourceVersion),
						attribute.Int("k8s.watch.events_processed", eventCount),
						attribute.Int("k8s.watch.filtered_count", filteredCount),
						attribute.Int("k8s.watch.logs_error_count", logsErrorCount),
						attribute.Int("k8s.watch.events_error_count", eventsErrorCount),
						attribute.Int("k8s.watch.deployments_error_count", deploymentsErrorCount),
						attribute.Int64("k8s.watch.connection_duration_ms", time.Since(watchStart).Milliseconds()),
					)
					endReasonSet = true
					watchSpan.End()
					return

				case event, ok := <-watcher.ResultChan():
					if !ok {
						break eventLoop
					}

					if event.Type == watch.Error {
						if status, ok := event.Object.(*metav1.Status); ok && status.Reason == metav1.StatusReasonGone {
							lastResourceVersion = ""
							watchSpan.SetAttributes(attribute.String("k8s.watch.end_reason", "resource_version_gone"))
							endReasonSet = true
						} else {
							watchSpan.SetAttributes(attribute.String("k8s.watch.end_reason", "watch_error"))
							endReasonSet = true
						}
						break eventLoop
					}

					pod, ok := event.Object.(*corev1.Pod)
					if !ok {
						continue
					}

					if event.Type == watch.Deleted {
						continue
					}
					lastResourceVersion = pod.ResourceVersion

					if !IsUnhealthyPod(pod) {
						filteredCount++
						watchSpan.AddEvent("pod_filtered_healthy",
							trace.WithAttributes(
								attribute.String(string(semconv.K8SPodNameKey), pod.Name),
								attribute.String(string(semconv.K8SNamespaceNameKey), pod.Namespace),
								attribute.String("k8s.pod.phase", string(pod.Status.Phase)),
							),
						)
						continue
					}

					receivedAt := time.Now()

					_, eventSpan := getTracer().Start(watchCtx, "k8s.watch.event",
						trace.WithSpanKind(trace.SpanKindInternal),
						trace.WithTimestamp(receivedAt),
						trace.WithAttributes(
							attribute.String(string(semconv.K8SPodNameKey), pod.Name),
							attribute.String(string(semconv.K8SNamespaceNameKey), pod.Namespace),
							attribute.String(string(semconv.K8SNodeNameKey), pod.Spec.NodeName),
							attribute.String("k8s.event.type", string(event.Type)),
							attribute.String("k8s.pod.phase", string(pod.Status.Phase)),
						),
					)

					Pod := ToDomainPod(pod)
					Pod.WatchReceivedAt = receivedAt

					enrichmentStart := time.Now()

					_, logsSpan := getTracer().Start(watchCtx, "k8s.watch.event.fetch_logs",
						trace.WithSpanKind(trace.SpanKindInternal),
						trace.WithAttributes(
							attribute.String(string(semconv.K8SPodNameKey), Pod.Name),
							attribute.String(string(semconv.K8SNamespaceNameKey), Pod.Namespace),
						),
					)
					if logs, err := c.GetPodLogs(watchCtx, Pod.Namespace, Pod.Name, Pod.ContainerName); err != nil {
						Pod.LogsError = err.Error()
						logsSpan.RecordError(err)
						logsSpan.SetStatus(codes.Error, "logs fetch failed")
						logsErrorCount++
					} else {
						Pod.Logs = logs
						logsSpan.SetStatus(codes.Ok, "")
					}
					logsSpan.End()

					_, eventsSpan := getTracer().Start(watchCtx, "k8s.watch.event.fetch_events",
						trace.WithSpanKind(trace.SpanKindInternal),
						trace.WithAttributes(
							attribute.String(string(semconv.K8SPodNameKey), Pod.Name),
							attribute.String(string(semconv.K8SNamespaceNameKey), Pod.Namespace),
						),
					)
					if events, err := c.GetRecentEvents(watchCtx, Pod.Namespace, Pod.Name); err != nil {
						Pod.EventsError = err.Error()
						eventsSpan.RecordError(err)
						eventsSpan.SetStatus(codes.Error, "events fetch failed")
						eventsErrorCount++
					} else {
						Pod.Events = events
						eventsSpan.SetStatus(codes.Ok, "")
					}
					eventsSpan.End()

					_, deploymentsSpan := getTracer().Start(watchCtx, "k8s.watch.event.fetch_deployments",
						trace.WithSpanKind(trace.SpanKindInternal),
						trace.WithAttributes(
							attribute.String(string(semconv.K8SNamespaceNameKey), Pod.Namespace),
						),
					)
					if deploy, err := c.GetRecentDeployments(watchCtx, Pod.Namespace); err != nil {
						Pod.DeploymentsError = err.Error()
						deploymentsSpan.RecordError(err)
						deploymentsSpan.SetStatus(codes.Error, "deployments fetch failed")
						deploymentsErrorCount++
					} else {
						Pod.Deployments = deploy
						deploymentsSpan.SetStatus(codes.Ok, "")
					}
					deploymentsSpan.End()

					eventSpan.SetAttributes(
						attribute.Int("k8s.pod.exit_code", Pod.ExitCode),
						attribute.Int("k8s.pod.restart_count", Pod.RestartCount),
						attribute.Int64("k8s.watch.enrichment_duration_ms", time.Since(enrichmentStart).Milliseconds()),
					)
					eventSpan.SetStatus(codes.Ok, "")
					eventSpan.End()

					eventCount++
					podCh <- PodResult{Pod: Pod}
				}
			}

			watcher.Stop()

			watchSpan.SetAttributes(
				attribute.String("k8s.watch.last_resource_version", lastResourceVersion),
				attribute.Int("k8s.watch.events_processed", eventCount),
				attribute.Int("k8s.watch.filtered_count", filteredCount),
				attribute.Int("k8s.watch.logs_error_count", logsErrorCount),
				attribute.Int("k8s.watch.events_error_count", eventsErrorCount),
				attribute.Int("k8s.watch.deployments_error_count", deploymentsErrorCount),
				attribute.Int64("k8s.watch.connection_duration_ms", time.Since(watchStart).Milliseconds()),
			)
			if !endReasonSet {
				watchSpan.SetAttributes(attribute.String("k8s.watch.end_reason", "channel_closed"))
			}
			watchSpan.End()

			_, reconnectSpan := getTracer().Start(ctx, "k8s.watch.reconnect",
				trace.WithAttributes(
					attribute.String("k8s.resource", "pods"),
					attribute.Int64("k8s.watch.backoff_ms", backoff.Milliseconds()),
				),
			)
			select {
			case <-ctx.Done():
				reconnectSpan.End()
				return
			case <-time.After(backoff):
			}
			reconnectSpan.End()
		}
	}()

	return podCh, nil
}

func IsUnhealthyPod(pod *corev1.Pod) bool {

	if pod.Status.Phase == corev1.PodFailed {
		return true
	}

	if HasUnhealthyContainer(pod) {
		return true
	}

	return false
}

func HasUnhealthyContainer(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			reason := cs.State.Waiting.Reason
			if reason == "CrashLoopBackOff" || reason == "ImagePullBackOff" || reason == "ErrImagePull" {
				return true
			}
		}
		if cs.State.Terminated != nil {
			if cs.State.Terminated.ExitCode != 0 {
				return true
			}
		}
		if cs.RestartCount > 5 {
			return true
		}
	}
	return false
}

func (c *Client) GetPodLogs(ctx context.Context, namespace, podName, containerName string) (string, error) {

	tailLines := int64(50)

	req := c.clientset.CoreV1().Pods(namespace).GetLogs(podName, &corev1.PodLogOptions{
		TailLines: &tailLines,
		Container: containerName,
	})

	stream, err := req.Stream(ctx)

	if err != nil {
		return "", err
	}
	defer stream.Close()

	bytes, err := io.ReadAll(stream)

	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

func (c *Client) GetRecentEvents(ctx context.Context, namespace, podName string) ([]Event, error) {

	ClientEvents, err := c.clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: "involvedObject.name=" + podName,
	})

	if err != nil {
		return nil, err
	}

	var returnEvents []Event

	for _, e := range ClientEvents.Items {

		event := Event{
			Reason:   e.Reason,
			Message:  e.Message,
			Type:     e.Type,
			Count:    int(e.Count),
			LastTime: e.LastTimestamp.Time,
		}

		returnEvents = append(returnEvents, event)
	}

	return returnEvents, nil
}

func (c *Client) GetRecentDeployments(ctx context.Context, namespace string) ([]Deployment, error) {

	ClientDeployments, err := c.clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{
		Limit: 5,
	})

	if err != nil {
		return nil, err
	}

	var returnDeployments []Deployment

	for _, d := range ClientDeployments.Items {

		image := ""
		if len(d.Spec.Template.Spec.Containers) > 0 {
			image = d.Spec.Template.Spec.Containers[0].Image
		}

		lastUpdated := d.CreationTimestamp.Time
		for _, condition := range d.Status.Conditions {
			if condition.LastUpdateTime.Time.After(lastUpdated) {
				lastUpdated = condition.LastUpdateTime.Time
			}
		}

		desired := 0
		if d.Spec.Replicas != nil {
			desired = int(*d.Spec.Replicas)
		}

		deploy := Deployment{
			Name:              d.Name,
			Image:             image,
			LastUpdated:       lastUpdated,
			DesiredReplicas:   desired,
			AvailableReplicas: int(d.Status.AvailableReplicas),
		}

		returnDeployments = append(returnDeployments, deploy)
	}

	return returnDeployments, nil
}

func (c *Client) ListAllPods(ctx context.Context) ([]Pod, error) {

	ClientList, err := c.clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{})

	if err != nil {
		return nil, err
	}

	var returnList []Pod

	for _, l := range ClientList.Items {

		pod := ToDomainPod(&l)

		returnList = append(returnList, pod)
	}

	return returnList, nil

}

func ToDomainPod(pod *corev1.Pod) Pod {

	p := Pod{
		Name:      pod.Name,
		Namespace: pod.Namespace,
		Phase:     string(pod.Status.Phase),
		NodeName:  pod.Spec.NodeName,
	}

	if pod.Status.StartTime != nil {
		p.StartTime = pod.Status.StartTime.Time
	}

	for _, condition := range pod.Status.Conditions {
		if condition.Status == corev1.ConditionFalse {
			p.Conditions = append(p.Conditions, string(condition.Type))
		}
	}

	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		if limit := c.Resources.Limits; limit != nil {
			if mem := limit.Memory(); mem != nil {
				p.MemoryLimit = mem.String()
			}
			if cpu := limit.Cpu(); cpu != nil {
				p.CPULimit = cpu.String()
			}
		}
		if request := c.Resources.Requests; request != nil {
			if mem := request.Memory(); mem != nil {
				p.MemoryRequest = mem.String()
			}
			if cpu := request.Cpu(); cpu != nil {
				p.CPURequest = cpu.String()
			}
		}
	}

	if len(pod.Status.ContainerStatuses) > 0 {
		cs := pod.Status.ContainerStatuses[0]
		p.ContainerName = cs.Name
		p.RestartCount = int(cs.RestartCount)

		if cs.State.Terminated != nil {
			p.ExitCode = int(cs.State.Terminated.ExitCode)
			p.FailureTime = cs.State.Terminated.FinishedAt.Time
		}

		if cs.LastTerminationState.Terminated != nil {
			p.LastRestartTime = cs.LastTerminationState.Terminated.FinishedAt.Time
		}
	}

	return p

}

func (c *Client) GetPodMetrics(ctx context.Context, namespace, podName string) (cpuUsage, memUsage string, err error) {
	metrics, err := c.metricsClient.MetricsV1beta1().PodMetricses(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	if len(metrics.Containers) == 0 {
		return "", "", nil
	}

	cpu := metrics.Containers[0].Usage.Cpu()
	mem := metrics.Containers[0].Usage.Memory()

	return cpu.String(), mem.String(), nil
}

func (c *Client) ListAllNodes(ctx context.Context) ([]Node, error) {
	nodeList, err := c.clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var nodes []Node
	for _, n := range nodeList.Items {
		status := "Ready"
		for _, condition := range n.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
				status = "NotReady"
			}
		}

		var roles []string
		for label := range n.Labels {
			if strings.HasPrefix(label, "node-role.kubernetes.io/") {
				roles = append(roles, strings.TrimPrefix(label, "node-role.kubernetes.io/"))
			}
		}

		nodes = append(nodes, Node{
			Name:           n.Name,
			Status:         status,
			Roles:          roles,
			Age:            n.CreationTimestamp.Time,
			KubeletVersion: n.Status.NodeInfo.KubeletVersion,
			CPUCapacity:    n.Status.Capacity.Cpu().String(),
			MemoryCapacity: n.Status.Capacity.Memory().String(),
		})
	}
	return nodes, nil
}

func (c *Client) ListAllEvents(ctx context.Context, namespace string) ([]Event, error) {
	if namespace == "" {
		namespace = metav1.NamespaceAll
	}
	eventList, err := c.clientset.CoreV1().Events(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var events []Event
	for _, e := range eventList.Items {
		events = append(events, Event{
			Reason:   e.Reason,
			Message:  e.Message,
			Type:     e.Type,
			Count:    int(e.Count),
			LastTime: e.LastTimestamp.Time,
		})
	}
	return events, nil
}

func (c *Client) GetNode(ctx context.Context, nodeName string) (*Node, error) {
	n, err := c.clientset.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	status := "Ready"
	for _, condition := range n.Status.Conditions {
		if condition.Type == corev1.NodeReady && condition.Status != corev1.ConditionTrue {
			status = "NotReady"
		}
	}

	var roles []string
	for label := range n.Labels {
		if strings.HasPrefix(label, "node-role.kubernetes.io/") {
			roles = append(roles, strings.TrimPrefix(label, "node-role.kubernetes.io/"))
		}
	}

	return &Node{
		Name:           n.Name,
		Status:         status,
		Roles:          roles,
		Age:            n.CreationTimestamp.Time,
		KubeletVersion: n.Status.NodeInfo.KubeletVersion,
		CPUCapacity:    n.Status.Capacity.Cpu().String(),
		MemoryCapacity: n.Status.Capacity.Memory().String(),
	}, nil
}

func NewClientWithClientset(clientset kubernetes.Interface) *Client {
	return &Client{clientset: clientset}
}

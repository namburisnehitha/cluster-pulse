package k8

import (
	"context"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Client struct {
	clientset *kubernetes.Clientset
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

	return &Client{clientset: clientset}, nil
}

func (c *Client) WatchPods(ctx context.Context) (<-chan PodResult, error) {
	podCh := make(chan PodResult)

	go func() {
		defer close(podCh)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			watcher, err := c.clientset.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{})
			if err != nil {
				podCh <- PodResult{Err: err}
				return
			}

			for event := range watcher.ResultChan() {
				select {
				case <-ctx.Done():
					watcher.Stop()
					return
				default:
				}

				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					continue
				}

				if !isUnhealthy(pod) {
					continue
				}

				Pod := toDomainPod(pod)

				logs, err := c.GetPodLogs(ctx, Pod.Namespace, Pod.Name, Pod.ContainerName)
				if err != nil {
					continue
				}
				Pod.Logs = logs

				events, err := c.GetRecentEvents(ctx, Pod.Namespace, Pod.Name)
				if err != nil {
					continue
				}
				Pod.Events = events

				deploy, err := c.GetRecentDeployments(ctx, Pod.Namespace)
				if err != nil {
					continue
				}
				Pod.Deployments = deploy

				Pod.WatchReceivedAt = time.Now()
				podCh <- PodResult{Pod: Pod}
			}

			watcher.Stop()
			// watch dropped — outer loop reconnects
		}
	}()

	return podCh, nil
}

func isUnhealthy(pod *corev1.Pod) bool {

	if pod.Status.Phase == corev1.PodFailed {
		return true
	}

	if hasUnhealthyContainer(pod) {
		return true
	}

	return false
}

func hasUnhealthyContainer(pod *corev1.Pod) bool {
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

		pod := toDomainPod(&l)

		returnList = append(returnList, pod)
	}

	return returnList, nil

}

func toDomainPod(pod *corev1.Pod) Pod {

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

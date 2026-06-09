package k8

import (
	"context"
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

		watcher, err := c.clientset.CoreV1().Pods("").Watch(ctx, metav1.ListOptions{})

		if err != nil {
			podCh <- PodResult{Err: err}
			return
		}

		for event := range watcher.ResultChan() {

			pod, ok := event.Object.(*corev1.Pod)

			if !ok {
				continue
			}

			if !isUnhealthy(pod) {
				continue
			}

			podCh <- PodResult{
				Pod: Pod{
					Name:            pod.Name,
					Namespace:       pod.Namespace,
					Phase:           string(pod.Status.Phase),
					WatchReceivedAt: time.Now(),
				},
			}
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

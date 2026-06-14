package k8_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func newTestClient(t *testing.T, objects ...runtime.Object) *k8.Client {
	t.Helper()
	fakeClient := fake.NewSimpleClientset(objects...)
	return k8.NewClientWithClientset(fakeClient)
}
func newTestClientWithLogs(t *testing.T, logs string, statusCode int) *k8.Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/log") {
			w.WriteHeader(statusCode)
			if statusCode == http.StatusOK {
				w.Write([]byte(logs))
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(func() { server.Close() })

	config := &rest.Config{
		Host: server.URL,
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("failed to create clientset: %v", err)
	}
	return k8.NewClientWithClientset(clientset)
}
func TestToDomainPod(t *testing.T) {

	// Situation 1: all basic fields set correctly — name, namespace, phase, node name
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
		},
	}
	p := k8.ToDomainPod(pod)
	if p.Name != "pod-1" {
		t.Errorf("basic fields: name: got %s, want pod-1", p.Name)
	}
	if p.Namespace != "default" {
		t.Errorf("basic fields: namespace: got %s, want default", p.Namespace)
	}
	if p.Phase != "Failed" {
		t.Errorf("basic fields: phase: got %s, want Failed", p.Phase)
	}
	if p.NodeName != "node-1" {
		t.Errorf("basic fields: node name: got %s, want node-1", p.NodeName)
	}

	// Situation 2: start time set — maps correctly
	startTime := time.Now().Round(time.Second)
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			StartTime: &metav1.Time{Time: startTime},
		},
	}
	p = k8.ToDomainPod(pod)
	if !p.StartTime.Equal(startTime) {
		t.Errorf("start time: got %v, want %v", p.StartTime, startTime)
	}

	// Situation 3: start time nil — stays zero value
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status:     corev1.PodStatus{},
	}
	p = k8.ToDomainPod(pod)
	if !p.StartTime.IsZero() {
		t.Errorf("nil start time: got %v, want zero", p.StartTime)
	}

	// Situation 4: condition with ConditionFalse — added to conditions slice
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if len(p.Conditions) != 1 {
		t.Fatalf("condition false: got %d conditions, want 1", len(p.Conditions))
	}
	if p.Conditions[0] != "Ready" {
		t.Errorf("condition false: got %s, want Ready", p.Conditions[0])
	}

	// Situation 5: condition with ConditionTrue — not added to slice
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if len(p.Conditions) != 0 {
		t.Errorf("condition true: got %d conditions, want 0", len(p.Conditions))
	}

	// Situation 6: mixed conditions — only ConditionFalse ones added
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if len(p.Conditions) != 2 {
		t.Fatalf("mixed conditions: got %d conditions, want 2", len(p.Conditions))
	}

	// Situation 7: no conditions — empty slice
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status:     corev1.PodStatus{},
	}
	p = k8.ToDomainPod(pod)
	if len(p.Conditions) != 0 {
		t.Errorf("no conditions: got %d conditions, want 0", len(p.Conditions))
	}

	// Situation 8: memory and CPU limits set — mapped correctly
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("512Mi"),
							corev1.ResourceCPU:    resource.MustParse("500m"),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("256Mi"),
							corev1.ResourceCPU:    resource.MustParse("250m"),
						},
					},
				},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if p.MemoryLimit == "" {
		t.Errorf("limits: memory limit: got empty, want 512Mi")
	}
	if p.CPULimit == "" {
		t.Errorf("limits: cpu limit: got empty, want 500m")
	}
	if p.MemoryRequest == "" {
		t.Errorf("limits: memory request: got empty, want 256Mi")
	}
	if p.CPURequest == "" {
		t.Errorf("limits: cpu request: got empty, want 250m")
	}

	// Situation 9: no containers — limits and requests stay empty, no panic
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Spec:       corev1.PodSpec{},
	}
	p = k8.ToDomainPod(pod)
	if p.MemoryLimit != "" {
		t.Errorf("no containers: memory limit: got %s, want empty", p.MemoryLimit)
	}
	if p.CPULimit != "" {
		t.Errorf("no containers: cpu limit: got %s, want empty", p.CPULimit)
	}
	if p.MemoryRequest != "" {
		t.Errorf("no containers: memory request: got %s, want empty", p.MemoryRequest)
	}
	if p.CPURequest != "" {
		t.Errorf("no containers: cpu request: got %s, want empty", p.CPURequest)
	}

	// Situation 10: multiple containers — only first container used for limits
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
				},
				{
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if p.MemoryLimit != "512Mi" {
		t.Errorf("multiple containers: memory limit: got %s, want 512Mi — only first container used", p.MemoryLimit)
	}

	// Situation 11: container status set — container name and restart count mapped
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "my-container",
					RestartCount: 3,
				},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if p.ContainerName != "my-container" {
		t.Errorf("container status: name: got %s, want my-container", p.ContainerName)
	}
	if p.RestartCount != 3 {
		t.Errorf("container status: restart count: got %d, want 3", p.RestartCount)
	}

	// Situation 12: terminated container with non-zero exit code — ExitCode and FailureTime set
	failureTime := time.Now().Round(time.Second)
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "my-container",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode:   137,
							FinishedAt: metav1.Time{Time: failureTime},
						},
					},
				},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if p.ExitCode != 137 {
		t.Errorf("terminated: exit code: got %d, want 137", p.ExitCode)
	}
	if !p.FailureTime.Equal(failureTime) {
		t.Errorf("terminated: failure time: got %v, want %v", p.FailureTime, failureTime)
	}

	// Situation 13: terminated container with exit code 0 — ExitCode 0, FailureTime still set
	cleanExitTime := time.Now().Round(time.Second)
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "my-container",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode:   0,
							FinishedAt: metav1.Time{Time: cleanExitTime},
						},
					},
				},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if p.ExitCode != 0 {
		t.Errorf("clean exit: exit code: got %d, want 0", p.ExitCode)
	}
	if !p.FailureTime.Equal(cleanExitTime) {
		t.Errorf("clean exit: failure time: got %v, want %v", p.FailureTime, cleanExitTime)
	}

	// Situation 14: last termination state set — LastRestartTime mapped correctly
	lastRestartTime := time.Now().Round(time.Second)
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "my-container",
					LastTerminationState: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							FinishedAt: metav1.Time{Time: lastRestartTime},
						},
					},
				},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if !p.LastRestartTime.Equal(lastRestartTime) {
		t.Errorf("last termination: got %v, want %v", p.LastRestartTime, lastRestartTime)
	}

	// Situation 15: no container statuses — RestartCount 0, ExitCode 0, no panic
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status:     corev1.PodStatus{},
	}
	p = k8.ToDomainPod(pod)
	if p.RestartCount != 0 {
		t.Errorf("no statuses: restart count: got %d, want 0", p.RestartCount)
	}
	if p.ExitCode != 0 {
		t.Errorf("no statuses: exit code: got %d, want 0", p.ExitCode)
	}
	if p.ContainerName != "" {
		t.Errorf("no statuses: container name: got %s, want empty", p.ContainerName)
	}

	// Situation 16: multiple container statuses — only first used
	pod = &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{Name: "first-container", RestartCount: 3},
				{Name: "second-container", RestartCount: 7},
			},
		},
	}
	p = k8.ToDomainPod(pod)
	if p.ContainerName != "first-container" {
		t.Errorf("multiple statuses: container name: got %s, want first-container", p.ContainerName)
	}
	if p.RestartCount != 3 {
		t.Errorf("multiple statuses: restart count: got %d, want 3 — only first status used", p.RestartCount)
	}
}

func TestIsUnhealthyPod(t *testing.T) {

	// Situation 1: PodFailed phase — unhealthy regardless of container state
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
		},
	}
	if !k8.IsUnhealthyPod(pod) {
		t.Errorf("failed phase: got false, want true")
	}

	// Situation 2: healthy running pod — no failed phase, no unhealthy containers
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:         "my-container",
					RestartCount: 0,
				},
			},
		},
	}
	if k8.IsUnhealthyPod(pod) {
		t.Errorf("healthy pod: got true, want false")
	}

	// Situation 3: phase not failed but container unhealthy — unhealthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: "CrashLoopBackOff",
						},
					},
				},
			},
		},
	}
	if !k8.IsUnhealthyPod(pod) {
		t.Errorf("running but crashloop: got false, want true")
	}
}

func TestHasUnhealthyContainer(t *testing.T) {

	// Situation 1: CrashLoopBackOff waiting reason — unhealthy
	pod := &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: "CrashLoopBackOff",
						},
					},
				},
			},
		},
	}
	if !k8.HasUnhealthyContainer(pod) {
		t.Errorf("CrashLoopBackOff: got false, want true")
	}

	// Situation 2: ImagePullBackOff waiting reason — unhealthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: "ImagePullBackOff",
						},
					},
				},
			},
		},
	}
	if !k8.HasUnhealthyContainer(pod) {
		t.Errorf("ImagePullBackOff: got false, want true")
	}

	// Situation 3: ErrImagePull waiting reason — unhealthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: "ErrImagePull",
						},
					},
				},
			},
		},
	}
	if !k8.HasUnhealthyContainer(pod) {
		t.Errorf("ErrImagePull: got false, want true")
	}

	// Situation 4: other waiting reason — healthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: "ContainerCreating",
						},
					},
				},
			},
		},
	}
	if k8.HasUnhealthyContainer(pod) {
		t.Errorf("ContainerCreating: got true, want false")
	}

	// Situation 5: terminated with non-zero exit code — unhealthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 1,
						},
					},
				},
			},
		},
	}
	if !k8.HasUnhealthyContainer(pod) {
		t.Errorf("non-zero exit code: got false, want true")
	}

	// Situation 6: terminated with exit code 137 OOMKilled — unhealthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 137,
						},
					},
				},
			},
		},
	}
	if !k8.HasUnhealthyContainer(pod) {
		t.Errorf("exit code 137 OOMKilled: got false, want true")
	}

	// Situation 7: terminated with exit code 0 — healthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
						},
					},
				},
			},
		},
	}
	if k8.HasUnhealthyContainer(pod) {
		t.Errorf("exit code 0: got true, want false")
	}

	// Situation 8: RestartCount exactly 5 — healthy, boundary is > 5
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{RestartCount: 5},
			},
		},
	}
	if k8.HasUnhealthyContainer(pod) {
		t.Errorf("restart count 5: got true, want false — boundary is > 5")
	}

	// Situation 9: RestartCount 6 — unhealthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{RestartCount: 6},
			},
		},
	}
	if !k8.HasUnhealthyContainer(pod) {
		t.Errorf("restart count 6: got false, want true")
	}

	// Situation 10: no container statuses — healthy, no panic
	pod = &corev1.Pod{
		Status: corev1.PodStatus{},
	}
	if k8.HasUnhealthyContainer(pod) {
		t.Errorf("no statuses: got true, want false")
	}

	// Situation 11: multiple containers, one unhealthy — whole pod unhealthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{RestartCount: 0},
				{
					State: corev1.ContainerState{
						Waiting: &corev1.ContainerStateWaiting{
							Reason: "CrashLoopBackOff",
						},
					},
				},
			},
		},
	}
	if !k8.HasUnhealthyContainer(pod) {
		t.Errorf("one unhealthy container: got false, want true")
	}

	// Situation 12: multiple containers all healthy — healthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{RestartCount: 0},
				{RestartCount: 1},
				{RestartCount: 2},
			},
		},
	}
	if k8.HasUnhealthyContainer(pod) {
		t.Errorf("all healthy containers: got true, want false")
	}

	// Situation 13: waiting state nil and terminated nil — only restart count checked, healthy
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{
				{
					RestartCount: 0,
					State:        corev1.ContainerState{},
				},
			},
		},
	}
	if k8.HasUnhealthyContainer(pod) {
		t.Errorf("nil states: got true, want false")
	}
}

func TestListAllPods(t *testing.T) {

	// Situation 1: happy path — returns all pods, correct count
	fakeClient := fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Status:     corev1.PodStatus{Phase: corev1.PodRunning},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default"},
			Status:     corev1.PodStatus{Phase: corev1.PodFailed},
		},
	)
	c := k8.NewClientWithClientset(fakeClient)
	pods, err := c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("happy path: got %v, want nil", err)
	}
	if len(pods) != 2 {
		t.Errorf("happy path: got %d pods, want 2", len(pods))
	}

	// Situation 2: empty cluster — returns empty slice, no error
	fakeClient = fake.NewSimpleClientset()
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("empty cluster: got %v, want nil", err)
	}
	if len(pods) != 0 {
		t.Errorf("empty cluster: got %d pods, want 0", len(pods))
	}

	// Situation 3: single pod — all fields mapped correctly
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Spec: corev1.PodSpec{
				NodeName: "node-1",
			},
			Status: corev1.PodStatus{
				Phase: corev1.PodFailed,
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("single pod: got %v, want nil", err)
	}
	if len(pods) != 1 {
		t.Fatalf("single pod: got %d pods, want 1", len(pods))
	}
	if pods[0].Name != "pod-1" {
		t.Errorf("single pod: name: got %s, want pod-1", pods[0].Name)
	}
	if pods[0].Namespace != "default" {
		t.Errorf("single pod: namespace: got %s, want default", pods[0].Namespace)
	}
	if pods[0].Phase != "Failed" {
		t.Errorf("single pod: phase: got %s, want Failed", pods[0].Phase)
	}
	if pods[0].NodeName != "node-1" {
		t.Errorf("single pod: node name: got %s, want node-1", pods[0].NodeName)
	}

	// Situation 4: pod with all resource limits — verify they survive mapping
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("512Mi"),
								corev1.ResourceCPU:    resource.MustParse("500m"),
							},
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("256Mi"),
								corev1.ResourceCPU:    resource.MustParse("250m"),
							},
						},
					},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("resource limits: got %v, want nil", err)
	}
	if pods[0].MemoryLimit == "" {
		t.Errorf("resource limits: memory limit: got empty, want 512Mi")
	}
	if pods[0].CPULimit == "" {
		t.Errorf("resource limits: cpu limit: got empty, want 500m")
	}
	if pods[0].MemoryRequest == "" {
		t.Errorf("resource limits: memory request: got empty, want 256Mi")
	}
	if pods[0].CPURequest == "" {
		t.Errorf("resource limits: cpu request: got empty, want 250m")
	}

	// Situation 5: pod with ConditionFalse — conditions mapped correctly
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Status: corev1.PodStatus{
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("conditions: got %v, want nil", err)
	}
	if len(pods[0].Conditions) != 1 {
		t.Errorf("conditions: got %d, want 1 — only ConditionFalse added", len(pods[0].Conditions))
	}

	// Situation 6: pod with start time — maps through correctly
	startTime := time.Now().Round(time.Second)
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Status: corev1.PodStatus{
				StartTime: &metav1.Time{Time: startTime},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("start time: got %v, want nil", err)
	}
	if !pods[0].StartTime.Equal(startTime) {
		t.Errorf("start time: got %v, want %v", pods[0].StartTime, startTime)
	}

	// Situation 7: pods across multiple namespaces — all returned, ListAllPods passes "" to list all
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "kube-system"},
		},
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "monitoring"},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("multiple namespaces: got %v, want nil", err)
	}
	if len(pods) != 3 {
		t.Errorf("multiple namespaces: got %d pods, want 3", len(pods))
	}

	// Situation 8: pod with no containers — no panic, empty limits
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Spec:       corev1.PodSpec{},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("no containers: got %v, want nil", err)
	}
	if pods[0].MemoryLimit != "" {
		t.Errorf("no containers: memory limit: got %s, want empty", pods[0].MemoryLimit)
	}
	if pods[0].CPULimit != "" {
		t.Errorf("no containers: cpu limit: got %s, want empty", pods[0].CPULimit)
	}

	// Situation 9: pod with no container statuses — RestartCount 0, ExitCode 0, no panic
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Status:     corev1.PodStatus{},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("no statuses: got %v, want nil", err)
	}
	if pods[0].RestartCount != 0 {
		t.Errorf("no statuses: restart count: got %d, want 0", pods[0].RestartCount)
	}
	if pods[0].ExitCode != 0 {
		t.Errorf("no statuses: exit code: got %d, want 0", pods[0].ExitCode)
	}

	// Situation 10: pod with terminated container exit code 137 — OOMKilled maps correctly
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Terminated: &corev1.ContainerStateTerminated{
								ExitCode: 137,
							},
						},
					},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("exit code 137: got %v, want nil", err)
	}
	if pods[0].ExitCode != 137 {
		t.Errorf("exit code 137: got %d, want 137", pods[0].ExitCode)
	}

	// Situation 11: pod with empty name — no panic, empty string passes through
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "", Namespace: "default"},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("empty name: got %v, want nil", err)
	}
	if pods[0].Name != "" {
		t.Errorf("empty name: got %s, want empty", pods[0].Name)
	}

	// Situation 12: pod with no node assigned yet — NodeName empty, no panic
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default"},
			Spec:       corev1.PodSpec{NodeName: ""},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("no node: got %v, want nil", err)
	}
	if pods[0].NodeName != "" {
		t.Errorf("no node: got %s, want empty", pods[0].NodeName)
	}

	// Situation 13: pod in kube-system — system pods included, no namespace filter
	fakeClient = fake.NewSimpleClientset(
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "coredns", Namespace: "kube-system"},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("kube-system: got %v, want nil", err)
	}
	if len(pods) != 1 {
		t.Errorf("kube-system: got %d pods, want 1", len(pods))
	}
	if pods[0].Namespace != "kube-system" {
		t.Errorf("kube-system: namespace: got %s, want kube-system", pods[0].Namespace)
	}

	// Situation 14: large number of pods — 100 pods, correct count, no truncation
	var largePodList []runtime.Object
	for i := 0; i < 100; i++ {
		largePodList = append(largePodList, &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("pod-%d", i),
				Namespace: "default",
			},
		})
	}
	fakeClient = fake.NewSimpleClientset(largePodList...)
	c = k8.NewClientWithClientset(fakeClient)
	pods, err = c.ListAllPods(context.Background())
	if err != nil {
		t.Fatalf("large list: got %v, want nil", err)
	}
	if len(pods) != 100 {
		t.Errorf("large list: got %d pods, want 100", len(pods))
	}
}

func TestGetRecentEvents(t *testing.T) {

	// Situation 1: happy path — all fields mapped correctly
	lastTime := time.Now().Round(time.Second)
	fakeClient := fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "event-1",
				Namespace: "default",
			},
			Reason:        "OOMKilling",
			Message:       "Memory limit exceeded",
			Type:          "Warning",
			Count:         3,
			LastTimestamp: metav1.Time{Time: lastTime},
			InvolvedObject: corev1.ObjectReference{
				Name: "pod-1",
			},
		},
	)
	c := k8.NewClientWithClientset(fakeClient)
	events, err := c.GetRecentEvents(context.Background(), "default", "pod-1")
	if err != nil {
		t.Fatalf("happy path: got %v, want nil", err)
	}
	if len(events) != 1 {
		t.Fatalf("happy path: got %d events, want 1", len(events))
	}
	if events[0].Reason != "OOMKilling" {
		t.Errorf("happy path: reason: got %s, want OOMKilling", events[0].Reason)
	}
	if events[0].Message != "Memory limit exceeded" {
		t.Errorf("happy path: message: got %s, want Memory limit exceeded", events[0].Message)
	}
	if events[0].Type != "Warning" {
		t.Errorf("happy path: type: got %s, want Warning", events[0].Type)
	}
	if events[0].Count != 3 {
		t.Errorf("happy path: count: got %d, want 3", events[0].Count)
	}
	if !events[0].LastTime.Equal(lastTime) {
		t.Errorf("happy path: last time: got %v, want %v", events[0].LastTime, lastTime)
	}

	// Situation 2: no events for pod — empty slice, no error
	fakeClient = fake.NewSimpleClientset()
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.GetRecentEvents(context.Background(), "default", "pod-1")
	if err != nil {
		t.Fatalf("no events: got %v, want nil", err)
	}
	if len(events) != 0 {
		t.Errorf("no events: got %d events, want 0", len(events))
	}

	// Situation 3: multiple events — all returned, correct count
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Reason:         "OOMKilling",
			Message:        "Memory limit exceeded",
			Type:           "Warning",
			Count:          3,
			LastTimestamp:  metav1.Time{Time: lastTime},
			InvolvedObject: corev1.ObjectReference{Name: "pod-1"},
		},
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "event-2", Namespace: "default"},
			Reason:         "Pulled",
			Message:        "Successfully pulled image",
			Type:           "Normal",
			Count:          1,
			LastTimestamp:  metav1.Time{Time: lastTime},
			InvolvedObject: corev1.ObjectReference{Name: "pod-1"},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.GetRecentEvents(context.Background(), "default", "pod-1")
	if err != nil {
		t.Fatalf("multiple events: got %v, want nil", err)
	}
	if len(events) != 2 {
		t.Errorf("multiple events: got %d events, want 2", len(events))
	}

	// Situation 4: event with zero last time — maps through, no panic
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Reason:         "OOMKilling",
			LastTimestamp:  metav1.Time{},
			InvolvedObject: corev1.ObjectReference{Name: "pod-1"},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.GetRecentEvents(context.Background(), "default", "pod-1")
	if err != nil {
		t.Fatalf("zero last time: got %v, want nil", err)
	}
	if !events[0].LastTime.IsZero() {
		t.Errorf("zero last time: got %v, want zero", events[0].LastTime)
	}

	// Situation 5: event count of 1 — maps correctly, not zero
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Count:          1,
			InvolvedObject: corev1.ObjectReference{Name: "pod-1"},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.GetRecentEvents(context.Background(), "default", "pod-1")
	if err != nil {
		t.Fatalf("count 1: got %v, want nil", err)
	}
	if events[0].Count != 1 {
		t.Errorf("count 1: got %d, want 1", events[0].Count)
	}

	// Situation 6: event with empty reason and message — no panic, empty strings
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta:     metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Reason:         "",
			Message:        "",
			InvolvedObject: corev1.ObjectReference{Name: "pod-1"},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.GetRecentEvents(context.Background(), "default", "pod-1")
	if err != nil {
		t.Fatalf("empty fields: got %v, want nil", err)
	}
	if events[0].Reason != "" {
		t.Errorf("empty fields: reason: got %s, want empty", events[0].Reason)
	}
	if events[0].Message != "" {
		t.Errorf("empty fields: message: got %s, want empty", events[0].Message)
	}
}

func TestGetRecentDeployments(t *testing.T) {

	// Situation 1: happy path — all fields mapped correctly
	lastUpdated := time.Now().Round(time.Second)
	replicas := int32(3)
	fakeClient := fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "my-deployment",
				Namespace: "default",
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Image: "my-image:latest"},
						},
					},
				},
			},
			Status: appsv1.DeploymentStatus{
				AvailableReplicas: 2,
				Conditions: []appsv1.DeploymentCondition{
					{
						Type:           appsv1.DeploymentAvailable,
						LastUpdateTime: metav1.Time{Time: lastUpdated},
					},
				},
			},
		},
	)
	c := k8.NewClientWithClientset(fakeClient)
	deployments, err := c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("happy path: got %v, want nil", err)
	}
	if len(deployments) != 1 {
		t.Fatalf("happy path: got %d deployments, want 1", len(deployments))
	}
	if deployments[0].Name != "my-deployment" {
		t.Errorf("happy path: name: got %s, want my-deployment", deployments[0].Name)
	}
	if deployments[0].Image != "my-image:latest" {
		t.Errorf("happy path: image: got %s, want my-image:latest", deployments[0].Image)
	}
	if deployments[0].DesiredReplicas != 3 {
		t.Errorf("happy path: desired replicas: got %d, want 3", deployments[0].DesiredReplicas)
	}
	if deployments[0].AvailableReplicas != 2 {
		t.Errorf("happy path: available replicas: got %d, want 2", deployments[0].AvailableReplicas)
	}
	if !deployments[0].LastUpdated.Equal(lastUpdated) {
		t.Errorf("happy path: last updated: got %v, want %v", deployments[0].LastUpdated, lastUpdated)
	}

	// Situation 2: no containers in spec — image stays empty, no panic
	fakeClient = fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "my-deployment", Namespace: "default"},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	deployments, err = c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("no containers: got %v, want nil", err)
	}
	if deployments[0].Image != "" {
		t.Errorf("no containers: image: got %s, want empty", deployments[0].Image)
	}

	// Situation 3: replicas nil — desired replicas 0, no panic
	fakeClient = fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "my-deployment", Namespace: "default"},
			Spec: appsv1.DeploymentSpec{
				Replicas: nil,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Image: "my-image:latest"},
						},
					},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	deployments, err = c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("nil replicas: got %v, want nil", err)
	}
	if deployments[0].DesiredReplicas != 0 {
		t.Errorf("nil replicas: desired replicas: got %d, want 0", deployments[0].DesiredReplicas)
	}

	// Situation 4: no deployments in namespace — empty slice, no error
	fakeClient = fake.NewSimpleClientset()
	c = k8.NewClientWithClientset(fakeClient)
	deployments, err = c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("empty namespace: got %v, want nil", err)
	}
	if len(deployments) != 0 {
		t.Errorf("empty namespace: got %d deployments, want 0", len(deployments))
	}

	// Situation 5: condition timestamp drives LastUpdated — latest condition wins over creation time
	creationTime := time.Now().Add(-time.Hour).Round(time.Second)
	conditionTime := time.Now().Round(time.Second)
	fakeClient = fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "my-deployment",
				Namespace:         "default",
				CreationTimestamp: metav1.Time{Time: creationTime},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "my-image:latest"}},
					},
				},
			},
			Status: appsv1.DeploymentStatus{
				Conditions: []appsv1.DeploymentCondition{
					{
						Type:           appsv1.DeploymentAvailable,
						LastUpdateTime: metav1.Time{Time: conditionTime},
					},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	deployments, err = c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("condition timestamp: got %v, want nil", err)
	}
	if !deployments[0].LastUpdated.Equal(conditionTime) {
		t.Errorf("condition timestamp: got %v, want %v — condition time should win", deployments[0].LastUpdated, conditionTime)
	}

	// Situation 6: no conditions — falls back to creation timestamp
	fakeClient = fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "my-deployment",
				Namespace:         "default",
				CreationTimestamp: metav1.Time{Time: creationTime},
			},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Image: "my-image:latest"}},
					},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	deployments, err = c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("no conditions: got %v, want nil", err)
	}
	if !deployments[0].LastUpdated.Equal(creationTime) {
		t.Errorf("no conditions: got %v, want %v — creation time should be used", deployments[0].LastUpdated, creationTime)
	}

	// Situation 7: multiple containers — only first container image used
	fakeClient = fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "my-deployment", Namespace: "default"},
			Spec: appsv1.DeploymentSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Image: "first-image:latest"},
							{Image: "second-image:latest"},
						},
					},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	deployments, err = c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("multiple containers: got %v, want nil", err)
	}
	if deployments[0].Image != "first-image:latest" {
		t.Errorf("multiple containers: image: got %s, want first-image:latest", deployments[0].Image)
	}

	// Situation 8: multiple deployments — all returned
	replicas = int32(1)
	fakeClient = fake.NewSimpleClientset(
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "deploy-1", Namespace: "default"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "img-1"}}}},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "deploy-2", Namespace: "default"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "img-2"}}}},
			},
		},
		&appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: "deploy-3", Namespace: "default"},
			Spec: appsv1.DeploymentSpec{
				Replicas: &replicas,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "img-3"}}}},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	deployments, err = c.GetRecentDeployments(context.Background(), "default")
	if err != nil {
		t.Fatalf("multiple deployments: got %v, want nil", err)
	}
	if len(deployments) != 3 {
		t.Errorf("multiple deployments: got %d, want 3", len(deployments))
	}
}

func TestListAllNodes(t *testing.T) {

	// Situation 1: ready node — status Ready, all fields mapped correctly
	fakeClient := fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
				Labels: map[string]string{
					"node-role.kubernetes.io/control-plane": "",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
				NodeInfo: corev1.NodeSystemInfo{
					KubeletVersion: "v1.28.0",
				},
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
		},
	)
	c := k8.NewClientWithClientset(fakeClient)
	nodes, err := c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("ready node: got %v, want nil", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("ready node: got %d nodes, want 1", len(nodes))
	}
	if nodes[0].Name != "node-1" {
		t.Errorf("ready node: name: got %s, want node-1", nodes[0].Name)
	}
	if nodes[0].Status != "Ready" {
		t.Errorf("ready node: status: got %s, want Ready", nodes[0].Status)
	}
	if nodes[0].KubeletVersion != "v1.28.0" {
		t.Errorf("ready node: kubelet version: got %s, want v1.28.0", nodes[0].KubeletVersion)
	}
	if nodes[0].CPUCapacity == "" {
		t.Errorf("ready node: cpu capacity: got empty, want value")
	}
	if nodes[0].MemoryCapacity == "" {
		t.Errorf("ready node: memory capacity: got empty, want value")
	}
	if len(nodes[0].Roles) != 1 {
		t.Errorf("ready node: roles: got %d, want 1", len(nodes[0].Roles))
	}

	// Situation 2: NotReady node — status NotReady
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	nodes, err = c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("not ready: got %v, want nil", err)
	}
	if nodes[0].Status != "NotReady" {
		t.Errorf("not ready: status: got %s, want NotReady", nodes[0].Status)
	}

	// Situation 3: node with multiple role labels — all roles extracted
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
				Labels: map[string]string{
					"node-role.kubernetes.io/control-plane": "",
					"node-role.kubernetes.io/master":        "",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	nodes, err = c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("multiple roles: got %v, want nil", err)
	}
	if len(nodes[0].Roles) != 2 {
		t.Errorf("multiple roles: got %d roles, want 2", len(nodes[0].Roles))
	}

	// Situation 4: node with no role labels — empty roles slice
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-1",
				Labels: map[string]string{},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	nodes, err = c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("no roles: got %v, want nil", err)
	}
	if len(nodes[0].Roles) != 0 {
		t.Errorf("no roles: got %d roles, want 0", len(nodes[0].Roles))
	}

	// Situation 5: empty cluster — empty slice, no error
	fakeClient = fake.NewSimpleClientset()
	c = k8.NewClientWithClientset(fakeClient)
	nodes, err = c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("empty cluster: got %v, want nil", err)
	}
	if len(nodes) != 0 {
		t.Errorf("empty cluster: got %d nodes, want 0", len(nodes))
	}

	// Situation 6: multiple nodes — all returned
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-2"},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	nodes, err = c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("multiple nodes: got %v, want nil", err)
	}
	if len(nodes) != 2 {
		t.Errorf("multiple nodes: got %d nodes, want 2", len(nodes))
	}

	// Situation 7: node with no conditions — status defaults to Ready
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status:     corev1.NodeStatus{},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	nodes, err = c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("no conditions: got %v, want nil", err)
	}
	if nodes[0].Status != "Ready" {
		t.Errorf("no conditions: status: got %s, want Ready", nodes[0].Status)
	}

	// Situation 8: non-role labels are ignored — only node-role.kubernetes.io/ prefix extracted
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
				Labels: map[string]string{
					"node-role.kubernetes.io/worker": "",
					"kubernetes.io/hostname":         "node-1",
					"beta.kubernetes.io/os":          "linux",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	nodes, err = c.ListAllNodes(context.Background())
	if err != nil {
		t.Fatalf("non-role labels: got %v, want nil", err)
	}
	if len(nodes[0].Roles) != 1 {
		t.Errorf("non-role labels: got %d roles, want 1 — only node-role prefix should be extracted", len(nodes[0].Roles))
	}
	if nodes[0].Roles[0] != "worker" {
		t.Errorf("non-role labels: role: got %s, want worker", nodes[0].Roles[0])
	}
}

func TestGetNode(t *testing.T) {

	// Situation 1: found — all fields correct
	fakeClient := fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
				Labels: map[string]string{
					"node-role.kubernetes.io/control-plane": "",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
				NodeInfo: corev1.NodeSystemInfo{
					KubeletVersion: "v1.28.0",
				},
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("4"),
					corev1.ResourceMemory: resource.MustParse("8Gi"),
				},
			},
		},
	)
	c := k8.NewClientWithClientset(fakeClient)
	node, err := c.GetNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("found: got %v, want nil", err)
	}
	if node == nil {
		t.Fatalf("found: got nil, want node")
	}
	if node.Name != "node-1" {
		t.Errorf("found: name: got %s, want node-1", node.Name)
	}
	if node.Status != "Ready" {
		t.Errorf("found: status: got %s, want Ready", node.Status)
	}
	if node.KubeletVersion != "v1.28.0" {
		t.Errorf("found: kubelet version: got %s, want v1.28.0", node.KubeletVersion)
	}
	if node.CPUCapacity == "" {
		t.Errorf("found: cpu capacity: got empty, want value")
	}
	if node.MemoryCapacity == "" {
		t.Errorf("found: memory capacity: got empty, want value")
	}
	if len(node.Roles) != 1 {
		t.Errorf("found: roles: got %d, want 1", len(node.Roles))
	}
	if node.Roles[0] != "control-plane" {
		t.Errorf("found: role: got %s, want control-plane", node.Roles[0])
	}

	// Situation 2: NotReady node — status NotReady
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionFalse},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	node, err = c.GetNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("not ready: got %v, want nil", err)
	}
	if node.Status != "NotReady" {
		t.Errorf("not ready: status: got %s, want NotReady", node.Status)
	}

	// Situation 3: node with multiple roles — all extracted
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "node-1",
				Labels: map[string]string{
					"node-role.kubernetes.io/control-plane": "",
					"node-role.kubernetes.io/master":        "",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	node, err = c.GetNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("multiple roles: got %v, want nil", err)
	}
	if len(node.Roles) != 2 {
		t.Errorf("multiple roles: got %d roles, want 2", len(node.Roles))
	}

	// Situation 4: node with no role labels — empty roles slice
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "node-1",
				Labels: map[string]string{},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
				},
			},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	node, err = c.GetNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("no roles: got %v, want nil", err)
	}
	if len(node.Roles) != 0 {
		t.Errorf("no roles: got %d roles, want 0", len(node.Roles))
	}

	// Situation 5: API error — node not found, returns error and nil
	fakeClient = fake.NewSimpleClientset()
	c = k8.NewClientWithClientset(fakeClient)
	node, err = c.GetNode(context.Background(), "missing-node")
	if err == nil {
		t.Errorf("not found: got nil, want error")
	}
	if node != nil {
		t.Errorf("not found: got %v, want nil", node)
	}

	// Situation 6: node with no conditions — status defaults to Ready
	fakeClient = fake.NewSimpleClientset(
		&corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "node-1"},
			Status:     corev1.NodeStatus{},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	node, err = c.GetNode(context.Background(), "node-1")
	if err != nil {
		t.Fatalf("no conditions: got %v, want nil", err)
	}
	if node.Status != "Ready" {
		t.Errorf("no conditions: status: got %s, want Ready", node.Status)
	}
}

func TestListAllEvents(t *testing.T) {

	// Situation 1: empty namespace string — returns events from all namespaces
	lastTime := time.Now().Round(time.Second)
	fakeClient := fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta:    metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Reason:        "OOMKilling",
			Message:       "Memory limit exceeded",
			Type:          "Warning",
			Count:         3,
			LastTimestamp: metav1.Time{Time: lastTime},
		},
		&corev1.Event{
			ObjectMeta:    metav1.ObjectMeta{Name: "event-2", Namespace: "kube-system"},
			Reason:        "Pulled",
			Message:       "Successfully pulled image",
			Type:          "Normal",
			Count:         1,
			LastTimestamp: metav1.Time{Time: lastTime},
		},
	)
	c := k8.NewClientWithClientset(fakeClient)
	events, err := c.ListAllEvents(context.Background(), "")
	if err != nil {
		t.Fatalf("all namespaces: got %v, want nil", err)
	}
	if len(events) != 2 {
		t.Errorf("all namespaces: got %d events, want 2", len(events))
	}

	// Situation 2: specific namespace — only returns events from that namespace
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Reason:     "OOMKilling",
		},
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "event-2", Namespace: "kube-system"},
			Reason:     "Pulled",
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.ListAllEvents(context.Background(), "default")
	if err != nil {
		t.Fatalf("specific namespace: got %v, want nil", err)
	}
	if len(events) != 1 {
		t.Errorf("specific namespace: got %d events, want 1", len(events))
	}
	if events[0].Reason != "OOMKilling" {
		t.Errorf("specific namespace: reason: got %s, want OOMKilling", events[0].Reason)
	}

	// Situation 3: no events — empty slice, no error
	fakeClient = fake.NewSimpleClientset()
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.ListAllEvents(context.Background(), "")
	if err != nil {
		t.Fatalf("no events: got %v, want nil", err)
	}
	if len(events) != 0 {
		t.Errorf("no events: got %d events, want 0", len(events))
	}

	// Situation 4: all fields mapped correctly — reason, message, type, count, last time
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta:    metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Reason:        "OOMKilling",
			Message:       "Memory limit exceeded",
			Type:          "Warning",
			Count:         5,
			LastTimestamp: metav1.Time{Time: lastTime},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.ListAllEvents(context.Background(), "default")
	if err != nil {
		t.Fatalf("field mapping: got %v, want nil", err)
	}
	if events[0].Reason != "OOMKilling" {
		t.Errorf("field mapping: reason: got %s, want OOMKilling", events[0].Reason)
	}
	if events[0].Message != "Memory limit exceeded" {
		t.Errorf("field mapping: message: got %s, want Memory limit exceeded", events[0].Message)
	}
	if events[0].Type != "Warning" {
		t.Errorf("field mapping: type: got %s, want Warning", events[0].Type)
	}
	if events[0].Count != 5 {
		t.Errorf("field mapping: count: got %d, want 5", events[0].Count)
	}
	if !events[0].LastTime.Equal(lastTime) {
		t.Errorf("field mapping: last time: got %v, want %v", events[0].LastTime, lastTime)
	}

	// Situation 5: multiple events all namespaces — all returned
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			Reason:     "OOMKilling",
		},
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "event-2", Namespace: "default"},
			Reason:     "Pulled",
		},
		&corev1.Event{
			ObjectMeta: metav1.ObjectMeta{Name: "event-3", Namespace: "kube-system"},
			Reason:     "NodeReady",
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.ListAllEvents(context.Background(), "")
	if err != nil {
		t.Fatalf("multiple events: got %v, want nil", err)
	}
	if len(events) != 3 {
		t.Errorf("multiple events: got %d events, want 3", len(events))
	}

	// Situation 6: event with zero last time — maps through, no panic
	fakeClient = fake.NewSimpleClientset(
		&corev1.Event{
			ObjectMeta:    metav1.ObjectMeta{Name: "event-1", Namespace: "default"},
			LastTimestamp: metav1.Time{},
		},
	)
	c = k8.NewClientWithClientset(fakeClient)
	events, err = c.ListAllEvents(context.Background(), "default")
	if err != nil {
		t.Fatalf("zero last time: got %v, want nil", err)
	}
	if !events[0].LastTime.IsZero() {
		t.Errorf("zero last time: got %v, want zero", events[0].LastTime)
	}
}

func TestGetPodLogs(t *testing.T) {

	// Situation 1: happy path — returns correct log content
	c := newTestClientWithLogs(t, "OOMKilled\nMemory limit exceeded\n", http.StatusOK)
	logs, err := c.GetPodLogs(context.Background(), "default", "pod-1", "my-container")
	if err != nil {
		t.Fatalf("happy path: got %v, want nil", err)
	}
	if logs != "OOMKilled\nMemory limit exceeded\n" {
		t.Errorf("happy path: got %q, want OOMKilled\\nMemory limit exceeded\\n", logs)
	}

	// Situation 2: empty logs — returns empty string, no error
	c = newTestClientWithLogs(t, "", http.StatusOK)
	logs, err = c.GetPodLogs(context.Background(), "default", "pod-1", "my-container")
	if err != nil {
		t.Fatalf("empty logs: got %v, want nil", err)
	}
	if logs != "" {
		t.Errorf("empty logs: got %q, want empty", logs)
	}

	// Situation 3: multiline logs — all lines returned correctly, no truncation
	multiline := "line1\nline2\nline3\nline4\nline5\n"
	c = newTestClientWithLogs(t, multiline, http.StatusOK)
	logs, err = c.GetPodLogs(context.Background(), "default", "pod-1", "my-container")
	if err != nil {
		t.Fatalf("multiline: got %v, want nil", err)
	}
	if logs != multiline {
		t.Errorf("multiline: got %q, want %q", logs, multiline)
	}

	// Situation 4: logs with special characters — newlines, unicode, no corruption
	special := "Error: connection refused\nStacktrace:\n\tat main.go:42\nUnicode: 日本語\n"
	c = newTestClientWithLogs(t, special, http.StatusOK)
	logs, err = c.GetPodLogs(context.Background(), "default", "pod-1", "my-container")
	if err != nil {
		t.Fatalf("special chars: got %v, want nil", err)
	}
	if logs != special {
		t.Errorf("special chars: got %q, want %q", logs, special)
	}

	// Situation 5: server returns 500 — returns error, empty string
	c = newTestClientWithLogs(t, "", http.StatusInternalServerError)
	logs, err = c.GetPodLogs(context.Background(), "default", "pod-1", "my-container")
	if err == nil {
		t.Errorf("500 error: got nil, want error")
	}
	if logs != "" {
		t.Errorf("500 error: got %q, want empty", logs)
	}

	// Situation 6: server returns 404 — pod not found, returns error
	c = newTestClientWithLogs(t, "", http.StatusNotFound)
	logs, err = c.GetPodLogs(context.Background(), "default", "pod-1", "my-container")
	if err == nil {
		t.Errorf("404 not found: got nil, want error")
	}
	if logs != "" {
		t.Errorf("404 not found: got %q, want empty", logs)
	}

	// Situation 7: context cancelled — returns error
	c = newTestClientWithLogs(t, "some logs", http.StatusOK)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	logs, err = c.GetPodLogs(ctx, "default", "pod-1", "my-container")
	if err == nil {
		t.Errorf("cancelled context: got nil, want error")
	}
	if logs != "" {
		t.Errorf("cancelled context: got %q, want empty", logs)
	}

	// Situation 8: large log output — no corruption, full content returned
	var sb strings.Builder
	for i := 0; i < 1000; i++ {
		sb.WriteString(fmt.Sprintf("log line %d: something happened here\n", i))
	}
	largeLogs := sb.String()
	c = newTestClientWithLogs(t, largeLogs, http.StatusOK)
	logs, err = c.GetPodLogs(context.Background(), "default", "pod-1", "my-container")
	if err != nil {
		t.Fatalf("large logs: got %v, want nil", err)
	}
	if logs != largeLogs {
		t.Errorf("large logs: content corrupted, lengths differ: got %d, want %d", len(logs), len(largeLogs))
	}

	// Situation 9: empty container name — no panic, request goes through
	c = newTestClientWithLogs(t, "some logs", http.StatusOK)
	logs, err = c.GetPodLogs(context.Background(), "default", "pod-1", "")
	if err != nil {
		t.Fatalf("empty container: got %v, want nil", err)
	}

	// Situation 10: empty pod name — no panic, server handles it
	c = newTestClientWithLogs(t, "", http.StatusNotFound)
	logs, err = c.GetPodLogs(context.Background(), "default", "", "my-container")
	if err == nil {
		t.Errorf("empty pod name: got nil, want error")
	}
	if logs != "" {
		t.Errorf("empty pod name: got %q, want empty", logs)
	}
}

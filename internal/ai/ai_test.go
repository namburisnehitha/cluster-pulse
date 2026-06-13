package ai_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/kafka"
	openai "github.com/sashabaranov/go-openai"
)

func makePodEvent() kafka.PodEvent {
	return kafka.PodEvent{
		Pod: k8.Pod{
			Name:         "pod-1",
			Namespace:    "default",
			Phase:        "Failed",
			RestartCount: 5,
			ExitCode:     137,
			Logs:         "OOMKilled",
			NodeName:     "node-1",
			MemoryLimit:  "512Mi",
			CPULimit:     "500m",
			Events: []k8.Event{
				{
					Type:     "Warning",
					Reason:   "OOMKilling",
					Message:  "Memory limit exceeded",
					Count:    3,
					LastTime: time.Now(),
				},
			},
			Deployments: []k8.Deployment{
				{
					Name:              "my-deployment",
					Image:             "my-image:latest",
					LastUpdated:       time.Now(),
					DesiredReplicas:   3,
					AvailableReplicas: 2,
				},
			},
		},
		Timestamp: time.Now(),
	}
}

func makeNode() *k8.Node {
	return &k8.Node{
		Name:           "node-1",
		Status:         "Ready",
		CPUCapacity:    "4",
		MemoryCapacity: "8Gi",
		KubeletVersion: "v1.28.0",
	}
}

func makeTrend() ai.ResourceTrend {
	return ai.ResourceTrend{
		AvgMemoryMi: 400,
		AvgCPUMilli: 300,
		Direction:   "increasing",
		SampleCount: 10,
	}
}

func makeAnalysisResponse() ai.Analysis {
	return ai.Analysis{
		RootCause:            "OOMKilled",
		Confidence:           "high",
		Severity:             "critical",
		Fix:                  "increase memory limit",
		KubectlCommand:       "kubectl describe pod pod-1",
		IfFixFails:           "check node memory",
		Summary:              "pod ran out of memory",
		SuggestedMemoryLimit: "1Gi",
		SuggestedCPULimit:    "500m",
		ExitCodeExplanation:  "137 = OOMKilled",
		RelevantLogLines:     []string{"OOMKilled"},
		TriggeringDeployment: "my-deployment",
		ResourceTrend:        "increasing",
		IsRecurring:          true,
		HistorySummary:       "recurring OOM",
		RelatedPods:          []string{"pod-2"},
	}
}

func makeTestServer(t *testing.T, response ai.Analysis, statusCode int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)

		if statusCode != http.StatusOK {
			return
		}

		body, _ := json.Marshal(response)
		resp := openai.ChatCompletionResponse{
			Choices: []openai.ChatCompletionChoice{
				{
					Message: openai.ChatCompletionMessage{
						Content: string(body),
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
}

func makeMalformedServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		resp := openai.ChatCompletionResponse{
			Choices: []openai.ChatCompletionChoice{
				{
					Message: openai.ChatCompletionMessage{
						Content: "not valid json {{{",
					},
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	}))
}

func TestBuildPrompt(t *testing.T) {

	// Situation 1: all pod fields present — name, namespace, phase, exit code,
	// restart count, node name, memory limit, cpu limit, logs
	event := makePodEvent()
	trend := makeTrend()
	node := makeNode()

	prompt := ai.BuildPrompt(event, trend, node)

	if !strings.Contains(prompt, "pod-1") {
		t.Errorf("pod fields: missing pod name")
	}
	if !strings.Contains(prompt, "default") {
		t.Errorf("pod fields: missing namespace")
	}
	if !strings.Contains(prompt, "Failed") {
		t.Errorf("pod fields: missing phase")
	}
	if !strings.Contains(prompt, "137") {
		t.Errorf("pod fields: missing exit code")
	}
	if !strings.Contains(prompt, "5") {
		t.Errorf("pod fields: missing restart count")
	}
	if !strings.Contains(prompt, "node-1") {
		t.Errorf("pod fields: missing node name")
	}
	if !strings.Contains(prompt, "512Mi") {
		t.Errorf("pod fields: missing memory limit")
	}
	if !strings.Contains(prompt, "500m") {
		t.Errorf("pod fields: missing cpu limit")
	}
	if !strings.Contains(prompt, "OOMKilled") {
		t.Errorf("pod fields: missing logs")
	}

	// Situation 2: trend fields present — avg memory, avg cpu, direction, sample count
	if !strings.Contains(prompt, "400") {
		t.Errorf("trend fields: missing avg memory")
	}
	if !strings.Contains(prompt, "300") {
		t.Errorf("trend fields: missing avg cpu")
	}
	if !strings.Contains(prompt, "increasing") {
		t.Errorf("trend fields: missing direction")
	}
	if !strings.Contains(prompt, "10") {
		t.Errorf("trend fields: missing sample count")
	}

	// Situation 3: node info present — status, cpu capacity, memory capacity, kubelet version
	if !strings.Contains(prompt, "Ready") {
		t.Errorf("node info: missing status")
	}
	if !strings.Contains(prompt, "8Gi") {
		t.Errorf("node info: missing memory capacity")
	}
	if !strings.Contains(prompt, "v1.28.0") {
		t.Errorf("node info: missing kubelet version")
	}

	// Situation 4: event fields present — type, reason, message
	if !strings.Contains(prompt, "OOMKilling") {
		t.Errorf("event fields: missing reason")
	}
	if !strings.Contains(prompt, "Memory limit exceeded") {
		t.Errorf("event fields: missing message")
	}
	if !strings.Contains(prompt, "Warning") {
		t.Errorf("event fields: missing type")
	}

	// Situation 5: deployment fields present — name, image
	if !strings.Contains(prompt, "my-deployment") {
		t.Errorf("deployment fields: missing name")
	}
	if !strings.Contains(prompt, "my-image:latest") {
		t.Errorf("deployment fields: missing image")
	}

	// Situation 6: nil node — should show "unknown" not panic
	prompt = ai.BuildPrompt(event, trend, nil)
	if !strings.Contains(prompt, "unknown") {
		t.Errorf("nil node: missing unknown placeholder")
	}

	// Situation 7: empty logs — prompt should still build correctly, pod name still present
	event = makePodEvent()
	event.Pod.Logs = ""
	prompt = ai.BuildPrompt(event, trend, node)
	if !strings.Contains(prompt, "pod-1") {
		t.Errorf("empty logs: prompt broken, missing pod name")
	}

	// Situation 8: zero trend values — zeros should appear in prompt, not be omitted
	event = makePodEvent()
	zeroTrend := ai.ResourceTrend{
		AvgMemoryMi: 0,
		AvgCPUMilli: 0,
		Direction:   "unknown",
		SampleCount: 0,
	}
	prompt = ai.BuildPrompt(event, zeroTrend, node)
	if !strings.Contains(prompt, "unknown") {
		t.Errorf("zero trend: missing unknown direction")
	}
	if !strings.Contains(prompt, "0") {
		t.Errorf("zero trend: missing zero values")
	}

	// Situation 9: no events — prompt should still build correctly, no panic on empty slice
	event = makePodEvent()
	event.Pod.Events = []k8.Event{}
	prompt = ai.BuildPrompt(event, trend, node)
	if !strings.Contains(prompt, "pod-1") {
		t.Errorf("no events: prompt broken, missing pod name")
	}

	// Situation 10: no deployments — prompt should still build correctly, no panic on empty slice
	event = makePodEvent()
	event.Pod.Deployments = []k8.Deployment{}
	prompt = ai.BuildPrompt(event, trend, node)
	if !strings.Contains(prompt, "pod-1") {
		t.Errorf("no deployments: prompt broken, missing pod name")
	}

	// Situation 11: multiple events — all events should appear in prompt
	event = makePodEvent()
	event.Pod.Events = []k8.Event{
		{Type: "Warning", Reason: "OOMKilling", Message: "Memory limit exceeded", Count: 3, LastTime: time.Now()},
		{Type: "Normal", Reason: "Pulled", Message: "Successfully pulled image", Count: 1, LastTime: time.Now()},
	}
	prompt = ai.BuildPrompt(event, trend, node)
	if !strings.Contains(prompt, "OOMKilling") {
		t.Errorf("multiple events: missing first event reason")
	}
	if !strings.Contains(prompt, "Pulled") {
		t.Errorf("multiple events: missing second event reason")
	}
	if !strings.Contains(prompt, "Successfully pulled image") {
		t.Errorf("multiple events: missing second event message")
	}

	// Situation 12: multiple deployments — all deployments should appear in prompt
	event = makePodEvent()
	event.Pod.Deployments = []k8.Deployment{
		{Name: "deploy-1", Image: "image-1:v1", LastUpdated: time.Now(), DesiredReplicas: 3, AvailableReplicas: 3},
		{Name: "deploy-2", Image: "image-2:v2", LastUpdated: time.Now(), DesiredReplicas: 2, AvailableReplicas: 1},
	}
	prompt = ai.BuildPrompt(event, trend, node)
	if !strings.Contains(prompt, "deploy-1") {
		t.Errorf("multiple deployments: missing first deployment name")
	}
	if !strings.Contains(prompt, "deploy-2") {
		t.Errorf("multiple deployments: missing second deployment name")
	}
	if !strings.Contains(prompt, "image-2:v2") {
		t.Errorf("multiple deployments: missing second deployment image")
	}
}

func TestAnalyze(t *testing.T) {

	// Situation 1: happy path — all fields correctly unmarshaled, AnalyzedAt set
	server := makeTestServer(t, makeAnalysisResponse(), http.StatusOK)
	defer server.Close()

	analyzer := ai.NewOpenAIAnalyzer("test-key", server.URL, "gpt-4o-mini")
	result, err := analyzer.Analyze(context.Background(), makePodEvent(), makeTrend(), makeNode())
	if err != nil {
		t.Fatalf("happy path: got %v, want nil", err)
	}
	if result.RootCause != "OOMKilled" {
		t.Errorf("happy path: root cause: got %s, want OOMKilled", result.RootCause)
	}
	if result.Confidence != "high" {
		t.Errorf("happy path: confidence: got %s, want high", result.Confidence)
	}
	if result.Severity != "critical" {
		t.Errorf("happy path: severity: got %s, want critical", result.Severity)
	}
	if result.Fix != "increase memory limit" {
		t.Errorf("happy path: fix: got %s, want increase memory limit", result.Fix)
	}
	if result.KubectlCommand != "kubectl describe pod pod-1" {
		t.Errorf("happy path: kubectl command: got %s, want kubectl describe pod pod-1", result.KubectlCommand)
	}
	if result.SuggestedMemoryLimit != "1Gi" {
		t.Errorf("happy path: memory limit: got %s, want 1Gi", result.SuggestedMemoryLimit)
	}
	if result.SuggestedCPULimit != "500m" {
		t.Errorf("happy path: cpu limit: got %s, want 500m", result.SuggestedCPULimit)
	}
	if result.ExitCodeExplanation != "137 = OOMKilled" {
		t.Errorf("happy path: exit code explanation: got %s, want 137 = OOMKilled", result.ExitCodeExplanation)
	}
	if !result.IsRecurring {
		t.Errorf("happy path: is_recurring: got false, want true")
	}
	if result.HistorySummary != "recurring OOM" {
		t.Errorf("happy path: history summary: got %s, want recurring OOM", result.HistorySummary)
	}
	if result.TriggeringDeployment != "my-deployment" {
		t.Errorf("happy path: triggering deployment: got %s, want my-deployment", result.TriggeringDeployment)
	}
	if len(result.RelevantLogLines) != 1 {
		t.Errorf("happy path: log lines: got %d, want 1", len(result.RelevantLogLines))
	}
	if result.RelevantLogLines[0] != "OOMKilled" {
		t.Errorf("happy path: log line value: got %s, want OOMKilled", result.RelevantLogLines[0])
	}
	if len(result.RelatedPods) != 1 {
		t.Errorf("happy path: related pods: got %d, want 1", len(result.RelatedPods))
	}
	if result.RelatedPods[0] != "pod-2" {
		t.Errorf("happy path: related pod value: got %s, want pod-2", result.RelatedPods[0])
	}
	if result.AnalyzedAt.IsZero() {
		t.Errorf("happy path: analyzed_at: got zero, want time set by Analyze")
	}

	// Situation 2: API returns 500 — should return error, empty analysis
	server = makeTestServer(t, ai.Analysis{}, http.StatusInternalServerError)
	defer server.Close()

	analyzer = ai.NewOpenAIAnalyzer("test-key", server.URL, "gpt-4o-mini")
	result, err = analyzer.Analyze(context.Background(), makePodEvent(), makeTrend(), makeNode())
	if err == nil {
		t.Errorf("500 error: got nil, want error")
	}
	if result.RootCause != "" {
		t.Errorf("500 error: got root cause %s, want empty", result.RootCause)
	}

	// Situation 3: API returns 401 unauthorized — wrong API key, should return error
	server = makeTestServer(t, ai.Analysis{}, http.StatusUnauthorized)
	defer server.Close()

	analyzer = ai.NewOpenAIAnalyzer("bad-key", server.URL, "gpt-4o-mini")
	result, err = analyzer.Analyze(context.Background(), makePodEvent(), makeTrend(), makeNode())
	if err == nil {
		t.Errorf("401 unauthorized: got nil, want error")
	}
	if result.RootCause != "" {
		t.Errorf("401 unauthorized: got root cause %s, want empty", result.RootCause)
	}

	// Situation 4: API returns 429 rate limited — should return error
	server = makeTestServer(t, ai.Analysis{}, http.StatusTooManyRequests)
	defer server.Close()

	analyzer = ai.NewOpenAIAnalyzer("test-key", server.URL, "gpt-4o-mini")
	result, err = analyzer.Analyze(context.Background(), makePodEvent(), makeTrend(), makeNode())
	if err == nil {
		t.Errorf("429 rate limited: got nil, want error")
	}
	if result.RootCause != "" {
		t.Errorf("429 rate limited: got root cause %s, want empty", result.RootCause)
	}

	// Situation 5: API returns malformed JSON in choices content — should return error
	server = makeMalformedServer(t)
	defer server.Close()

	analyzer = ai.NewOpenAIAnalyzer("test-key", server.URL, "gpt-4o-mini")
	result, err = analyzer.Analyze(context.Background(), makePodEvent(), makeTrend(), makeNode())
	if err == nil {
		t.Errorf("malformed json: got nil, want error")
	}
	if result.RootCause != "" {
		t.Errorf("malformed json: got root cause %s, want empty", result.RootCause)
	}

	// Situation 6: nil node — should not panic, prompt builds with unknown node info
	server = makeTestServer(t, makeAnalysisResponse(), http.StatusOK)
	defer server.Close()

	analyzer = ai.NewOpenAIAnalyzer("test-key", server.URL, "gpt-4o-mini")
	result, err = analyzer.Analyze(context.Background(), makePodEvent(), makeTrend(), nil)
	if err != nil {
		t.Errorf("nil node: got %v, want nil", err)
	}
	if result.RootCause != "OOMKilled" {
		t.Errorf("nil node: root cause: got %s, want OOMKilled", result.RootCause)
	}

	// Situation 7: context cancelled — should return error
	server = makeTestServer(t, makeAnalysisResponse(), http.StatusOK)
	defer server.Close()

	analyzer = ai.NewOpenAIAnalyzer("test-key", server.URL, "gpt-4o-mini")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = analyzer.Analyze(ctx, makePodEvent(), makeTrend(), makeNode())
	if err == nil {
		t.Errorf("cancelled context: got nil, want error")
	}
	if result.RootCause != "" {
		t.Errorf("cancelled context: got root cause %s, want empty", result.RootCause)
	}

	// Situation 8: empty pod name — prompt builds, no panic, analyze completes
	server = makeTestServer(t, makeAnalysisResponse(), http.StatusOK)
	defer server.Close()

	analyzer = ai.NewOpenAIAnalyzer("test-key", server.URL, "gpt-4o-mini")
	emptyPodEvent := makePodEvent()
	emptyPodEvent.Pod.Name = ""
	result, err = analyzer.Analyze(context.Background(), emptyPodEvent, makeTrend(), makeNode())
	if err != nil {
		t.Errorf("empty pod name: got %v, want nil", err)
	}
	if result.AnalyzedAt.IsZero() {
		t.Errorf("empty pod name: analyzed_at: got zero, want time set")
	}
}

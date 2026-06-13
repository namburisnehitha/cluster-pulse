package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/kafka"
	"github.com/sashabaranov/go-openai"
)

type OpenAIAnalyzer struct {
	client *openai.Client
	model  string
}

func NewOpenAIAnalyzer(apiKey, baseURL, model string) *OpenAIAnalyzer {
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = baseURL
	client := openai.NewClientWithConfig(config)
	return &OpenAIAnalyzer{
		client: client,
		model:  model,
	}
}

func (oa *OpenAIAnalyzer) Analyze(ctx context.Context, event kafka.PodEvent, trend ResourceTrend, node *k8.Node) (Analysis, error) {

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	prompt := BuildPrompt(event, trend, node)

	resp, err := oa.client.CreateChatCompletion(ctx, openai.ChatCompletionRequest{
		Model: oa.model,
		Messages: []openai.ChatCompletionMessage{
			{
				Role:    openai.ChatMessageRoleUser,
				Content: prompt,
			},
		},
	})

	if err != nil {
		return Analysis{}, err
	}

	if len(resp.Choices) == 0 {
		return Analysis{}, fmt.Errorf("no choices returned from model")
	}

	ans := resp.Choices[0].Message.Content

	ans = strings.TrimSpace(ans)
	ans = strings.TrimPrefix(ans, "```json")
	ans = strings.TrimPrefix(ans, "```")
	ans = strings.TrimSuffix(ans, "```")
	ans = strings.TrimSpace(ans)

	var analysis Analysis
	if err := json.Unmarshal([]byte(ans), &analysis); err != nil {
		return Analysis{}, fmt.Errorf("failed to parse model response: %w (raw: %s)", err, ans)
	}

	if analysis.RootCause == "" {
		return Analysis{}, fmt.Errorf("model returned empty root_cause")
	}

	analysis.AnalyzedAt = time.Now()

	return analysis, nil

}

func BuildPrompt(event kafka.PodEvent, trend ResourceTrend, node *k8.Node) string {

	logs := event.Pod.Logs
	const maxLogChars = 4000
	if len(logs) > maxLogChars {
		logs = logs[len(logs)-maxLogChars:]
	}

	nodeInfo := "unknown"
	if node != nil {
		nodeInfo = fmt.Sprintf("status: %s, CPU capacity: %s, memory capacity: %s, kubelet: %s",
			node.Status, node.CPUCapacity, node.MemoryCapacity, node.KubeletVersion)
	}
	return fmt.Sprintf(`You are a Kubernetes SRE expert. Analyze this pod failure step by step.

Pod: %s
Namespace: %s
Phase: %s
Exit Code: %d
Restart Count: %d
Node: %s
Memory Limit: %s
CPU Limit: %s

Logs:
%s

Recent Events:
%s

Recent Deployments:
%s

Resource trend (last %d readings): avg memory %dMi, avg cpu %dm, direction: %s

Node Info: %s

Respond ONLY in this JSON format, no other text:
{
  "root_cause": "",
  "confidence": "high|medium|low",
  "severity": "critical|warning|info",
  "fix": "",
  "kubectl_command": "",
  "if_fix_fails": "",
  "summary": "",
  "suggested_memory_limit": "",
  "suggested_cpu_limit": "",
  "exit_code_explanation": "",
  "relevant_log_lines": [],
  "triggering_deployment": "",
  "resource_trend": "",
  "is_recurring": false,
  "history_summary": "",
  "related_pods": []
}`,
		event.Pod.Name,
		event.Pod.Namespace,
		event.Pod.Phase,
		event.Pod.ExitCode,
		event.Pod.RestartCount,
		event.Pod.NodeName,
		event.Pod.MemoryLimit,
		event.Pod.CPULimit,
		logs,
		formatEvents(event.Pod.Events),
		formatDeployments(event.Pod.Deployments),
		trend.SampleCount,
		trend.AvgMemoryMi,
		trend.AvgCPUMilli,
		trend.Direction,
		nodeInfo,
	)
}

func formatEvents(events []k8.Event) string {
	result := ""
	for _, e := range events {
		result += fmt.Sprintf("- %s: %s - %s (count: %d, last: %s)\n",
			e.Type,
			e.Reason,
			e.Message,
			e.Count,
			e.LastTime.Format("2006-01-02 15:04:05"),
		)
	}
	return result
}

func formatDeployments(deployments []k8.Deployment) string {
	result := ""
	for _, d := range deployments {
		result += fmt.Sprintf("%s, %s, last updated: %s, desired: %d, available: %d\n",
			d.Name,
			d.Image,
			d.LastUpdated.Format("2006-01-02 15:04:05"),
			d.DesiredReplicas,
			d.AvailableReplicas,
		)
	}
	return result
}

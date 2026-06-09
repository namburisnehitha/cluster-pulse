package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
	"github.com/namburisnehitha/cluster-pulse/internal/kafka"
	"github.com/sashabaranov/go-openai"
)

type OpenAIAnalyzer struct {
	client *openai.Client
	model  string
}

func NewOpenAIAnalyzer(apiKey, baseURL string) *OpenAIAnalyzer {
	config := openai.DefaultConfig(apiKey)
	config.BaseURL = baseURL
	client := openai.NewClientWithConfig(config)
	return &OpenAIAnalyzer{
		client: client,
		model:  "llama-3.1-8b-instant",
	}
}

func (oa *OpenAIAnalyzer) Analyze(ctx context.Context, event kafka.PodEvent) (Analysis, error) {

	prompt := buildPrompt(event)

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

	ans := resp.Choices[0].Message.Content

	var analysis Analysis
	err = json.Unmarshal([]byte(ans), &analysis)

	if err != nil {
		return Analysis{}, err
	}

	analysis.AnalyzedAt = time.Now()

	return analysis, nil

}

func buildPrompt(event kafka.PodEvent) string {
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
  "relevant_log_lines": "",
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
		event.Pod.Logs,
		formatEvents(event.Pod.Events),
		formatDeployments(event.Pod.Deployments),
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

func formatDeployments(depolyments []k8.Deployment) string {
	result := ""
	for _, d := range depolyments {
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

package ai

import (
	"time"
)

type Analysis struct {
	PodName              string    `json:"pod_name"`
	Namespace            string    `json:"namespace"`
	RootCause            string    `json:"root_cause"`
	Confidence           string    `json:"confidence"`
	Severity             string    `json:"severity"`
	Fix                  string    `json:"fix"`
	KubectlCommand       string    `json:"kubectl_command"`
	IfFixFails           string    `json:"if_fix_fails"`
	Summary              string    `json:"summary"`
	SuggestedMemoryLimit string    `json:"suggested_memory_limit"`
	SuggestedCPULimit    string    `json:"suggested_cpu_limit"`
	ExitCodeExplanation  string    `json:"exit_code_explanation"`
	RelevantLogLines     []string  `json:"relevant_log_lines"`
	RelatedPods          []string  `json:"related_pods"`
	TriggeringDeployment string    `json:"triggering_deployment"`
	HistorySummary       string    `json:"history_summary"`
	ResourceTrend        string    `json:"resource_trend"`
	IsRecurring          bool      `json:"is_recurring"`
	FailureTime          time.Time `json:"failure_time"`
	AnalyzedAt           time.Time `json:"analyzed_at"`
}

type ResourceTrend struct {
	AvgMemoryMi int
	AvgCPUMilli int
	Direction   string
	SampleCount int
}

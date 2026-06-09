package ai

import "time"

type Analysis struct {
	PodName              string
	Namespace            string
	RootCause            string
	Confidence           string
	Severity             string
	Fix                  string
	KubectlCommand       string
	IfFixFails           string
	Summary              string
	SuggestedMemoryLimit string
	SuggestedCPULimit    string
	ExitCodeExplanation  string
	RelevantLogLines     string
	RelatedPods          []string
	TriggeringDeployment string
	HistorySummary       string
	ResourceTrend        string
	IsRecurring          bool
	FailureTime          time.Time
	AnalyzedAt           time.Time
}

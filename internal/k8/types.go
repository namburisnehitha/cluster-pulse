package k8

import "time"

type Pod struct {
	Name            string
	Namespace       string
	Phase           string
	Conditions      []string
	RestartCount    int
	ExitCode        int
	Logs            string
	StartTime       time.Time
	ContainerName   string
	FailureTime     time.Time
	MemoryLimit     string
	CPULimit        string
	MemoryRequest   string
	CPURequest      string
	LastRestartTime time.Time
	NodeName        string
	WatchReceivedAt time.Time
}

const (
	EventAdded    = "ADDED"
	EventModified = "MODIFIED"
	EventDeleted  = "DELETED"
)

type Event struct {
	Reason   string
	Message  string
	Type     string
	Count    int
	LastTime time.Time
}

type Deployment struct {
	Name              string
	Image             string
	LastUpdated       time.Time
	DesiredReplicas   int
	AvailableReplicas int
}

type PodResult struct {
	Pod Pod
	Err error
}

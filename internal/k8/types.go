package k8

import "time"

type Pod struct {
	Name             string
	Namespace        string
	Phase            string
	Conditions       []string
	RestartCount     int
	ExitCode         int
	Logs             string
	StartTime        time.Time
	ContainerName    string
	FailureTime      time.Time
	MemoryLimit      string
	CPULimit         string
	MemoryRequest    string
	CPURequest       string
	LastRestartTime  time.Time
	NodeName         string
	WatchReceivedAt  time.Time
	Events           []Event
	Deployments      []Deployment
	LogsError        string
	EventsError      string
	DeploymentsError string
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

type Node struct {
	Name           string
	Status         string
	Roles          []string
	Age            time.Time
	KubeletVersion string
	CPUCapacity    string
	MemoryCapacity string
}

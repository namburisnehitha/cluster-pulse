package k8_test

import (
	"testing"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
)

func TestIsUnhealthy(t *testing.T) {
	tests := []struct {
		name     string
		pod      k8.Pod
		expected bool
	}{
		{
			name:     "healthy pod",
			pod:      k8.Pod{Phase: "Running", RestartCount: 0, ExitCode: 0},
			expected: false,
		},
		{
			name:     "failed phase",
			pod:      k8.Pod{Phase: "Failed"},
			expected: true,
		},
		{
			name:     "restart count at boundary",
			pod:      k8.Pod{Phase: "Running", RestartCount: 5},
			expected: true,
		},
		{
			name:     "restart count below boundary",
			pod:      k8.Pod{Phase: "Running", RestartCount: 4},
			expected: false,
		},
		{
			name:     "restart count above boundary",
			pod:      k8.Pod{Phase: "Running", RestartCount: 6},
			expected: true,
		},
		{
			name:     "non-zero exit code",
			pod:      k8.Pod{Phase: "Running", ExitCode: 1},
			expected: true,
		},
		{
			name:     "OOMKilled exit code 137",
			pod:      k8.Pod{Phase: "Running", ExitCode: 137},
			expected: true,
		},
		{
			name:     "succeeded phase",
			pod:      k8.Pod{Phase: "Succeeded"},
			expected: false,
		},
		{
			name:     "pending phase",
			pod:      k8.Pod{Phase: "Pending"},
			expected: false,
		},
		{
			name:     "empty phase",
			pod:      k8.Pod{Phase: ""},
			expected: false,
		},
		{
			name:     "all unhealthy conditions",
			pod:      k8.Pod{Phase: "Failed", RestartCount: 10, ExitCode: 1},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if k8.IsUnhealthy(tt.pod) != tt.expected {
				t.Errorf("IsUnhealthy(%+v) = %v, expected %v", tt.pod, !tt.expected, tt.expected)
			}
		})
	}
}

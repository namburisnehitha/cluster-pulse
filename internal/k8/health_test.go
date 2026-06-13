package k8_test

import (
	"testing"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
)

func TestIsUnhealthy(t *testing.T) {

	// Situation 1: healthy running pod — all conditions normal, should return false
	pod := k8.Pod{Phase: "Running", RestartCount: 0, ExitCode: 0}
	if k8.IsUnhealthy(pod) {
		t.Errorf("healthy pod: got true, want false")
	}

	// Situation 2: Failed phase — regardless of other fields, should return true
	pod = k8.Pod{Phase: "Failed", RestartCount: 0, ExitCode: 0}
	if !k8.IsUnhealthy(pod) {
		t.Errorf("failed phase: got false, want true")
	}

	// Situation 3: restart count exactly at boundary (>= 5) — should return true
	pod = k8.Pod{Phase: "Running", RestartCount: 5, ExitCode: 0}
	if !k8.IsUnhealthy(pod) {
		t.Errorf("restart count 5: got false, want true")
	}

	// Situation 4: restart count just below boundary (4) — should return false
	pod = k8.Pod{Phase: "Running", RestartCount: 4, ExitCode: 0}
	if k8.IsUnhealthy(pod) {
		t.Errorf("restart count 4: got true, want false")
	}

	// Situation 5: restart count above boundary (6) — should return true
	pod = k8.Pod{Phase: "Running", RestartCount: 6, ExitCode: 0}
	if !k8.IsUnhealthy(pod) {
		t.Errorf("restart count 6: got false, want true")
	}

	// Situation 6: non-zero exit code — pod crashed, should return true
	pod = k8.Pod{Phase: "Running", RestartCount: 0, ExitCode: 1}
	if !k8.IsUnhealthy(pod) {
		t.Errorf("exit code 1: got false, want true")
	}

	// Situation 7: OOMKilled exit code 137 — specific real-world crash, should return true
	pod = k8.Pod{Phase: "Running", RestartCount: 0, ExitCode: 137}
	if !k8.IsUnhealthy(pod) {
		t.Errorf("exit code 137 OOMKilled: got false, want true")
	}

	// Situation 8: Succeeded phase — completed job pod, not unhealthy
	pod = k8.Pod{Phase: "Succeeded", RestartCount: 0, ExitCode: 0}
	if k8.IsUnhealthy(pod) {
		t.Errorf("succeeded phase: got true, want false")
	}

	// Situation 9: Pending phase — not yet started, not unhealthy
	pod = k8.Pod{Phase: "Pending", RestartCount: 0, ExitCode: 0}
	if k8.IsUnhealthy(pod) {
		t.Errorf("pending phase: got true, want false")
	}

	// Situation 10: empty phase — unscheduled pod, not unhealthy
	pod = k8.Pod{Phase: "", RestartCount: 0, ExitCode: 0}
	if k8.IsUnhealthy(pod) {
		t.Errorf("empty phase: got true, want false")
	}

	// Situation 11: all unhealthy conditions at once — any one is enough, should return true
	pod = k8.Pod{Phase: "Failed", RestartCount: 10, ExitCode: 1}
	if !k8.IsUnhealthy(pod) {
		t.Errorf("all unhealthy conditions: got false, want true")
	}
}

package k8

func IsUnhealthy(p Pod) bool {
	if p.Phase == "Failed" {
		return true
	}
	if p.RestartCount > 5 {
		return true
	}
	if p.ExitCode != 0 {
		return true
	}
	return false
}

package notifier

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/namburisnehitha/cluster-pulse/internal/ai"
)

type SlackNotifier struct {
	WebHookURL string
}

func NewNotifier(url string) *SlackNotifier {
	return &SlackNotifier{
		WebHookURL: url,
	}
}

func (n *SlackNotifier) Notify(ctx context.Context, analysis ai.Analysis) error {
	text := fmt.Sprintf("*Pod Alert: %s/%s*\nSeverity: %s\nRoot cause: %s\nFix: %s\nCommand: `%s`",
		analysis.Namespace, analysis.PodName, analysis.Severity, analysis.RootCause, analysis.Fix, analysis.KubectlCommand)

	payload, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.WebHookURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("slack webhook returned status %d", resp.StatusCode)
	}

	return nil
}

package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/mysql"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/namburisnehitha/cluster-pulse/internal/ai"
)

type MySQL struct {
	db *sql.DB
}

func New(dsn string) (*MySQL, error) {

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	err = db.Ping()

	if err != nil {
		return nil, err
	}

	migrator, err := migrate.New("file://migrations", "mysql://"+dsn)
	if err != nil {
		return nil, err
	}
	if err := migrator.Up(); err != nil && err != migrate.ErrNoChange {
		return nil, err
	}

	return &MySQL{db: db}, nil
}

func NewWithDB(db *sql.DB) *MySQL {
	return &MySQL{db: db}
}

func (m *MySQL) SaveAnalysis(ctx context.Context, a ai.Analysis) error {
	details, err := json.Marshal(struct {
		RootCause            string   `json:"root_cause"`
		Fix                  string   `json:"fix"`
		KubectlCommand       string   `json:"kubectl_command"`
		IfFixFails           string   `json:"if_fix_fails"`
		Summary              string   `json:"summary"`
		SuggestedMemoryLimit string   `json:"suggested_memory_limit"`
		SuggestedCPULimit    string   `json:"suggested_cpu_limit"`
		ExitCodeExplanation  string   `json:"exit_code_explanation"`
		RelevantLogLines     []string `json:"relevant_log_lines"`
		RelatedPods          []string `json:"related_pods"`
		TriggeringDeployment string   `json:"triggering_deployment"`
		HistorySummary       string   `json:"history_summary"`
		ResourceTrend        string   `json:"resource_trend"`
	}{
		a.RootCause, a.Fix, a.KubectlCommand, a.IfFixFails, a.Summary,
		a.SuggestedMemoryLimit, a.SuggestedCPULimit, a.ExitCodeExplanation,
		a.RelevantLogLines, a.RelatedPods, a.TriggeringDeployment,
		a.HistorySummary, a.ResourceTrend,
	})
	if err != nil {
		return err
	}

	var failure_time sql.NullTime

	if !a.FailureTime.IsZero() {
		failure_time = sql.NullTime{Time: a.FailureTime, Valid: true}
	}

	backoff := 100 * time.Millisecond
	for attempt := 0; attempt < 3; attempt++ {
		_, err = m.db.ExecContext(ctx, `
		INSERT INTO analyses (pod_name, namespace, severity, confidence, is_recurring, failure_time, analyzed_at, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, a.PodName, a.Namespace, a.Severity, a.Confidence, a.IsRecurring, failure_time, a.AnalyzedAt, details)
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

func (m *MySQL) SaveResourceSnapshot(ctx context.Context, snap ResourceSnapshot) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO resource_snapshots (pod_name, namespace, cpu_usage, memory_usage, recorded_at)
		VALUES (?, ?, ?, ?, ?)
	`, snap.PodName, snap.Namespace, snap.CPUUsage, snap.MemoryUsage, snap.RecordedAt)
	return err
}

func (m *MySQL) GetAnalysis(ctx context.Context, podName, namespace string) (*ai.Analysis, error) {
	row := m.db.QueryRowContext(ctx, `
		SELECT id, pod_name, namespace, severity, confidence, is_recurring, failure_time, analyzed_at, details
		FROM analyses 
		WHERE pod_name = ? AND namespace = ?
		ORDER BY analyzed_at DESC LIMIT 1
	`, podName, namespace)

	var a ai.Analysis
	var details []byte
	var failureTime sql.NullTime

	var id int64

	err := row.Scan(
		&id,
		&a.PodName,
		&a.Namespace,
		&a.Severity,
		&a.Confidence,
		&a.IsRecurring,
		&failureTime,
		&a.AnalyzedAt,
		&details,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if failureTime.Valid {
		a.FailureTime = failureTime.Time
	}

	if err := json.Unmarshal(details, &a); err != nil {
		return nil, err
	}

	return &a, nil
}

func (m *MySQL) GetPodHistory(ctx context.Context, podName, namespace string, limit int) ([]ResourceSnapshot, error) {
	rows, err := m.db.QueryContext(ctx, `
		SELECT id, pod_name, namespace, cpu_usage, memory_usage, recorded_at
		FROM resource_snapshots
		WHERE pod_name = ? AND namespace = ?
		ORDER BY recorded_at DESC LIMIT ?
	`, podName, namespace, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var snaps []ResourceSnapshot
	for rows.Next() {
		var s ResourceSnapshot
		if err := rows.Scan(&s.ID, &s.PodName, &s.Namespace, &s.CPUUsage, &s.MemoryUsage, &s.RecordedAt); err != nil {
			return nil, err
		}
		snaps = append(snaps, s)
	}

	for i, j := 0, len(snaps)-1; i < j; i, j = i+1, j-1 {
		snaps[i], snaps[j] = snaps[j], snaps[i]
	}

	return snaps, rows.Err()

}

func (m *MySQL) ListAnalyses(ctx context.Context, cursor string, limit int) ([]ai.Analysis, string, error) {
	var rows *sql.Rows
	var err error

	if cursor == "" {
		rows, err = m.db.QueryContext(ctx, `
			SELECT id, pod_name, namespace, severity, confidence, is_recurring, failure_time, analyzed_at, details
			FROM analyses
			ORDER BY id DESC
			LIMIT ?
		`, limit)
	} else {
		var lastID int64
		decoded, decodeErr := base64.StdEncoding.DecodeString(cursor)
		if decodeErr != nil {
			return nil, "", decodeErr
		}
		lastID, decodeErr = strconv.ParseInt(string(decoded), 10, 64)
		if decodeErr != nil {
			return nil, "", decodeErr
		}
		rows, err = m.db.QueryContext(ctx, `
        SELECT id, pod_name, namespace, severity, confidence, is_recurring, failure_time, analyzed_at, details
        FROM analyses
        WHERE id < ?
        ORDER BY id DESC
        LIMIT ?
    `, lastID, limit)
	}

	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	var analyses []ai.Analysis
	var lastSeenID int64

	for rows.Next() {
		var a ai.Analysis
		var details []byte
		var failureTime sql.NullTime
		var id int64

		if err := rows.Scan(&id, &a.PodName, &a.Namespace, &a.Severity, &a.Confidence, &a.IsRecurring, &failureTime, &a.AnalyzedAt, &details); err != nil {
			return nil, "", err
		}
		if failureTime.Valid {
			a.FailureTime = failureTime.Time
		}
		if err := json.Unmarshal(details, &a); err != nil {
			return nil, "", err
		}
		lastSeenID = id
		analyses = append(analyses, a)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(analyses) == limit {
		nextCursor = base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(lastSeenID, 10)))
	}

	return analyses, nextCursor, nil
}

func (m *MySQL) Close() error {
	return m.db.Close()
}

func (m *MySQL) CallPrune(ctx context.Context, retentionDays int) error {
	if retentionDays < 0 {
		return fmt.Errorf("retention days must be non-negative, got %d", retentionDays)
	}
	_, err := m.db.ExecContext(ctx, "CALL prune_old_data(?)", retentionDays)
	return err
}

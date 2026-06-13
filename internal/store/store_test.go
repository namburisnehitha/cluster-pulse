package store_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/namburisnehitha/cluster-pulse/internal/ai"
	"github.com/namburisnehitha/cluster-pulse/internal/store"
)

func setupMock(t *testing.T) (*store.MySQL, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return store.NewWithDB(db), mock
}

func makeAnalysis() ai.Analysis {
	return ai.Analysis{
		PodName:              "pod-1",
		Namespace:            "default",
		Severity:             "high",
		Confidence:           "high",
		IsRecurring:          false,
		FailureTime:          time.Now().Round(time.Second),
		AnalyzedAt:           time.Now().Round(time.Second),
		RootCause:            "OOMKilled",
		Fix:                  "increase memory limit",
		KubectlCommand:       "kubectl describe pod pod-1",
		IfFixFails:           "check node memory",
		Summary:              "pod ran out of memory",
		SuggestedMemoryLimit: "512Mi",
		SuggestedCPULimit:    "500m",
		ExitCodeExplanation:  "137 = OOMKilled",
		RelevantLogLines:     []string{"OOM", "killed"},
		RelatedPods:          []string{"pod-2"},
		TriggeringDeployment: "my-deployment",
		HistorySummary:       "recurring OOM",
		ResourceTrend:        "increasing",
	}
}

func detailsJSON(a ai.Analysis) []byte {
	b, _ := json.Marshal(struct {
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
	return b
}

func encodeCursor(id int64) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(id, 10)))
}

func TestSaveAnalysis(t *testing.T) {

	// Situation 1: happy path — all fields valid, should save without error
	s, mock := setupMock(t)
	a := makeAnalysis()
	mock.ExpectExec("INSERT INTO analyses").
		WithArgs(
			a.PodName, a.Namespace, a.Severity, a.Confidence, a.IsRecurring,
			sqlmock.AnyArg(), a.AnalyzedAt, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.SaveAnalysis(context.Background(), a)
	if err != nil {
		t.Errorf("happy path: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("happy path: unmet expectations: %v", err)
	}

	// Situation 2: zero FailureTime — NullTime should be invalid, no error
	s, mock = setupMock(t)
	a = makeAnalysis()
	a.FailureTime = time.Time{}
	mock.ExpectExec("INSERT INTO analyses").
		WithArgs(
			a.PodName, a.Namespace, a.Severity, a.Confidence, a.IsRecurring,
			sqlmock.AnyArg(), a.AnalyzedAt, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.SaveAnalysis(context.Background(), a)
	if err != nil {
		t.Errorf("zero failure time: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("zero failure time: unmet expectations: %v", err)
	}

	// Situation 3: nil RelevantLogLines and RelatedPods — marshals as null, no error
	s, mock = setupMock(t)
	a = makeAnalysis()
	a.RelevantLogLines = nil
	a.RelatedPods = nil
	mock.ExpectExec("INSERT INTO analyses").
		WithArgs(
			a.PodName, a.Namespace, a.Severity, a.Confidence, a.IsRecurring,
			sqlmock.AnyArg(), a.AnalyzedAt, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.SaveAnalysis(context.Background(), a)
	if err != nil {
		t.Errorf("nil slices: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("nil slices: unmet expectations: %v", err)
	}

	// Situation 4: DB connection lost — should return error
	s, mock = setupMock(t)
	a = makeAnalysis()
	mock.ExpectExec("INSERT INTO analyses").
		WillReturnError(fmt.Errorf("db connection lost"))

	err = s.SaveAnalysis(context.Background(), a)
	if err == nil {
		t.Errorf("db error: got nil, want error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("db error: unmet expectations: %v", err)
	}

	// Situation 5: IsRecurring true — verify bool field saves correctly
	s, mock = setupMock(t)
	a = makeAnalysis()
	a.IsRecurring = true
	mock.ExpectExec("INSERT INTO analyses").
		WithArgs(
			a.PodName, a.Namespace, a.Severity, a.Confidence, true,
			sqlmock.AnyArg(), a.AnalyzedAt, sqlmock.AnyArg(),
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.SaveAnalysis(context.Background(), a)
	if err != nil {
		t.Errorf("is_recurring true: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("is_recurring true: unmet expectations: %v", err)
	}
}

func TestGetAnalysis(t *testing.T) {
	now := time.Now().Round(time.Second)

	// Situation 1: found — returns correct analysis with all fields unmarshaled
	s, mock := setupMock(t)
	a := makeAnalysis()
	a.AnalyzedAt = now
	rows := sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "severity", "confidence",
		"is_recurring", "failure_time", "analyzed_at", "details",
	}).AddRow(
		1, a.PodName, a.Namespace, a.Severity, a.Confidence,
		a.IsRecurring, sql.NullTime{Time: a.FailureTime, Valid: true},
		a.AnalyzedAt, detailsJSON(a),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, err := s.GetAnalysis(context.Background(), "pod-1", "default")
	if err != nil {
		t.Fatalf("found: got %v, want nil", err)
	}
	if result == nil {
		t.Fatalf("found: got nil, want analysis")
	}
	if result.PodName != "pod-1" {
		t.Errorf("found: pod name: got %s, want pod-1", result.PodName)
	}
	if result.Namespace != "default" {
		t.Errorf("found: namespace: got %s, want default", result.Namespace)
	}
	if result.RootCause != "OOMKilled" {
		t.Errorf("found: root cause: got %s, want OOMKilled", result.RootCause)
	}
	if result.Severity != "high" {
		t.Errorf("found: severity: got %s, want high", result.Severity)
	}
	if result.Confidence != "high" {
		t.Errorf("found: confidence: got %s, want high", result.Confidence)
	}
	if result.Fix != "increase memory limit" {
		t.Errorf("found: fix: got %s, want increase memory limit", result.Fix)
	}
	if result.SuggestedMemoryLimit != "512Mi" {
		t.Errorf("found: memory limit: got %s, want 512Mi", result.SuggestedMemoryLimit)
	}
	if len(result.RelevantLogLines) != 2 {
		t.Errorf("found: log lines: got %d, want 2", len(result.RelevantLogLines))
	}
	if len(result.RelatedPods) != 1 {
		t.Errorf("found: related pods: got %d, want 1", len(result.RelatedPods))
	}
	if !result.AnalyzedAt.Equal(now) {
		t.Errorf("found: analyzed at: got %v, want %v", result.AnalyzedAt, now)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("found: unmet expectations: %v", err)
	}

	// Situation 2: not found — returns nil, nil
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnError(sql.ErrNoRows)

	result, err = s.GetAnalysis(context.Background(), "missing", "default")
	if err != nil {
		t.Errorf("not found: got %v, want nil", err)
	}
	if result != nil {
		t.Errorf("not found: got %v, want nil", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("not found: unmet expectations: %v", err)
	}

	// Situation 3: invalid failure time — FailureTime stays zero value
	s, mock = setupMock(t)
	a = makeAnalysis()
	rows = sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "severity", "confidence",
		"is_recurring", "failure_time", "analyzed_at", "details",
	}).AddRow(
		1, a.PodName, a.Namespace, a.Severity, a.Confidence,
		a.IsRecurring, sql.NullTime{Valid: false},
		a.AnalyzedAt, detailsJSON(a),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, err = s.GetAnalysis(context.Background(), "pod-1", "default")
	if err != nil {
		t.Fatalf("invalid failure time: got %v, want nil", err)
	}
	if result == nil {
		t.Fatalf("invalid failure time: got nil, want analysis")
	}
	if !result.FailureTime.IsZero() {
		t.Errorf("invalid failure time: got %v, want zero", result.FailureTime)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("invalid failure time: unmet expectations: %v", err)
	}

	// Situation 4: malformed details JSON in DB — should return error not panic
	s, mock = setupMock(t)
	a = makeAnalysis()
	rows = sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "severity", "confidence",
		"is_recurring", "failure_time", "analyzed_at", "details",
	}).AddRow(
		1, a.PodName, a.Namespace, a.Severity, a.Confidence,
		a.IsRecurring, sql.NullTime{Valid: false},
		a.AnalyzedAt, []byte("not valid json {{{"),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, err = s.GetAnalysis(context.Background(), "pod-1", "default")
	if err == nil {
		t.Errorf("malformed json: got nil, want error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("malformed json: unmet expectations: %v", err)
	}

	// Situation 5: multiple analyses for same pod — ORDER BY analyzed_at DESC LIMIT 1 returns most recent
	s, mock = setupMock(t)
	a = makeAnalysis()
	recentTime := now.Add(time.Hour)
	a.AnalyzedAt = recentTime
	rows = sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "severity", "confidence",
		"is_recurring", "failure_time", "analyzed_at", "details",
	}).AddRow(
		2, a.PodName, a.Namespace, a.Severity, a.Confidence,
		a.IsRecurring, sql.NullTime{Time: a.FailureTime, Valid: true},
		recentTime, detailsJSON(a),
	)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	result, err = s.GetAnalysis(context.Background(), "pod-1", "default")
	if err != nil {
		t.Fatalf("most recent: got %v, want nil", err)
	}
	if !result.AnalyzedAt.Equal(recentTime) {
		t.Errorf("most recent: got %v, want %v", result.AnalyzedAt, recentTime)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("most recent: unmet expectations: %v", err)
	}

	// Situation 6: DB error — should return error
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnError(fmt.Errorf("connection reset"))

	result, err = s.GetAnalysis(context.Background(), "pod-1", "default")
	if err == nil {
		t.Errorf("db error: got nil, want error")
	}
	if result != nil {
		t.Errorf("db error: got %v, want nil", result)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("db error: unmet expectations: %v", err)
	}
}

func TestGetPodHistory(t *testing.T) {
	now := time.Now().Round(time.Second)

	// Situation 1: returns snapshots in ASC order — oldest first, newest last
	s, mock := setupMock(t)
	rows := sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "cpu_usage", "memory_usage", "recorded_at",
	}).
		AddRow(3, "pod-1", "default", "300m", "600Mi", now).
		AddRow(2, "pod-1", "default", "200m", "400Mi", now.Add(-time.Minute)).
		AddRow(1, "pod-1", "default", "100m", "200Mi", now.Add(-2*time.Minute))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	snaps, err := s.GetPodHistory(context.Background(), "pod-1", "default", 10)
	if err != nil {
		t.Fatalf("asc order: got %v, want nil", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("asc order: got %d snapshots, want 3", len(snaps))
	}
	if snaps[0].CPUUsage != "100m" {
		t.Errorf("asc order: first snap cpu: got %s, want 100m", snaps[0].CPUUsage)
	}
	if snaps[2].CPUUsage != "300m" {
		t.Errorf("asc order: last snap cpu: got %s, want 300m", snaps[2].CPUUsage)
	}
	if snaps[0].PodName != "pod-1" {
		t.Errorf("asc order: pod name: got %s, want pod-1", snaps[0].PodName)
	}
	if snaps[0].Namespace != "default" {
		t.Errorf("asc order: namespace: got %s, want default", snaps[0].Namespace)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("asc order: unmet expectations: %v", err)
	}

	// Situation 2: no snapshots — returns empty slice not nil
	s, mock = setupMock(t)
	rows = sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "cpu_usage", "memory_usage", "recorded_at",
	})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	snaps, err = s.GetPodHistory(context.Background(), "pod-1", "default", 10)
	if err != nil {
		t.Errorf("no snapshots: got %v, want nil", err)
	}
	if len(snaps) != 0 {
		t.Errorf("no snapshots: got %d, want 0", len(snaps))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no snapshots: unmet expectations: %v", err)
	}

	// Situation 3: respects limit — returns only as many rows as DB sends back
	s, mock = setupMock(t)
	rows = sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "cpu_usage", "memory_usage", "recorded_at",
	}).
		AddRow(1, "pod-1", "default", "100m", "200Mi", now.Add(-time.Minute)).
		AddRow(2, "pod-1", "default", "200m", "400Mi", now)
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	snaps, err = s.GetPodHistory(context.Background(), "pod-1", "default", 2)
	if err != nil {
		t.Errorf("limit: got %v, want nil", err)
	}
	if len(snaps) != 2 {
		t.Errorf("limit: got %d, want 2", len(snaps))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("limit: unmet expectations: %v", err)
	}

	// Situation 4: limit zero — MySQL returns empty, no panic
	s, mock = setupMock(t)
	rows = sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "cpu_usage", "memory_usage", "recorded_at",
	})
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	snaps, err = s.GetPodHistory(context.Background(), "pod-1", "default", 0)
	if err != nil {
		t.Errorf("limit zero: got %v, want nil", err)
	}
	if len(snaps) != 0 {
		t.Errorf("limit zero: got %d, want 0", len(snaps))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("limit zero: unmet expectations: %v", err)
	}

	// Situation 5: rows.Err() after partial scan — network dropped mid-scan, should return error
	s, mock = setupMock(t)
	rows = sqlmock.NewRows([]string{
		"id", "pod_name", "namespace", "cpu_usage", "memory_usage", "recorded_at",
	}).
		AddRow(1, "pod-1", "default", "100m", "200Mi", now).
		RowError(0, fmt.Errorf("network error mid scan"))
	mock.ExpectQuery("SELECT").WillReturnRows(rows)

	snaps, err = s.GetPodHistory(context.Background(), "pod-1", "default", 10)
	if err == nil {
		t.Errorf("rows error: got nil, want error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("rows error: unmet expectations: %v", err)
	}

	// Situation 6: DB query error — should return error immediately
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnError(fmt.Errorf("connection reset"))

	snaps, err = s.GetPodHistory(context.Background(), "pod-1", "default", 10)
	if err == nil {
		t.Errorf("db error: got nil, want error")
	}
	if snaps != nil {
		t.Errorf("db error: got %v, want nil", snaps)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("db error: unmet expectations: %v", err)
	}
}

func TestListAnalyses(t *testing.T) {
	now := time.Now().Round(time.Second)

	makeRows := func(count int, startID int64) *sqlmock.Rows {
		rows := sqlmock.NewRows([]string{
			"id", "pod_name", "namespace", "severity", "confidence",
			"is_recurring", "failure_time", "analyzed_at", "details",
		})
		a := makeAnalysis()
		for i := 0; i < count; i++ {
			rows.AddRow(
				startID-int64(i), a.PodName, a.Namespace, a.Severity, a.Confidence,
				a.IsRecurring, sql.NullTime{Time: now, Valid: true},
				now, detailsJSON(a),
			)
		}
		return rows
	}

	// Situation 1: no cursor — returns first page, sets next cursor when results == limit
	s, mock := setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(10, 100))

	analyses, nextCursor, err := s.ListAnalyses(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("first page: got %v, want nil", err)
	}
	if len(analyses) != 10 {
		t.Errorf("first page: got %d analyses, want 10", len(analyses))
	}
	if nextCursor == "" {
		t.Errorf("first page: got empty cursor, want cursor set")
	}
	if analyses[0].PodName != "pod-1" {
		t.Errorf("first page: pod name: got %s, want pod-1", analyses[0].PodName)
	}
	if analyses[0].RootCause != "OOMKilled" {
		t.Errorf("first page: root cause: got %s, want OOMKilled", analyses[0].RootCause)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("first page: unmet expectations: %v", err)
	}

	// Situation 2: with valid cursor — queries with WHERE id < lastID
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(10, 99))

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), encodeCursor(100), 10)
	if err != nil {
		t.Fatalf("with cursor: got %v, want nil", err)
	}
	if len(analyses) != 10 {
		t.Errorf("with cursor: got %d analyses, want 10", len(analyses))
	}
	if nextCursor == "" {
		t.Errorf("with cursor: got empty cursor, want cursor set")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("with cursor: unmet expectations: %v", err)
	}

	// Situation 3: results less than limit — no next cursor
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(5, 5))

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("partial page: got %v, want nil", err)
	}
	if len(analyses) != 5 {
		t.Errorf("partial page: got %d analyses, want 5", len(analyses))
	}
	if nextCursor != "" {
		t.Errorf("partial page: got cursor %s, want empty", nextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("partial page: unmet expectations: %v", err)
	}

	// Situation 4: invalid base64 cursor — should return error immediately, no DB call
	s, mock = setupMock(t)

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), "not-valid-base64!!!", 10)
	if err == nil {
		t.Errorf("invalid cursor: got nil, want error")
	}
	if analyses != nil {
		t.Errorf("invalid cursor: got %v, want nil", analyses)
	}
	if nextCursor != "" {
		t.Errorf("invalid cursor: got cursor %s, want empty", nextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("invalid cursor: unmet expectations: %v", err)
	}

	// Situation 5: empty table no cursor — returns empty slice, empty cursor
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(0, 0))

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("empty table: got %v, want nil", err)
	}
	if len(analyses) != 0 {
		t.Errorf("empty table: got %d analyses, want 0", len(analyses))
	}
	if nextCursor != "" {
		t.Errorf("empty table: got cursor %s, want empty", nextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("empty table: unmet expectations: %v", err)
	}

	// Situation 6: limit 1 — single result equals limit, cursor should be set
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(1, 1))

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), "", 1)
	if err != nil {
		t.Fatalf("limit 1: got %v, want nil", err)
	}
	if len(analyses) != 1 {
		t.Errorf("limit 1: got %d analyses, want 1", len(analyses))
	}
	if nextCursor == "" {
		t.Errorf("limit 1: got empty cursor, want cursor set")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("limit 1: unmet expectations: %v", err)
	}

	// Situation 7: cursor ID not in DB — empty result, no next cursor
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnRows(makeRows(0, 0))

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), encodeCursor(99999), 10)
	if err != nil {
		t.Fatalf("cursor not in db: got %v, want nil", err)
	}
	if len(analyses) != 0 {
		t.Errorf("cursor not in db: got %d analyses, want 0", len(analyses))
	}
	if nextCursor != "" {
		t.Errorf("cursor not in db: got cursor %s, want empty", nextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("cursor not in db: unmet expectations: %v", err)
	}

	// Situation 8: DB error with no cursor — should return error
	s, mock = setupMock(t)
	mock.ExpectQuery("SELECT").WillReturnError(fmt.Errorf("connection reset"))

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), "", 10)
	if err == nil {
		t.Errorf("db error: got nil, want error")
	}
	if analyses != nil {
		t.Errorf("db error: got %v, want nil", analyses)
	}
	if nextCursor != "" {
		t.Errorf("db error: got cursor %s, want empty", nextCursor)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("db error: unmet expectations: %v", err)
	}

	// Situation 9: valid cursor but non-numeric decoded value — should return error
	s, mock = setupMock(t)
	badCursor := base64.StdEncoding.EncodeToString([]byte("not-a-number"))

	analyses, nextCursor, err = s.ListAnalyses(context.Background(), badCursor, 10)
	if err == nil {
		t.Errorf("non-numeric cursor: got nil, want error")
	}
	if analyses != nil {
		t.Errorf("non-numeric cursor: got %v, want nil", analyses)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("non-numeric cursor: unmet expectations: %v", err)
	}
}

func TestSaveResourceSnapshot(t *testing.T) {

	// Situation 1: happy path — all fields valid, should save without error
	s, mock := setupMock(t)
	snap := store.ResourceSnapshot{
		PodName:     "pod-1",
		Namespace:   "default",
		CPUUsage:    "100m",
		MemoryUsage: "200Mi",
		RecordedAt:  time.Now().Round(time.Second),
	}
	mock.ExpectExec("INSERT INTO resource_snapshots").
		WithArgs(snap.PodName, snap.Namespace, snap.CPUUsage, snap.MemoryUsage, snap.RecordedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err := s.SaveResourceSnapshot(context.Background(), snap)
	if err != nil {
		t.Errorf("happy path: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("happy path: unmet expectations: %v", err)
	}

	// Situation 2: DB error — should return error
	s, mock = setupMock(t)
	mock.ExpectExec("INSERT INTO resource_snapshots").
		WillReturnError(fmt.Errorf("db connection lost"))

	err = s.SaveResourceSnapshot(context.Background(), snap)
	if err == nil {
		t.Errorf("db error: got nil, want error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("db error: unmet expectations: %v", err)
	}

	// Situation 3: empty pod name — no panic, passes empty string to DB
	s, mock = setupMock(t)
	emptySnap := store.ResourceSnapshot{
		PodName:     "",
		Namespace:   "default",
		CPUUsage:    "100m",
		MemoryUsage: "200Mi",
		RecordedAt:  time.Now().Round(time.Second),
	}
	mock.ExpectExec("INSERT INTO resource_snapshots").
		WithArgs(emptySnap.PodName, emptySnap.Namespace, emptySnap.CPUUsage, emptySnap.MemoryUsage, emptySnap.RecordedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.SaveResourceSnapshot(context.Background(), emptySnap)
	if err != nil {
		t.Errorf("empty pod name: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("empty pod name: unmet expectations: %v", err)
	}

	// Situation 4: empty CPU and memory usage — no panic, saves empty strings
	s, mock = setupMock(t)
	emptyUsageSnap := store.ResourceSnapshot{
		PodName:     "pod-1",
		Namespace:   "default",
		CPUUsage:    "",
		MemoryUsage: "",
		RecordedAt:  time.Now().Round(time.Second),
	}
	mock.ExpectExec("INSERT INTO resource_snapshots").
		WithArgs(emptyUsageSnap.PodName, emptyUsageSnap.Namespace, emptyUsageSnap.CPUUsage, emptyUsageSnap.MemoryUsage, emptyUsageSnap.RecordedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.SaveResourceSnapshot(context.Background(), emptyUsageSnap)
	if err != nil {
		t.Errorf("empty usage: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("empty usage: unmet expectations: %v", err)
	}

	// Situation 5: zero RecordedAt time — time.Time{} passes through, no panic
	s, mock = setupMock(t)
	zeroTimeSnap := store.ResourceSnapshot{
		PodName:     "pod-1",
		Namespace:   "default",
		CPUUsage:    "100m",
		MemoryUsage: "200Mi",
		RecordedAt:  time.Time{},
	}
	mock.ExpectExec("INSERT INTO resource_snapshots").
		WithArgs(zeroTimeSnap.PodName, zeroTimeSnap.Namespace, zeroTimeSnap.CPUUsage, zeroTimeSnap.MemoryUsage, zeroTimeSnap.RecordedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.SaveResourceSnapshot(context.Background(), zeroTimeSnap)
	if err != nil {
		t.Errorf("zero recorded at: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("zero recorded at: unmet expectations: %v", err)
	}

	// Situation 6: very large CPU and memory strings — no truncation, passes through
	s, mock = setupMock(t)
	largeSnap := store.ResourceSnapshot{
		PodName:     "pod-1",
		Namespace:   "default",
		CPUUsage:    "999999999m",
		MemoryUsage: "999999999Mi",
		RecordedAt:  time.Now().Round(time.Second),
	}
	mock.ExpectExec("INSERT INTO resource_snapshots").
		WithArgs(largeSnap.PodName, largeSnap.Namespace, largeSnap.CPUUsage, largeSnap.MemoryUsage, largeSnap.RecordedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = s.SaveResourceSnapshot(context.Background(), largeSnap)
	if err != nil {
		t.Errorf("large values: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("large values: unmet expectations: %v", err)
	}
}

func TestCallPrune(t *testing.T) {

	// Situation 1: happy path — calls procedure with correct retention days, no error
	s, mock := setupMock(t)
	mock.ExpectExec("CALL prune_old_data").
		WithArgs(30).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := s.CallPrune(context.Background(), 30)
	if err != nil {
		t.Errorf("happy path: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("happy path: unmet expectations: %v", err)
	}

	// Situation 2: DB error — should return error
	s, mock = setupMock(t)
	mock.ExpectExec("CALL prune_old_data").
		WillReturnError(fmt.Errorf("procedure not found"))

	err = s.CallPrune(context.Background(), 30)
	if err == nil {
		t.Errorf("db error: got nil, want error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("db error: unmet expectations: %v", err)
	}

	// Situation 3: zero retention days — passes 0, no panic
	s, mock = setupMock(t)
	mock.ExpectExec("CALL prune_old_data").
		WithArgs(0).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = s.CallPrune(context.Background(), 0)
	if err != nil {
		t.Errorf("zero retention: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("zero retention: unmet expectations: %v", err)
	}

	// Situation 4: negative retention days — guard should return error before hitting DB
	s, mock = setupMock(t)

	err = s.CallPrune(context.Background(), -1)
	if err == nil {
		t.Errorf("negative retention: got nil, want error")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("negative retention: unmet expectations: %v", err)
	}

	// Situation 5: very large retention days — no overflow, passes through correctly
	s, mock = setupMock(t)
	mock.ExpectExec("CALL prune_old_data").
		WithArgs(99999).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = s.CallPrune(context.Background(), 99999)
	if err != nil {
		t.Errorf("large retention: got %v, want nil", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("large retention: unmet expectations: %v", err)
	}
}

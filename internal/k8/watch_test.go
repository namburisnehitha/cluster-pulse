package k8_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/namburisnehitha/cluster-pulse/internal/k8"
)

func setupTracedClient(t *testing.T) (*k8.Client, *watch.FakeWatcher, *tracetest.SpanRecorder) {
	t.Helper()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		tp.Shutdown(context.Background())
		otel.SetTracerProvider(otel.GetTracerProvider())
	})

	fakeClient := fake.NewSimpleClientset()
	fw := watch.NewFake()
	fakeClient.Fake.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		return true, fw, nil
	})

	return k8.NewClientWithClientset(fakeClient), fw, sr
}

func makeUnhealthyPod(name, namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: "100",
		},
		Spec: corev1.PodSpec{
			NodeName: "node-1",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodFailed,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "my-container",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 137,
						},
					},
					RestartCount: 3,
				},
			},
		},
	}
}

func makeHealthyPod(name, namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			ResourceVersion: "200",
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
}

func findSpanByName(spans []sdktrace.ReadOnlySpan, name string) sdktrace.ReadOnlySpan {
	for _, s := range spans {
		if s.Name() == name {
			return s
		}
	}
	return nil
}

func findAllSpansByName(spans []sdktrace.ReadOnlySpan, name string) []sdktrace.ReadOnlySpan {
	var result []sdktrace.ReadOnlySpan
	for _, s := range spans {
		if s.Name() == name {
			result = append(result, s)
		}
	}
	return result
}

func waitForPod(t *testing.T, podCh <-chan k8.PodResult) k8.PodResult {
	t.Helper()
	select {
	case result := <-podCh:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for pod")
		return k8.PodResult{}
	}
}

func TestWatchPods(t *testing.T) {

	// Situation 1: ADDED unhealthy pod — sent to channel with all fields correct
	// WatchReceivedAt set immediately on event arrival
	c, fw, _ := setupTracedClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	podCh, err := c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	result := waitForPod(t, podCh)
	if result.Err != nil {
		t.Fatalf("added unhealthy: got error %v, want nil", result.Err)
	}
	if result.Pod.Name != "pod-1" {
		t.Errorf("added unhealthy: name: got %s, want pod-1", result.Pod.Name)
	}
	if result.Pod.Namespace != "default" {
		t.Errorf("added unhealthy: namespace: got %s, want default", result.Pod.Namespace)
	}
	if result.Pod.Phase != "Failed" {
		t.Errorf("added unhealthy: phase: got %s, want Failed", result.Pod.Phase)
	}
	if result.Pod.ExitCode != 137 {
		t.Errorf("added unhealthy: exit code: got %d, want 137", result.Pod.ExitCode)
	}
	if result.Pod.WatchReceivedAt.IsZero() {
		t.Errorf("added unhealthy: WatchReceivedAt: got zero, want time set")
	}
	cancel()
	for range podCh {
	}

	// Situation 2: MODIFIED unhealthy pod — sent to channel
	c, fw, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("modified: start: got %v, want nil", err)
	}
	fw.Modify(makeUnhealthyPod("pod-1", "default"))
	result = waitForPod(t, podCh)
	if result.Err != nil {
		t.Fatalf("modified unhealthy: got error %v, want nil", result.Err)
	}
	if result.Pod.Name != "pod-1" {
		t.Errorf("modified unhealthy: name: got %s, want pod-1", result.Pod.Name)
	}
	cancel()
	for range podCh {
	}

	// Situation 3: healthy pod ADDED — filtered, not sent to channel
	c, fw, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("healthy filtered: start: got %v, want nil", err)
	}
	fw.Add(makeHealthyPod("pod-1", "default"))
	select {
	case result := <-podCh:
		if result.Err == nil {
			t.Errorf("healthy filtered: got pod %s, want nothing sent", result.Pod.Name)
		}
	case <-time.After(500 * time.Millisecond):
		// correct — nothing sent
	}
	cancel()
	for range podCh {
	}

	// Situation 4: DELETED pod — not sent regardless of health
	c, fw, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("deleted: start: got %v, want nil", err)
	}
	fw.Delete(makeUnhealthyPod("pod-1", "default"))
	select {
	case result := <-podCh:
		if result.Err == nil {
			t.Errorf("deleted: got pod %s, want nothing sent", result.Pod.Name)
		}
	case <-time.After(500 * time.Millisecond):
		// correct — nothing sent
	}
	cancel()
	for range podCh {
	}

	// Situation 5: multiple unhealthy pods in sequence — both sent in order
	c, fw, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("multiple pods: start: got %v, want nil", err)
	}
	pod1 := makeUnhealthyPod("pod-1", "default")
	pod2 := makeUnhealthyPod("pod-2", "default")
	pod2.ResourceVersion = "101"
	fw.Add(pod1)
	result1 := waitForPod(t, podCh)
	fw.Add(pod2)
	result2 := waitForPod(t, podCh)
	if result1.Pod.Name != "pod-1" {
		t.Errorf("multiple pods: first: got %s, want pod-1", result1.Pod.Name)
	}
	if result2.Pod.Name != "pod-2" {
		t.Errorf("multiple pods: second: got %s, want pod-2", result2.Pod.Name)
	}
	cancel()
	for range podCh {
	}

	// Situation 6: mix of healthy and unhealthy — only unhealthy sent
	c, fw, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("mix: start: got %v, want nil", err)
	}
	fw.Add(makeHealthyPod("healthy-pod", "default"))
	fw.Add(makeUnhealthyPod("unhealthy-pod", "default"))
	result = waitForPod(t, podCh)
	if result.Pod.Name != "unhealthy-pod" {
		t.Errorf("mix: got %s, want unhealthy-pod", result.Pod.Name)
	}
	cancel()
	for range podCh {
	}

	// Situation 7: non-pod object in watch event — silently skipped, no extra span
	// child spans are ended before pod is sent so assert immediately after waitForPod
	c, fw, sr := setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("non-pod: start: got %v, want nil", err)
	}
	fw.Add(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "not-a-pod", Namespace: "default"},
	})
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	result = waitForPod(t, podCh)

	// child spans are already ended here — assert immediately
	eventSpans := findAllSpansByName(sr.Ended(), "k8s.watch.event")
	if len(eventSpans) != 1 {
		t.Errorf("non-pod: got %d event spans, want 1 — non-pod should not create a span", len(eventSpans))
	}
	if result.Pod.Name != "pod-1" {
		t.Errorf("non-pod: got %s, want pod-1 — non-pod should be skipped", result.Pod.Name)
	}
	cancel()
	for range podCh {
	}

	// Situation 8: context cancelled — goroutine exits, channel closed
	c, _, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("cancel: start: got %v, want nil", err)
	}
	cancel()
	select {
	case _, ok := <-podCh:
		if ok {
			for range podCh {
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel: timed out waiting for channel to close")
	}

	// Situation 9: WatchReceivedAt is set BEFORE enrichment completes
	// core claim of the proposal — receivedAt captured before logs/events/deployments fetched
	c, fw, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("receivedAt timing: start: got %v, want nil", err)
	}
	beforeSend := time.Now()
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	result = waitForPod(t, podCh)
	afterReceive := time.Now()
	if result.Pod.WatchReceivedAt.Before(beforeSend) {
		t.Errorf("receivedAt timing: WatchReceivedAt %v is before event was sent %v",
			result.Pod.WatchReceivedAt, beforeSend)
	}
	if result.Pod.WatchReceivedAt.After(afterReceive) {
		t.Errorf("receivedAt timing: WatchReceivedAt %v is after pod was received %v — must be set before enrichment",
			result.Pod.WatchReceivedAt, afterReceive)
	}
	cancel()
	for range podCh {
	}

	// Situation 10: eventSpan start time matches WatchReceivedAt
	// critical span timing claim — child spans ended before pod sent, assert immediately
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("span timing: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	result = waitForPod(t, podCh)
	// eventSpan is ended before pod is sent — assert immediately
	eSpan := findSpanByName(sr.Ended(), "k8s.watch.event")
	if eSpan == nil {
		t.Fatalf("span timing: event span not found")
	}
	diff := eSpan.StartTime().Sub(result.Pod.WatchReceivedAt)
	if diff < 0 {
		diff = -diff
	}
	if diff > 10*time.Millisecond {
		t.Errorf("span timing: eventSpan start %v differs from WatchReceivedAt %v by %v — should match within 10ms",
			eSpan.StartTime(), result.Pod.WatchReceivedAt, diff)
	}
	cancel()
	for range podCh {
	}

	// Situation 11: eventSpan is child of watchSpan — correct parent-child hierarchy
	// watchSpan ends after drain, child spans end before pod sent
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("hierarchy: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	// eventSpan already ended — get it before cancel
	evSpan := findSpanByName(sr.Ended(), "k8s.watch.event")
	if evSpan == nil {
		t.Fatalf("hierarchy: event span not found")
	}
	cancel()
	for range podCh {
	}
	// watchSpan now ended after drain
	connSpan := findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("hierarchy: connection span not found")
	}
	if evSpan.Parent().SpanID() != connSpan.SpanContext().SpanID() {
		t.Errorf("hierarchy: eventSpan parent %v does not match watchSpan %v — eventSpan must be child of watchSpan",
			evSpan.Parent().SpanID(), connSpan.SpanContext().SpanID())
	}
	if evSpan.SpanContext().TraceID() != connSpan.SpanContext().TraceID() {
		t.Errorf("hierarchy: eventSpan trace ID %v does not match watchSpan trace ID %v — must be same trace",
			evSpan.SpanContext().TraceID(), connSpan.SpanContext().TraceID())
	}

	// Situation 12: logsSpan, eventsSpan, deploymentsSpan are siblings of eventSpan
	// all children of watchSpan — assert child spans before cancel, watchSpan after drain
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("siblings: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	// all enrichment spans ended before pod sent
	spans := sr.Ended()
	logsSpan := findSpanByName(spans, "k8s.watch.event.fetch_logs")
	eventsSpan := findSpanByName(spans, "k8s.watch.event.fetch_events")
	deploymentsSpan := findSpanByName(spans, "k8s.watch.event.fetch_deployments")
	if logsSpan == nil {
		t.Fatalf("siblings: logs span not found")
	}
	if eventsSpan == nil {
		t.Fatalf("siblings: events span not found")
	}
	if deploymentsSpan == nil {
		t.Fatalf("siblings: deployments span not found")
	}
	cancel()
	for range podCh {
	}
	// watchSpan ended after drain
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("siblings: connection span not found")
	}
	if logsSpan.Parent().SpanID() != connSpan.SpanContext().SpanID() {
		t.Errorf("siblings: logsSpan parent %v does not match watchSpan %v",
			logsSpan.Parent().SpanID(), connSpan.SpanContext().SpanID())
	}
	if eventsSpan.Parent().SpanID() != connSpan.SpanContext().SpanID() {
		t.Errorf("siblings: eventsSpan parent %v does not match watchSpan %v",
			eventsSpan.Parent().SpanID(), connSpan.SpanContext().SpanID())
	}
	if deploymentsSpan.Parent().SpanID() != connSpan.SpanContext().SpanID() {
		t.Errorf("siblings: deploymentsSpan parent %v does not match watchSpan %v",
			deploymentsSpan.Parent().SpanID(), connSpan.SpanContext().SpanID())
	}

	// Situation 13: all spans share same trace ID — nothing orphaned
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("trace id: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	cancel()
	for range podCh {
	}
	// all spans ended after drain
	spans = sr.Ended()
	if len(spans) == 0 {
		t.Fatalf("trace id: no spans recorded")
	}
	expectedTraceID := spans[0].SpanContext().TraceID()
	for _, s := range spans {
		if s.SpanContext().TraceID() != expectedTraceID {
			t.Errorf("trace id: span %s has trace ID %v, want %v — all spans must share same trace",
				s.Name(), s.SpanContext().TraceID(), expectedTraceID)
		}
	}

	// Situation 14: SpanKindConsumer on connection span
	// watchSpan ends after drain
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("span kind: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	cancel()
	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("span kind: connection span not found")
	}
	if connSpan.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("span kind: got %v, want SpanKindConsumer", connSpan.SpanKind())
	}

	// Situation 15: SpanKindInternal on event span and enrichment spans
	// child spans ended before pod sent — assert immediately after waitForPod
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("span kind internal: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	spans = sr.Ended()
	for _, name := range []string{
		"k8s.watch.event",
		"k8s.watch.event.fetch_logs",
		"k8s.watch.event.fetch_events",
		"k8s.watch.event.fetch_deployments",
	} {
		s := findSpanByName(spans, name)
		if s == nil {
			t.Fatalf("span kind internal: %s span not found", name)
		}
		if s.SpanKind() != trace.SpanKindInternal {
			t.Errorf("span kind internal: %s: got %v, want SpanKindInternal", s.Name(), s.SpanKind())
		}
	}
	cancel()
	for range podCh {
	}

	// Situation 16: filteredCount increments on watchSpan when healthy pods filtered
	// watchSpan ends after drain — assert after drain
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("filtered count: start: got %v, want nil", err)
	}
	fw.Add(makeHealthyPod("healthy-1", "default"))
	fw.Add(makeHealthyPod("healthy-2", "default"))
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	cancel()
	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("filtered count: connection span not found")
	}
	foundFilteredCount := false
	for _, attr := range connSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.filtered_count" {
			if attr.Value.AsInt64() != 2 {
				t.Errorf("filtered count: got %d, want 2", attr.Value.AsInt64())
			}
			foundFilteredCount = true
		}
	}
	if !foundFilteredCount {
		t.Errorf("filtered count: attribute not found on connection span")
	}

	// Situation 17: events_processed correct on watchSpan after multiple events
	// watchSpan ends after drain — assert after drain
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("events processed: start: got %v, want nil", err)
	}
	p1 := makeUnhealthyPod("pod-1", "default")
	p2 := makeUnhealthyPod("pod-2", "default")
	p2.ResourceVersion = "101"
	fw.Add(p1)
	waitForPod(t, podCh)
	fw.Add(p2)
	waitForPod(t, podCh)
	cancel()
	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("events processed: connection span not found")
	}
	foundEventsProcessed := false
	for _, attr := range connSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.events_processed" {
			if attr.Value.AsInt64() != 2 {
				t.Errorf("events processed: got %d, want 2", attr.Value.AsInt64())
			}
			foundEventsProcessed = true
		}
	}
	if !foundEventsProcessed {
		t.Errorf("events processed: attribute not found on connection span")
	}

	// Situation 18: end_reason context_cancelled when context cancelled mid-event-loop
	// watchSpan ends after drain — assert after drain
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("end reason cancel: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	cancel()
	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("end reason cancel: connection span not found")
	}
	foundEndReason := false
	for _, attr := range connSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.end_reason" {
			if attr.Value.AsString() != "context_cancelled" {
				t.Errorf("end reason cancel: got %s, want context_cancelled", attr.Value.AsString())
			}
			foundEndReason = true
		}
	}
	if !foundEndReason {
		t.Errorf("end reason cancel: attribute not found on connection span")
	}

	// Situation 19: StatusReasonGone — end_reason resource_version_gone
	// error event breaks the loop, watchSpan ends, then outer loop tries to reconnect
	// cancel stops the reconnect, drain ensures watchSpan is recorded
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("gone: start: got %v, want nil", err)
	}
	fw.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Reason: metav1.StatusReasonGone,
	})
	// error breaks event loop, watchSpan ends, reconnect starts
	// cancel during reconnect backoff
	time.Sleep(50 * time.Millisecond)
	cancel()
	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("gone: connection span not found")
	}
	foundGoneReason := false
	for _, attr := range connSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.end_reason" {
			if attr.Value.AsString() != "resource_version_gone" {
				t.Errorf("gone: end_reason: got %s, want resource_version_gone", attr.Value.AsString())
			}
			foundGoneReason = true
		}
	}
	if !foundGoneReason {
		t.Errorf("gone: end_reason attribute not found on connection span")
	}

	// Situation 20: watch error event — end_reason watch_error
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("watch error: start: got %v, want nil", err)
	}
	fw.Error(&metav1.Status{
		Status: metav1.StatusFailure,
		Reason: metav1.StatusReasonInternalError,
	})
	time.Sleep(50 * time.Millisecond)
	cancel()
	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("watch error: connection span not found")
	}
	foundWatchError := false
	for _, attr := range connSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.end_reason" {
			if attr.Value.AsString() != "watch_error" {
				t.Errorf("watch error: end_reason: got %s, want watch_error", attr.Value.AsString())
			}
			foundWatchError = true
		}
	}
	if !foundWatchError {
		t.Errorf("watch error: end_reason attribute not found on connection span")
	}

	// Situation 21: two consecutive Watch connections produce sibling watchSpans
	// reconnection model — second connection span must not be child of first
	fw1 := watch.NewFake()
	fw2 := watch.NewFake()
	callCount := 0
	fakeClient2 := fake.NewSimpleClientset()
	fakeClient2.Fake.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		callCount++
		if callCount == 1 {
			return true, fw1, nil
		}
		return true, fw2, nil
	})
	sr = tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { tp.Shutdown(context.Background()) })
	c2 := k8.NewClientWithClientset(fakeClient2)

	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c2.WatchPods(ctx)
	if err != nil {
		t.Fatalf("sibling spans: start: got %v, want nil", err)
	}
	fw1.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	fw1.Stop()
	// wait for reconnect to happen
	time.Sleep(1100 * time.Millisecond)
	fw2.Add(makeUnhealthyPod("pod-2", "default"))
	waitForPod(t, podCh)
	cancel()
	for range podCh {
	}
	connSpans := findAllSpansByName(sr.Ended(), "k8s.watch.connection")
	if len(connSpans) != 2 {
		t.Fatalf("sibling spans: got %d connection spans, want 2", len(connSpans))
	}
	if connSpans[0].Parent().SpanID() == connSpans[1].SpanContext().SpanID() {
		t.Errorf("sibling spans: second watchSpan is child of first — should be sibling")
	}
	if connSpans[1].Parent().SpanID() == connSpans[0].SpanContext().SpanID() {
		t.Errorf("sibling spans: second watchSpan is child of first — should be sibling")
	}

	// Situation 22: last_resource_version on watchSpan matches last processed pod
	// watchSpan ends after drain — assert after drain
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("resource version: start: got %v, want nil", err)
	}
	rv := makeUnhealthyPod("pod-1", "default")
	rv.ResourceVersion = "999"
	fw.Add(rv)
	waitForPod(t, podCh)
	cancel()

	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("resource version: connection span not found")
	}
	foundRV := false
	for _, attr := range connSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.last_resource_version" {
			if attr.Value.AsString() != "999" {
				t.Errorf("resource version: got %s, want 999", attr.Value.AsString())
			}
			foundRV = true
		}
	}
	if !foundRV {
		t.Errorf("resource version: last_resource_version attribute not found on connection span")
	}

	// Situation 23: enrichment_duration_ms present and non-negative on eventSpan
	// child spans ended before pod sent — assert immediately after waitForPod
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("enrichment duration: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	evSpan = findSpanByName(sr.Ended(), "k8s.watch.event")
	if evSpan == nil {
		t.Fatalf("enrichment duration: event span not found")
	}
	foundEnrichment := false
	for _, attr := range evSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.enrichment_duration_ms" {
			if attr.Value.AsInt64() < 0 {
				t.Errorf("enrichment duration: got %d, want non-negative", attr.Value.AsInt64())
			}
			foundEnrichment = true
		}
	}
	if !foundEnrichment {
		t.Errorf("enrichment duration: attribute not found on event span")
	}
	cancel()
	for range podCh {
	}

	// Situation 24: eventSpan start time is before eventSpan end time
	// child spans ended before pod sent — assert immediately after waitForPod
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("span time order: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	evSpan = findSpanByName(sr.Ended(), "k8s.watch.event")
	if evSpan == nil {
		t.Fatalf("span time order: event span not found")
	}
	if !evSpan.StartTime().Before(evSpan.EndTime()) {
		t.Errorf("span time order: start %v is not before end %v", evSpan.StartTime(), evSpan.EndTime())
	}
	cancel()
	for range podCh {
	}

	// Situation 25: WatchReceivedAt on two consecutive pods is monotonically increasing
	c, fw, _ = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("monotonic: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	r1 := waitForPod(t, podCh)
	p2b := makeUnhealthyPod("pod-2", "default")
	p2b.ResourceVersion = "101"
	fw.Add(p2b)
	r2 := waitForPod(t, podCh)
	if !r2.Pod.WatchReceivedAt.After(r1.Pod.WatchReceivedAt) {
		t.Errorf("monotonic: second WatchReceivedAt %v is not after first %v",
			r2.Pod.WatchReceivedAt, r1.Pod.WatchReceivedAt)
	}
	cancel()
	for range podCh {
	}

	// Situation 26: connection_duration_ms is non-negative on watchSpan
	// watchSpan ends after drain — assert after drain
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("connection duration: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	cancel()
	for range podCh {
	}
	connSpan = findSpanByName(sr.Ended(), "k8s.watch.connection")
	if connSpan == nil {
		t.Fatalf("connection duration: connection span not found")
	}
	for _, attr := range connSpan.Attributes() {
		if string(attr.Key) == "k8s.watch.connection_duration_ms" {
			if attr.Value.AsInt64() < 0 {
				t.Errorf("connection duration: got %d, want non-negative", attr.Value.AsInt64())
			}
		}
	}

	// Situation 27: logsSpan start time is after eventSpan start time
	// enrichment starts after event is received — child spans ended before pod sent
	c, fw, sr = setupTracedClient(t)
	ctx, cancel = context.WithCancel(context.Background())
	podCh, err = c.WatchPods(ctx)
	if err != nil {
		t.Fatalf("logs after event: start: got %v, want nil", err)
	}
	fw.Add(makeUnhealthyPod("pod-1", "default"))
	waitForPod(t, podCh)
	spans = sr.Ended()
	evSpan = findSpanByName(spans, "k8s.watch.event")
	logsSpan = findSpanByName(spans, "k8s.watch.event.fetch_logs")
	if evSpan == nil {
		t.Fatalf("logs after event: event span not found")
	}
	if logsSpan == nil {
		t.Fatalf("logs after event: logs span not found")
	}
	if !logsSpan.StartTime().After(evSpan.StartTime()) {
		t.Errorf("logs after event: logsSpan start %v is not after eventSpan start %v — enrichment must start after event received",
			logsSpan.StartTime(), evSpan.StartTime())
	}
	cancel()
	for range podCh {
	}
}

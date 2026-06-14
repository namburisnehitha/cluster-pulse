package k8

import (
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "github.com/namburisnehitha/cluster-pulse/internal/k8"
	instrumentationVersion = "0.1.0"
)

func getTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(instrumentationVersion),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
}

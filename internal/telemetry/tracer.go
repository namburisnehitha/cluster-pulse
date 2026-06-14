package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

func InitTracer(serviceName string) (func(), error) {

	res, err := resource.New(context.Background(), resource.WithAttributes(semconv.ServiceName(serviceName)))

	if err != nil {
		return nil, err
	}

	exporter, err := otlptracegrpc.New(context.Background(),
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
	)

	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)

	return func() {
		tp.Shutdown(context.Background())
	}, nil
}

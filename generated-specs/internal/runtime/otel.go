package runtime

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// StartTelemetry exports spans over OTLP/HTTP when OTEL_EXPORTER_OTLP_ENDPOINT is set (service.name from
// OTEL_SERVICE_NAME); the returned function flushes them on shutdown. Without the variable it does nothing.
func StartTelemetry(ctx context.Context) (func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return noop, nil
	}
	exporter, err := otlptracehttp.New(ctx)
	if err != nil {
		return noop, err
	}
	name := os.Getenv("OTEL_SERVICE_NAME")
	if name == "" {
		name = "transactional-system-go-chi"
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(attribute.String("service.name", name)))
	if err != nil {
		return noop, err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return provider.Shutdown, nil
}

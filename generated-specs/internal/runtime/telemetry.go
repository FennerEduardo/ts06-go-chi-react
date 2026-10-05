package runtime

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var propagator = propagation.TraceContext{}

// Tracer uses the globally registered provider (configure exporters with the OTEL_* variables).
func Tracer() trace.Tracer { return otel.Tracer("orquestacion-de-saga-idempotente-con-goroutines-en-go122-y-chi") }

// ContextFrom returns a context whose parent is the span described by an incoming traceparent.
func ContextFrom(ctx context.Context, traceparent string) context.Context {
	if traceparent == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
}

// TraceparentOf returns the traceparent header value for a context ("" when it carries no span).
func TraceparentOf(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier["traceparent"]
}

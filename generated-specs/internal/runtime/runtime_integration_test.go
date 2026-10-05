//go:build integration

// Runtime integration tests (docs/RUNTIME-KERNEL.md, IT1-IT7) against PostgreSQL and RabbitMQ.
// Requires DATABASE_URL and AMQP_URL. Run with: go test -tags integration ./internal/runtime/...
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const (
	command = "process_orquestacion_de_saga_idempotente_con_goroutines_en_go122_y_chi"
	event   = "ProcessOrquestacionDeSagaIdempotenteConGoroutinesEnGo122YChiCompleted"
)

var exporter = tracetest.NewInMemoryExporter()

func init() {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
}

type env struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	conn   *amqp.Connection
	schema string
	topo   Topology
}

func uid() string { return strings.ReplaceAll(uuid.NewString(), "-", "")[:12] }

func setup(t *testing.T) *env {
	t.Helper()
	dbURL, amqpURL := os.Getenv("DATABASE_URL"), os.Getenv("AMQP_URL")
	if dbURL == "" || amqpURL == "" {
		t.Fatal("integration tests need DATABASE_URL and AMQP_URL (see docs/RUNTIME-KERNEL.md)")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	must(t, err)
	conn, err := amqp.Dial(amqpURL)
	must(t, err)
	e := &env{ctx: ctx, pool: pool, conn: conn, schema: "it_" + uid(), topo: TopologyFor("it-" + uid())}
	must(t, Migrate(ctx, pool, e.schema))
	exporter.Reset()
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+e.schema+" CASCADE")
		_ = conn.Close()
		pool.Close()
	})
	return e
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func (e *env) count(t *testing.T, table, where string, args ...any) int {
	t.Helper()
	var n int
	must(t, e.pool.QueryRow(e.ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE %s", e.schema, table, where), args...).Scan(&n))
	return n
}

func (e *env) service(t *testing.T) *CommandService {
	svc, err := NewCommandService(e.pool, e.schema)
	must(t, err)
	return svc
}

func (e *env) channel(t *testing.T) *amqp.Channel {
	ch, err := e.conn.Channel()
	must(t, err)
	return ch
}

func drainQueue(t *testing.T, ch *amqp.Channel, queue string) []amqp.Delivery {
	var out []amqp.Delivery
	for {
		d, ok, err := ch.Get(queue, true)
		must(t, err)
		if !ok {
			return out
		}
		out = append(out, d)
	}
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestIT1AtomicWriteAndRollback(t *testing.T) {
	e := setup(t)
	svc := e.service(t)
	res, err := svc.Handle(e.ctx, CommandRequest{TenantID: "t1", AggregateID: "agg-1", Command: command})
	must(t, err)
	if res.Status != "created" || res.EventType != event || res.Version != 1 {
		t.Fatalf("unexpected result %+v", res)
	}
	if snap, _ := svc.Load(e.ctx, "t1", "agg-1"); snap == nil || snap.Version != 1 {
		t.Fatalf("aggregate not persisted: %+v", snap)
	}
	if n := e.count(t, "ghk_outbox", "aggregate_id = $1", "agg-1"); n != 1 {
		t.Fatalf("outbox rows = %d", n)
	}
	_, err = svc.Handle(e.ctx, CommandRequest{TenantID: "t1", AggregateID: "agg-2", Command: "no_such_command", IdempotencyKey: "k-fail"})
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got %v", err)
	}
	if snap, _ := svc.Load(e.ctx, "t1", "agg-2"); snap != nil {
		t.Fatal("aggregate persisted despite domain error")
	}
	if e.count(t, "ghk_outbox", "aggregate_id = $1", "agg-2") != 0 || e.count(t, "ghk_idempotency", "key = $1", "k-fail") != 0 {
		t.Fatal("rollback incomplete")
	}
}

func TestIT2ConcurrentIdempotentRequests(t *testing.T) {
	e := setup(t)
	svc := e.service(t)
	results := make([]CommandResult, 5)
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := svc.Handle(e.ctx, CommandRequest{TenantID: "t1", AggregateID: "agg-1", Command: command, IdempotencyKey: "key-1"})
			results[i] = r
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		must(t, err)
	}
	if n := e.count(t, "ghk_outbox", "true"); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}
	created := 0
	for _, r := range results {
		if r.Status == "created" {
			created++
		}
		if r.EventType != event || r.Version != 1 {
			t.Fatalf("divergent response %+v", r)
		}
	}
	if created != 1 {
		t.Fatalf("created = %d, want 1", created)
	}
}

func TestIT3ConcurrentRelaysPublishExactlyOnce(t *testing.T) {
	e := setup(t)
	svc := e.service(t)
	for i := 0; i < 20; i++ {
		_, err := svc.Handle(e.ctx, CommandRequest{TenantID: "t1", AggregateID: fmt.Sprintf("agg-%d", i), Command: command})
		must(t, err)
	}
	setupCh := e.channel(t)
	must(t, DeclareTopology(setupCh, e.topo))
	totals := make([]int, 2)
	var wg sync.WaitGroup
	for i, worker := range []string{"relay-a", "relay-b"} {
		wg.Add(1)
		go func(i int, worker string) {
			defer wg.Done()
			relay, err := NewOutboxRelay(e.pool, e.channel(t), e.topo.Exchange, e.schema)
			if err != nil {
				t.Error(err)
				return
			}
			for {
				n, err := relay.PublishBatch(e.ctx, worker, 3)
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
				totals[i] += n
			}
		}(i, worker)
	}
	wg.Wait()
	if totals[0]+totals[1] != 20 {
		t.Fatalf("published %v, want 20 in total", totals)
	}
	if n := e.count(t, "ghk_outbox", "published_at IS NULL"); n != 0 {
		t.Fatalf("%d rows unpublished", n)
	}
	waitFor(t, func() bool { q, err := setupCh.QueueDeclarePassive(e.topo.Queue, true, false, false, false, nil); return err == nil && q.Messages == 20 })
	ids := map[string]bool{}
	for _, d := range drainQueue(t, setupCh, e.topo.Queue) {
		ids[d.MessageId] = true
	}
	if len(ids) != 20 {
		t.Fatalf("unique messages = %d, want 20", len(ids))
	}
}

func TestIT4TenantIsolation(t *testing.T) {
	e := setup(t)
	svc := e.service(t)
	_, err := svc.Handle(e.ctx, CommandRequest{TenantID: "tenant-a", AggregateID: "shared-id", Command: command})
	must(t, err)
	if snap, _ := svc.Load(e.ctx, "tenant-b", "shared-id"); snap != nil {
		t.Fatal("tenant-b can read tenant-a's aggregate")
	}
	_, err = svc.Handle(e.ctx, CommandRequest{TenantID: "tenant-b", AggregateID: "shared-id", Command: command})
	must(t, err)
	a, _ := svc.Load(e.ctx, "tenant-a", "shared-id")
	b, _ := svc.Load(e.ctx, "tenant-b", "shared-id")
	if a == nil || b == nil || a.Version != 1 || b.Version != 1 {
		t.Fatalf("tenant aggregates not isolated: a=%+v b=%+v", a, b)
	}
	if n := e.count(t, "ghk_outbox", "tenant_id = $1", "tenant-a"); n != 1 {
		t.Fatalf("tenant-a outbox rows = %d", n)
	}
}

func TestIT5SagaCompensatesInReverseOrder(t *testing.T) {
	e := setup(t)
	saga, err := NewSagaOrchestrator(e.pool, e.schema)
	must(t, err)
	var log []string
	step := func(name string, fail bool) SagaStep {
		return SagaStep{
			Name: name,
			Action: func(context.Context) error {
				if fail {
					return errors.New(name + " failed")
				}
				log = append(log, "do:"+name)
				return nil
			},
			Compensate: func(context.Context) error { log = append(log, "undo:"+name); return nil },
		}
	}
	status, err := saga.Run(e.ctx, "saga-1", "t1", []SagaStep{step("reserve", false), step("charge", false), step("ship", true)})
	must(t, err)
	if status != "COMPENSATED" || !reflect.DeepEqual(log, []string{"do:reserve", "do:charge", "undo:charge", "undo:reserve"}) {
		t.Fatalf("status=%s log=%v", status, log)
	}
	persisted, completed, err := saga.Status(e.ctx, "saga-1")
	must(t, err)
	if persisted != "COMPENSATED" || len(completed) != 0 {
		t.Fatalf("persisted %s %v", persisted, completed)
	}
	if status, err := saga.Run(e.ctx, "saga-2", "t1", []SagaStep{step("reserve", false), step("charge", false)}); err != nil || status != "COMPLETED" {
		t.Fatalf("status=%s err=%v", status, err)
	}
}

func TestIT6InboxDeduplicatesAndDeadLetters(t *testing.T) {
	e := setup(t)
	ch := e.channel(t)
	must(t, DeclareTopology(ch, e.topo))
	var handled []string
	consumer, err := NewInboxConsumer(e.pool, ch, e.topo.Queue, "it-consumer", func(_ context.Context, event map[string]any, meta Meta) error {
		if event["poison"] == true {
			return errors.New("cannot process")
		}
		handled = append(handled, meta.MessageID)
		return nil
	}, e.schema)
	must(t, err)
	publish := func(id, body string) {
		must(t, ch.PublishWithContext(e.ctx, e.topo.Exchange, "Test", false, false, amqp.Publishing{MessageId: id, Headers: amqp.Table{"tenant_id": "t1"}, Body: []byte(body)}))
	}
	publish("m-1", `{"ok": true}`)
	publish("m-1", `{"ok": true}`)
	publish("m-poison", `{"poison": true}`)
	_, err = consumer.Drain(e.ctx, time.Second)
	must(t, err)
	if !reflect.DeepEqual(handled, []string{"m-1"}) {
		t.Fatalf("handled %v", handled)
	}
	if n := e.count(t, "ghk_inbox", "consumer = $1", "it-consumer"); n != 1 {
		t.Fatalf("inbox rows = %d", n)
	}
	waitFor(t, func() bool { q, err := ch.QueueDeclarePassive(e.topo.DLQ, true, false, false, false, nil); return err == nil && q.Messages == 1 })
	dead := drainQueue(t, ch, e.topo.DLQ)
	if len(dead) != 1 || dead[0].MessageId != "m-poison" {
		t.Fatalf("dead-lettered %v", dead)
	}
}

func TestIT7TraceContextPropagation(t *testing.T) {
	e := setup(t)
	svc := e.service(t)
	_, err := svc.Handle(e.ctx, CommandRequest{TenantID: "t1", AggregateID: "agg-1", Command: command, Traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"})
	must(t, err)
	var stored string
	must(t, e.pool.QueryRow(e.ctx, "SELECT traceparent FROM "+e.schema+".ghk_outbox").Scan(&stored))
	if !strings.Contains(stored, "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Fatalf("outbox traceparent %q", stored)
	}
	ch := e.channel(t)
	must(t, DeclareTopology(ch, e.topo))
	relay, err := NewOutboxRelay(e.pool, ch, e.topo.Exchange, e.schema)
	must(t, err)
	if n, err := relay.PublishBatch(e.ctx, "relay", 10); err != nil || n != 1 {
		t.Fatalf("published %d err=%v", n, err)
	}
	received := 0
	consumer, err := NewInboxConsumer(e.pool, e.channel(t), e.topo.Queue, "trace-consumer", func(context.Context, map[string]any, Meta) error { received++; return nil }, e.schema)
	must(t, err)
	_, err = consumer.Drain(e.ctx, time.Second)
	must(t, err)
	if received != 1 {
		t.Fatalf("received %d", received)
	}
	byKind := map[trace.SpanKind]tracetest.SpanStub{}
	for _, s := range exporter.GetSpans() {
		byKind[s.SpanKind] = s
	}
	command, producer, consumerSpan := byKind[trace.SpanKindInternal], byKind[trace.SpanKindProducer], byKind[trace.SpanKindConsumer]
	for name, s := range map[string]tracetest.SpanStub{"command": command, "publish": producer, "consume": consumerSpan} {
		if got := s.SpanContext.TraceID().String(); got != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("%s span trace id = %s", name, got)
		}
	}
	if got := command.Parent.SpanID().String(); got != "00f067aa0ba902b7" {
		t.Fatalf("command span parent = %s", got)
	}
	if consumerSpan.Parent.SpanID() != producer.SpanContext.SpanID() {
		t.Fatal("consume span is not a child of the publish span")
	}
	_ = json.Valid // keep encoding/json for handlers that decode payloads
}

package runtime

// The event broker behind the outbox relay and the inbox consumer, chosen at run time so one image runs on any
// cloud: GHK_BROKER=rabbitmq (AMQP_URL), pubsub (PUBSUB_PROJECT_ID or GOOGLE_CLOUD_PROJECT; PUBSUB_EMULATOR_HOST). Without GHK_BROKER the configured
// variables decide, then stack.messaging.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Topology: publish to Exchange (topic), consume from Queue (subscription); dead letters go to DLX, read from DLQ.
type Topology struct{ Exchange, Queue, DLX, DLQ string }

// TopologyFor returns the topology for a prefix (use one per environment or test).
func TopologyFor(prefix string) Topology {
	return Topology{prefix + ".events", prefix + ".consumer", prefix + ".dlx", prefix + ".dlq"}
}

var DefaultTopology = Topology{"transactional-system-go-chi.events", "transactional-system-go-chi.consumer", "transactional-system-go-chi.dlx", "transactional-system-go-chi.dlq"}

// DefaultBrokerKind is the broker of the generated configuration (stack.messaging).
const DefaultBrokerKind = "rabbitmq"

// BrokerMessage is a domain event on the wire, independent of the broker.
type BrokerMessage struct {
	MessageID, EventType, TenantID, Traceparent string
	Body                                        []byte
}

// EventPublisher publishes to one destination; Publish returns only once the broker has durably accepted the message.
type EventPublisher interface {
	System() string
	Destination() string
	Publish(ctx context.Context, m BrokerMessage) error
	Close() error
}

// Delivery is a received message: Ack once processed; DeadLetter moves it to the DLQ (it is not redelivered).
type Delivery struct {
	Message    BrokerMessage
	Ack        func(ctx context.Context) error
	DeadLetter func(ctx context.Context) error
}

// EventSubscription pulls messages from one queue / subscription.
type EventSubscription interface {
	System() string
	Source() string
	// Poll returns the next waiting message, or nil when there is none.
	Poll(ctx context.Context) (*Delivery, error)
	Close() error
}

type Broker interface {
	System() string
	// DynamicTopology is false when topics cannot be created at run time (Service Bus emulator: fixed Config.json).
	DynamicTopology() bool
	// Declare creates the topology if missing (idempotent); in production it usually comes from infrastructure as code.
	Declare(ctx context.Context, t Topology) error
	Publisher(ctx context.Context, destination string) (EventPublisher, error)
	// Subscribe: t says where dead-lettered messages go.
	Subscribe(ctx context.Context, source string, t Topology) (EventSubscription, error)
	Close() error
}

// BrokerKind is GHK_BROKER, else the broker whose variables are set, else DefaultBrokerKind.
func BrokerKind() string {
	if k := strings.TrimSpace(os.Getenv("GHK_BROKER")); k != "" {
		return strings.ToLower(k)
	}
	switch {
	case os.Getenv("AMQP_URL") != "":
		return "rabbitmq"
	case os.Getenv("PUBSUB_PROJECT_ID") != "" || os.Getenv("PUBSUB_EMULATOR_HOST") != "":
		return "pubsub"
	case os.Getenv("SERVICEBUS_CONNECTION_STRING") != "" || os.Getenv("SERVICEBUS_NAMESPACE") != "":
		return "servicebus"
	case os.Getenv("KAFKA_BOOTSTRAP_SERVERS") != "":
		return "kafka"
	}
	return DefaultBrokerKind
}

// BrokerConfigured reports whether the variables of the selected broker are set.
func BrokerConfigured() bool {
	switch BrokerKind() {
	case "pubsub":
		return os.Getenv("PUBSUB_PROJECT_ID") != "" || os.Getenv("GOOGLE_CLOUD_PROJECT") != ""
	case "servicebus":
		return os.Getenv("SERVICEBUS_CONNECTION_STRING") != "" || os.Getenv("SERVICEBUS_NAMESPACE") != ""
	case "kafka":
		return os.Getenv("KAFKA_BOOTSTRAP_SERVERS") != ""
	default:
		return os.Getenv("AMQP_URL") != ""
	}
}

// ConnectBroker connects to the selected broker.
func ConnectBroker(ctx context.Context) (Broker, error) {
	switch kind := BrokerKind(); kind {
	case "rabbitmq":
		url := os.Getenv("AMQP_URL")
		if url == "" {
			return nil, errors.New("AMQP_URL is not set (amqp://user:password@host:5672)")
		}
		return ConnectRabbitMQ(ctx, url)
	case "pubsub":
		return NewPubSubBrokerFromEnv()
	case "servicebus":
		return nil, errors.New("the Azure Service Bus adapter is not generated: set stack.messaging to \"servicebus\" and regenerate")
	case "kafka":
		return nil, errors.New("the Kafka adapter is not generated: set stack.messaging to \"kafka\" and regenerate")
	default:
		return nil, fmt.Errorf("unsupported GHK_BROKER: %s (rabbitmq, pubsub, servicebus, kafka)", kind)
	}
}

// RabbitMQBroker: RabbitMQ 0-9-1 with a topic exchange, publisher confirms and dead-lettering through the queue's DLX.
type RabbitMQBroker struct{ conn *amqp.Connection }

func ConnectRabbitMQ(ctx context.Context, url string) (*RabbitMQBroker, error) {
	// A busy host can delay the AMQP handshake: retry before failing.
	for attempt := 1; ; attempt++ {
		conn, err := amqp.Dial(url)
		if err == nil {
			return &RabbitMQBroker{conn: conn}, nil
		}
		if attempt == 5 {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(2*attempt) * time.Second):
		}
	}
}

func (b *RabbitMQBroker) System() string        { return "rabbitmq" }
func (b *RabbitMQBroker) DynamicTopology() bool { return true }
func (b *RabbitMQBroker) Close() error          { return b.conn.Close() }

func (b *RabbitMQBroker) Declare(_ context.Context, t Topology) error {
	ch, err := b.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err := ch.ExchangeDeclare(t.Exchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(t.DLX, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(t.DLQ, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(t.DLQ, "", t.DLX, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(t.Queue, true, false, false, false, amqp.Table{"x-dead-letter-exchange": t.DLX}); err != nil {
		return err
	}
	return ch.QueueBind(t.Queue, "#", t.Exchange, false, nil)
}

func (b *RabbitMQBroker) Publisher(_ context.Context, exchange string) (EventPublisher, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, err
	}
	return &rabbitPublisher{ch: ch, exchange: exchange}, nil
}

func (b *RabbitMQBroker) Subscribe(_ context.Context, queue string, _ Topology) (EventSubscription, error) {
	ch, err := b.conn.Channel()
	if err != nil {
		return nil, err
	}
	return &rabbitSubscription{ch: ch, queue: queue}, nil
}

type rabbitPublisher struct {
	ch       *amqp.Channel
	exchange string
}

func (p *rabbitPublisher) System() string      { return "rabbitmq" }
func (p *rabbitPublisher) Destination() string { return p.exchange }
func (p *rabbitPublisher) Close() error        { return p.ch.Close() }

func (p *rabbitPublisher) Publish(ctx context.Context, m BrokerMessage) error {
	headers := amqp.Table{}
	if m.TenantID != "" {
		headers["tenant_id"] = m.TenantID
	}
	if m.Traceparent != "" {
		headers["traceparent"] = m.Traceparent
	}
	confirm, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, p.exchange, m.EventType, false, false, amqp.Publishing{
		MessageId: m.MessageID, DeliveryMode: amqp.Persistent, ContentType: "application/json", Type: m.EventType, Headers: headers, Body: m.Body,
	})
	if err != nil {
		return err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	acked, err := confirm.WaitContext(waitCtx)
	if err != nil {
		return err
	}
	if !acked {
		return amqp.ErrClosed
	}
	return nil
}

type rabbitSubscription struct {
	ch    *amqp.Channel
	queue string
}

func (s *rabbitSubscription) System() string { return "rabbitmq" }
func (s *rabbitSubscription) Source() string { return s.queue }
func (s *rabbitSubscription) Close() error   { return s.ch.Close() }

func (s *rabbitSubscription) Poll(_ context.Context) (*Delivery, error) {
	d, ok, err := s.ch.Get(s.queue, false)
	if err != nil || !ok {
		return nil, err
	}
	tenant, _ := d.Headers["tenant_id"].(string)
	traceparent, _ := d.Headers["traceparent"].(string)
	return &Delivery{
		Message: BrokerMessage{MessageID: d.MessageId, EventType: d.Type, TenantID: tenant, Traceparent: traceparent, Body: d.Body},
		Ack:     func(context.Context) error { return d.Ack(false) },
		// Rejected without requeue: RabbitMQ dead-letters it through the queue's DLX.
		DeadLetter: func(context.Context) error { return d.Nack(false, false) },
	}, nil
}

const metadataToken = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"

// PubSubBroker: Google Cloud Pub/Sub over its REST API (no client library): topics for the exchange and the DLX, pull
// subscriptions for the queue and the DLQ. Publish returns once Pub/Sub has stored the message. A failed message is
// dead-lettered by the consumer: published to the DLX topic, then acknowledged.
// Auth: none against the emulator (PUBSUB_EMULATOR_HOST); otherwise GOOGLE_OAUTH_ACCESS_TOKEN or the metadata-server
// token of the workload's service account (Cloud Run, GKE workload identity).
type PubSubBroker struct {
	project, root string
	emulator      bool
	http          *http.Client
	mu            sync.Mutex
	token         string
	expiresAt     time.Time
}

func NewPubSubBroker(project, emulatorHost string) *PubSubBroker {
	root := "https://pubsub.googleapis.com"
	if emulatorHost != "" {
		root = "http://" + emulatorHost
	}
	return &PubSubBroker{project: project, root: root + "/v1/projects/" + project, emulator: emulatorHost != "", http: &http.Client{Timeout: 30 * time.Second}}
}

func NewPubSubBrokerFromEnv() (*PubSubBroker, error) {
	project := os.Getenv("PUBSUB_PROJECT_ID")
	if project == "" {
		project = os.Getenv("GOOGLE_CLOUD_PROJECT")
	}
	if project == "" {
		return nil, errors.New("PUBSUB_PROJECT_ID (or GOOGLE_CLOUD_PROJECT) is not set")
	}
	return NewPubSubBroker(project, os.Getenv("PUBSUB_EMULATOR_HOST")), nil
}

func (b *PubSubBroker) System() string        { return "gcp_pubsub" }
func (b *PubSubBroker) DynamicTopology() bool { return true }
func (b *PubSubBroker) Close() error          { return nil }

func (b *PubSubBroker) bearer(ctx context.Context) (string, error) {
	if b.emulator {
		return "", nil
	}
	if tok := os.Getenv("GOOGLE_OAUTH_ACCESS_TOKEN"); tok != "" {
		return tok, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.token == "" || time.Now().After(b.expiresAt) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataToken, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("Metadata-Flavor", "Google")
		res, err := b.http.Do(req)
		if err != nil {
			return "", err
		}
		defer res.Body.Close()
		var body struct {
			AccessToken string `json:"access_token"`
			ExpiresIn   int    `json:"expires_in"`
		}
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			return "", err
		}
		b.token, b.expiresAt = body.AccessToken, time.Now().Add(time.Duration(body.ExpiresIn-60)*time.Second)
	}
	return b.token, nil
}

// call sends a request to the API; a status listed in accepted returns (nil, nil) instead of failing.
func (b *PubSubBroker) call(ctx context.Context, method, path string, body any, accepted ...int) (map[string]any, error) {
	var payload io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		payload = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, b.root+path, payload)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if tok, err := b.bearer(ctx); err != nil {
		return nil, err
	} else if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	res, err := b.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode/100 == 2 {
		out := map[string]any{}
		if len(bytes.TrimSpace(raw)) > 0 {
			if err := json.Unmarshal(raw, &out); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	for _, status := range accepted {
		if res.StatusCode == status {
			return nil, nil
		}
	}
	return nil, fmt.Errorf("pub/sub %s %s failed: %d %s", method, path, res.StatusCode, raw)
}

func (b *PubSubBroker) Declare(ctx context.Context, t Topology) error {
	// 409: it already exists (created earlier or by infrastructure as code).
	steps := []struct {
		path string
		body any
	}{
		{"/topics/" + t.Exchange, map[string]any{}},
		{"/topics/" + t.DLX, map[string]any{}},
		{"/subscriptions/" + t.Queue, map[string]any{"topic": "projects/" + b.project + "/topics/" + t.Exchange, "ackDeadlineSeconds": 60}},
		{"/subscriptions/" + t.DLQ, map[string]any{"topic": "projects/" + b.project + "/topics/" + t.DLX, "ackDeadlineSeconds": 60}},
	}
	for _, s := range steps {
		if _, err := b.call(ctx, http.MethodPut, s.path, s.body, http.StatusConflict); err != nil {
			return err
		}
	}
	return nil
}

func (b *PubSubBroker) publishTo(ctx context.Context, topic string, m BrokerMessage) error {
	attributes := map[string]string{"ghk_message_id": m.MessageID}
	if m.EventType != "" {
		attributes["event_type"] = m.EventType
	}
	if m.TenantID != "" {
		attributes["tenant_id"] = m.TenantID
	}
	if m.Traceparent != "" {
		attributes["traceparent"] = m.Traceparent
	}
	res, err := b.call(ctx, http.MethodPost, "/topics/"+topic+":publish",
		map[string]any{"messages": []any{map[string]any{"data": base64.StdEncoding.EncodeToString(m.Body), "attributes": attributes}}})
	if err != nil {
		return err
	}
	if ids, _ := res["messageIds"].([]any); len(ids) != 1 {
		return fmt.Errorf("pub/sub did not accept message %s", m.MessageID)
	}
	return nil
}

func (b *PubSubBroker) Publisher(_ context.Context, topic string) (EventPublisher, error) {
	return &pubsubPublisher{b: b, topic: topic}, nil
}

func (b *PubSubBroker) Subscribe(_ context.Context, subscription string, t Topology) (EventSubscription, error) {
	return &pubsubSubscription{b: b, subscription: subscription, t: t}, nil
}

type pubsubPublisher struct {
	b     *PubSubBroker
	topic string
}

func (p *pubsubPublisher) System() string      { return "gcp_pubsub" }
func (p *pubsubPublisher) Destination() string { return p.topic }
func (p *pubsubPublisher) Close() error        { return nil }
func (p *pubsubPublisher) Publish(ctx context.Context, m BrokerMessage) error {
	return p.b.publishTo(ctx, p.topic, m)
}

type pubsubSubscription struct {
	b            *PubSubBroker
	subscription string
	t            Topology
}

func (s *pubsubSubscription) System() string { return "gcp_pubsub" }
func (s *pubsubSubscription) Source() string { return s.subscription }
func (s *pubsubSubscription) Close() error   { return nil }

func (s *pubsubSubscription) Poll(ctx context.Context) (*Delivery, error) {
	res, err := s.b.call(ctx, http.MethodPost, "/subscriptions/"+s.subscription+":pull", map[string]any{"maxMessages": 1, "returnImmediately": true})
	if err != nil {
		return nil, err
	}
	received, _ := res["receivedMessages"].([]any)
	if len(received) == 0 {
		return nil, nil
	}
	first, _ := received[0].(map[string]any)
	msg, _ := first["message"].(map[string]any)
	attrs, _ := msg["attributes"].(map[string]any)
	attr := func(key string) string { v, _ := attrs[key].(string); return v }
	ackID, _ := first["ackId"].(string)
	data, _ := msg["data"].(string)
	body, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return nil, err
	}
	id := attr("ghk_message_id")
	if id == "" {
		id, _ = msg["messageId"].(string)
	}
	m := BrokerMessage{MessageID: id, EventType: attr("event_type"), TenantID: attr("tenant_id"), Traceparent: attr("traceparent"), Body: body}
	ack := func(ctx context.Context) error {
		_, err := s.b.call(ctx, http.MethodPost, "/subscriptions/"+s.subscription+":acknowledge", map[string]any{"ackIds": []string{ackID}})
		return err
	}
	return &Delivery{Message: m, Ack: ack, DeadLetter: func(ctx context.Context) error {
		if err := s.b.publishTo(ctx, s.t.DLX, m); err != nil {
			return err
		}
		return ack(ctx)
	}}, nil
}

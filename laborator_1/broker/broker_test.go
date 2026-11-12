package broker

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_1/contract"
)

// fakeClock permite testarea determinista a lease-urilor si backoff-ului.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func newTestBroker() (*Broker, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)}
	cfg := DefaultConfig()
	cfg.Now = clk.Now
	cfg.MaxAttempts = 3
	cfg.DefaultLease = 10 * time.Second
	cfg.BackoffBase = time.Second
	cfg.BackoffMax = 8 * time.Second
	return New(cfg), clk
}

func msg(key string, seq uint64) contract.Message {
	return contract.Message{
		ID:        contract.NewID(),
		Type:      contract.TypeCredit,
		Version:   contract.SchemaVersion,
		Source:    "test-producer",
		Key:       key,
		Seq:       seq,
		CreatedAt: time.Now(),
		Payload:   json.RawMessage(`{"account":"` + key + `","amount":10}`),
	}
}

func recv(b *Broker, n int) []contract.Delivery {
	return b.Receive(context.Background(), "q", "c1", n, 0, 0)
}

func TestPublishReceiveAck(t *testing.T) {
	b, _ := newTestBroker()
	m := msg("A", 1)
	res, err := b.Publish("q", m)
	if err != nil || res.Status != "accepted" || res.Offset != 1 {
		t.Fatalf("publish: %+v %v", res, err)
	}
	ds := recv(b, 10)
	if len(ds) != 1 || ds[0].Message.ID != m.ID || ds[0].Attempt != 1 {
		t.Fatalf("receive: %+v", ds)
	}
	if again := recv(b, 10); len(again) != 0 {
		t.Fatal("mesajul in zbor nu trebuie livrat altui consumer")
	}
	if err := b.Ack("q", ds[0].DeliveryID); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("q", ds[0].DeliveryID); !errors.Is(err, ErrStaleDelivery) {
		t.Fatalf("ACK dublu trebuie respins, primit %v", err)
	}
	s := b.Stats()[0]
	if s.Ready+s.InFlight+s.Dead != 0 || s.Counters.Acked != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestPublishIsIdempotentByID(t *testing.T) {
	b, _ := newTestBroker()
	m := msg("A", 1)
	b.Publish("q", m)
	res, err := b.Publish("q", m) // producer-ul nu a primit ACK si retrimite
	if err != nil || res.Status != "duplicate" {
		t.Fatalf("astepta duplicate, primit %+v %v", res, err)
	}
	if n := len(recv(b, 10)); n != 1 {
		t.Fatalf("astepta 1 mesaj in coada, sunt %d", n)
	}
}

func TestInvalidMessageRejected(t *testing.T) {
	b, _ := newTestBroker()
	m := msg("A", 1)
	m.Payload = json.RawMessage(`"nu-e-obiect"`)
	if _, err := b.Publish("q", m); !contract.IsValidationError(err) {
		t.Fatalf("astepta eroare de validare, primit %v", err)
	}
}

func TestStaleSequenceRejected(t *testing.T) {
	b, _ := newTestBroker()
	b.Publish("q", msg("A", 2))
	if _, err := b.Publish("q", msg("A", 1)); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("seq vechi acceptat: %v", err)
	}
	if _, err := b.Publish("q", msg("B", 1)); err != nil {
		t.Fatalf("alta cheie are secventa proprie: %v", err)
	}
}

// Scenariul critic din perspectiva brokerului: consumer-ul primeste mesajul,
// dispare inainte de ACK -> dupa expirarea lease-ului mesajul este relivrat,
// iar ACK-ul intarziat al primei tentative este respins.
func TestLeaseExpiryRedelivers(t *testing.T) {
	b, clk := newTestBroker()
	m := msg("A", 1)
	b.Publish("q", m)
	first := recv(b, 1)[0]

	clk.Advance(9 * time.Second)
	if len(recv(b, 1)) != 0 {
		t.Fatal("redat inainte de expirarea lease-ului")
	}
	clk.Advance(2 * time.Second)
	second := recv(b, 1)
	if len(second) != 1 || second[0].Message.ID != m.ID || second[0].Attempt != 2 {
		t.Fatalf("astepta relivrare cu attempt=2, primit %+v", second)
	}
	if second[0].DeliveryID == first.DeliveryID {
		t.Fatal("fiecare tentativa trebuie sa aiba alt delivery_id")
	}
	if err := b.Ack("q", first.DeliveryID); !errors.Is(err, ErrStaleDelivery) {
		t.Fatalf("ACK-ul tentativei expirate trebuie respins, primit %v", err)
	}
	if err := b.Ack("q", second[0].DeliveryID); err != nil {
		t.Fatal(err)
	}
	if c := b.Stats()[0].Counters; c.Expired != 1 || c.Redelivered != 1 {
		t.Fatalf("counters: %+v", c)
	}
}

func TestLongPollWakesOnPublish(t *testing.T) {
	b := New(DefaultConfig())
	done := make(chan []contract.Delivery)
	go func() {
		done <- b.Receive(context.Background(), "q", "c", 1, time.Second, 2*time.Second)
	}()
	time.Sleep(50 * time.Millisecond)
	b.Publish("q", msg("A", 1))
	select {
	case ds := <-done:
		if len(ds) != 1 {
			t.Fatalf("long-poll a intors %d mesaje", len(ds))
		}
	case <-time.After(time.Second):
		t.Fatal("long-poll nu s-a trezit la publicare")
	}
}

func TestNackRetryWithExponentialBackoff(t *testing.T) {
	b, clk := newTestBroker()
	b.Publish("q", msg("A", 1))

	d1 := recv(b, 1)[0]
	r, err := b.Nack("q", d1.DeliveryID, "db indisponibila", true)
	if err != nil || r.Outcome != "retry_scheduled" {
		t.Fatalf("nack 1: %+v %v", r, err)
	}
	if len(recv(b, 1)) != 0 {
		t.Fatal("livrat inainte de expirarea backoff-ului")
	}
	clk.Advance(time.Second) // backoff 1: 1s
	d2 := recv(b, 1)
	if len(d2) != 1 || d2[0].Attempt != 2 {
		t.Fatalf("astepta tentativa 2: %+v", d2)
	}
	b.Nack("q", d2[0].DeliveryID, "db indisponibila", true)
	clk.Advance(1500 * time.Millisecond) // backoff 2: 2s -> inca nu
	if len(recv(b, 1)) != 0 {
		t.Fatal("backoff-ul nu a crescut exponential")
	}
	clk.Advance(500 * time.Millisecond)
	d3 := recv(b, 1)
	if len(d3) != 1 || d3[0].Attempt != 3 {
		t.Fatalf("astepta tentativa 3: %+v", d3)
	}
	// a 3-a tentativa = MaxAttempts -> DLQ
	r, _ = b.Nack("q", d3[0].DeliveryID, "db indisponibila", true)
	if r.Outcome != "dead_lettered" {
		t.Fatalf("astepta DLQ dupa MaxAttempts, primit %+v", r)
	}
	dl := b.DeadLetters("q")
	if len(dl) != 1 || dl[0].Attempts != 3 {
		t.Fatalf("DLQ: %+v", dl)
	}
}

func TestPermanentNackGoesStraightToDLQ(t *testing.T) {
	b, _ := newTestBroker()
	b.Publish("q", msg("A", 1))
	d := recv(b, 1)[0]
	r, _ := b.Nack("q", d.DeliveryID, "suma negativa", false)
	if r.Outcome != "dead_lettered" || r.Attempt != 1 {
		t.Fatalf("%+v", r)
	}
	if s := b.Stats()[0]; s.Dead != 1 || s.Ready != 0 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestRepeatedLeaseExpiryEndsInDLQ(t *testing.T) {
	b, clk := newTestBroker()
	b.Publish("q", msg("A", 1))
	for i := 0; i < 3; i++ {
		if len(recv(b, 1)) != 1 {
			t.Fatalf("tentativa %d nu a fost livrata", i+1)
		}
		clk.Advance(11 * time.Second)
	}
	if len(recv(b, 1)) != 0 {
		t.Fatal("mesajul trebuia sa fie in DLQ")
	}
	if dl := b.DeadLetters("q"); len(dl) != 1 {
		t.Fatalf("DLQ: %+v", dl)
	}
}

func TestReplayFromDLQ(t *testing.T) {
	b, _ := newTestBroker()
	m := msg("A", 1)
	b.Publish("q", m)
	d := recv(b, 1)[0]
	b.Nack("q", d.DeliveryID, "bug in consumer", false)
	if err := b.Replay("q", m.ID); err != nil {
		t.Fatal(err)
	}
	ds := recv(b, 1)
	if len(ds) != 1 || ds[0].Attempt != 1 {
		t.Fatalf("replay: %+v", ds)
	}
	if err := b.Replay("q", m.ID); !errors.Is(err, ErrNotFound) {
		t.Fatal("replay pe mesaj care nu e in DLQ")
	}
	// un mesaj din DLQ ramane in fereastra de deduplicare
	if res, _ := b.Publish("q", m); res.Status != "duplicate" {
		t.Fatal("republicarea unui ID cunoscut trebuie ignorata")
	}
}

func TestOrderingPerKeyBlocksUntilAck(t *testing.T) {
	b, _ := newTestBroker()
	a1, a2, b1 := msg("A", 1), msg("A", 2), msg("B", 1)
	b.Publish("q", a1)
	b.Publish("q", a2)
	b.Publish("q", b1)

	ds := recv(b, 10)
	if len(ds) != 2 || ds[0].Message.ID != a1.ID || ds[1].Message.ID != b1.ID {
		t.Fatalf("astepta capetele cheilor A si B, primit %d mesaje", len(ds))
	}
	if len(recv(b, 10)) != 0 {
		t.Fatal("A#2 nu trebuie livrat cat timp A#1 e in zbor")
	}
	b.Ack("q", ds[0].DeliveryID)
	next := recv(b, 10)
	if len(next) != 1 || next[0].Message.ID != a2.ID {
		t.Fatalf("dupa ACK pe A#1 trebuia livrat A#2: %+v", next)
	}
}

func TestOrderingPreservedAcrossRetry(t *testing.T) {
	b, clk := newTestBroker()
	a1, a2 := msg("A", 1), msg("A", 2)
	b.Publish("q", a1)
	b.Publish("q", a2)
	d := recv(b, 10)[0]
	b.Nack("q", d.DeliveryID, "temporar", true)
	if len(recv(b, 10)) != 0 {
		t.Fatal("A#2 a depasit A#1 aflat in backoff")
	}
	clk.Advance(time.Second)
	if ds := recv(b, 10); len(ds) != 1 || ds[0].Message.ID != a1.ID {
		t.Fatalf("astepta A#1 reincercat: %+v", ds)
	}
}

func TestDeadLetterUnblocksKey(t *testing.T) {
	b, _ := newTestBroker()
	a1, a2 := msg("A", 1), msg("A", 2)
	b.Publish("q", a1)
	b.Publish("q", a2)
	d := recv(b, 10)[0]
	b.Nack("q", d.DeliveryID, "otravit", false)
	if ds := recv(b, 10); len(ds) != 1 || ds[0].Message.ID != a2.ID {
		t.Fatalf("dupa DLQ cheia trebuie deblocata: %+v", ds)
	}
}

func TestCompetingConsumersDifferentKeys(t *testing.T) {
	b, _ := newTestBroker()
	for _, k := range []string{"A", "B", "C"} {
		b.Publish("q", msg(k, 1))
	}
	c1 := b.Receive(context.Background(), "q", "c1", 2, 0, 0)
	c2 := b.Receive(context.Background(), "q", "c2", 2, 0, 0)
	if len(c1) != 2 || len(c2) != 1 {
		t.Fatalf("distributie: c1=%d c2=%d", len(c1), len(c2))
	}
}

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

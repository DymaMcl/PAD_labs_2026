package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T, path string, clk *fakeClock) *Broker {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Now = clk.Now
	cfg.MaxAttempts = 3
	cfg.DefaultLease = 10 * time.Second
	cfg.BackoffBase = time.Second
	b, err := Open(path, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Brokerul cade (fara Close) si este repornit: mesajele confirmate producer-ului
// nu se pierd, cele confirmate de consumer nu reapar, iar mesajele care erau
// in zbor sunt relivrate cu contorul de tentative pastrat.
func TestRecoveryAfterCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.journal")
	clk := &fakeClock{t: time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)}

	b := openTest(t, path, clk)
	acked, inflight, waiting, poison := msg("A", 1), msg("B", 1), msg("C", 1), msg("D", 1)
	b.Publish("q", acked)
	b.Publish("q", inflight)
	b.Publish("q", waiting)
	b.Publish("q", poison)
	ds := recv(b, 4) // toate 4 sunt capete de cheie
	byID := map[string]string{}
	for _, d := range ds {
		byID[d.Message.ID] = d.DeliveryID
	}
	b.Ack("q", byID[acked.ID])
	b.Nack("q", byID[poison.ID], "otravit", false)
	b.Nack("q", byID[waiting.ID], "temporar", true)
	// "inflight" ramane neconfirmat -> simulam caderea brokerului
	b.j.f.Close()

	b2 := openTest(t, path, clk)
	defer b2.Close()
	s := b2.Stats()[0]
	if s.Dead != 1 || s.Ready+s.Delayed != 2 || s.InFlight != 0 {
		t.Fatalf("stare dupa recuperare: %+v", s)
	}
	clk.Advance(2 * time.Second)
	got := map[string]int{}
	for _, d := range recv(b2, 10) {
		got[d.Message.ID] = d.Attempt
	}
	if got[inflight.ID] != 2 || got[waiting.ID] != 2 || len(got) != 2 {
		t.Fatalf("relivrari dupa recuperare: %v", got)
	}
	if res, _ := b2.Publish("q", acked); res.Status != "duplicate" {
		t.Fatal("deduplicarea trebuie sa supravietuiasca restartului")
	}
	if _, err := b2.Publish("q", msg("A", 1)); !errors.Is(err, ErrStaleSequence) {
		t.Fatalf("secventele trebuie sa supravietuiasca restartului: %v", err)
	}
	if dl := b2.DeadLetters("q"); len(dl) != 1 || dl[0].Reason != "otravit" {
		t.Fatalf("DLQ dupa recuperare: %+v", dl)
	}
}

func TestTruncatedLastLineIsIgnored(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.journal")
	clk := &fakeClock{t: time.Now()}
	b := openTest(t, path, clk)
	b.Publish("q", msg("A", 1))
	b.Close()

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	f.WriteString(`{"op":"publish","q":"q","msg":{"id":"x`) // scriere intrerupta
	f.Close()

	b2 := openTest(t, path, clk)
	defer b2.Close()
	if s := b2.Stats()[0]; s.Ready != 1 {
		t.Fatalf("astepta 1 mesaj, stare %+v", s)
	}
}

func TestReopenTwiceIsStable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "broker.journal")
	clk := &fakeClock{t: time.Now()}
	b := openTest(t, path, clk)
	b.Publish("q", msg("A", 1))
	b.Publish("q", msg("A", 2))
	b.Close()
	for i := 0; i < 3; i++ {
		b = openTest(t, path, clk)
		if s := b.Stats()[0]; s.Ready != 2 || s.Counters.Published != 2 {
			t.Fatalf("redeschiderea %d: %+v", i, s)
		}
		b.Close()
	}
}

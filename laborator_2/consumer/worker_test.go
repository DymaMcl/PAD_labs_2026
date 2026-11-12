package consumer

import (
	"context"
	"errors"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/broker"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/client"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
)

func startBroker(t *testing.T) *client.Client {
	t.Helper()
	cfg := broker.DefaultConfig()
	cfg.MaxAttempts = 4
	cfg.BackoffBase = 20 * time.Millisecond
	cfg.BackoffMax = 100 * time.Millisecond
	ts := httptest.NewServer(broker.NewServer(broker.New(cfg)))
	t.Cleanup(ts.Close)
	return client.New(ts.URL)
}

func publishCredits(t *testing.T, c *client.Client, key string, amounts ...int64) []contract.Message {
	t.Helper()
	var out []contract.Message
	for i, a := range amounts {
		m, _ := contract.New(contract.TypeCredit, "p", key, uint64(i+1), contract.CreditPayload{Account: key, Amount: a})
		if _, err := c.Publish(context.Background(), "plati", m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

// runCriticalScenario: 5 credite x 100. Consumer-ul cade dupa efectul
// mesajului #3, inainte de ACK. Este repornit (aceeasi stare locala) si
// termina coada dupa expirarea lease-ului.
func runCriticalScenario(t *testing.T, idempotent bool) Summary {
	c := startBroker(t)
	publishCredits(t, c, "ACC-1", 100, 100, 100, 100, 100)
	path := filepath.Join(t.TempDir(), "ledger.json")
	lease := 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	l1, _ := OpenLedger(path)
	w1 := NewLocalWorker(c, l1, idempotent, Options{Queue: "plati", Name: "c1", Lease: lease, CrashAfterEffect: 3})
	if err := w1.Run(ctx); !errors.Is(err, ErrCrashed) {
		t.Fatalf("astepta cadere simulata, primit %v", err)
	}

	// "repornire": stare reincarcata de pe disc, proces nou
	l2, err := OpenLedger(path)
	if err != nil {
		t.Fatal(err)
	}
	w2 := NewLocalWorker(c, l2, idempotent, Options{Queue: "plati", Name: "c1-restart", Lease: lease, Wait: 100 * time.Millisecond})
	if err := w2.RunUntilIdle(ctx, lease+400*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	return l2.Summary()
}

func TestCriticalScenarioNaiveConsumerDuplicatesEffect(t *testing.T) {
	s := runCriticalScenario(t, false)
	if s.Total != 600 || s.DuplicateEffect != 1 {
		t.Fatalf("consumer-ul naiv trebuia sa dubleze mesajul #3 (total 600): %+v", s)
	}
}

func TestCriticalScenarioIdempotentConsumerIsExactlyOnceEffect(t *testing.T) {
	s := runCriticalScenario(t, true)
	if s.Total != 500 || s.DuplicateEffect != 0 || s.InboxSize != 5 || s.OrderViolations != 0 {
		t.Fatalf("consumer-ul idempotent trebuia sa ajunga la 500 fara duplicate: %+v", s)
	}
}

func TestPoisonMessageGoesToDLQ(t *testing.T) {
	c := startBroker(t)
	publishCredits(t, c, "ACC-1", 100, -5, 100)
	l, _ := OpenLedger("")
	w := NewLocalWorker(c, l, true, Options{Queue: "plati", Name: "c"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.RunUntilIdle(ctx, 300*time.Millisecond)
	if s := l.Summary(); s.Total != 200 || s.Messages != 2 {
		t.Fatalf("sumar: %+v", s)
	}
}

func TestTransientFailuresAreRetried(t *testing.T) {
	c := startBroker(t)
	publishCredits(t, c, "ACC-1", 10, 20, 30, 40)
	publishCredits(t, c, "ACC-2", 1, 2, 3)
	l, _ := OpenLedger("")
	w := NewLocalWorker(c, l, true, Options{Queue: "plati", Name: "c", FailRate: 0.3, Batch: 2})
	w.rnd.Seed(42)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.RunUntilIdle(ctx, time.Second)
	s := l.Summary()
	// cu 4 tentative si 30% esec, cu seed fix toate ajung procesate
	if s.Balances["ACC-1"] != 100 || s.Balances["ACC-2"] != 6 || s.OrderViolations != 0 {
		t.Fatalf("sumar: %+v", s)
	}
}

func TestLedgerSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "l.json")
	l, _ := OpenLedger(path)
	m, _ := contract.New(contract.TypeCredit, "p", "A", 1, contract.CreditPayload{Account: "A", Amount: 7})
	if err := l.ApplyCredit(m, contract.CreditPayload{Account: "A", Amount: 7}, true); err != nil {
		t.Fatal(err)
	}
	l2, _ := OpenLedger(path)
	if !l2.Seen(m.ID) || l2.Summary().Total != 7 {
		t.Fatal("inbox-ul si efectul trebuie persistate impreuna")
	}
	if err := l2.ApplyCredit(m, contract.CreditPayload{Account: "A", Amount: 7}, true); !errors.Is(err, ErrAlreadyApplied) {
		t.Fatalf("aplicarea repetata trebuie refuzata: %v", err)
	}
}

package integration

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/accounts"
)

type fixture struct {
	srv  *accounts.Server
	g    *grpc.Server
	addr string
	c    *Client
}

func setup(t *testing.T, so accounts.Options, opts accounts.ServerOptions, cfg Config) *fixture {
	t.Helper()
	store, _ := accounts.Open("", so)
	srv := accounts.NewServer(store, opts)
	g := srv.GRPC()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	if cfg.PerAttemptTimeout == 0 {
		cfg.PerAttemptTimeout = 300 * time.Millisecond
	}
	cfg.BackoffBase = 10 * time.Millisecond
	cfg.BackoffMax = 40 * time.Millisecond
	c, err := Dial(lis.Addr().String(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return &fixture{srv: srv, g: g, addr: lis.Addr().String(), c: c}
}

func bal(f *fixture, acc string) int64 {
	b, _ := f.srv.Store.Balance(acc)
	return b
}

// SCENARIUL CRITIC: raspunsul se pierde dupa efectuarea operatiei.
// Clientul reincearca cu aceeasi cheie si primeste rezultatul original.
func TestCriticalLostResponseResolvedByIdempotentRetry(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{})
	f.srv.SetFaults(accounts.Faults{DropResponseNext: 1})
	r := f.c.Credit(context.Background(), CreditInput{Key: "msg-1", Account: "ACC-1", Amount: 100})
	if r.Outcome != Applied || !r.Replayed || r.Attempts != 2 || r.BalanceAfter != 100 {
		t.Fatalf("rezultat: %+v", r)
	}
	if bal(f, "ACC-1") != 100 || f.srv.Stats().Executed != 1 {
		t.Fatalf("efect dublat: sold=%d stats=%+v", bal(f, "ACC-1"), f.srv.Stats())
	}
}

// Anti-modelul: cheie noua la fiecare incercare -> efect dublu.
func TestCriticalNaiveClientDuplicatesEffect(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{})
	f.srv.SetFaults(accounts.Faults{DropResponseNext: 1})
	if _, _, err := NaiveCredit(context.Background(), f.c, "ACC-1", 100, 3); err != nil {
		t.Fatal(err)
	}
	if bal(f, "ACC-1") != 200 {
		t.Fatalf("clientul naiv trebuia sa dubleze efectul, sold=%d", bal(f, "ACC-1"))
	}
}

// Raspuns intarziat dupa commit: clientul isi depaseste deadline-ul, dar
// operatia a avut loc; reincercarea o descopera, fara efect dublu.
func TestSlowResponseAfterCommit(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{})
	f.srv.SetFaults(accounts.Faults{SlowResponseMs: 500})
	r := f.c.Credit(context.Background(), CreditInput{Key: "slow", Account: "ACC-2", Amount: 70})
	if r.Outcome != Applied || bal(f, "ACC-2") != 70 {
		t.Fatalf("rezultat: %+v sold=%d", r, bal(f, "ACC-2"))
	}
}

// Toate raspunsurile se pierd: reincercarile nu rezolva, interogarea
// GetOperation confirma ca operatia a avut loc.
func TestAllResponsesLostResolvedByGetOperation(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{MaxAttempts: 3, BreakerThreshold: 10})
	f.srv.SetFaults(accounts.Faults{DropResponseRate: 1})
	r := f.c.Credit(context.Background(), CreditInput{Key: "lost-all", Account: "ACC-1", Amount: 30})
	if r.Outcome != Applied || r.Attempts != 3 || bal(f, "ACC-1") != 30 {
		t.Fatalf("rezultat: %+v sold=%d", r, bal(f, "ACC-1"))
	}
	if f.srv.Stats().Executed != 1 || f.srv.Stats().Replayed != 2 {
		t.Fatalf("stats: %+v", f.srv.Stats())
	}
}

// Serviciul respinge INAINTE de executie de fiecare data: GetOperation
// confirma NOT_FOUND -> stim sigur ca efectul nu exista.
func TestNeverExecutedIsNotApplied(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{MaxAttempts: 3, BreakerThreshold: 10})
	f.srv.SetFaults(accounts.Faults{UnavailableNext: 100})
	r := f.c.Credit(context.Background(), CreditInput{Key: "never", Account: "ACC-1", Amount: 30})
	if r.Outcome != NotApplied || bal(f, "ACC-1") != 0 {
		t.Fatalf("rezultat: %+v", r)
	}
}

// Serviciul complet cazut: rezultatul este UNKNOWN (nu "esec"), apoi
// circuitul se deschide si cererile urmatoare nu mai sunt trimise.
func TestServiceDownUnknownThenCircuitOpen(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{MaxAttempts: 2, BreakerThreshold: 3, BreakerCooldown: time.Hour})
	f.g.Stop()
	r := f.c.Credit(context.Background(), CreditInput{Key: "down", Account: "ACC-1", Amount: 1})
	if r.Outcome != Unknown || !errors.Is(r.Err, ErrAmbiguous) {
		t.Fatalf("astepta UNKNOWN, primit %+v", r)
	}
	r = f.c.Credit(context.Background(), CreditInput{Key: "down2", Account: "ACC-1", Amount: 1})
	if f.c.Breaker.State() != BreakerOpen {
		t.Fatalf("circuitul trebuia deschis, stare %s (%+v)", f.c.Breaker.State(), r)
	}
	start := time.Now()
	r = f.c.Credit(context.Background(), CreditInput{Key: "down3", Account: "ACC-1", Amount: 1})
	if r.Outcome != NotApplied || !errors.Is(r.Err, ErrCircuitOpen) || time.Since(start) > 50*time.Millisecond {
		t.Fatalf("fallback cu circuit deschis: %+v", r)
	}
}

func TestBusinessRejectionNotRetried(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{})
	r := f.c.Credit(context.Background(), CreditInput{Key: "big", Account: "ACC-1", Amount: accounts.MaxCredit + 1})
	if r.Outcome != Rejected || r.Attempts != 1 || r.Reason == "" {
		t.Fatalf("rezultat: %+v", r)
	}
	r = f.c.Credit(context.Background(), CreditInput{Key: "bad", Account: "nope", Amount: 1})
	if r.Outcome != Rejected || r.Attempts != 1 {
		t.Fatalf("cerere invalida: %+v", r)
	}
}

// Server vechi (doar v1): clientul negociaza automat versiunea.
func TestContractFallbackToV1(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{DisableV2: true}, Config{})
	if f.c.APIVersion() != "v2" {
		t.Fatal("clientul porneste cu v2")
	}
	r := f.c.Credit(context.Background(), CreditInput{Key: "k", Account: "ACC-1", Amount: 5})
	if r.Outcome != Applied || f.c.APIVersion() != "v1" {
		t.Fatalf("fallback: %+v versiune=%s", r, f.c.APIVersion())
	}
}

// Cancellation de la apelant in timpul operatiei: serverul abandoneaza
// inainte de commit, iar clientul stabileste ca efectul NU exista.
func TestCallerCancellation(t *testing.T) {
	f := setup(t, accounts.Options{ProcessingTime: 400 * time.Millisecond}, accounts.ServerOptions{},
		Config{PerAttemptTimeout: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(80*time.Millisecond, cancel)
	r := f.c.Credit(ctx, CreditInput{Key: "cancel", Account: "ACC-1", Amount: 9})
	if r.Outcome != NotApplied {
		t.Fatalf("rezultat: %+v", r)
	}
	time.Sleep(500 * time.Millisecond)
	if bal(f, "ACC-1") != 0 {
		t.Fatal("operatia anulata a fost aplicata")
	}
}

// Deadline-ul apelantului (ex. lease-ul mesajului) limiteaza toate incercarile.
func TestCallerDeadlineBoundsAllAttempts(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{MaxAttempts: 10, PerAttemptTimeout: 200 * time.Millisecond})
	f.srv.SetFaults(accounts.Faults{UnavailableNext: 1000})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	r := f.c.Credit(ctx, CreditInput{Key: "budget", Account: "ACC-1", Amount: 1})
	if el := time.Since(start); el > 900*time.Millisecond {
		t.Fatalf("bugetul apelantului nu a fost respectat: %s (%+v)", el, r)
	}
}

func TestBalanceFallbackToStaleCache(t *testing.T) {
	f := setup(t, accounts.Options{}, accounts.ServerOptions{}, Config{BreakerThreshold: 1, BreakerCooldown: time.Hour})
	f.c.Credit(context.Background(), CreditInput{Key: "k", Account: "ACC-5", Amount: 42})
	v, err := f.c.Balance(context.Background(), "ACC-5")
	if err != nil || v.Amount != 42 || v.Stale {
		t.Fatalf("citire normala: %+v %v", v, err)
	}
	f.g.Stop()
	v, err = f.c.Balance(context.Background(), "ACC-5")
	if err != nil || !v.Stale || v.Amount != 42 {
		t.Fatalf("fallback din cache: %+v %v", v, err)
	}
	if _, err := f.c.Balance(context.Background(), "ACC-6"); err == nil {
		t.Fatal("fara cache trebuie intoarsa eroare")
	}
}

func TestBreakerHalfOpenRecovers(t *testing.T) {
	b := NewBreaker(2, time.Minute)
	now := time.Now()
	b.now = func() time.Time { return now }
	b.Failure()
	b.Failure()
	if b.Allow() {
		t.Fatal("circuit deschis trebuie sa refuze")
	}
	now = now.Add(time.Minute)
	if !b.Allow() || b.Allow() {
		t.Fatal("half-open: exact o cerere de proba")
	}
	b.Success()
	if b.State() != BreakerClosed || !b.Allow() {
		t.Fatal("dupa proba reusita circuitul se inchide")
	}
}

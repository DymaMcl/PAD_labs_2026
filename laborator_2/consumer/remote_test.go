package consumer

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/accounts"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/integration"
)

type svc struct {
	store *accounts.Store
	srv   *accounts.Server
	g     *grpc.Server
	addr  string
}

func startAccounts(t *testing.T, addr string, store *accounts.Store) *svc {
	t.Helper()
	if store == nil {
		store, _ = accounts.Open("", accounts.Options{ProcessingTime: 5 * time.Millisecond})
	}
	srv := accounts.NewServer(store, accounts.ServerOptions{})
	g := srv.GRPC()
	var lis net.Listener
	var err error
	for i := 0; i < 50; i++ { // la repornire portul se poate elibera cu intarziere
		if lis, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	return &svc{store: store, srv: srv, g: g, addr: lis.Addr().String()}
}

func remoteEffect(t *testing.T, addr string, cfg integration.Config) *RemoteEffect {
	t.Helper()
	if cfg.PerAttemptTimeout == 0 {
		cfg.PerAttemptTimeout = 200 * time.Millisecond
	}
	cfg.BackoffBase, cfg.BackoffMax = 10*time.Millisecond, 40*time.Millisecond
	ic, err := integration.Dial(addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ic.Close() })
	return &RemoteEffect{Client: ic, LeaseMargin: 100 * time.Millisecond}
}

func runUntilIdle(t *testing.T, w *Worker, idle time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return w.RunUntilIdle(ctx, idle)
}

// Lab. 2, scenariul critic in lantul complet: serviciul pierde raspunsul
// la fiecare al doilea Credit; soldul final trebuie sa fie exact.
func TestRemoteLostResponsesExactBalance(t *testing.T) {
	c := startBroker(t)
	s := startAccounts(t, "127.0.0.1:0", nil)
	s.srv.SetFaults(accounts.Faults{DropResponseRate: 0.5})
	publishCredits(t, c, "ACC-1", 100, 100, 100, 100, 100)
	publishCredits(t, c, "ACC-2", 10, 20, 30)

	w := NewWorker(c, remoteEffect(t, s.addr, integration.Config{BreakerThreshold: 50}), Options{Queue: "plati", Name: "c", Lease: 3 * time.Second})
	runUntilIdle(t, w, 600*time.Millisecond)

	b1, _ := s.store.Balance("ACC-1")
	b2, _ := s.store.Balance("ACC-2")
	if b1 != 500 || b2 != 60 || s.srv.Stats().Executed != 8 {
		t.Fatalf("ACC-1=%d ACC-2=%d stats=%+v", b1, b2, s.srv.Stats())
	}
}

// Scenariul critic din lab. 1 peste serviciul din lab. 2: consumer-ul cade
// dupa ce serviciul a executat operatia, inainte de ACK.
func TestRemoteConsumerCrashBeforeAck(t *testing.T) {
	c := startBroker(t)
	s := startAccounts(t, "127.0.0.1:0", nil)
	publishCredits(t, c, "ACC-1", 100, 100, 100)
	eff := remoteEffect(t, s.addr, integration.Config{})
	lease := 400 * time.Millisecond

	w1 := NewWorker(c, eff, Options{Queue: "plati", Name: "c1", Lease: lease, CrashAfterEffect: 2})
	if err := runUntilIdle(t, w1, time.Second); !errors.Is(err, ErrCrashed) {
		t.Fatalf("astepta cadere, %v", err)
	}
	w2 := NewWorker(c, eff, Options{Queue: "plati", Name: "c1'", Lease: lease, Wait: 100 * time.Millisecond})
	runUntilIdle(t, w2, lease+500*time.Millisecond)

	if b, _ := s.store.Balance("ACC-1"); b != 300 || s.srv.Stats().Replayed != 1 {
		t.Fatalf("sold=%d stats=%+v", b, s.srv.Stats())
	}
}

// Serviciul este oprit: mesajele NU se pierd si NU sunt confirmate - raman in
// broker (fallback). Dupa repornire sunt procesate, fara duplicate.
func TestRemoteServiceDownMessagesWaitInBroker(t *testing.T) {
	c := startBroker(t)
	s := startAccounts(t, "127.0.0.1:0", nil)
	addr := s.addr
	store := s.store
	s.g.Stop()

	publishCredits(t, c, "ACC-1", 1, 2, 3)
	eff := remoteEffect(t, addr, integration.Config{MaxAttempts: 2, BreakerThreshold: 2, BreakerCooldown: time.Second})
	w := NewWorker(c, eff, Options{Queue: "plati", Name: "c", Lease: 2 * time.Second, Wait: 50 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	w.Run(ctx)
	cancel()
	if b, _ := store.Balance("ACC-1"); b != 0 {
		t.Fatal("serviciul era oprit")
	}

	s2 := startAccounts(t, addr, store) // acelasi serviciu, repornit
	runUntilIdle(t, w, 1500*time.Millisecond)
	if b, _ := s2.store.Balance("ACC-1"); b != 6 {
		t.Fatalf("dupa repornire sold=%d", b)
	}
}

// Respingerea de business ajunge in DLQ (nu se reincearca la nesfarsit).
func TestRemoteRejectionGoesToDLQ(t *testing.T) {
	c := startBroker(t)
	s := startAccounts(t, "127.0.0.1:0", nil)
	publishCredits(t, c, "ACC-1", 10, accounts.MaxCredit+1, 20)
	w := NewWorker(c, remoteEffect(t, s.addr, integration.Config{}), Options{Queue: "plati", Name: "c", Lease: 2 * time.Second})
	runUntilIdle(t, w, 500*time.Millisecond)
	if b, _ := s.store.Balance("ACC-1"); b != 30 {
		t.Fatalf("sold=%d", b)
	}
}

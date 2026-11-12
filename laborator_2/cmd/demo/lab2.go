package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/accounts"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/broker"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/consumer"
	accountsv1 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v1"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/compat"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/integration"
)

type svc struct {
	store *accounts.Store
	srv   *accounts.Server
	g     *grpc.Server
	addr  string
}

func startAccounts(so accounts.Options, opts accounts.ServerOptions) *svc {
	store, _ := accounts.Open("", so)
	if *verbose {
		opts.Logf = func(f string, a ...any) { log.Printf("  [serviciu] "+f, a...) }
	}
	srv := accounts.NewServer(store, opts)
	g := srv.GRPC()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go g.Serve(lis)
	return &svc{store: store, srv: srv, g: g, addr: lis.Addr().String()}
}

func dialIntegration(addr string, cfg integration.Config) *integration.Client {
	if cfg.PerAttemptTimeout == 0 {
		cfg.PerAttemptTimeout = 300 * time.Millisecond
	}
	if *verbose {
		cfg.Logf = func(f string, a ...any) { log.Printf("  [client] "+f, a...) }
	}
	c, err := integration.Dial(addr, cfg)
	if err != nil {
		log.Fatal(err)
	}
	return c
}

func bal(s *svc, acc string) int64 { b, _ := s.store.Balance(acc); return b }

// ── Scenariul critic al laboratorului 2 ─────────────────────────────────────

func scenarioRPCCritical() {
	title("LAB 2 · SCENARIUL CRITIC: raspunsul se pierde dupa efectuarea operatiei")
	fmt.Println(`Clientul cere "alimenteaza ACC-1 cu 100". Serviciul executa si salveaza
operatia, dar raspunsul se pierde pe drum: clientul primeste UNAVAILABLE.
Din punctul de vedere al clientului rezultatul este AMBIGUU.`)

	fmt.Println("\n── client NAIV: eroare = \"nu s-a executat\", reincearca cu o cheie NOUA ──")
	s1 := startAccounts(accounts.Options{}, accounts.ServerOptions{})
	s1.srv.SetFaults(accounts.Faults{DropResponseNext: 1})
	c1 := dialIntegration(s1.addr, integration.Config{})
	_, attempts, _ := integration.NaiveCredit(context.Background(), c1, "ACC-1", 100, 3)
	naive := bal(s1, "ACC-1")
	naiveExec := s1.srv.Stats().Executed

	fmt.Println("\n── client de INTEGRARE: aceeasi cheie la reincercare + GetOperation ──")
	s2 := startAccounts(accounts.Options{}, accounts.ServerOptions{})
	s2.srv.SetFaults(accounts.Faults{DropResponseNext: 1})
	c2 := dialIntegration(s2.addr, integration.Config{})
	r := c2.Credit(context.Background(), integration.CreditInput{Key: "plata-001", Account: "ACC-1", Amount: 100})

	fmt.Println("\nRezultat:")
	fmt.Printf("  %-11s %6s %9s %10s %10s  %s\n", "client", "sold", "asteptat", "executii", "incercari", "verdict")
	fmt.Printf("  %-11s %6d %9d %10d %10d  %s\n", "NAIV", naive, 100, naiveExec, attempts, verdict(naive == 100))
	fmt.Printf("  %-11s %6d %9d %10d %10d  %s\n", "INTEGRARE", bal(s2, "ACC-1"), 100, s2.srv.Stats().Executed, r.Attempts, verdict(bal(s2, "ACC-1") == 100))
	fmt.Printf("\n  rezultat explicit: %s · %s\n", r.Outcome, r.Resolution)
	fmt.Println(`
Concluzie: o eroare de transport NU inseamna ca operatia nu a avut loc.
Cheia de idempotenta, stabila intre reincercari, transforma reincercarea intr-o
operatie sigura: serviciul recunoaste cheia si intoarce rezultatul original.`)
}

func verdict(ok bool) string {
	if ok {
		return "✓ corect"
	}
	return "✗ EFECT DUBLAT"
}

// ── Tratarea explicita a ambiguitatii ───────────────────────────────────────

func scenarioAmbiguity() {
	title("LAB 2 · REZULTATE EXPLICITE: APPLIED / REJECTED / NOT_APPLIED / UNKNOWN")
	fmt.Println(`Acelasi apel Credit in sapte situatii. Clientul nu raspunde niciodata cu un
simplu "eroare": spune ce stie despre efect si cum a aflat.`)
	type row struct {
		caz, outcome, resolution string
		sold                     int64
	}
	var rows []row
	run := func(caz string, f accounts.Faults, so accounts.Options, cfg integration.Config, amount int64, stop bool) {
		s := startAccounts(so, accounts.ServerOptions{})
		s.srv.SetFaults(f)
		if stop {
			s.g.Stop()
		}
		c := dialIntegration(s.addr, cfg)
		r := c.Credit(context.Background(), integration.CreditInput{Key: "op-" + caz, Account: "ACC-1", Amount: amount})
		rows = append(rows, row{caz, string(r.Outcome), r.Resolution, bal(s, "ACC-1")})
		c.Close()
		s.g.Stop()
	}
	quiet := *verbose
	*verbose = false
	run("normal", accounts.Faults{}, accounts.Options{}, integration.Config{}, 100, false)
	run("raspuns pierdut o data", accounts.Faults{DropResponseNext: 1}, accounts.Options{}, integration.Config{}, 100, false)
	run("raspuns pierdut mereu", accounts.Faults{DropResponseRate: 1}, accounts.Options{}, integration.Config{MaxAttempts: 3, BreakerThreshold: 10}, 100, false)
	run("raspuns intarziat", accounts.Faults{SlowResponseMs: 600}, accounts.Options{}, integration.Config{}, 100, false)
	run("refuz inainte de executie", accounts.Faults{UnavailableNext: 99}, accounts.Options{}, integration.Config{MaxAttempts: 3, BreakerThreshold: 10}, 100, false)
	run("suma peste limita", accounts.Faults{}, accounts.Options{}, integration.Config{}, accounts.MaxCredit+1, false)
	run("serviciu oprit", accounts.Faults{}, accounts.Options{}, integration.Config{MaxAttempts: 2}, 100, true)
	*verbose = quiet

	fmt.Printf("\n  %-26s %-12s %5s  %s\n", "situatie", "rezultat", "sold", "cum a fost stabilit")
	for _, r := range rows {
		fmt.Printf("  %-26s %-12s %5d  %s\n", r.caz, r.outcome, r.sold, r.resolution)
	}
	fmt.Println(`
  APPLIED     -> consumer-ul confirma mesajul (ACK)
  REJECTED    -> mesajul merge in DLQ (reincercarea nu ar schimba nimic)
  NOT_APPLIED -> mesajul ramane in broker, se reincearca mai tarziu
  UNKNOWN     -> mesajul ramane in broker; reluarea cu ACEEASI cheie va lamuri`)
}

// ── Versionare si compatibilitate ───────────────────────────────────────────

func scenarioVersions() {
	title("LAB 2 · VERSIONARE SI COMPATIBILITATE")
	*verbose = false
	s := startAccounts(accounts.Options{}, accounts.ServerOptions{})
	conn, _ := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	ctx := context.Background()

	var hdr metadata.MD
	r1, err := accountsv1.NewAccountsClient(conn).Credit(ctx,
		&accountsv1.CreditRequest{IdempotencyKey: "v1-a", Account: "ACC-1", Amount: 100, Note: "client v1.1"}, grpc.Header(&hdr))
	fmt.Printf("  client v1.1 (accounts.v1)          -> sold %d, err=%v, antete: versiune=%v deprecation=%v sunset=%v\n",
		r1.GetBalanceAfter(), err, hdr.Get("x-api-version"), hdr.Get("deprecation"), hdr.Get("sunset"))

	old := &compat.CreditResponse{}
	err = conn.Invoke(ctx, "/accounts.v1.Accounts/Credit",
		&compat.CreditRequest{IdempotencyKey: "v1-old", Account: "ACC-1", Amount: 50, Currency: "MDL"}, old)
	fmt.Printf("  client VECHI v1.0 (camp eliminat)  -> sold %d, err=%v, campuri necunoscute ignorate: %d octeti\n",
		old.GetBalanceAfter(), err, len(old.ProtoReflect().GetUnknown()))

	c2 := dialIntegration(s.addr, integration.Config{})
	r := c2.Credit(ctx, integration.CreditInput{Key: "v2-a", Account: "ACC-1", Amount: 25})
	fmt.Printf("  client v2 (Money)                  -> sold %d, versiune folosita: %s\n", r.BalanceAfter, r.APIVersion)

	r = c2.Credit(ctx, integration.CreditInput{Key: "v1-a", Account: "ACC-1", Amount: 100})
	fmt.Printf("  v2 reia operatia creata prin v1    -> %s, replayed=%v (aceeasi stare, aceeasi cheie)\n", r.Outcome, r.Replayed)

	r = c2.Credit(ctx, integration.CreditInput{Key: "v1-a", Account: "ACC-1", Amount: 999})
	fmt.Printf("  aceeasi cheie, alta suma           -> %s: %s\n", r.Outcome, r.Reason)

	sOld := startAccounts(accounts.Options{}, accounts.ServerOptions{DisableV2: true})
	c3 := dialIntegration(sOld.addr, integration.Config{})
	r = c3.Credit(ctx, integration.CreditInput{Key: "x", Account: "ACC-1", Amount: 10})
	fmt.Printf("  client v2 -> SERVER VECHI (doar v1)-> %s, versiune negociata: %s (fallback de contract)\n", r.Outcome, r.APIVersion)
	fmt.Printf("\n  sold final ACC-1 pe serverul principal: %d (100 + 50 + 25)\n", bal(s, "ACC-1"))
	*verbose = true
}

// ── Deadline si cancellation ────────────────────────────────────────────────

func scenarioDeadlines() {
	title("LAB 2 · DEADLINE SI CANCELLATION")
	*verbose = false
	ctx := context.Background()
	s := startAccounts(accounts.Options{ProcessingTime: 300 * time.Millisecond}, accounts.ServerOptions{MinBudget: 300 * time.Millisecond})
	conn, _ := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	raw := accountsv1.NewAccountsClient(conn)

	c, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	_, err := raw.Credit(c, &accountsv1.CreditRequest{IdempotencyKey: "short", Account: "ACC-1", Amount: 1})
	cancel()
	fmt.Printf("  deadline 100ms < bugetul minim 300ms  -> %v\n", err)

	sNoMin := startAccounts(accounts.Options{ProcessingTime: 300 * time.Millisecond}, accounts.ServerOptions{})
	conn2, _ := grpc.NewClient(sNoMin.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	raw2 := accountsv1.NewAccountsClient(conn2)
	c, cancel = context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = raw2.Credit(c, &accountsv1.CreditRequest{IdempotencyKey: "mid", Account: "ACC-1", Amount: 1})
	cancel()
	time.Sleep(250 * time.Millisecond)
	st, _ := sNoMin.store.Lookup("mid")
	fmt.Printf("  deadline expirat in timpul lucrului    -> %v\n      serverul a abandonat inainte de commit: stare=%s\n", err, st)

	c, cancel = context.WithCancel(ctx)
	time.AfterFunc(80*time.Millisecond, cancel)
	_, err = raw2.Credit(c, &accountsv1.CreditRequest{IdempotencyKey: "cancel", Account: "ACC-1", Amount: 1})
	time.Sleep(300 * time.Millisecond)
	st, _ = sNoMin.store.Lookup("cancel")
	fmt.Printf("  clientul anuleaza (ex. Ctrl+C)         -> %v\n      stare pe server: %s\n", err, st)

	sSlow := startAccounts(accounts.Options{}, accounts.ServerOptions{})
	sSlow.srv.SetFaults(accounts.Faults{SlowResponseMs: 400})
	conn3, _ := grpc.NewClient(sSlow.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	c, cancel = context.WithTimeout(ctx, 150*time.Millisecond)
	_, err = accountsv1.NewAccountsClient(conn3).Credit(c, &accountsv1.CreditRequest{IdempotencyKey: "late", Account: "ACC-1", Amount: 1})
	cancel()
	st, _ = sSlow.store.Lookup("late")
	fmt.Printf("  commit facut, raspuns dupa deadline    -> %v\n      stare pe server: %s  <- de aceea DEADLINE_EXCEEDED este AMBIGUU\n", err, st)
	fmt.Printf("\n  sold ACC-1 pe serverul fara buget minim: %d (nicio operatie anulata nu a fost aplicata)\n", bal(sNoMin, "ACC-1"))
	*verbose = true
}

// ── Lantul complet ──────────────────────────────────────────────────────────

func scenarioPipeline() {
	title("LAB 1 + LAB 2 · LANTUL COMPLET: producer -> broker -> consumer -> serviciu gRPC")
	fmt.Println(`20 de alimentari pe 4 conturi. Serviciul pierde 30% din raspunsuri dupa
executie, iar consumer-ul cade o data dupa efect, inainte de ACK. Cheia de
idempotenta este ID-ul mesajului, deci fiecare reluare (din broker sau din
clientul RPC) este recunoscuta de serviciu.`)
	*verbose = false
	cfg := broker.DefaultConfig()
	cfg.BackoffBase, cfg.BackoffMax = 50*time.Millisecond, 200*time.Millisecond
	e := newEnv(cfg)
	expected := map[string]int64{}
	for i, acc := range []string{"ACC-1", "ACC-2", "ACC-3", "ACC-4"} {
		amounts := []int64{}
		for j := 1; j <= 5; j++ {
			a := int64((i + 1) * 10 * j)
			amounts = append(amounts, a)
			expected[acc] += a
		}
		publish(e, acc, amounts...)
	}
	s := startAccounts(accounts.Options{ProcessingTime: 5 * time.Millisecond}, accounts.ServerOptions{})
	s.srv.SetFaults(accounts.Faults{DropResponseRate: 0.3})
	ic := dialIntegration(s.addr, integration.Config{BreakerThreshold: 50})
	eff := &consumer.RemoteEffect{Client: ic, LeaseMargin: 100 * time.Millisecond}
	lease := 1500 * time.Millisecond

	w1 := consumer.NewWorker(e.c, eff, consumer.Options{Queue: "plati", Name: "c1", Lease: lease, CrashAfterEffect: 7})
	err := w1.Run(context.Background())
	fmt.Printf("\n  consumer c1: %v\n", err)
	w2 := consumer.NewWorker(e.c, eff, consumer.Options{Queue: "plati", Name: "c1-repornit", Lease: lease, Wait: 200 * time.Millisecond})
	_ = w2.RunUntilIdle(context.Background(), lease+700*time.Millisecond)

	st := s.srv.Stats()
	q := queueStats(e)
	fmt.Printf("\n  %-8s %8s %9s\n", "cont", "sold", "asteptat")
	ok := true
	for _, acc := range []string{"ACC-1", "ACC-2", "ACC-3", "ACC-4"} {
		fmt.Printf("  %-8s %8d %9d\n", acc, bal(s, acc), expected[acc])
		ok = ok && bal(s, acc) == expected[acc]
	}
	fmt.Printf("\n  serviciu: executate=%d reluate-din-registru=%d raspunsuri-pierdute=%d\n", st.Executed, st.Replayed, st.Dropped)
	fmt.Printf("  broker:   ack=%d relivrari=%d lease-expirat=%d DLQ=%d\n", q.Counters.Acked, q.Counters.Redelivered, q.Counters.Expired, q.Dead)
	fmt.Printf("  %s\n", verdict(ok && st.Executed == 20))
	*verbose = true
}

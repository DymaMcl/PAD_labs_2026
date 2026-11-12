// Comanda demo ruleaza scenariile laboratorului cap-coada, intr-un singur
// proces (broker HTTP real pe un port local + producer + consumeri).
//
//	go run ./cmd/demo              # toate scenariile
//	go run ./cmd/demo -s critic    # doar scenariul critic
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/broker"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/client"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/consumer"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
)

var verbose = flag.Bool("v", true, "afiseaza jurnalul consumerilor")

func main() {
	scenario := flag.String("s", "all", "scenariu: critic | producer | retry | ordering | all")
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	run := map[string]func(){
		"critic":   scenarioCritical,
		"producer": scenarioProducerRetry,
		"retry":    scenarioRetryDLQ,
		"ordering": scenarioOrdering,
	}
	order := []string{"critic", "producer", "retry", "ordering"}
	if *scenario != "all" {
		f, ok := run[*scenario]
		if !ok {
			log.Fatalf("scenariu necunoscut %q", *scenario)
		}
		f()
		return
	}
	for _, name := range order {
		run[name]()
	}
}

type env struct {
	srv *broker.Server
	b   *broker.Broker
	c   *client.Client
	dir string
}

func newEnv(cfg broker.Config) *env {
	b := broker.New(cfg)
	srv := broker.NewServer(b)
	ts := httptest.NewServer(srv)
	dir, _ := os.MkdirTemp("", "lab1-demo-")
	return &env{srv: srv, b: b, c: client.New(ts.URL), dir: dir}
}

func title(s string) {
	line := strings.Repeat("═", 72)
	fmt.Printf("\n%s\n  %s\n%s\n", line, s, line)
}

func logf(prefix string) func(string, ...any) {
	return func(f string, a ...any) {
		if *verbose {
			log.Printf("  [%s] "+f, append([]any{prefix}, a...)...)
		}
	}
}

func publish(e *env, key string, amounts ...int64) {
	for i, a := range amounts {
		m, _ := contract.New(contract.TypeCredit, "demo-producer", key, uint64(i+1),
			contract.CreditPayload{Account: key, Amount: a})
		if _, _, err := e.c.PublishWithRetry(context.Background(), "plati", m, client.DefaultRetry, nil); err != nil {
			log.Fatal(err)
		}
	}
}

func queueStats(e *env) broker.QueueStats {
	for _, s := range e.b.Stats() {
		if s.Name == "plati" {
			return s
		}
	}
	return broker.QueueStats{}
}

// ── Scenariul critic ────────────────────────────────────────────────────────

func scenarioCritical() {
	title("SCENARIUL CRITIC: consumer-ul cade dupa efect, inainte de ACK")
	fmt.Println(`Producer-ul publica 5 alimentari de 100 pe contul ACC-1 (sold corect: 500).
Consumer-ul aplica efectul mesajului #3, apoi "cade" fara sa trimita ACK.
Brokerul nu stie ca efectul s-a produs: dupa expirarea lease-ului relivreaza #3.
Rulam scenariul de doua ori: cu consumer NAIV si cu consumer IDEMPOTENT.`)

	type result struct {
		mode string
		s    consumer.Summary
		q    broker.QueueStats
	}
	var results []result
	for _, idem := range []bool{false, true} {
		mode := "NAIV"
		if idem {
			mode = "IDEMPOTENT"
		}
		fmt.Printf("\n── consumer %s ──\n", mode)
		cfg := broker.DefaultConfig()
		e := newEnv(cfg)
		publish(e, "ACC-1", 100, 100, 100, 100, 100)
		path := filepath.Join(e.dir, "ledger.json")
		lease := 800 * time.Millisecond
		ctx := context.Background()

		l1, _ := consumer.OpenLedger(path)
		w1 := consumer.NewWorker(e.c, l1, consumer.Options{Queue: "plati", Name: "c1", Idempotent: idem,
			Lease: lease, CrashAfterEffect: 3, Logf: logf("c1")})
		if err := w1.Run(ctx); !errors.Is(err, consumer.ErrCrashed) {
			log.Fatalf("astepta cadere: %v", err)
		}
		log.Printf("  [sistem] procesul c1 a murit; repornim consumer-ul cu aceeasi stare locala (%s)", filepath.Base(path))

		l2, _ := consumer.OpenLedger(path)
		w2 := consumer.NewWorker(e.c, l2, consumer.Options{Queue: "plati", Name: "c1-restart", Idempotent: idem,
			Lease: lease, Wait: 200 * time.Millisecond, Logf: logf("c1'")})
		_ = w2.RunUntilIdle(ctx, lease+500*time.Millisecond)
		results = append(results, result{mode, l2.Summary(), queueStats(e)})
	}

	fmt.Println("\nRezultat:")
	fmt.Printf("  %-11s %6s %10s %12s %12s %11s\n", "consumer", "sold", "asteptat", "efecte dupl.", "relivrari", "lease exp.")
	for _, r := range results {
		verdict := "✓ corect"
		if r.s.Total != 500 {
			verdict = "✗ GRESIT"
		}
		fmt.Printf("  %-11s %6d %10d %12d %12d %11d   %s\n", r.mode, r.s.Total, 500,
			r.s.DuplicateEffect, r.q.Counters.Redelivered, r.q.Counters.Expired, verdict)
	}
	fmt.Println(`
Concluzie: brokerul garanteaza at-least-once (mesajul nu se pierde), deci
relivrarea dupa cadere este INEVITABILA. Corectitudinea efectului vine din
consumer: efectul si marcarea in inbox se scriu atomic, iar la relivrare
mesajul este recunoscut si doar confirmat (efect exactly-once).`)
}

// ── ACK pierdut catre producer ──────────────────────────────────────────────

func scenarioProducerRetry() {
	title("ACK PIERDUT CATRE PRODUCER: retry + deduplicare in broker")
	fmt.Println(`Brokerul persista mesajul, dar raspunsul se pierde in 50% din cazuri (defect
injectat). Producer-ul retrimite ACELASI ID cu backoff; brokerul recunoaste
duplicatul si nu il adauga a doua oara.`)
	e := newEnv(broker.DefaultConfig())
	e.srv.SetFaults(broker.Faults{DropPublishAck: 0.5})
	const n = 10
	retries := 0
	for i := 1; i <= n; i++ {
		m, _ := contract.New(contract.TypeCredit, "p", "ACC-1", uint64(i), contract.CreditPayload{Account: "ACC-1", Amount: 10})
		pol := client.RetryPolicy{MaxAttempts: 10, Base: 5 * time.Millisecond, Max: 50 * time.Millisecond}
		res, attempts, err := e.c.PublishWithRetry(context.Background(), "plati", m, pol, nil)
		if err != nil {
			log.Fatal(err)
		}
		retries += attempts - 1
		log.Printf("  [producer] mesaj %2d: %d incercari -> %s", i, attempts, res.Status)
	}
	q := queueStats(e)
	fmt.Printf("\nRezultat: %d mesaje trimise, %d reincercari, in coada: %d mesaje, duplicate filtrate de broker: %d\n",
		n, retries, q.Ready, q.Counters.Duplicates)
	if q.Ready == n {
		fmt.Println("  ✓ fiecare mesaj apare o singura data, desi unele au fost trimise de mai multe ori")
	}
}

// ── Retry si DLQ ────────────────────────────────────────────────────────────

func scenarioRetryDLQ() {
	title("NACK, RETRY CU BACKOFF SI DEAD-LETTER QUEUE")
	fmt.Println(`Consumer-ul esueaza tranzitoriu in 40% din cazuri (NACK cu retry -> backoff
exponential). Un mesaj are suma negativa: eroare permanenta, NACK fara retry,
mesajul ajunge direct in DLQ si nu blocheaza restul cozii.`)
	cfg := broker.DefaultConfig()
	cfg.MaxAttempts = 6
	cfg.BackoffBase = 50 * time.Millisecond
	cfg.BackoffMax = 400 * time.Millisecond
	e := newEnv(cfg)
	publish(e, "ACC-1", 100, 200, -50, 300)
	publish(e, "ACC-2", 10, 20, 30)

	l, _ := consumer.OpenLedger("")
	w := consumer.NewWorker(e.c, l, consumer.Options{Queue: "plati", Name: "c1", Idempotent: true,
		FailRate: 0.4, Batch: 2, Wait: 100 * time.Millisecond, Logf: logf("c1")})
	_ = w.RunUntilIdle(context.Background(), 1500*time.Millisecond)

	s := l.Summary()
	q := queueStats(e)
	fmt.Printf("\nRezultat: ACC-1=%d (asteptat 600)  ACC-2=%d (asteptat 60)\n", s.Balances["ACC-1"], s.Balances["ACC-2"])
	fmt.Printf("  NACK-uri=%d  relivrari=%d  in DLQ=%d\n", q.Counters.Nacked, q.Counters.Redelivered, q.Dead)
	for _, d := range e.b.DeadLetters("plati") {
		fmt.Printf("  DLQ: %s key=%s tentative=%d motiv=%q\n", d.Message.ID[:8], d.Message.Key, d.Attempts, d.Reason)
	}
}

// ── Ordonare per cheie ──────────────────────────────────────────────────────

func scenarioOrdering() {
	title("ORDONARE PER CHEIE CU CONSUMERI CONCURENTI")
	fmt.Println(`3 consumeri lucreaza in paralel pe 4 conturi (chei). Procesarea dureaza
aleator, iar unii consumeri esueaza tranzitoriu. Brokerul livreaza doar capul
fiecarei chei, deci ordinea seq pe fiecare cont trebuie sa ramana stricta.`)
	cfg := broker.DefaultConfig()
	cfg.BackoffBase = 20 * time.Millisecond
	cfg.BackoffMax = 100 * time.Millisecond
	cfg.MaxAttempts = 10
	e := newEnv(cfg)
	for _, k := range []string{"ACC-1", "ACC-2", "ACC-3", "ACC-4"} {
		publish(e, k, 1, 2, 3, 4, 5, 6)
	}

	l, _ := consumer.OpenLedger("") // registru comun (ca o baza de date partajata)
	var wg sync.WaitGroup
	for i := 1; i <= 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			w := consumer.NewWorker(e.c, l, consumer.Options{Queue: "plati", Name: fmt.Sprintf("c%d", i),
				Idempotent: true, FailRate: 0.2, Batch: 2, ProcessDelay: time.Duration(i*7) * time.Millisecond,
				Wait: 100 * time.Millisecond})
			_ = w.RunUntilIdle(context.Background(), time.Second)
		}(i)
	}
	wg.Wait()
	s := l.Summary()
	fmt.Printf("\nRezultat: %d mesaje procesate, incalcari de ordine=%d, efecte duplicate=%d\n",
		s.Messages, s.OrderViolations, s.DuplicateEffect)
	for _, k := range []string{"ACC-1", "ACC-2", "ACC-3", "ACC-4"} {
		fmt.Printf("  %s = %d (asteptat 21)\n", k, s.Balances[k])
	}
}

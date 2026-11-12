// Comanda consumer preia mesaje din broker si aplica efectul fie local (ca in
// laboratorul 1), fie prin serviciul de conturi gRPC (laboratorul 2).
//
//	go run ./cmd/consumer -name c1                         # efect prin serviciul gRPC (implicit)
//	go run ./cmd/consumer -name c1 -effect local           # ca in laboratorul 1
//	go run ./cmd/consumer -name c1 -effect local -naive    # lab. 1 fara deduplicare
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/client"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/consumer"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/integration"
)

func main() {
	brokerURL := flag.String("broker", "http://localhost:7070", "adresa brokerului")
	queue := flag.String("queue", "plati", "coada")
	name := flag.String("name", "consumer-1", "numele consumer-ului")
	effect := flag.String("effect", "remote", "unde se aplica efectul: remote (serviciul gRPC) | local (registru, lab. 1)")
	accountsAddr := flag.String("accounts", "localhost:7080", "adresa gRPC a serviciului de conturi")
	api := flag.String("api", "auto", "versiunea contractului: auto (v2 cu fallback la v1) | v1")
	perAttempt := flag.Duration("attempt-timeout", time.Second, "deadline per incercare RPC")
	attempts := flag.Int("attempts", 4, "incercari RPC per livrare")
	ledger := flag.String("ledger", "", "registrul local pentru -effect local (implicit data/<name>.ledger.json)")
	naive := flag.Bool("naive", false, "pentru -effect local: fara deduplicare prin inbox")
	lease := flag.Duration("lease", 5*time.Second, "lease cerut brokerului")
	batch := flag.Int("batch", 1, "mesaje preluate per cerere")
	crashAfter := flag.Int("crash-after-effect", 0, "cade (exit 3) dupa al N-lea efect, inainte de ACK")
	failRate := flag.Float64("fail-rate", 0, "probabilitatea unei erori tranzitorii simulate (NACK cu retry)")
	flag.Parse()

	var eff consumer.Effect
	var l *consumer.Ledger
	switch *effect {
	case "local":
		if *ledger == "" {
			*ledger = "data/" + *name + ".ledger.json"
		}
		var err error
		if l, err = consumer.OpenLedger(*ledger); err != nil {
			log.Fatal(err)
		}
		eff = &consumer.LocalEffect{Ledger: l, Idempotent: !*naive}
	case "remote":
		ic, err := integration.Dial(*accountsAddr, integration.Config{
			PerAttemptTimeout: *perAttempt, MaxAttempts: *attempts, ForceV1: *api == "v1",
			Logf: func(f string, a ...any) { log.Printf("   [rpc] "+f, a...) },
		})
		if err != nil {
			log.Fatal(err)
		}
		defer ic.Close()
		eff = &consumer.RemoteEffect{Client: ic}
	default:
		log.Fatalf("-effect necunoscut: %s", *effect)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	log.Printf("consumer %s pe %s/%s, efect: %s", *name, *brokerURL, *queue, eff.Name())

	w := consumer.NewWorker(client.New(*brokerURL), eff, consumer.Options{
		Queue: *queue, Name: *name, Lease: *lease, Wait: 10 * time.Second, Batch: *batch,
		CrashAfterEffect: *crashAfter, FailRate: *failRate, Logf: log.Printf,
	})
	err := w.Run(ctx)
	if l != nil {
		fmt.Printf("\nRegistrul %s\n%s\n", *ledger, l.Summary())
	}
	if errors.Is(err, consumer.ErrCrashed) {
		os.Exit(3)
	}
}

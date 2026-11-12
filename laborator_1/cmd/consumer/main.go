// Comanda consumer preia mesaje si aplica efectul pe un registru local.
//
//	go run ./cmd/consumer -name c1 -ledger data/c1.ledger.json
//	go run ./cmd/consumer -name c1 -crash-after-effect 3     # scenariul critic
//	go run ./cmd/consumer -naive ...                          # fara deduplicare
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

	"github.com/DymaMcl/PAD_labs_2026/laborator_1/client"
	"github.com/DymaMcl/PAD_labs_2026/laborator_1/consumer"
)

func main() {
	brokerURL := flag.String("broker", "http://localhost:7070", "adresa brokerului")
	queue := flag.String("queue", "plati", "coada")
	name := flag.String("name", "consumer-1", "numele consumer-ului")
	ledger := flag.String("ledger", "", "fisierul registrului local (implicit data/<name>.ledger.json)")
	naive := flag.Bool("naive", false, "dezactiveaza deduplicarea prin inbox (pentru a vedea efectul duplicat)")
	lease := flag.Duration("lease", 5*time.Second, "lease cerut brokerului")
	batch := flag.Int("batch", 1, "mesaje preluate per cerere")
	crashAfter := flag.Int("crash-after-effect", 0, "cade (exit 3) dupa al N-lea efect, inainte de ACK")
	failRate := flag.Float64("fail-rate", 0, "probabilitatea unei erori tranzitorii simulate (NACK cu retry)")
	delay := flag.Duration("delay", 0, "durata simulata a procesarii")
	show := flag.Bool("show", false, "afiseaza registrul si iese")
	flag.Parse()

	if *ledger == "" {
		*ledger = "data/" + *name + ".ledger.json"
	}
	l, err := consumer.OpenLedger(*ledger)
	if err != nil {
		log.Fatal(err)
	}
	if *show {
		fmt.Printf("Registrul %s\n%s\n", *ledger, l.Summary())
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	mode := "IDEMPOTENT (inbox)"
	if *naive {
		mode = "NAIV (fara deduplicare)"
	}
	log.Printf("consumer %s pe %s/%s, mod %s, registru %s", *name, *brokerURL, *queue, mode, *ledger)

	w := consumer.NewWorker(client.New(*brokerURL), l, consumer.Options{
		Queue: *queue, Name: *name, Idempotent: !*naive, Lease: *lease, Wait: 10 * time.Second,
		Batch: *batch, CrashAfterEffect: *crashAfter, FailRate: *failRate, ProcessDelay: *delay,
		Logf: log.Printf,
	})
	err = w.Run(ctx)
	fmt.Printf("\nRegistrul %s\n%s\n", *ledger, l.Summary())
	if errors.Is(err, consumer.ErrCrashed) {
		os.Exit(3) // cadere "dura": fara ACK, fara cleanup
	}
}

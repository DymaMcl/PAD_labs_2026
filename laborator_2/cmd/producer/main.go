// Comanda producer genereaza alimentari de cont (payment.credit) si le
// publica cu retry pana la confirmarea brokerului.
//
//	go run ./cmd/producer -count 20 -accounts 3 -amount 100
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/client"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
)

func main() {
	brokerURL := flag.String("broker", "http://localhost:7070", "adresa brokerului")
	queue := flag.String("queue", "plati", "coada")
	source := flag.String("source", "", "identitatea producer-ului (implicit: generata)")
	count := flag.Int("count", 10, "numarul de mesaje")
	accounts := flag.Int("accounts", 3, "numarul de conturi (= chei de ordonare)")
	amount := flag.Int64("amount", 100, "suma per mesaj")
	interval := flag.Duration("interval", 100*time.Millisecond, "pauza intre mesaje")
	poisonEvery := flag.Int("poison-every", 0, "fiecare al N-lea mesaj are suma negativa (respins de consumer -> DLQ)")
	sendInvalid := flag.Bool("invalid", false, "trimite la final si un mesaj care incalca contractul (respins de broker)")
	flag.Parse()

	if *source == "" {
		*source = "producer-" + contract.NewID()[:8]
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	c := client.New(*brokerURL)
	seq := map[string]uint64{}
	expected := map[string]int64{}
	var accepted, duplicates, retries, failed int

	log.Printf("producer %s -> %s/%s, %d mesaje", *source, *brokerURL, *queue, *count)
	for i := 1; i <= *count && ctx.Err() == nil; i++ {
		acc := fmt.Sprintf("ACC-%d", (i-1)%*accounts+1)
		amt := *amount
		if *poisonEvery > 0 && i%*poisonEvery == 0 {
			amt = -*amount
		}
		seq[acc]++
		m, err := contract.New(contract.TypeCredit, *source, acc, seq[acc],
			contract.CreditPayload{Account: acc, Amount: amt, Note: fmt.Sprintf("mesaj %d", i)})
		if err != nil {
			log.Fatalf("contract: %v", err)
		}
		res, attempts, err := c.PublishWithRetry(ctx, *queue, m, client.DefaultRetry,
			func(a int, err error, wait time.Duration) {
				log.Printf("  #%d %s incercarea %d esuata (%v), reincerc peste %s", i, short(m.ID), a, err, wait.Round(time.Millisecond))
			})
		retries += attempts - 1
		switch {
		case err != nil:
			failed++
			log.Printf("✗ #%d %s %s seq=%d NEPUBLICAT: %v", i, short(m.ID), acc, m.Seq, err)
			continue
		case res.Status == "duplicate":
			duplicates++
		default:
			accepted++
		}
		if amt > 0 {
			expected[acc] += amt
		}
		log.Printf("✓ #%d %s %s seq=%d suma=%d -> %s (offset %d, %d incercari)",
			i, short(m.ID), acc, m.Seq, amt, res.Status, res.Offset, attempts)
		time.Sleep(*interval)
	}

	if *sendInvalid {
		bad := contract.Message{ID: contract.NewID(), Type: "", Version: 9, Source: *source, Key: "", Payload: []byte(`[]`)}
		_, err := c.Publish(ctx, *queue, bad)
		log.Printf("mesaj invalid trimis intentionat -> %v", err)
	}

	fmt.Println()
	fmt.Printf("Rezumat producer %s\n", *source)
	fmt.Printf("  acceptate=%d  confirmate-ca-duplicat=%d  reincercari=%d  esuate=%d\n", accepted, duplicates, retries, failed)
	fmt.Println("  Sold asteptat (doar mesaje valide):")
	keys := make([]string, 0, len(expected))
	for k := range expected {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var total int64
	for _, k := range keys {
		fmt.Printf("    %-8s %8d\n", k, expected[k])
		total += expected[k]
	}
	fmt.Printf("    %-8s %8d\n", "TOTAL", total)
	if failed > 0 {
		os.Exit(1)
	}
}

func short(id string) string { return id[:8] }

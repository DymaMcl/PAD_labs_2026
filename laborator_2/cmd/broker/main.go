// Comanda broker porneste brokerul de mesaje cu jurnal persistent.
//
//	go run ./cmd/broker -addr :7070 -data data/broker.journal
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/broker"
)

func main() {
	addr := flag.String("addr", ":7070", "adresa HTTP")
	data := flag.String("data", "data/broker.journal", "fisierul jurnal (gol = doar in memorie)")
	fsync := flag.Bool("fsync", true, "fsync dupa fiecare inregistrare (durabilitate la cadere de sistem)")
	maxAttempts := flag.Int("max-attempts", 5, "tentative inainte de dead-letter")
	lease := flag.Duration("lease", 5*time.Second, "lease implicit pentru o livrare")
	backoff := flag.Duration("backoff", 500*time.Millisecond, "backoff initial dupa NACK")
	backoffMax := flag.Duration("backoff-max", 30*time.Second, "backoff maxim")
	dropAck := flag.Float64("drop-publish-ack", 0, "probabilitatea de a pierde ACK-ul catre producer (defect injectat)")
	flag.Parse()

	cfg := broker.DefaultConfig()
	cfg.MaxAttempts = *maxAttempts
	cfg.DefaultLease = *lease
	cfg.BackoffBase = *backoff
	cfg.BackoffMax = *backoffMax

	var b *broker.Broker
	if *data == "" {
		b = broker.New(cfg)
		log.Printf("broker in memorie (fara persistenta)")
	} else {
		var err error
		if b, err = broker.Open(*data, cfg, *fsync); err != nil {
			log.Fatalf("deschidere jurnal: %v", err)
		}
		log.Printf("jurnal: %s (fsync=%v)", *data, *fsync)
	}
	srv := broker.NewServer(b)
	srv.SetFaults(broker.Faults{DropPublishAck: *dropAck})
	mountDashboard(srv)

	hs := &http.Server{Addr: *addr, Handler: srv, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Printf("broker asculta pe %s  (dashboard: http://localhost%s/)", *addr, *addr)
		if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = hs.Shutdown(ctx)
	_ = b.Close()
	log.Printf("broker oprit")
}

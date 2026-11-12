// Comanda accounts porneste serviciul de conturi: API gRPC (accounts.v1 si
// accounts.v2) + o pagina HTTP de administrare.
//
//	go run ./cmd/accounts                         # gRPC :7080, admin http://localhost:7081/
//	go run ./cmd/accounts -drop-next 1            # primul raspuns la Credit se pierde
package main

import (
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/accounts"
)

func main() {
	grpcAddr := flag.String("grpc", ":7080", "adresa gRPC")
	httpAddr := flag.String("http", ":7081", "adresa paginii de administrare (gol = dezactivata)")
	data := flag.String("data", "data/accounts.journal", "jurnalul operatiilor (gol = doar in memorie)")
	processing := flag.Duration("processing", 50*time.Millisecond, "durata simulata a unei operatii Credit")
	minBudget := flag.Duration("min-budget", 0, "respinge imediat cererile cu mai putin timp ramas (implicit = -processing)")
	defDeadline := flag.Duration("default-deadline", 10*time.Second, "deadline aplicat cererilor fara deadline")
	disableV2 := flag.Bool("disable-v2", false, "simuleaza un server vechi care stie doar accounts.v1")
	dropNext := flag.Int("drop-next", 0, "urmatoarele N raspunsuri la Credit se pierd DUPA executie")
	dropRate := flag.Float64("drop-rate", 0, "probabilitatea pierderii raspunsului dupa executie")
	slowMs := flag.Int("slow-ms", 0, "intarziere a raspunsului dupa executie (ms)")
	unavailNext := flag.Int("unavailable-next", 0, "urmatoarele N cereri Credit sunt respinse inainte de executie")
	quiet := flag.Bool("quiet", false, "fara jurnal per operatie")
	flag.Parse()

	if *minBudget == 0 {
		*minBudget = *processing
	}
	store, err := accounts.Open(*data, accounts.Options{ProcessingTime: *processing})
	if err != nil {
		log.Fatal(err)
	}
	logf := log.Printf
	if *quiet {
		logf = func(string, ...any) {}
	}
	srv := accounts.NewServer(store, accounts.ServerOptions{
		DefaultDeadline: *defDeadline, MinBudget: *minBudget, DisableV2: *disableV2, Logf: logf,
	})
	srv.SetFaults(accounts.Faults{DropResponseNext: *dropNext, DropResponseRate: *dropRate,
		SlowResponseMs: *slowMs, UnavailableNext: *unavailNext})

	g := srv.GRPC()
	lis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	versions := "accounts.v1 + accounts.v2"
	if *disableV2 {
		versions = "doar accounts.v1 (server vechi)"
	}
	go func() {
		log.Printf("serviciul de conturi: gRPC pe %s (%s), jurnal %q", *grpcAddr, versions, *data)
		if err := g.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	var hs *http.Server
	if *httpAddr != "" {
		mux := srv.AdminHandler()
		mux.Handle("GET /{$}", adminPage())
		hs = &http.Server{Addr: *httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			log.Printf("administrare: http://localhost%s/", *httpAddr)
			if err := hs.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatal(err)
			}
		}()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop
	g.GracefulStop()
	if hs != nil {
		_ = hs.Close()
	}
	_ = store.Close()
	log.Printf("serviciu oprit")
}

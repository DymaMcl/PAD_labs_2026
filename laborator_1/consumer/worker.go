package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_1/client"
	"github.com/DymaMcl/PAD_labs_2026/laborator_1/contract"
)

// ErrCrashed este intors cand consumer-ul simuleaza o cadere dupa ce a
// realizat efectul local, dar inainte sa trimita ACK (scenariul critic).
var ErrCrashed = errors.New("CADERE simulata: efect aplicat, ACK netrimis")

// Options configureaza un worker.
type Options struct {
	Queue      string
	Name       string
	Idempotent bool          // foloseste inbox-ul pentru deduplicare
	Lease      time.Duration // cat timp are worker-ul pentru un mesaj
	Wait       time.Duration // long-poll
	Batch      int
	// CrashAfterEffect: la al N-lea efect aplicat, worker-ul "cade" fara ACK.
	CrashAfterEffect int
	// FailRate: probabilitatea unei erori tranzitorii simulate (-> NACK cu retry).
	FailRate     float64
	ProcessDelay time.Duration
	Logf         func(format string, args ...any)
}

// Worker preia mesaje si le aplica efectul pe registru.
type Worker struct {
	c       *client.Client
	l       *Ledger
	o       Options
	rnd     *rand.Rand
	applied int
}

// NewWorker creeaza un worker.
func NewWorker(c *client.Client, l *Ledger, o Options) *Worker {
	if o.Batch <= 0 {
		o.Batch = 1
	}
	if o.Lease <= 0 {
		o.Lease = 5 * time.Second
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	return &Worker{c: c, l: l, o: o, rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// Run proceseaza pana la anularea contextului sau o cadere simulata.
func (w *Worker) Run(ctx context.Context) error {
	return w.loop(ctx, 0)
}

// RunUntilIdle se opreste dupa ce coada a fost goala pe durata idle
// (util in teste si in scenariul demo).
func (w *Worker) RunUntilIdle(ctx context.Context, idle time.Duration) error {
	return w.loop(ctx, idle)
}

func (w *Worker) loop(ctx context.Context, idle time.Duration) error {
	lastWork := time.Now()
	for ctx.Err() == nil {
		ds, err := w.c.Receive(ctx, w.o.Queue, client.ReceiveOptions{
			Consumer: w.o.Name, Max: w.o.Batch, Lease: w.o.Lease, Wait: w.o.Wait,
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.o.Logf("receive esuat: %v (reincerc)", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		if len(ds) == 0 {
			if idle > 0 && time.Since(lastWork) >= idle {
				return nil
			}
			if w.o.Wait == 0 {
				time.Sleep(50 * time.Millisecond)
			}
			continue
		}
		lastWork = time.Now()
		for _, d := range ds {
			if err := w.Handle(ctx, d); err != nil {
				return err
			}
		}
	}
	return ctx.Err()
}

// Handle proceseaza o singura livrare. Ordinea pasilor este esentiala:
//
//  1. validare de business  -> NACK fara retry (DLQ) daca mesajul e "otravit"
//  2. deduplicare (inbox)   -> daca efectul exista deja: doar ACK
//  3. efect + inbox atomic  -> o singura scriere locala
//  4. ACK                   -> abia acum brokerul poate uita mesajul
//
// O cadere intre 3 si 4 lasa efectul aplicat si mesajul neconfirmat: brokerul
// il va relivra, iar pasul 2 impiedica dublarea efectului.
func (w *Worker) Handle(ctx context.Context, d contract.Delivery) error {
	m := d.Message
	tag := fmt.Sprintf("%s %s seq=%d try=%d", m.ID[:8], m.Key, m.Seq, d.Attempt)

	p, perr := decodeCredit(m)
	if perr != nil {
		res, err := w.c.Nack(ctx, d.Queue, d.DeliveryID, "permanent: "+perr.Error(), false)
		w.o.Logf("☠ %s respins definitiv (%v) -> %s %v", tag, perr, res.Outcome, errOrEmpty(err))
		return nil
	}

	if w.o.FailRate > 0 && w.rnd.Float64() < w.o.FailRate {
		res, err := w.c.Nack(ctx, d.Queue, d.DeliveryID, "tranzitoriu: dependinta indisponibila (simulat)", true)
		w.o.Logf("↻ %s eroare tranzitorie -> %s %v", tag, res.Outcome, errOrEmpty(err))
		return nil
	}

	if w.o.Idempotent && w.l.Seen(m.ID) {
		err := w.c.Ack(ctx, d.Queue, d.DeliveryID)
		w.o.Logf("≡ %s DUPLICAT detectat in inbox -> efect omis, doar ACK %v", tag, errOrEmpty(err))
		return nil
	}

	if w.o.ProcessDelay > 0 {
		time.Sleep(w.o.ProcessDelay)
	}
	if err := w.l.ApplyCredit(m, p, w.o.Idempotent); errors.Is(err, ErrAlreadyApplied) {
		aerr := w.c.Ack(ctx, d.Queue, d.DeliveryID)
		w.o.Logf("≡ %s DUPLICAT (concurent) -> efect omis, doar ACK %v", tag, errOrEmpty(aerr))
		return nil
	} else if err != nil {
		res, nerr := w.c.Nack(ctx, d.Queue, d.DeliveryID, "stocare locala: "+err.Error(), true)
		w.o.Logf("↻ %s efect esuat (%v) -> %s %v", tag, err, res.Outcome, errOrEmpty(nerr))
		return nil
	}
	w.applied++
	w.o.Logf("✚ %s efect aplicat: %s += %d", tag, p.Account, p.Amount)

	if w.o.CrashAfterEffect > 0 && w.applied == w.o.CrashAfterEffect {
		w.o.Logf("💥 %s consumer-ul cade INAINTE de ACK", tag)
		return ErrCrashed
	}

	if err := w.c.Ack(ctx, d.Queue, d.DeliveryID); err != nil {
		if errors.Is(err, client.ErrStaleDelivery) {
			w.o.Logf("⚠ %s ACK respins: lease expirat, mesajul va fi relivrat", tag)
			return nil
		}
		// ACK pierdut pe retea: la fel, mesajul va reveni; inbox-ul ne protejeaza
		w.o.Logf("⚠ %s ACK esuat: %v", tag, err)
		return nil
	}
	w.o.Logf("✓ %s ACK", tag)
	return nil
}

func decodeCredit(m contract.Message) (contract.CreditPayload, error) {
	var p contract.CreditPayload
	if m.Type != contract.TypeCredit {
		return p, fmt.Errorf("tip necunoscut %q", m.Type)
	}
	if err := json.Unmarshal(m.Payload, &p); err != nil {
		return p, fmt.Errorf("payload: %v", err)
	}
	if p.Account == "" {
		return p, errors.New("cont lipsa")
	}
	if p.Account != m.Key {
		return p, fmt.Errorf("contul %s nu corespunde cheii %s", p.Account, m.Key)
	}
	if p.Amount <= 0 {
		return p, fmt.Errorf("suma %d nu este pozitiva", p.Amount)
	}
	return p, nil
}

func errOrEmpty(err error) string {
	if err == nil {
		return ""
	}
	return "(" + err.Error() + ")"
}

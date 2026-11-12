package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/client"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
)

// ErrCrashed este intors cand consumer-ul simuleaza o cadere dupa ce a
// realizat efectul, dar inainte sa trimita ACK (scenariul critic din lab. 1).
var ErrCrashed = errors.New("CADERE simulata: efect aplicat, ACK netrimis")

// Options configureaza un worker.
type Options struct {
	Queue string
	Name  string
	Lease time.Duration // cat timp are worker-ul pentru un mesaj
	Wait  time.Duration // long-poll
	Batch int
	// CrashAfterEffect: la al N-lea efect aplicat, worker-ul "cade" fara ACK.
	CrashAfterEffect int
	// FailRate: probabilitatea unei erori tranzitorii simulate (-> NACK cu retry).
	FailRate     float64
	ProcessDelay time.Duration
	Logf         func(format string, args ...any)
}

// Worker preia mesaje si le aplica efectul.
type Worker struct {
	c       *client.Client
	eff     Effect
	o       Options
	rnd     *rand.Rand
	applied int
}

// NewWorker creeaza un worker cu efectul dat (local sau la distanta).
func NewWorker(c *client.Client, eff Effect, o Options) *Worker {
	if o.Batch <= 0 {
		o.Batch = 1
	}
	if o.Lease <= 0 {
		o.Lease = 5 * time.Second
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
	return &Worker{c: c, eff: eff, o: o, rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// NewLocalWorker pastreaza interfata din laboratorul 1.
func NewLocalWorker(c *client.Client, l *Ledger, idempotent bool, o Options) *Worker {
	return NewWorker(c, &LocalEffect{Ledger: l, Idempotent: idempotent}, o)
}

// Run proceseaza pana la anularea contextului sau o cadere simulata.
func (w *Worker) Run(ctx context.Context) error { return w.loop(ctx, 0) }

// RunUntilIdle se opreste dupa ce coada a fost goala pe durata idle.
func (w *Worker) RunUntilIdle(ctx context.Context, idle time.Duration) error {
	return w.loop(ctx, idle)
}

func (w *Worker) loop(ctx context.Context, idle time.Duration) error {
	lastWork := time.Now()
	gate, _ := w.eff.(Gate)
	paused := false
	for ctx.Err() == nil {
		if gate != nil && !gate.Ready() {
			if !paused {
				w.o.Logf("⏸ serviciul este indisponibil (circuit deschis): opresc preluarea, mesajele asteapta in broker")
				paused = true
			}
			select {
			case <-ctx.Done():
			case <-time.After(100 * time.Millisecond):
			}
			lastWork = time.Now()
			continue
		}
		if paused {
			w.o.Logf("▶ reiau preluarea (circuit half-open/inchis)")
			paused = false
		}
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

// Handle proceseaza o livrare: validare -> efect -> ACK/NACK in functie de
// rezultatul EXPLICIT al efectului. ACK se trimite doar cand stim ca efectul
// exista; in orice alta situatie mesajul ramane la broker.
func (w *Worker) Handle(ctx context.Context, d contract.Delivery) error {
	m := d.Message
	tag := fmt.Sprintf("%s %s seq=%d try=%d", m.ID[:8], m.Key, m.Seq, d.Attempt)

	p, perr := decodeCredit(m)
	if perr != nil {
		res, err := w.c.Nack(ctx, d.Queue, d.DeliveryID, "permanent: "+perr.Error(), false)
		w.o.Logf("☠ %s respins definitiv (%v) -> %s %s", tag, perr, res.Outcome, errOrEmpty(err))
		return nil
	}

	if w.o.FailRate > 0 && w.rnd.Float64() < w.o.FailRate {
		res, err := w.c.Nack(ctx, d.Queue, d.DeliveryID, "tranzitoriu: dependinta indisponibila (simulat)", true)
		w.o.Logf("↻ %s eroare tranzitorie -> %s %s", tag, res.Outcome, errOrEmpty(err))
		return nil
	}
	if w.o.ProcessDelay > 0 {
		time.Sleep(w.o.ProcessDelay)
	}

	r := w.eff.Apply(ctx, d, p)
	switch r.Status {
	case EffectPermanent:
		res, err := w.c.Nack(ctx, d.Queue, d.DeliveryID, "permanent: "+r.Detail, false)
		w.o.Logf("☠ %s %s -> %s %s", tag, r.Detail, res.Outcome, errOrEmpty(err))
		return nil
	case EffectRetry:
		res, err := w.c.Nack(ctx, d.Queue, d.DeliveryID, r.Detail, true)
		w.o.Logf("↻ %s %s -> %s %s", tag, r.Detail, res.Outcome, errOrEmpty(err))
		return nil
	case EffectDuplicate:
		err := w.c.Ack(ctx, d.Queue, d.DeliveryID)
		w.o.Logf("≡ %s %s, doar ACK %s", tag, r.Detail, errOrEmpty(err))
		return nil
	}

	w.applied++
	w.o.Logf("✚ %s %s", tag, r.Detail)
	if w.o.CrashAfterEffect > 0 && w.applied == w.o.CrashAfterEffect {
		w.o.Logf("💥 %s consumer-ul cade INAINTE de ACK", tag)
		return ErrCrashed
	}
	if err := w.c.Ack(ctx, d.Queue, d.DeliveryID); err != nil {
		if errors.Is(err, client.ErrStaleDelivery) {
			w.o.Logf("⚠ %s ACK respins: lease expirat, mesajul va fi relivrat", tag)
			return nil
		}
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

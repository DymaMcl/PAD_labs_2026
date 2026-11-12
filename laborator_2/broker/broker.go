// Package broker implementeaza un broker de mesaje cu livrare at-least-once:
// un mesaj ramane in coada pana cand consumer-ul confirma explicit (ACK)
// efectul lui. Livrarile au un "lease" (timp de vizibilitate); daca lease-ul
// expira fara ACK, mesajul este livrat din nou.
package broker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
)

// Erori expuse de broker (mapate pe coduri HTTP in http.go).
var (
	ErrStaleDelivery = errors.New("livrarea nu mai este activa (lease expirat, deja confirmata sau necunoscuta)")
	ErrStaleSequence = errors.New("seq mai mic sau egal cu ultimul acceptat pentru (source, key)")
	ErrNotFound      = errors.New("mesaj inexistent")
)

// Config parametrizeaza comportamentul brokerului.
type Config struct {
	MaxAttempts  int           // dupa atatea livrari esuate mesajul ajunge in DLQ
	DefaultLease time.Duration // lease implicit daca consumer-ul nu cere altul
	MaxLease     time.Duration
	BackoffBase  time.Duration // intarzierea primei reincercari dupa NACK
	BackoffMax   time.Duration
	DedupWindow  time.Duration // cat timp tinem minte ID-urile vazute
	Now          func() time.Time
}

// DefaultConfig intoarce valori rezonabile pentru laborator.
func DefaultConfig() Config {
	return Config{
		MaxAttempts:  5,
		DefaultLease: 5 * time.Second,
		MaxLease:     5 * time.Minute,
		BackoffBase:  500 * time.Millisecond,
		BackoffMax:   30 * time.Second,
		DedupWindow:  24 * time.Hour,
		Now:          time.Now,
	}
}

// State este starea unui mesaj in broker.
type State string

const (
	StateReady    State = "ready"
	StateInFlight State = "in_flight"
	StateDead     State = "dead"
)

type entry struct {
	msg        contract.Message
	offset     uint64 // ordinea de acceptare in coada
	state      State
	attempts   int
	notBefore  time.Time
	deliveryID string
	leaseUntil time.Time
	consumer   string
	lastError  string
	enqueuedAt time.Time
	deadAt     time.Time
}

type counters struct {
	Published   uint64 `json:"published"`
	Duplicates  uint64 `json:"duplicates"`
	Rejected    uint64 `json:"rejected"`
	Delivered   uint64 `json:"delivered"`
	Redelivered uint64 `json:"redelivered"`
	Acked       uint64 `json:"acked"`
	Nacked      uint64 `json:"nacked"`
	Expired     uint64 `json:"lease_expired"`
	DeadLetters uint64 `json:"dead_lettered"`
	StaleAcks   uint64 `json:"stale_acks"`
}

type queue struct {
	name       string
	nextOffset uint64
	pending    []*entry          // ready + in_flight, in ordinea de acceptare
	byID       map[string]*entry // mesaje vii (pending + dead)
	inflight   map[string]*entry // deliveryID -> entry
	seen       map[string]time.Time
	lastSeq    map[string]uint64 // source|key -> ultimul seq acceptat
	stats      counters
}

func newQueue(name string) *queue {
	return &queue{
		name:     name,
		byID:     map[string]*entry{},
		inflight: map[string]*entry{},
		seen:     map[string]time.Time{},
		lastSeq:  map[string]uint64{},
	}
}

// Broker este sigur pentru utilizare concurenta.
type Broker struct {
	cfg     Config
	mu      sync.Mutex
	queues  map[string]*queue
	changed chan struct{} // inchis si recreat la orice schimbare (long-poll)
	events  *eventLog
	j       *journal // nil pentru brokerul pur in memorie
}

// New creeaza un broker in memorie.
func New(cfg Config) *Broker {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.DefaultLease <= 0 {
		cfg.DefaultLease = 5 * time.Second
	}
	if cfg.MaxLease < cfg.DefaultLease {
		cfg.MaxLease = cfg.DefaultLease
	}
	if cfg.DedupWindow <= 0 {
		cfg.DedupWindow = 24 * time.Hour
	}
	return &Broker{
		cfg:     cfg,
		queues:  map[string]*queue{},
		changed: make(chan struct{}),
		events:  newEventLog(500),
	}
}

func (b *Broker) queueLocked(name string) *queue {
	q, ok := b.queues[name]
	if !ok {
		q = newQueue(name)
		b.queues[name] = q
	}
	return q
}

func (b *Broker) notifyLocked() {
	close(b.changed)
	b.changed = make(chan struct{})
}

// PublishResult este raspunsul la publicare (ACK-ul brokerului catre producer).
type PublishResult struct {
	Status string `json:"status"` // "accepted" sau "duplicate"
	Offset uint64 `json:"offset"`
}

func seqKey(m contract.Message) string { return m.Source + "|" + m.Key }

// Publish valideaza si accepta un mesaj. Publicarea este idempotenta dupa ID:
// un producer care nu a primit ACK poate retrimite in siguranta acelasi mesaj.
func (b *Broker) Publish(queueName string, m contract.Message) (PublishResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	q := b.queueLocked(queueName)
	now := b.cfg.Now()

	if err := m.Validate(); err != nil {
		q.stats.Rejected++
		b.events.add(now, queueName, "rejected", m.ID, m.Key, err.Error())
		return PublishResult{}, err
	}
	b.pruneSeenLocked(q, now)
	if _, dup := q.seen[m.ID]; dup {
		q.stats.Duplicates++
		b.events.add(now, queueName, "duplicate", m.ID, m.Key, "publicare repetata ignorata")
		return PublishResult{Status: "duplicate"}, nil
	}
	if last := q.lastSeq[seqKey(m)]; m.Seq <= last {
		q.stats.Rejected++
		b.events.add(now, queueName, "rejected", m.ID, m.Key, fmt.Sprintf("seq %d <= %d", m.Seq, last))
		return PublishResult{}, fmt.Errorf("%w: primit %d, ultimul %d", ErrStaleSequence, m.Seq, last)
	}

	// write-ahead: mesajul ajunge in jurnal inainte de ACK-ul catre producer
	if err := b.logLocked(record{Op: "publish", Q: queueName, ID: m.ID, Msg: &m, At: now}); err != nil {
		return PublishResult{}, fmt.Errorf("persistare esuata: %w", err)
	}
	e := b.acceptLocked(q, m, now)
	b.events.add(now, queueName, "published", m.ID, m.Key, fmt.Sprintf("offset=%d seq=%d", e.offset, m.Seq))
	b.notifyLocked()
	return PublishResult{Status: "accepted", Offset: e.offset}, nil
}

func (b *Broker) acceptLocked(q *queue, m contract.Message, now time.Time) *entry {
	q.nextOffset++
	e := &entry{msg: m, offset: q.nextOffset, state: StateReady, enqueuedAt: now}
	q.pending = append(q.pending, e)
	q.byID[m.ID] = e
	q.seen[m.ID] = now
	if m.Seq > q.lastSeq[seqKey(m)] {
		q.lastSeq[seqKey(m)] = m.Seq
	}
	q.stats.Published++
	return e
}

func (b *Broker) pruneSeenLocked(q *queue, now time.Time) {
	for id, t := range q.seen {
		if now.Sub(t) > b.cfg.DedupWindow {
			if _, live := q.byID[id]; !live {
				delete(q.seen, id)
			}
		}
	}
}

// reapLocked trateaza lease-urile expirate: livrarea este considerata esuata.
func (b *Broker) reapLocked(now time.Time) {
	for _, q := range b.queues {
		for did, e := range q.inflight {
			if now.Before(e.leaseUntil) {
				continue
			}
			delete(q.inflight, did)
			e.deliveryID = ""
			q.stats.Expired++
			e.lastError = fmt.Sprintf("lease expirat (consumer %s nu a confirmat)", e.consumer)
			b.events.add(now, q.name, "lease_expired", e.msg.ID, e.msg.Key, fmt.Sprintf("tentativa %d", e.attempts))
			if e.attempts >= b.cfg.MaxAttempts {
				b.deadLetterLocked(q, e, now, fmt.Sprintf("lease expirat de %d ori", e.attempts))
				continue
			}
			e.state = StateReady
			e.notBefore = now
		}
	}
}

// backoff calculeaza intarzierea exponentiala dupa tentativa n (1, 2, 4, 8 ... x base).
func (b *Broker) backoff(n int) time.Duration {
	d := b.cfg.BackoffBase
	for i := 1; i < n && d < b.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > b.cfg.BackoffMax {
		d = b.cfg.BackoffMax
	}
	return d
}

func (b *Broker) deadLetterLocked(q *queue, e *entry, now time.Time, reason string) {
	if e.deliveryID != "" {
		delete(q.inflight, e.deliveryID)
		e.deliveryID = ""
	}
	_ = b.logLocked(record{Op: "dead", Q: q.name, ID: e.msg.ID, Reason: reason, At: now})
	q.removePending(e)
	e.state = StateDead
	e.deadAt = now
	e.lastError = reason
	q.stats.DeadLetters++
	b.events.add(now, q.name, "dead_lettered", e.msg.ID, e.msg.Key, reason)
	b.notifyLocked()
}

// NackResult descrie ce s-a intamplat cu mesajul dupa NACK.
type NackResult struct {
	Outcome string    `json:"outcome"` // "retry_scheduled" sau "dead_lettered"
	RetryAt time.Time `json:"retry_at,omitempty"`
	Attempt int       `json:"attempt"`
}

// Nack semnaleaza ca procesarea a esuat. Cu retry=true mesajul este reprogramat
// cu backoff exponential (pana la MaxAttempts); cu retry=false (eroare
// permanenta, ex. mesaj "otravit") merge direct in dead-letter queue.
func (b *Broker) Nack(queueName, deliveryID, reason string, retry bool) (NackResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	b.reapLocked(now)
	q := b.queueLocked(queueName)
	e, ok := q.inflight[deliveryID]
	if !ok {
		q.stats.StaleAcks++
		b.events.add(now, queueName, "stale_ack", "", "", "nack pe delivery "+short(deliveryID))
		return NackResult{}, ErrStaleDelivery
	}
	q.stats.Nacked++
	if reason == "" {
		reason = "nack fara motiv"
	}
	if !retry || e.attempts >= b.cfg.MaxAttempts {
		why := reason
		if retry {
			why = fmt.Sprintf("%s (epuizat %d/%d tentative)", reason, e.attempts, b.cfg.MaxAttempts)
		}
		b.deadLetterLocked(q, e, now, why)
		return NackResult{Outcome: "dead_lettered", Attempt: e.attempts}, nil
	}
	retryAt := now.Add(b.backoff(e.attempts))
	if err := b.logLocked(record{Op: "nack", Q: queueName, ID: e.msg.ID, Reason: reason, NotBefore: &retryAt, At: now}); err != nil {
		return NackResult{}, fmt.Errorf("persistare esuata: %w", err)
	}
	delete(q.inflight, deliveryID)
	e.deliveryID = ""
	e.state = StateReady
	e.lastError = reason
	e.notBefore = retryAt
	b.events.add(now, queueName, "retry_scheduled", e.msg.ID, e.msg.Key,
		fmt.Sprintf("%s; reincercare peste %s", reason, e.notBefore.Sub(now)))
	b.notifyLocked()
	return NackResult{Outcome: "retry_scheduled", RetryAt: e.notBefore, Attempt: e.attempts}, nil
}

// DeadLetter este un mesaj din DLQ impreuna cu motivul.
type DeadLetter struct {
	Message  contract.Message `json:"message"`
	Attempts int              `json:"attempts"`
	Reason   string           `json:"reason"`
	DeadAt   time.Time        `json:"dead_at"`
}

// DeadLetters listeaza continutul DLQ pentru o coada.
func (b *Broker) DeadLetters(queueName string) []DeadLetter {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reapLocked(b.cfg.Now())
	q := b.queueLocked(queueName)
	out := []DeadLetter{}
	for _, e := range q.byID {
		if e.state == StateDead {
			out = append(out, DeadLetter{Message: e.msg, Attempts: e.attempts, Reason: e.lastError, DeadAt: e.deadAt})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].DeadAt.Before(out[j].DeadAt) })
	return out
}

// Replay muta un mesaj din DLQ inapoi in coada (dupa corectarea cauzei),
// cu contorul de tentative resetat. Mesajul este pus la coada cozii.
func (b *Broker) Replay(queueName, msgID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	q := b.queueLocked(queueName)
	e, ok := q.byID[msgID]
	if !ok || e.state != StateDead {
		return ErrNotFound
	}
	b.replayLocked(q, e, now)
	return nil
}

func (b *Broker) replayLocked(q *queue, e *entry, now time.Time) {
	_ = b.logLocked(record{Op: "replay", Q: q.name, ID: e.msg.ID, At: now})
	q.nextOffset++
	e.offset = q.nextOffset
	e.state = StateReady
	e.attempts = 0
	e.notBefore = now
	e.lastError = ""
	q.pending = append(q.pending, e)
	b.events.add(now, q.name, "replayed", e.msg.ID, e.msg.Key, "mutat din DLQ in coada")
	b.notifyLocked()
}

// Receive preia pana la max mesaje pentru consumer. Daca nu exista mesaje
// disponibile asteapta pana la wait (long-poll).
func (b *Broker) Receive(ctx context.Context, queueName, consumer string, max int, lease, wait time.Duration) []contract.Delivery {
	if max <= 0 {
		max = 1
	}
	if lease <= 0 {
		lease = b.cfg.DefaultLease
	}
	if lease > b.cfg.MaxLease {
		lease = b.cfg.MaxLease
	}
	deadline := time.Now().Add(wait)
	for {
		b.mu.Lock()
		now := b.cfg.Now()
		b.reapLocked(now)
		out := b.takeLocked(b.queueLocked(queueName), consumer, max, lease, now)
		ch := b.changed
		b.mu.Unlock()

		if len(out) > 0 || wait <= 0 {
			return out
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		// re-verificam periodic: lease-uri care expira, backoff care se termina
		tick := 100 * time.Millisecond
		if remaining < tick {
			tick = remaining
		}
		t := time.NewTimer(tick)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-ch:
		case <-t.C:
		}
		t.Stop()
	}
}

// takeLocked selecteaza mesajele livrabile respectand ordonarea per cheie:
// doar mesajul cel mai vechi (capul) al fiecarei chei poate fi livrat. Cat
// timp capul este in zbor sau asteapta reincercarea (backoff), mesajele
// urmatoare cu aceeasi cheie sunt blocate. Cheile diferite nu se blocheaza
// intre ele, deci mai multi consumeri pot lucra in paralel pe chei diferite.
func (b *Broker) takeLocked(q *queue, consumer string, max int, lease time.Duration, now time.Time) []contract.Delivery {
	var out []contract.Delivery
	headSeen := map[string]bool{}
	for _, e := range q.pending {
		if len(out) >= max {
			break
		}
		key := e.msg.Key
		if headSeen[key] {
			continue // exista un mesaj mai vechi nefinalizat cu aceeasi cheie
		}
		headSeen[key] = true
		if e.state != StateReady || now.Before(e.notBefore) {
			continue
		}
		out = append(out, b.deliverLocked(q, e, consumer, lease, now))
	}
	return out
}

func (b *Broker) deliverLocked(q *queue, e *entry, consumer string, lease time.Duration, now time.Time) contract.Delivery {
	e.attempts++
	_ = b.logLocked(record{Op: "attempt", Q: q.name, ID: e.msg.ID, Attempt: e.attempts, At: now})
	e.state = StateInFlight
	e.deliveryID = contract.NewID()
	e.leaseUntil = now.Add(lease)
	e.consumer = consumer
	q.inflight[e.deliveryID] = e
	q.stats.Delivered++
	kind := "delivered"
	if e.attempts > 1 {
		q.stats.Redelivered++
		kind = "redelivered"
	}
	b.events.add(now, q.name, kind, e.msg.ID, e.msg.Key, fmt.Sprintf("catre %s, tentativa %d", consumer, e.attempts))
	return contract.Delivery{
		Message:    e.msg,
		Queue:      q.name,
		DeliveryID: e.deliveryID,
		Attempt:    e.attempts,
		LeaseUntil: e.leaseUntil,
	}
}

func (q *queue) removePending(e *entry) {
	for i, p := range q.pending {
		if p == e {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			return
		}
	}
}

// Ack confirma ca efectul mesajului a fost realizat; mesajul este eliminat.
func (b *Broker) Ack(queueName, deliveryID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	b.reapLocked(now)
	q := b.queueLocked(queueName)
	e, ok := q.inflight[deliveryID]
	if !ok {
		q.stats.StaleAcks++
		b.events.add(now, queueName, "stale_ack", "", "", "delivery "+short(deliveryID))
		return ErrStaleDelivery
	}
	if err := b.logLocked(record{Op: "ack", Q: queueName, ID: e.msg.ID, At: now}); err != nil {
		return fmt.Errorf("persistare esuata: %w", err)
	}
	delete(q.inflight, deliveryID)
	q.removePending(e)
	delete(q.byID, e.msg.ID)
	q.stats.Acked++
	b.events.add(now, queueName, "acked", e.msg.ID, e.msg.Key, fmt.Sprintf("dupa %d tentativ(e)", e.attempts))
	b.notifyLocked()
	return nil
}

// QueueStats este o fotografie a unei cozi.
type QueueStats struct {
	Name     string   `json:"name"`
	Ready    int      `json:"ready"`
	Delayed  int      `json:"delayed"`
	InFlight int      `json:"in_flight"`
	Dead     int      `json:"dead"`
	Counters counters `json:"counters"`
}

// Stats intoarce statistici pentru toate cozile, sortate dupa nume.
func (b *Broker) Stats() []QueueStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	b.reapLocked(now)
	out := make([]QueueStats, 0, len(b.queues))
	for _, q := range b.queues {
		s := QueueStats{Name: q.name, Counters: q.stats}
		for _, e := range q.pending {
			switch {
			case e.state == StateInFlight:
				s.InFlight++
			case now.Before(e.notBefore):
				s.Delayed++
			default:
				s.Ready++
			}
		}
		s.Dead = len(q.byID) - len(q.pending)
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Events intoarce evenimentele cu numar de ordine > since.
func (b *Broker) Events(since uint64) []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.events.since(since)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// PublicConfig expune parametrii relevanti pentru clienti si dashboard.
func (b *Broker) PublicConfig() map[string]any {
	return map[string]any{
		"max_attempts":     b.cfg.MaxAttempts,
		"default_lease_ms": b.cfg.DefaultLease.Milliseconds(),
		"backoff_base_ms":  b.cfg.BackoffBase.Milliseconds(),
		"backoff_max_ms":   b.cfg.BackoffMax.Milliseconds(),
		"persistent":       b.j != nil,
	}
}

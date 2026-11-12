// Package consumer contine logica consumer-ului: efectul local (un registru
// de solduri) si bucla de procesare cu ACK/NACK.
package consumer

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_1/contract"
)

// InboxEntry inregistreaza un mesaj al carui efect a fost deja aplicat.
type InboxEntry struct {
	Key       string    `json:"key"`
	Seq       uint64    `json:"seq"`
	AppliedAt time.Time `json:"applied_at"`
}

type ledgerState struct {
	Balances map[string]int64 `json:"balances"`
	// Inbox = tabela de deduplicare. Este scrisa in ACEEASI tranzactie locala
	// cu efectul, deci "efect aplicat" si "mesaj marcat ca procesat" devin
	// adevarate simultan sau deloc.
	Inbox map[string]InboxEntry `json:"inbox"`
	// Executions este audit: de cate ori s-a executat efectul pentru fiecare
	// mesaj. >1 inseamna efect duplicat (bug-ul pe care il demonstram).
	Executions      map[string]int    `json:"executions"`
	LastSeq         map[string]uint64 `json:"last_seq"`
	OrderViolations int               `json:"order_violations"`
}

// Ledger este starea locala a consumer-ului, persistata atomic intr-un fisier
// JSON (scriere in fisier temporar + fsync + rename).
type Ledger struct {
	mu   sync.Mutex
	path string
	st   ledgerState
}

func emptyState() ledgerState {
	return ledgerState{
		Balances:   map[string]int64{},
		Inbox:      map[string]InboxEntry{},
		Executions: map[string]int{},
		LastSeq:    map[string]uint64{},
	}
}

// OpenLedger incarca registrul (path gol = doar in memorie).
func OpenLedger(path string) (*Ledger, error) {
	l := &Ledger{path: path, st: emptyState()}
	if path == "" {
		return l, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &l.st); err != nil {
		return nil, fmt.Errorf("registru corupt %s: %w", path, err)
	}
	if l.st.Balances == nil {
		l.st = emptyState()
	}
	return l, nil
}

// Seen raporteaza daca efectul mesajului a fost deja aplicat (inbox).
func (l *Ledger) Seen(msgID string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.st.Inbox[msgID]
	return ok
}

// ErrAlreadyApplied: efectul mesajului exista deja in registru (inbox).
var ErrAlreadyApplied = errors.New("efect deja aplicat")

// ApplyCredit aplica efectul unui mesaj payment.credit si il persista.
// Cu recordInbox=true, marcarea in inbox face parte din aceeasi scriere atomica.
func (l *Ledger) ApplyCredit(m contract.Message, p contract.CreditPayload, recordInbox bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	// verificare si aplicare sub acelasi lock: doi workeri care primesc acelasi
	// mesaj (lease expirat in timpul procesarii) nu pot aplica efectul de doua ori
	if _, done := l.st.Inbox[m.ID]; recordInbox && done {
		return ErrAlreadyApplied
	}

	next := cloneState(l.st)
	next.Balances[p.Account] += p.Amount
	next.Executions[m.ID]++
	sk := m.Source + "|" + m.Key
	if last := next.LastSeq[sk]; m.Seq < last {
		next.OrderViolations++
	} else {
		next.LastSeq[sk] = m.Seq
	}
	if recordInbox {
		next.Inbox[m.ID] = InboxEntry{Key: m.Key, Seq: m.Seq, AppliedAt: time.Now().UTC()}
	}
	if err := l.persist(next); err != nil {
		return err // starea din memorie ramane neschimbata
	}
	l.st = next
	return nil
}

func (l *Ledger) persist(st ledgerState) error {
	if l.path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := l.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, l.path)
}

func cloneState(s ledgerState) ledgerState {
	n := emptyState()
	for k, v := range s.Balances {
		n.Balances[k] = v
	}
	for k, v := range s.Inbox {
		n.Inbox[k] = v
	}
	for k, v := range s.Executions {
		n.Executions[k] = v
	}
	for k, v := range s.LastSeq {
		n.LastSeq[k] = v
	}
	n.OrderViolations = s.OrderViolations
	return n
}

// Summary este un rezumat al registrului.
type Summary struct {
	Balances        map[string]int64 `json:"balances"`
	Total           int64            `json:"total"`
	Messages        int              `json:"messages"`         // mesaje distincte executate
	Executions      int              `json:"executions"`       // executii totale ale efectului
	DuplicateEffect int              `json:"duplicate_effect"` // executii in plus
	InboxSize       int              `json:"inbox_size"`
	OrderViolations int              `json:"order_violations"`
}

// Summary calculeaza rezumatul curent.
func (l *Ledger) Summary() Summary {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := Summary{Balances: map[string]int64{}, InboxSize: len(l.st.Inbox), OrderViolations: l.st.OrderViolations}
	for k, v := range l.st.Balances {
		s.Balances[k] = v
		s.Total += v
	}
	for _, n := range l.st.Executions {
		s.Messages++
		s.Executions += n
		s.DuplicateEffect += n - 1
	}
	return s
}

// String formateaza rezumatul ca tabel text.
func (s Summary) String() string {
	keys := make([]string, 0, len(s.Balances))
	for k := range s.Balances {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := ""
	for _, k := range keys {
		out += fmt.Sprintf("    %-8s %8d\n", k, s.Balances[k])
	}
	out += fmt.Sprintf("    %-8s %8d\n", "TOTAL", s.Total)
	out += fmt.Sprintf("  mesaje distincte=%d  executii efect=%d  efecte DUPLICATE=%d  inbox=%d  incalcari ordine=%d",
		s.Messages, s.Executions, s.DuplicateEffect, s.InboxSize, s.OrderViolations)
	return out
}

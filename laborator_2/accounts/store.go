// Package accounts implementeaza serviciul de conturi: starea (solduri +
// registrul de idempotenta) si adaptoarele gRPC pentru contractele v1 si v2.
package accounts

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Currency este singura moneda suportata de registru.
const Currency = "MDL"

// MaxCredit este limita de business per operatie (in bani). O cerere peste
// limita este valida sintactic, dar RESPINSA; respingerea este memorata sub
// cheia de idempotenta, deci retrimiterea primeste aceeasi respingere.
const MaxCredit = 1_000_000

var (
	ErrInvalid    = errors.New("cerere invalida")
	ErrKeyReuse   = errors.New("cheie de idempotenta refolosita pentru o alta cerere")
	ErrInProgress = errors.New("o cerere cu aceeasi cheie este in curs")
	ErrNotFound   = errors.New("operatie inexistenta")
)

// Status este starea unei operatii in registrul de idempotenta.
type Status string

const (
	StatusNotFound   Status = "NOT_FOUND"
	StatusInProgress Status = "IN_PROGRESS"
	StatusSucceeded  Status = "SUCCEEDED"
	StatusFailed     Status = "FAILED"
)

// Operation este o operatie executata (sau respinsa) si memorata.
type Operation struct {
	ID           string    `json:"id"`
	Key          string    `json:"key"`
	Fingerprint  string    `json:"fingerprint"`
	Account      string    `json:"account"`
	Amount       int64     `json:"amount"`
	Currency     string    `json:"currency"`
	Reference    string    `json:"reference,omitempty"`
	Note         string    `json:"note,omitempty"`
	Status       Status    `json:"status"`
	BalanceAfter int64     `json:"balance_after"`
	Failure      string    `json:"failure,omitempty"`
	AppliedAt    time.Time `json:"applied_at"`
	APIVersion   string    `json:"api_version"`
}

// CreditCmd este comanda interna, independenta de versiunea contractului.
type CreditCmd struct {
	Key        string
	Account    string
	Amount     int64
	Currency   string
	Reference  string
	Note       string
	APIVersion string
}

var accountRe = regexp.MustCompile(`^ACC-[0-9]{1,9}$`)

func (c CreditCmd) validate() error {
	var p []string
	if c.Key == "" || len(c.Key) > 128 {
		p = append(p, "idempotency_key trebuie sa aiba 1..128 caractere")
	}
	if !accountRe.MatchString(c.Account) {
		p = append(p, fmt.Sprintf("account %q nu respecta formatul ACC-<numar>", c.Account))
	}
	if c.Amount <= 0 {
		p = append(p, "amount trebuie sa fie > 0")
	}
	if c.Currency != Currency {
		p = append(p, fmt.Sprintf("moneda %q nesuportata (doar %s)", c.Currency, Currency))
	}
	if len(p) > 0 {
		return fmt.Errorf("%w: %v", ErrInvalid, p)
	}
	return nil
}

// Fingerprint identifica semantica cererii: aceeasi cheie trebuie folosita
// doar pentru aceeasi operatie (cont, suma, moneda). Campurile informative
// (nota, referinta) nu intra in amprenta, ca un client v1 si unul v2 sa poata
// relua aceeasi operatie.
func (c CreditCmd) Fingerprint() string {
	h := sha256.Sum256([]byte(fmt.Sprintf("credit|%s|%d|%s", c.Account, c.Amount, c.Currency)))
	return hex.EncodeToString(h[:8])
}

// Options configureaza un Store.
type Options struct {
	// ProcessingTime simuleaza durata operatiei (ex. apel la core banking).
	ProcessingTime time.Duration
	// Now permite un ceas fals in teste.
	Now func() time.Time
}

// Store pastreaza soldurile si registrul de idempotenta. Fiecare operatie
// finalizata este scrisa intr-un jurnal (JSON Lines + fsync) INAINTE de a fi
// vizibila si inainte de raspuns: "efectuat" inseamna "durabil".
type Store struct {
	mu         sync.Mutex
	opts       Options
	path       string
	f          *os.File
	balances   map[string]int64
	versions   map[string]int64
	ops        map[string]*Operation // cheie de idempotenta -> operatie
	inProgress map[string]string     // cheie -> amprenta
	nextID     int64
}

// Open deschide (sau creeaza) un store. path gol = doar in memorie.
func Open(path string, opts Options) (*Store, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	s := &Store{
		opts:       opts,
		path:       path,
		balances:   map[string]int64{},
		versions:   map[string]int64{},
		ops:        map[string]*Operation{},
		inProgress: map[string]string{},
	}
	if path == "" {
		return s, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if f, err := os.Open(path); err == nil {
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var op Operation
			if json.Unmarshal(sc.Bytes(), &op) != nil {
				break // linie trunchiata la final: operatia nu a fost confirmata
			}
			s.applyLocked(&op)
		}
		f.Close()
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	s.f = f
	return s, nil
}

// Close inchide jurnalul.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

func (s *Store) applyLocked(op *Operation) {
	s.ops[op.Key] = op
	s.nextID++
	if op.Status == StatusSucceeded {
		s.balances[op.Account] = op.BalanceAfter
		s.versions[op.Account]++
	}
}

func (s *Store) persistLocked(op *Operation) error {
	if s.f == nil {
		return nil
	}
	line, err := json.Marshal(op)
	if err != nil {
		return err
	}
	if _, err := s.f.Write(append(line, '\n')); err != nil {
		return err
	}
	return s.f.Sync()
}

// Credit executa comanda o singura data per cheie de idempotenta.
//
// Intoarce (operatia, replayed, eroare). Pasii:
//  1. validare (fara efect, nememorata);
//  2. cautare in registrul de idempotenta -> raspuns memorat (replayed=true);
//  3. "lucrul" propriu-zis, care respecta ctx (deadline/cancellation);
//  4. PUNCTUL DE COMMIT: ultima verificare a ctx, apoi scriere durabila.
//
// Dupa punctul de commit operatia ramane efectuata chiar daca clientul a
// renuntat intre timp: din acest motiv un client care nu primeste raspunsul
// nu poate presupune ca operatia NU a avut loc.
func (s *Store) Credit(ctx context.Context, c CreditCmd) (Operation, bool, error) {
	if err := c.validate(); err != nil {
		return Operation{}, false, err
	}
	fp := c.Fingerprint()

	s.mu.Lock()
	if op, ok := s.ops[c.Key]; ok {
		s.mu.Unlock()
		if op.Fingerprint != fp {
			return Operation{}, false, ErrKeyReuse
		}
		return *op, true, nil
	}
	if inFp, busy := s.inProgress[c.Key]; busy {
		s.mu.Unlock()
		if inFp != fp {
			return Operation{}, false, ErrKeyReuse
		}
		return Operation{}, false, ErrInProgress
	}
	s.inProgress[c.Key] = fp
	s.mu.Unlock()

	release := func() {
		s.mu.Lock()
		delete(s.inProgress, c.Key)
		s.mu.Unlock()
	}

	// 3. lucrul: anulabil
	if d := s.opts.ProcessingTime; d > 0 {
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			release()
			return Operation{}, false, ctx.Err() // NIMIC nu a fost aplicat
		case <-t.C:
		}
	}

	// 4. punctul de commit
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inProgress, c.Key)
	if err := ctx.Err(); err != nil {
		return Operation{}, false, err
	}
	s.nextID++
	op := &Operation{
		ID:          fmt.Sprintf("op-%06d", s.nextID),
		Key:         c.Key,
		Fingerprint: fp,
		Account:     c.Account,
		Amount:      c.Amount,
		Currency:    c.Currency,
		Reference:   c.Reference,
		Note:        c.Note,
		AppliedAt:   s.opts.Now().UTC(),
		APIVersion:  c.APIVersion,
	}
	if c.Amount > MaxCredit {
		op.Status = StatusFailed
		op.Failure = fmt.Sprintf("suma %d depaseste limita de %d per operatie", c.Amount, MaxCredit)
		op.BalanceAfter = s.balances[c.Account]
	} else {
		op.Status = StatusSucceeded
		op.BalanceAfter = s.balances[c.Account] + c.Amount
	}
	if err := s.persistLocked(op); err != nil {
		s.nextID--
		return Operation{}, false, fmt.Errorf("persistare: %w", err)
	}
	s.ops[c.Key] = op
	if op.Status == StatusSucceeded {
		s.balances[c.Account] = op.BalanceAfter
		s.versions[c.Account]++
	}
	return *op, false, nil
}

// Lookup intoarce starea operatiei cu cheia data.
func (s *Store) Lookup(key string) (Status, Operation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if op, ok := s.ops[key]; ok {
		return op.Status, *op
	}
	if _, busy := s.inProgress[key]; busy {
		return StatusInProgress, Operation{Key: key}
	}
	return StatusNotFound, Operation{Key: key}
}

// Balance intoarce soldul si versiunea contului.
func (s *Store) Balance(account string) (int64, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.balances[account], s.versions[account]
}

// Snapshot intoarce soldurile si ultimele operatii (pentru administrare).
func (s *Store) Snapshot(lastN int) (map[string]int64, []Operation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	bal := map[string]int64{}
	for k, v := range s.balances {
		bal[k] = v
	}
	ops := make([]Operation, 0, len(s.ops))
	for _, op := range s.ops {
		ops = append(ops, *op)
	}
	sort.Slice(ops, func(i, j int) bool { return ops[i].ID > ops[j].ID })
	if lastN > 0 && len(ops) > lastN {
		ops = ops[:lastN]
	}
	return bal, ops
}

package broker

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_1/contract"
)

// Jurnalul (write-ahead log) este un fisier JSON Lines append-only. Fiecare
// schimbare de stare relevanta pentru recuperare este scrisa INAINTE de a fi
// aplicata in memorie si inainte de a raspunde clientului. In particular,
// brokerul confirma publicarea (ACK catre producer) doar dupa ce mesajul a
// ajuns in jurnal (si, cu fsync activ, pe disc).
//
// Lease-urile NU sunt persistate: dupa restart, mesajele care erau in zbor
// redevin disponibile si sunt relivrate (at-least-once).

type record struct {
	Op        string            `json:"op"` // publish|attempt|nack|ack|dead|replay|mark|seq
	Q         string            `json:"q"`
	ID        string            `json:"id,omitempty"`
	Msg       *contract.Message `json:"msg,omitempty"`
	Attempt   int               `json:"attempt,omitempty"`
	Reason    string            `json:"reason,omitempty"`
	NotBefore *time.Time        `json:"not_before,omitempty"`
	SeqKey    string            `json:"seq_key,omitempty"`
	Seq       uint64            `json:"seq,omitempty"`
	At        time.Time         `json:"at"`
}

type journal struct {
	f    *os.File
	sync bool
}

func (j *journal) append(r record) error {
	line, err := json.Marshal(r)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := j.f.Write(line); err != nil {
		return err
	}
	if j.sync {
		return j.f.Sync()
	}
	return nil
}

// logLocked scrie o inregistrare in jurnal (no-op pentru brokerul in memorie).
func (b *Broker) logLocked(r record) error {
	if b.j == nil {
		return nil
	}
	if r.At.IsZero() {
		r.At = b.cfg.Now()
	}
	return b.j.append(r)
}

// Open creeaza un broker persistent: reconstruieste starea din jurnal,
// il compacteaza si continua sa scrie in el. fsync=true garanteaza ca un
// mesaj confirmat producer-ului supravietuieste si unei caderi a sistemului
// de operare, nu doar a procesului.
func Open(path string, cfg Config, fsync bool) (*Broker, error) {
	b := New(cfg)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	n, err := b.load(path)
	if err != nil {
		return nil, err
	}
	if err := b.compact(path); err != nil {
		return nil, fmt.Errorf("compactare jurnal: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	b.j = &journal{f: f, sync: fsync}
	b.events.add(b.cfg.Now(), "*", "recovered", "", "", fmt.Sprintf("%d inregistrari citite din %s", n, filepath.Base(path)))
	return b, nil
}

// Close inchide jurnalul.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.j == nil {
		return nil
	}
	err := b.j.f.Close()
	b.j = nil
	return err
}

func (b *Broker) load(path string) (int, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r := bufio.NewReader(bytes.NewReader(data))
	n := 0
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			var rec record
			if jerr := json.Unmarshal(line, &rec); jerr != nil {
				// O linie trunchiata la final = cadere in timpul scrierii;
				// acea operatie nu a fost confirmata nimanui, deci o ignoram.
				if err == io.EOF {
					break
				}
				return n, fmt.Errorf("jurnal corupt la inregistrarea %d: %w", n+1, jerr)
			}
			b.applyLocked(rec)
			n++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// applyLocked reda o inregistrare peste starea din memorie (fara a o rescrie).
func (b *Broker) applyLocked(r record) {
	q := b.queueLocked(r.Q)
	switch r.Op {
	case "publish":
		if r.Msg == nil {
			return
		}
		if _, dup := q.seen[r.Msg.ID]; dup {
			return
		}
		b.acceptLocked(q, *r.Msg, r.At)
	case "mark":
		q.seen[r.ID] = r.At
	case "seq":
		if r.Seq > q.lastSeq[r.SeqKey] {
			q.lastSeq[r.SeqKey] = r.Seq
		}
	}
	e, ok := q.byID[r.ID]
	if !ok {
		return
	}
	switch r.Op {
	case "attempt":
		e.attempts = r.Attempt
	case "nack":
		e.lastError = r.Reason
		if r.NotBefore != nil {
			e.notBefore = *r.NotBefore
		}
	case "ack":
		q.removePending(e)
		delete(q.byID, e.msg.ID)
	case "dead":
		q.removePending(e)
		e.state = StateDead
		e.deadAt = r.At
		e.lastError = r.Reason
	case "replay":
		q.nextOffset++
		e.offset = q.nextOffset
		e.state = StateReady
		e.attempts = 0
		e.notBefore = r.At
		e.lastError = ""
		q.pending = append(q.pending, e)
	}
}

// compact rescrie jurnalul pastrand doar starea vie (scriere atomica prin rename).
func (b *Broker) compact(path string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	j := &journal{f: f}
	write := func(r record) {
		if err == nil {
			err = j.append(r)
		}
	}
	for _, q := range b.queues {
		for id, at := range q.seen {
			if _, live := q.byID[id]; !live {
				write(record{Op: "mark", Q: q.name, ID: id, At: at})
			}
		}
		for k, s := range q.lastSeq {
			write(record{Op: "seq", Q: q.name, SeqKey: k, Seq: s, At: b.cfg.Now()})
		}
		emit := func(e *entry) {
			m := e.msg
			write(record{Op: "publish", Q: q.name, ID: m.ID, Msg: &m, At: e.enqueuedAt})
			if e.attempts > 0 {
				write(record{Op: "attempt", Q: q.name, ID: m.ID, Attempt: e.attempts, At: e.enqueuedAt})
			}
			if !e.notBefore.IsZero() || e.lastError != "" {
				nb := e.notBefore
				write(record{Op: "nack", Q: q.name, ID: m.ID, Reason: e.lastError, NotBefore: &nb, At: e.enqueuedAt})
			}
		}
		for _, e := range q.byID {
			if e.state == StateDead {
				emit(e)
				write(record{Op: "dead", Q: q.name, ID: e.msg.ID, Reason: e.lastError, At: e.deadAt})
			}
		}
		for _, e := range q.pending {
			emit(e)
		}
	}
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

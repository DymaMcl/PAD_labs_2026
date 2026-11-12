// Package contract defineste contractul de mesaj partajat de producer,
// broker si consumer. Orice mesaj care nu respecta contractul este respins
// la intrarea in broker (nu ajunge niciodata intr-o coada).
package contract

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// SchemaVersion este singura versiune a plicului acceptata de broker.
const SchemaVersion = 1

// MaxPayloadBytes limiteaza dimensiunea payload-ului (protectie la mesaje gigant).
const MaxPayloadBytes = 64 * 1024

// Message este plicul (envelope) unui mesaj.
//
//   - ID        identificator unic, generat de producer; este si cheia de
//     idempotenta: retrimiterea aceluiasi ID nu creeaza un mesaj nou.
//   - Type      tipul evenimentului, ex. "payment.credit".
//   - Version   versiunea schemei plicului (SchemaVersion).
//   - Source    identitatea producer-ului.
//   - Key       cheia de ordonare: mesajele cu aceeasi cheie sunt livrate
//     strict in ordinea in care au fost acceptate.
//   - Seq       numar de secventa monoton crescator pe perechea (Source, Key).
//   - CreatedAt momentul crearii la producer.
//   - Payload   obiect JSON specific tipului.
type Message struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Version   int             `json:"version"`
	Source    string          `json:"source"`
	Key       string          `json:"key"`
	Seq       uint64          `json:"seq"`
	CreatedAt time.Time       `json:"created_at"`
	Payload   json.RawMessage `json:"payload"`
}

// ValidationError descrie toate incalcarile de contract gasite intr-un mesaj.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return "mesaj invalid: " + strings.Join(e.Problems, "; ")
}

// IsValidationError raporteaza daca err provine din Validate.
func IsValidationError(err error) bool {
	var ve *ValidationError
	return errors.As(err, &ve)
}

// Validate verifica plicul conform contractului.
func (m *Message) Validate() error {
	var p []string
	if strings.TrimSpace(m.ID) == "" {
		p = append(p, "id lipsa")
	} else if len(m.ID) > 128 {
		p = append(p, "id mai lung de 128 caractere")
	}
	if strings.TrimSpace(m.Type) == "" {
		p = append(p, "type lipsa")
	}
	if m.Version != SchemaVersion {
		p = append(p, fmt.Sprintf("version=%d nesuportata (se accepta %d)", m.Version, SchemaVersion))
	}
	if strings.TrimSpace(m.Source) == "" {
		p = append(p, "source lipsa")
	}
	if strings.TrimSpace(m.Key) == "" {
		p = append(p, "key (cheia de ordonare) lipsa")
	}
	if m.Seq == 0 {
		p = append(p, "seq trebuie sa fie >= 1")
	}
	if m.CreatedAt.IsZero() {
		p = append(p, "created_at lipsa")
	}
	switch {
	case len(m.Payload) == 0:
		p = append(p, "payload lipsa")
	case len(m.Payload) > MaxPayloadBytes:
		p = append(p, fmt.Sprintf("payload depaseste %d octeti", MaxPayloadBytes))
	default:
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(m.Payload, &obj); err != nil {
			p = append(p, "payload trebuie sa fie un obiect JSON")
		}
	}
	if len(p) > 0 {
		return &ValidationError{Problems: p}
	}
	return nil
}

// NewID genereaza un UUID v4 (fara dependinte externe).
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}

// New construieste un mesaj valid cu ID si timestamp generate.
func New(msgType, source, key string, seq uint64, payload any) (Message, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Message{}, err
	}
	m := Message{
		ID:        NewID(),
		Type:      msgType,
		Version:   SchemaVersion,
		Source:    source,
		Key:       key,
		Seq:       seq,
		CreatedAt: time.Now().UTC(),
		Payload:   raw,
	}
	return m, m.Validate()
}

// Delivery este o livrare concreta a unui mesaj catre un consumer.
// DeliveryID (receipt) identifica tentativa curenta: ACK/NACK se fac pe
// DeliveryID, nu pe ID-ul mesajului, astfel incat un ACK intarziat al unei
// tentative expirate sa nu confirme o redare ulterioara.
type Delivery struct {
	Message    Message   `json:"message"`
	Queue      string    `json:"queue"`
	DeliveryID string    `json:"delivery_id"`
	Attempt    int       `json:"attempt"`
	LeaseUntil time.Time `json:"lease_until"`
}

// CreditPayload este payload-ul tipului "payment.credit" folosit in laborator:
// alimentarea unui cont cu o suma in bani (cents). Efectul NU este idempotent
// in mod natural: aplicat de doua ori, dubleaza suma.
type CreditPayload struct {
	Account string `json:"account"`
	Amount  int64  `json:"amount"`
	Note    string `json:"note,omitempty"`
}

// TypeCredit este tipul de mesaj procesat de consumer.
const TypeCredit = "payment.credit"

package contract

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validMsg() Message {
	return Message{
		ID:        NewID(),
		Type:      TypeCredit,
		Version:   SchemaVersion,
		Source:    "test",
		Key:       "ACC-1",
		Seq:       1,
		CreatedAt: time.Now(),
		Payload:   json.RawMessage(`{"account":"ACC-1","amount":100}`),
	}
}

func TestValidMessagePasses(t *testing.T) {
	m := validMsg()
	if err := m.Validate(); err != nil {
		t.Fatalf("mesaj valid respins: %v", err)
	}
}

func TestValidationCollectsAllProblems(t *testing.T) {
	m := Message{Version: 7, Payload: json.RawMessage(`[1,2]`)}
	err := m.Validate()
	if err == nil || !IsValidationError(err) {
		t.Fatalf("astepta ValidationError, primit %v", err)
	}
	ve := err.(*ValidationError)
	for _, want := range []string{"id", "type", "version", "source", "key", "seq", "created_at", "obiect JSON"} {
		if !strings.Contains(ve.Error(), want) {
			t.Errorf("lipseste problema %q in %q", want, ve.Error())
		}
	}
}

func TestPayloadTooLarge(t *testing.T) {
	m := validMsg()
	m.Payload = json.RawMessage(`{"x":"` + strings.Repeat("a", MaxPayloadBytes) + `"}`)
	if err := m.Validate(); err == nil {
		t.Fatal("payload prea mare acceptat")
	}
}

func TestNewIDUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewID()
		if len(id) != 36 || seen[id] {
			t.Fatalf("id invalid sau duplicat: %s", id)
		}
		seen[id] = true
	}
}

func TestNewBuildsValidMessage(t *testing.T) {
	m, err := New(TypeCredit, "p1", "ACC-9", 3, CreditPayload{Account: "ACC-9", Amount: 50})
	if err != nil {
		t.Fatal(err)
	}
	var p CreditPayload
	if err := json.Unmarshal(m.Payload, &p); err != nil || p.Amount != 50 {
		t.Fatalf("payload gresit: %s", m.Payload)
	}
}

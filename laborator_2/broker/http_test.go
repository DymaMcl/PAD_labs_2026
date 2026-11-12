package broker

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func do(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, &buf))
	out := map[string]any{}
	json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestHTTPFlow(t *testing.T) {
	s := NewServer(New(DefaultConfig()))
	m := msg("A", 1)
	if code, _ := do(t, s, "POST", "/v1/queues/plati/messages", m); code != http.StatusCreated {
		t.Fatalf("publish: %d", code)
	}
	if code, body := do(t, s, "POST", "/v1/queues/plati/messages", m); code != http.StatusOK || body["status"] != "duplicate" {
		t.Fatalf("duplicate: %d %v", code, body)
	}
	bad := m
	bad.ID = ""
	if code, body := do(t, s, "POST", "/v1/queues/plati/messages", bad); code != http.StatusBadRequest || body["error"] != "invalid_message" {
		t.Fatalf("invalid: %d %v", code, body)
	}
	code, body := do(t, s, "POST", "/v1/queues/plati/receive", ReceiveRequest{Consumer: "c1", Max: 5, LeaseMs: 1000})
	ds := body["deliveries"].([]any)
	if code != 200 || len(ds) != 1 {
		t.Fatalf("receive: %d %v", code, body)
	}
	did := ds[0].(map[string]any)["delivery_id"].(string)
	if code, _ := do(t, s, "POST", "/v1/queues/plati/ack", AckRequest{DeliveryID: did}); code != 200 {
		t.Fatalf("ack: %d", code)
	}
	if code, body := do(t, s, "POST", "/v1/queues/plati/ack", AckRequest{DeliveryID: did}); code != http.StatusConflict {
		t.Fatalf("ack repetat: %d %v", code, body)
	}
}

func TestHTTPNackToDLQAndReplay(t *testing.T) {
	s := NewServer(New(DefaultConfig()))
	m := msg("A", 1)
	do(t, s, "POST", "/v1/queues/q/messages", m)
	_, body := do(t, s, "POST", "/v1/queues/q/receive", ReceiveRequest{Consumer: "c1"})
	did := body["deliveries"].([]any)[0].(map[string]any)["delivery_id"].(string)
	_, body = do(t, s, "POST", "/v1/queues/q/nack", AckRequest{DeliveryID: did, Reason: "otravit"})
	if body["outcome"] != "dead_lettered" {
		t.Fatalf("nack: %v", body)
	}
	_, body = do(t, s, "GET", "/v1/queues/q/dlq", nil)
	if n := len(body["dead_letters"].([]any)); n != 1 {
		t.Fatalf("dlq: %v", body)
	}
	if code, _ := do(t, s, "POST", "/v1/queues/q/dlq/"+m.ID+"/replay", nil); code != 200 {
		t.Fatalf("replay: %d", code)
	}
}

func TestHTTPDroppedAckStillPersists(t *testing.T) {
	s := NewServer(New(DefaultConfig()))
	s.SetFaults(Faults{DropPublishAck: 1})
	m := msg("A", 1)
	if code, _ := do(t, s, "POST", "/v1/queues/q/messages", m); code != http.StatusServiceUnavailable {
		t.Fatalf("astepta 503, primit %d", code)
	}
	s.SetFaults(Faults{})
	if _, body := do(t, s, "POST", "/v1/queues/q/messages", m); body["status"] != "duplicate" {
		t.Fatalf("retrimiterea dupa ACK pierdut trebuie sa fie duplicate: %v", body)
	}
}

package broker

import (
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
)

// Faults permite injectarea de defecte pentru demonstratii si teste.
type Faults struct {
	// DropPublishAck: probabilitatea ca brokerul sa accepte mesajul dar sa
	// "piarda" raspunsul (intoarce 503). Simuleaza un ACK pierdut pe retea:
	// producer-ul nu stie daca mesajul a ajuns si trebuie sa retrimita.
	DropPublishAck float64 `json:"drop_publish_ack"`
}

// Server expune brokerul prin HTTP/JSON.
//
//	POST /v1/queues/{q}/messages          publicare (ACK catre producer)
//	POST /v1/queues/{q}/receive           preluare cu lease (long-poll)
//	POST /v1/queues/{q}/ack               confirmare efect {delivery_id}
//	POST /v1/queues/{q}/nack              esec {delivery_id, reason, retry}
//	GET  /v1/queues                       statistici
//	GET  /v1/queues/{q}/dlq               continut dead-letter queue
//	POST /v1/queues/{q}/dlq/{id}/replay   mutare din DLQ inapoi in coada
//	GET  /v1/events?since=N               jurnal de evenimente (observabilitate)
//	GET|PUT /v1/admin/faults              injectare de defecte
type Server struct {
	B      *Broker
	mu     sync.Mutex
	faults Faults
	rnd    *rand.Rand
	mux    *http.ServeMux
}

// NewServer construieste handler-ul HTTP.
func NewServer(b *Broker) *Server {
	s := &Server{B: b, rnd: rand.New(rand.NewSource(time.Now().UnixNano())), mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /v1/queues/{q}/messages", s.publish)
	s.mux.HandleFunc("POST /v1/queues/{q}/receive", s.receive)
	s.mux.HandleFunc("POST /v1/queues/{q}/ack", s.ack)
	s.mux.HandleFunc("POST /v1/queues/{q}/nack", s.nack)
	s.mux.HandleFunc("GET /v1/queues", s.stats)
	s.mux.HandleFunc("GET /v1/queues/{q}/dlq", s.dlq)
	s.mux.HandleFunc("POST /v1/queues/{q}/dlq/{id}/replay", s.replay)
	s.mux.HandleFunc("GET /v1/events", s.eventsH)
	s.mux.HandleFunc("GET /v1/admin/faults", s.getFaults)
	s.mux.HandleFunc("PUT /v1/admin/faults", s.putFaults)
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return s
}

// Handle permite inregistrarea de rute suplimentare (ex. dashboard).
func (s *Server) Handle(pattern string, h http.Handler) { s.mux.Handle(pattern, h) }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// SetFaults modifica defectele injectate.
func (s *Server) SetFaults(f Faults) {
	s.mu.Lock()
	s.faults = f
	s.mu.Unlock()
}

type apiError struct {
	Error  string `json:"error"`
	Detail string `json:"detail,omitempty"`
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, contract.MaxPayloadBytes*2))
	if err == nil {
		err = json.Unmarshal(body, v)
	}
	if err != nil {
		writeJSON(w, http.StatusBadRequest, apiError{Error: "bad_json", Detail: err.Error()})
		return false
	}
	return true
}

func (s *Server) publish(w http.ResponseWriter, r *http.Request) {
	var m contract.Message
	if !readJSON(w, r, &m) {
		return
	}
	res, err := s.B.Publish(r.PathValue("q"), m)
	switch {
	case contract.IsValidationError(err):
		writeJSON(w, http.StatusBadRequest, apiError{Error: "invalid_message", Detail: err.Error()})
		return
	case errors.Is(err, ErrStaleSequence):
		writeJSON(w, http.StatusConflict, apiError{Error: "stale_sequence", Detail: err.Error()})
		return
	case err != nil:
		writeJSON(w, http.StatusServiceUnavailable, apiError{Error: "unavailable", Detail: err.Error()})
		return
	}
	s.mu.Lock()
	drop := s.rnd.Float64() < s.faults.DropPublishAck
	s.mu.Unlock()
	if drop {
		// mesajul A FOST persistat, dar producer-ul nu afla
		writeJSON(w, http.StatusServiceUnavailable, apiError{Error: "ack_lost", Detail: "defect injectat: raspuns pierdut"})
		return
	}
	code := http.StatusCreated
	if res.Status == "duplicate" {
		code = http.StatusOK
	}
	writeJSON(w, code, res)
}

// ReceiveRequest este corpul cererii de preluare.
type ReceiveRequest struct {
	Consumer string `json:"consumer"`
	Max      int    `json:"max"`
	LeaseMs  int    `json:"lease_ms"`
	WaitMs   int    `json:"wait_ms"`
}

func (s *Server) receive(w http.ResponseWriter, r *http.Request) {
	var req ReceiveRequest
	if !readJSON(w, r, &req) {
		return
	}
	if req.Consumer == "" {
		writeJSON(w, http.StatusBadRequest, apiError{Error: "consumer_required"})
		return
	}
	if req.WaitMs > 30000 {
		req.WaitMs = 30000
	}
	ds := s.B.Receive(r.Context(), r.PathValue("q"), req.Consumer, req.Max,
		time.Duration(req.LeaseMs)*time.Millisecond, time.Duration(req.WaitMs)*time.Millisecond)
	if ds == nil {
		ds = []contract.Delivery{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": ds})
}

// AckRequest este corpul pentru ACK/NACK.
type AckRequest struct {
	DeliveryID string `json:"delivery_id"`
	Reason     string `json:"reason,omitempty"`
	Retry      bool   `json:"retry,omitempty"`
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request) {
	var req AckRequest
	if !readJSON(w, r, &req) {
		return
	}
	err := s.B.Ack(r.PathValue("q"), req.DeliveryID)
	switch {
	case errors.Is(err, ErrStaleDelivery):
		writeJSON(w, http.StatusConflict, apiError{Error: "stale_delivery", Detail: err.Error()})
	case err != nil:
		writeJSON(w, http.StatusServiceUnavailable, apiError{Error: "unavailable", Detail: err.Error()})
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
	}
}

func (s *Server) nack(w http.ResponseWriter, r *http.Request) {
	var req AckRequest
	if !readJSON(w, r, &req) {
		return
	}
	res, err := s.B.Nack(r.PathValue("q"), req.DeliveryID, req.Reason, req.Retry)
	switch {
	case errors.Is(err, ErrStaleDelivery):
		writeJSON(w, http.StatusConflict, apiError{Error: "stale_delivery", Detail: err.Error()})
	case err != nil:
		writeJSON(w, http.StatusServiceUnavailable, apiError{Error: "unavailable", Detail: err.Error()})
	default:
		writeJSON(w, http.StatusOK, res)
	}
}

func (s *Server) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"queues": s.B.Stats(), "config": s.B.PublicConfig()})
}

func (s *Server) dlq(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"dead_letters": s.B.DeadLetters(r.PathValue("q"))})
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	if err := s.B.Replay(r.PathValue("q"), r.PathValue("id")); err != nil {
		writeJSON(w, http.StatusNotFound, apiError{Error: "not_in_dlq"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "replayed"})
}

func (s *Server) eventsH(w http.ResponseWriter, r *http.Request) {
	since, _ := strconv.ParseUint(r.URL.Query().Get("since"), 10, 64)
	writeJSON(w, http.StatusOK, map[string]any{"events": s.B.Events(since)})
}

func (s *Server) getFaults(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	f := s.faults
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, f)
}

func (s *Server) putFaults(w http.ResponseWriter, r *http.Request) {
	var f Faults
	if !readJSON(w, r, &f) {
		return
	}
	if f.DropPublishAck < 0 || f.DropPublishAck > 1 {
		writeJSON(w, http.StatusBadRequest, apiError{Error: "probabilitate in afara intervalului [0,1]"})
		return
	}
	s.SetFaults(f)
	writeJSON(w, http.StatusOK, f)
}

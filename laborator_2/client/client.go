// Package client este clientul HTTP folosit de producer si consumer.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
)

// ErrStaleDelivery: brokerul a respins ACK/NACK pentru ca livrarea nu mai e activa.
var ErrStaleDelivery = errors.New("livrare expirata sau deja confirmata")

// HTTPError este un raspuns non-2xx de la broker.
type HTTPError struct {
	Code   int
	Kind   string
	Detail string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("broker %d %s: %s", e.Code, e.Kind, e.Detail)
}

// Retryable spune daca o eroare merita reincercata: erorile de retea si 5xx
// sunt tranzitorii (nu stim daca operatia a avut loc), 4xx sunt definitive.
func Retryable(err error) bool {
	if err == nil {
		return false
	}
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Code >= 500 || he.Code == http.StatusTooManyRequests
	}
	return !errors.Is(err, context.Canceled)
}

// Client vorbeste cu un broker.
type Client struct {
	Base string
	HTTP *http.Client
}

// New creeaza un client pentru adresa data (ex. http://localhost:7070).
func New(base string) *Client {
	return &Client{Base: base, HTTP: &http.Client{Timeout: 40 * time.Second}}
}

func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		var e struct{ Error, Detail string }
		_ = json.Unmarshal(raw, &e)
		he := &HTTPError{Code: resp.StatusCode, Kind: e.Error, Detail: e.Detail}
		if resp.StatusCode == http.StatusConflict && e.Error == "stale_delivery" {
			return fmt.Errorf("%w: %v", ErrStaleDelivery, he)
		}
		return he
	}
	if out != nil {
		return json.Unmarshal(raw, out)
	}
	return nil
}

func qpath(q, suffix string) string { return "/v1/queues/" + url.PathEscape(q) + suffix }

// PublishResult este ACK-ul brokerului pentru publicare.
type PublishResult struct {
	Status string `json:"status"` // accepted | duplicate
	Offset uint64 `json:"offset"`
}

// Publish trimite un mesaj o singura data.
func (c *Client) Publish(ctx context.Context, q string, m contract.Message) (PublishResult, error) {
	var res PublishResult
	err := c.call(ctx, http.MethodPost, qpath(q, "/messages"), m, &res)
	return res, err
}

// RetryPolicy descrie reincercarile producer-ului.
type RetryPolicy struct {
	MaxAttempts int
	Base        time.Duration
	Max         time.Duration
}

// DefaultRetry: 6 incercari, 100ms, 200ms, 400ms ... plafonat la 3s, cu jitter.
var DefaultRetry = RetryPolicy{MaxAttempts: 6, Base: 100 * time.Millisecond, Max: 3 * time.Second}

// Delay intoarce asteptarea dinaintea incercarii n+1 ("full jitter").
func (p RetryPolicy) Delay(n int) time.Duration {
	d := p.Base << (n - 1)
	if d > p.Max || d <= 0 {
		d = p.Max
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

// PublishWithRetry retrimite ACELASI mesaj (acelasi ID) pana primeste ACK.
// Brokerul deduplica dupa ID, deci retrimiterea este sigura chiar daca
// incercarea anterioara a ajuns de fapt la broker si doar raspunsul s-a pierdut.
func (c *Client) PublishWithRetry(ctx context.Context, q string, m contract.Message, p RetryPolicy,
	onRetry func(attempt int, err error, wait time.Duration)) (PublishResult, int, error) {
	var lastErr error
	for attempt := 1; attempt <= p.MaxAttempts; attempt++ {
		res, err := c.Publish(ctx, q, m)
		if err == nil {
			return res, attempt, nil
		}
		lastErr = err
		if !Retryable(err) || attempt == p.MaxAttempts {
			return res, attempt, err
		}
		wait := p.Delay(attempt)
		if onRetry != nil {
			onRetry(attempt, err, wait)
		}
		select {
		case <-ctx.Done():
			return res, attempt, ctx.Err()
		case <-time.After(wait):
		}
	}
	return PublishResult{}, p.MaxAttempts, lastErr
}

// ReceiveOptions parametrizeaza preluarea.
type ReceiveOptions struct {
	Consumer string        `json:"consumer"`
	Max      int           `json:"max"`
	Lease    time.Duration `json:"-"`
	Wait     time.Duration `json:"-"`
	LeaseMs  int64         `json:"lease_ms"`
	WaitMs   int64         `json:"wait_ms"`
}

// Receive preia livrari (long-poll pana la Wait).
func (c *Client) Receive(ctx context.Context, q string, o ReceiveOptions) ([]contract.Delivery, error) {
	o.LeaseMs = o.Lease.Milliseconds()
	o.WaitMs = o.Wait.Milliseconds()
	var out struct {
		Deliveries []contract.Delivery `json:"deliveries"`
	}
	err := c.call(ctx, http.MethodPost, qpath(q, "/receive"), o, &out)
	return out.Deliveries, err
}

// Ack confirma efectul unei livrari.
func (c *Client) Ack(ctx context.Context, q, deliveryID string) error {
	return c.call(ctx, http.MethodPost, qpath(q, "/ack"), map[string]any{"delivery_id": deliveryID}, nil)
}

// NackResult spune ce a decis brokerul.
type NackResult struct {
	Outcome string    `json:"outcome"`
	RetryAt time.Time `json:"retry_at"`
	Attempt int       `json:"attempt"`
}

// Nack raporteaza esecul procesarii.
func (c *Client) Nack(ctx context.Context, q, deliveryID, reason string, retry bool) (NackResult, error) {
	var res NackResult
	err := c.call(ctx, http.MethodPost, qpath(q, "/nack"),
		map[string]any{"delivery_id": deliveryID, "reason": reason, "retry": retry}, &res)
	return res, err
}

// SetDropPublishAck configureaza defectul injectat in broker.
func (c *Client) SetDropPublishAck(ctx context.Context, p float64) error {
	return c.call(ctx, http.MethodPut, "/v1/admin/faults", map[string]any{"drop_publish_ack": p}, nil)
}

package client

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_1/broker"
	"github.com/DymaMcl/PAD_labs_2026/laborator_1/contract"
)

func setup(t *testing.T) (*Client, *broker.Server) {
	srv := broker.NewServer(broker.New(broker.DefaultConfig()))
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return New(ts.URL), srv
}

// ACK-ul brokerului se pierde de doua ori: producer-ul retrimite acelasi ID,
// iar in coada ajunge un singur mesaj.
func TestPublishRetryAfterLostAck(t *testing.T) {
	c, srv := setup(t)
	srv.SetFaults(broker.Faults{DropPublishAck: 1})
	m, _ := contract.New(contract.TypeCredit, "p", "ACC-1", 1, contract.CreditPayload{Account: "ACC-1", Amount: 5})
	pol := RetryPolicy{MaxAttempts: 5, Base: time.Millisecond, Max: 5 * time.Millisecond}
	res, attempts, err := c.PublishWithRetry(context.Background(), "q", m, pol, func(a int, _ error, _ time.Duration) {
		if a == 2 {
			srv.SetFaults(broker.Faults{})
		}
	})
	if err != nil || attempts != 3 || res.Status != "duplicate" {
		t.Fatalf("res=%+v attempts=%d err=%v", res, attempts, err)
	}
	ds, _ := c.Receive(context.Background(), "q", ReceiveOptions{Consumer: "c", Max: 10})
	if len(ds) != 1 {
		t.Fatalf("astepta exact 1 mesaj, sunt %d", len(ds))
	}
}

func TestInvalidMessageNotRetried(t *testing.T) {
	c, _ := setup(t)
	bad := contract.Message{ID: "x"}
	_, attempts, err := c.PublishWithRetry(context.Background(), "q", bad, DefaultRetry, nil)
	var he *HTTPError
	if !errors.As(err, &he) || he.Code != 400 || attempts != 1 {
		t.Fatalf("eroarea 4xx nu trebuie reincercata: attempts=%d err=%v", attempts, err)
	}
}

func TestStaleAckMapped(t *testing.T) {
	c, _ := setup(t)
	if err := c.Ack(context.Background(), "q", "inexistent"); !errors.Is(err, ErrStaleDelivery) {
		t.Fatalf("astepta ErrStaleDelivery, primit %v", err)
	}
}

func TestBrokerDownIsRetryable(t *testing.T) {
	c := New("http://127.0.0.1:1")
	_, err := c.Publish(context.Background(), "q", contract.Message{})
	if !Retryable(err) {
		t.Fatalf("eroarea de retea trebuie sa fie reincercabila: %v", err)
	}
}

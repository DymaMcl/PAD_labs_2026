// Package integration este clientul serviciului de conturi. Rolul lui este
// sa transforme rezultatul incert al unui apel la distanta intr-un rezultat
// EXPLICIT: aplicat, respins, sigur neaplicat sau necunoscut.
package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	accountsv1 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v1"
	accountsv2 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v2"
)

// Outcome este rezultatul explicit al unei operatii la distanta.
type Outcome string

const (
	// Applied: efectul exista in serviciu (executat acum sau la o incercare
	// anterioara cu aceeasi cheie).
	Applied Outcome = "APPLIED"
	// Rejected: serviciul a refuzat definitiv cererea; efectul NU exista.
	// Reincercarea nu schimba rezultatul.
	Rejected Outcome = "REJECTED"
	// NotApplied: stim SIGUR ca efectul nu exista (cererea nu a fost trimisa
	// sau serviciul confirma ca nu are operatia). Se poate reincerca oricand.
	NotApplied Outcome = "NOT_APPLIED"
	// Unknown: nu se poate stabili. Apelantul NU are voie sa presupuna nici
	// succes, nici esec; trebuie sa reincerce mai tarziu cu ACEEASI cheie.
	Unknown Outcome = "UNKNOWN"
)

var (
	// ErrAmbiguous: rezultatul nu a putut fi stabilit.
	ErrAmbiguous = errors.New("rezultat ambiguu: operatia poate sau nu sa fi avut loc")
	// ErrCircuitOpen: cererea nu a fost trimisa, circuitul este deschis.
	ErrCircuitOpen = errors.New("circuit deschis: serviciul este considerat indisponibil")
)

// Result descrie complet ce s-a intamplat.
type Result struct {
	Outcome      Outcome
	OperationID  string
	BalanceAfter int64
	Replayed     bool
	Attempts     int
	APIVersion   string
	Resolution   string // cum a fost stabilit rezultatul
	Reason       string // motivul respingerii
	Err          error
}

// Config parametrizeaza clientul.
type Config struct {
	PerAttemptTimeout time.Duration // deadline-ul fiecarei incercari
	MaxAttempts       int
	BackoffBase       time.Duration
	BackoffMax        time.Duration
	// ReconcileTimeout: timpul alocat interogarii GetOperation la final.
	ReconcileTimeout time.Duration
	// ReconcileGrace: pauza inainte de interogare, ca o cerere aflata inca pe
	// fir sa apuce sa se termine (sau sa fie abandonata) pe server.
	ReconcileGrace   time.Duration
	BreakerThreshold int
	BreakerCooldown  time.Duration
	// ForceV1 dezactiveaza negocierea si foloseste direct contractul v1.
	ForceV1 bool
	Logf    func(string, ...any)
}

func (c *Config) defaults() {
	if c.PerAttemptTimeout <= 0 {
		c.PerAttemptTimeout = time.Second
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 4
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = 50 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = time.Second
	}
	if c.ReconcileTimeout <= 0 {
		c.ReconcileTimeout = 500 * time.Millisecond
	}
	if c.ReconcileGrace <= 0 {
		c.ReconcileGrace = 50 * time.Millisecond
	}
	if c.Logf == nil {
		c.Logf = func(string, ...any) {}
	}
}

// Client este clientul de integrare.
type Client struct {
	conn    *grpc.ClientConn
	v1      accountsv1.AccountsClient
	v2      accountsv2.AccountsClient
	cfg     Config
	useV1   atomic.Bool
	Breaker *Breaker

	mu    sync.Mutex
	cache map[string]BalanceView
}

// Dial conecteaza clientul la adresa serviciului (ex. localhost:7080).
func Dial(addr string, cfg Config) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return New(conn, cfg), nil
}

// New construieste clientul peste o conexiune existenta.
func New(conn *grpc.ClientConn, cfg Config) *Client {
	cfg.defaults()
	c := &Client{
		conn: conn, v1: accountsv1.NewAccountsClient(conn), v2: accountsv2.NewAccountsClient(conn),
		cfg: cfg, Breaker: NewBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown), cache: map[string]BalanceView{},
	}
	c.useV1.Store(cfg.ForceV1)
	return c
}

// Close inchide conexiunea.
func (c *Client) Close() error { return c.conn.Close() }

// APIVersion intoarce versiunea de contract folosita in prezent.
func (c *Client) APIVersion() string {
	if c.useV1.Load() {
		return "v1"
	}
	return "v2"
}

// CreditInput este cererea logica.
type CreditInput struct {
	Key       string // cheia de idempotenta: aceeasi la TOATE incercarile
	Account   string
	Amount    int64
	Reference string
}

type attemptResult struct {
	opID     string
	balance  int64
	replayed bool
	rejected string
}

// kind clasifica raspunsul unei incercari.
type kind int

const (
	kOK kind = iota
	kRejected
	kAmbiguous
)

// classify: ce stim despre efect dupa un anumit cod gRPC (vezi CONTRACT.md).
func classify(err error) kind {
	switch status.Code(err) {
	case codes.OK:
		return kOK
	case codes.InvalidArgument, codes.FailedPrecondition, codes.AlreadyExists,
		codes.PermissionDenied, codes.Unauthenticated, codes.NotFound, codes.OutOfRange:
		return kRejected
	default:
		// UNAVAILABLE, DEADLINE_EXCEEDED, CANCELED, ABORTED, INTERNAL, UNKNOWN,
		// RESOURCE_EXHAUSTED: cererea poate sau nu sa fi fost executata.
		return kAmbiguous
	}
}

func (c *Client) attempt(ctx context.Context, in CreditInput) (attemptResult, error) {
	if !c.useV1.Load() {
		r, err := c.v2.Credit(ctx, &accountsv2.CreditRequest{
			IdempotencyKey: in.Key, Account: in.Account, Reference: in.Reference,
			Amount: &accountsv2.Money{AmountMinor: in.Amount, Currency: "MDL"},
		})
		if status.Code(err) == codes.Unimplemented {
			// FALLBACK DE CONTRACT: serverul nu cunoaste v2 -> trecem la v1.
			c.useV1.Store(true)
			c.cfg.Logf("serverul nu implementeaza accounts.v2 -> fallback la accounts.v1")
			return c.attempt(ctx, in)
		}
		if err != nil {
			return attemptResult{}, err
		}
		op := r.GetOperation()
		out := attemptResult{opID: op.GetId(), balance: op.GetBalanceAfter().GetAmountMinor(), replayed: op.GetReplayed()}
		if op.GetStatus() == accountsv2.OperationStatus_OPERATION_STATUS_FAILED {
			out.rejected = op.GetFailureReason()
		}
		return out, nil
	}
	r, err := c.v1.Credit(ctx, &accountsv1.CreditRequest{
		IdempotencyKey: in.Key, Account: in.Account, Amount: in.Amount, Note: in.Reference,
	})
	if err != nil {
		return attemptResult{}, err
	}
	return attemptResult{opID: r.GetOperationId(), balance: r.GetBalanceAfter(), replayed: r.GetReplayed()}, nil
}

func (c *Client) backoff(n int) time.Duration {
	d := c.cfg.BackoffBase << (n - 1)
	if d > c.cfg.BackoffMax || d <= 0 {
		d = c.cfg.BackoffMax
	}
	return d/2 + time.Duration(rand.Int63n(int64(d/2)+1))
}

// Credit executa operatia cu tratarea explicita a ambiguitatii:
//
//  1. fiecare incercare are propriul deadline, limitat de deadline-ul
//     apelantului (ctx) - deadline-ul se propaga automat la server;
//  2. TOATE incercarile folosesc aceeasi cheie de idempotenta, deci
//     reincercarea dupa un raspuns pierdut nu dubleaza efectul;
//  3. erorile definitive opresc imediat reincercarile;
//  4. daca incercarile se epuizeaza intr-o stare ambigua, clientul intreaba
//     serviciul (GetOperation) ce s-a intamplat de fapt;
//  5. daca nici asa nu se poate stabili, rezultatul este UNKNOWN - niciodata
//     un "esec" presupus.
func (c *Client) Credit(ctx context.Context, in CreditInput) Result {
	res := Result{APIVersion: c.APIVersion()}
	if !c.Breaker.Allow() {
		res.Outcome = NotApplied
		res.Err = ErrCircuitOpen
		res.Resolution = "fallback: circuit deschis, cererea nu a fost trimisa"
		return res
	}
	var lastErr error
	ambiguous := false
	for attempt := 1; attempt <= c.cfg.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			break
		}
		actx, cancel := context.WithTimeout(ctx, c.cfg.PerAttemptTimeout)
		r, err := c.attempt(actx, in)
		cancel()
		res.Attempts = attempt
		res.APIVersion = c.APIVersion()

		switch classify(err) {
		case kOK:
			c.Breaker.Success()
			res.OperationID, res.BalanceAfter, res.Replayed = r.opID, r.balance, r.replayed
			if r.rejected != "" {
				res.Outcome, res.Reason = Rejected, r.rejected
				res.Resolution = "respins de serviciu (rezultat de business)"
				return res
			}
			res.Outcome = Applied
			res.Resolution = "raspuns direct"
			if r.replayed && ambiguous {
				res.Resolution = "reincercare cu aceeasi cheie: rezultat reluat din registrul de idempotenta"
			} else if r.replayed {
				res.Resolution = "operatia exista deja (cheie folosita anterior)"
			}
			return res
		case kRejected:
			c.Breaker.Success() // serviciul a raspuns: functioneaza
			res.Outcome, res.Err = Rejected, err
			res.Reason = status.Convert(err).Message()
			res.Resolution = "eroare definitiva " + status.Code(err).String()
			return res
		default:
			c.Breaker.Failure()
			ambiguous, lastErr = true, err
			c.cfg.Logf("incercarea %d key=%s: %s (%s) -> ambiguu", attempt, shortKey(in.Key),
				status.Code(err), status.Convert(err).Message())
			if attempt < c.cfg.MaxAttempts {
				select {
				case <-ctx.Done():
				case <-time.After(c.backoff(attempt)):
				}
			}
		}
		if !c.Breaker.Allow() {
			c.cfg.Logf("circuitul s-a deschis, opresc reincercarile")
			break
		}
	}
	if !ambiguous {
		res.Outcome, res.Err = NotApplied, ctx.Err()
		res.Resolution = "nicio incercare trimisa (context anulat)"
		return res
	}
	return c.reconcile(ctx, in, res, lastErr)
}

// reconcile intreaba serviciul ce s-a intamplat cu cheia data.
func (c *Client) reconcile(ctx context.Context, in CreditInput, res Result, lastErr error) Result {
	time.Sleep(c.cfg.ReconcileGrace)
	// O interogare scurta chiar daca apelantul a anulat: este doar o citire
	// si transforma un rezultat ambiguu intr-unul cunoscut.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.ReconcileTimeout)
	defer cancel()
	st, op, err := c.lookup(rctx, in.Key)
	if err != nil {
		res.Outcome = Unknown
		res.Err = fmt.Errorf("%w: ultima eroare %v; GetOperation: %v", ErrAmbiguous, lastErr, err)
		res.Resolution = "nerezolvat: serviciul nu raspunde nici la interogarea de stare"
		return res
	}
	switch st {
	case accountsv1.Operation_STATUS_SUCCEEDED:
		res.Outcome = Applied
		res.OperationID, res.BalanceAfter, res.Replayed = op.GetOperationId(), op.GetBalanceAfter(), true
		res.Resolution = "confirmat prin GetOperation: operatia ESTE aplicata (raspunsul se pierduse)"
	case accountsv1.Operation_STATUS_FAILED:
		res.Outcome, res.Reason = Rejected, op.GetOperationId()
		res.Resolution = "confirmat prin GetOperation: operatia a fost respinsa"
	case accountsv1.Operation_STATUS_NOT_FOUND:
		res.Outcome = NotApplied
		res.Err = lastErr
		res.Resolution = "confirmat prin GetOperation: operatia NU exista (serverul nu comite dupa deadline)"
	default:
		res.Outcome = Unknown
		res.Err = fmt.Errorf("%w: operatia este inca in curs", ErrAmbiguous)
		res.Resolution = "nerezolvat: operatia este in curs pe server"
	}
	return res
}

func (c *Client) lookup(ctx context.Context, key string) (accountsv1.Operation_Status, *accountsv1.CreditResponse, error) {
	if !c.useV1.Load() {
		op, err := c.v2.GetOperation(ctx, &accountsv2.GetOperationRequest{IdempotencyKey: key})
		if status.Code(err) == codes.Unimplemented {
			c.useV1.Store(true)
			return c.lookup(ctx, key)
		}
		if err != nil {
			return 0, nil, err
		}
		res := &accountsv1.CreditResponse{OperationId: op.GetId(), BalanceAfter: op.GetBalanceAfter().GetAmountMinor()}
		switch op.GetStatus() {
		case accountsv2.OperationStatus_OPERATION_STATUS_SUCCEEDED:
			return accountsv1.Operation_STATUS_SUCCEEDED, res, nil
		case accountsv2.OperationStatus_OPERATION_STATUS_FAILED:
			res.OperationId = op.GetFailureReason()
			return accountsv1.Operation_STATUS_FAILED, res, nil
		case accountsv2.OperationStatus_OPERATION_STATUS_NOT_FOUND:
			return accountsv1.Operation_STATUS_NOT_FOUND, res, nil
		}
		return accountsv1.Operation_STATUS_IN_PROGRESS, res, nil
	}
	op, err := c.v1.GetOperation(ctx, &accountsv1.GetOperationRequest{IdempotencyKey: key})
	if err != nil {
		return 0, nil, err
	}
	res := op.GetResult()
	if op.GetStatus() == accountsv1.Operation_STATUS_FAILED {
		res = &accountsv1.CreditResponse{OperationId: op.GetError()}
	}
	return op.GetStatus(), res, nil
}

// BalanceView este un sold, eventual servit din cache (fallback la citire).
type BalanceView struct {
	Account string
	Amount  int64
	Version int64
	Stale   bool      // true = valoare din cache, serviciul nu a raspuns
	AsOf    time.Time // momentul in care valoarea a fost obtinuta de la serviciu
}

// Balance citeste soldul; daca serviciul nu raspunde sau circuitul e deschis,
// intoarce ultima valoare cunoscuta, marcata explicit ca Stale.
func (c *Client) Balance(ctx context.Context, account string) (BalanceView, error) {
	if c.Breaker.Allow() {
		actx, cancel := context.WithTimeout(ctx, c.cfg.PerAttemptTimeout)
		v, err := c.fetchBalance(actx, account)
		cancel()
		if err == nil {
			c.Breaker.Success()
			c.mu.Lock()
			c.cache[account] = v
			c.mu.Unlock()
			return v, nil
		}
		if classify(err) == kRejected {
			c.Breaker.Success()
			return BalanceView{}, err
		}
		c.Breaker.Failure()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.cache[account]; ok {
		v.Stale = true
		return v, nil
	}
	return BalanceView{}, fmt.Errorf("serviciu indisponibil si nicio valoare in cache pentru %s", account)
}

func (c *Client) fetchBalance(ctx context.Context, account string) (BalanceView, error) {
	if !c.useV1.Load() {
		b, err := c.v2.GetBalance(ctx, &accountsv2.GetBalanceRequest{Account: account})
		if status.Code(err) == codes.Unimplemented {
			c.useV1.Store(true)
			return c.fetchBalance(ctx, account)
		}
		if err != nil {
			return BalanceView{}, err
		}
		return BalanceView{Account: account, Amount: b.GetAmount().GetAmountMinor(), Version: b.GetVersion(), AsOf: time.Now()}, nil
	}
	b, err := c.v1.GetBalance(ctx, &accountsv1.GetBalanceRequest{Account: account})
	if err != nil {
		return BalanceView{}, err
	}
	return BalanceView{Account: account, Amount: b.GetAmount(), Version: b.GetVersion(), AsOf: time.Now()}, nil
}

func shortKey(k string) string {
	if len(k) > 8 {
		return k[:8]
	}
	return k
}

package accounts

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Faults descrie defectele injectate in serviciu (demonstratii si teste).
type Faults struct {
	// UnavailableNext: urmatoarele N cereri Credit sunt respinse cu UNAVAILABLE
	// INAINTE de executie (operatia NU are loc).
	UnavailableNext int `json:"unavailable_next"`
	// DropResponseNext / DropResponseRate: dupa executia si commit-ul operatiei,
	// raspunsul se "pierde" (clientul primeste UNAVAILABLE). SCENARIUL CRITIC.
	DropResponseNext int     `json:"drop_response_next"`
	DropResponseRate float64 `json:"drop_response_rate"`
	// SlowResponse: raspunsul este trimis cu intarziere dupa commit, deci
	// clientul isi depaseste deadline-ul desi operatia a avut loc.
	SlowResponseMs int `json:"slow_response_ms"`
}

// ServerOptions configureaza serverul gRPC.
type ServerOptions struct {
	// DefaultDeadline se aplica cererilor fara deadline (protectie: nicio
	// cerere nu ruleaza la nesfarsit).
	DefaultDeadline time.Duration
	// MinBudget: daca timpul ramas pana la deadline e mai mic, cererea Credit
	// este respinsa imediat (fail fast), fara a incepe operatia.
	MinBudget time.Duration
	// DisableV2 simuleaza un server vechi, care cunoaste doar contractul v1.
	DisableV2 bool
	Logf      func(string, ...any)
}

// Stats sunt contoare de observabilitate.
type Stats struct {
	Executed    int64 `json:"executed"`
	Replayed    int64 `json:"replayed"`
	Rejected    int64 `json:"rejected"`
	Cancelled   int64 `json:"cancelled_before_commit"`
	FailFast    int64 `json:"deadline_fail_fast"`
	Dropped     int64 `json:"responses_dropped"`
	Unavailable int64 `json:"unavailable_injected"`
}

// Server leaga Store-ul de contractele gRPC.
type Server struct {
	Store *Store
	opts  ServerOptions

	mu     sync.Mutex
	faults Faults
	rnd    *rand.Rand

	executed, replayed, rejected, cancelled, failFast, dropped, unavailable atomic.Int64
}

// NewServer creeaza serverul.
func NewServer(store *Store, opts ServerOptions) *Server {
	if opts.DefaultDeadline <= 0 {
		opts.DefaultDeadline = 10 * time.Second
	}
	if opts.Logf == nil {
		opts.Logf = func(string, ...any) {}
	}
	return &Server{Store: store, opts: opts, rnd: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// SetFaults inlocuieste defectele injectate.
func (s *Server) SetFaults(f Faults) {
	s.mu.Lock()
	s.faults = f
	s.mu.Unlock()
}

// Faults intoarce defectele curente.
func (s *Server) Faults() Faults {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.faults
}

// Stats intoarce contoarele.
func (s *Server) Stats() Stats {
	return Stats{
		Executed: s.executed.Load(), Replayed: s.replayed.Load(), Rejected: s.rejected.Load(),
		Cancelled: s.cancelled.Load(), FailFast: s.failFast.Load(), Dropped: s.dropped.Load(),
		Unavailable: s.unavailable.Load(),
	}
}

// GRPC construieste *grpc.Server cu ambele versiuni ale contractului.
func (s *Server) GRPC(extra ...grpc.ServerOption) *grpc.Server {
	opts := append([]grpc.ServerOption{grpc.ChainUnaryInterceptor(s.versionHeaders, s.deadlinePolicy, s.faultInjection)}, extra...)
	g := grpc.NewServer(opts...)
	registerV1(g, s)
	if !s.opts.DisableV2 {
		registerV2(g, s)
	}
	return g
}

func isCredit(method string) bool { return strings.HasSuffix(method, "/Credit") }

func apiVersion(method string) string {
	switch {
	case strings.HasPrefix(method, "/accounts.v1."):
		return "v1"
	case strings.HasPrefix(method, "/accounts.v2."):
		return "v2"
	}
	return ""
}

// versionHeaders anunta versiunea servita; v1 este marcata ca depreciata
// (clientii vad din timp ca trebuie sa migreze, fara a fi rupti).
func (s *Server) versionHeaders(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	md := metadata.Pairs("x-api-version", apiVersion(info.FullMethod))
	if apiVersion(info.FullMethod) == "v1" {
		md.Append("deprecation", "true")
		md.Append("sunset", "2027-06-30")
	}
	_ = grpc.SetHeader(ctx, md)
	return h(ctx, req)
}

// deadlinePolicy: aplica un deadline implicit si respinge devreme cererile
// care oricum nu ar mai avea timp sa se termine.
func (s *Server) deadlinePolicy(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.opts.DefaultDeadline)
		defer cancel()
	}
	if isCredit(info.FullMethod) && s.opts.MinBudget > 0 {
		dl, _ := ctx.Deadline()
		if time.Until(dl) < s.opts.MinBudget {
			s.failFast.Add(1)
			return nil, status.Errorf(codes.DeadlineExceeded,
				"deadline prea scurt (%s ramase, minim %s): operatia NU a fost inceputa",
				time.Until(dl).Round(time.Millisecond), s.opts.MinBudget)
		}
	}
	return h(ctx, req)
}

func (s *Server) faultInjection(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
	if !isCredit(info.FullMethod) {
		return h(ctx, req)
	}
	s.mu.Lock()
	unavailable := s.faults.UnavailableNext > 0
	if unavailable {
		s.faults.UnavailableNext--
	}
	s.mu.Unlock()
	if unavailable {
		s.unavailable.Add(1)
		s.opts.Logf("defect: UNAVAILABLE inainte de executie")
		return nil, status.Error(codes.Unavailable, "serviciu indisponibil (defect injectat): operatia NU a fost executata")
	}

	resp, err := h(ctx, req)
	if err != nil {
		return resp, err
	}

	s.mu.Lock()
	drop := s.faults.DropResponseNext > 0 || s.rnd.Float64() < s.faults.DropResponseRate
	if s.faults.DropResponseNext > 0 {
		s.faults.DropResponseNext--
	}
	slow := time.Duration(s.faults.SlowResponseMs) * time.Millisecond
	s.mu.Unlock()

	if slow > 0 {
		s.opts.Logf("defect: raspuns intarziat %s dupa commit", slow)
		time.Sleep(slow) // intentionat fara ctx: serverul "ingheata" dupa commit
	}
	if drop {
		s.dropped.Add(1)
		s.opts.Logf("defect: operatia a fost EXECUTATA, dar raspunsul se pierde")
		return nil, status.Error(codes.Unavailable, "conexiune intrerupta inainte de raspuns")
	}
	return resp, nil
}

// toStatus traduce erorile domeniului in coduri gRPC (vezi CONTRACT.md).
func (s *Server) toStatus(err error) error {
	switch {
	case errors.Is(err, ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrKeyReuse):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, ErrInProgress):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		s.cancelled.Add(1)
		return status.Error(codes.DeadlineExceeded, "deadline depasit inainte de commit: operatia NU a fost aplicata")
	case errors.Is(err, context.Canceled):
		s.cancelled.Add(1)
		return status.Error(codes.Canceled, "cerere anulata de client inainte de commit: operatia NU a fost aplicata")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// credit este logica comuna v1/v2.
func (s *Server) credit(ctx context.Context, c CreditCmd) (Operation, bool, error) {
	op, replayed, err := s.Store.Credit(ctx, c)
	if err != nil {
		return op, false, s.toStatus(err)
	}
	switch {
	case replayed:
		s.replayed.Add(1)
		s.opts.Logf("%s Credit key=%s -> REPLAY %s (%s)", c.APIVersion, short(c.Key), op.ID, op.Status)
	case op.Status == StatusFailed:
		s.rejected.Add(1)
		s.opts.Logf("%s Credit key=%s -> RESPINS: %s", c.APIVersion, short(c.Key), op.Failure)
	default:
		s.executed.Add(1)
		s.opts.Logf("%s Credit key=%s %s += %d -> EXECUTAT %s, sold %d", c.APIVersion, short(c.Key), c.Account, c.Amount, op.ID, op.BalanceAfter)
	}
	return op, replayed, nil
}

func short(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

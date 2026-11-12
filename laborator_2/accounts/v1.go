package accounts

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountsv1 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v1"
)

// v1Server adapteaza contractul accounts.v1 la Store.
// In v1 o respingere de business este o EROARE gRPC (FAILED_PRECONDITION).
type v1Server struct {
	accountsv1.UnimplementedAccountsServer
	s *Server
}

func registerV1(g *grpc.Server, s *Server) { accountsv1.RegisterAccountsServer(g, &v1Server{s: s}) }

func (v *v1Server) Credit(ctx context.Context, r *accountsv1.CreditRequest) (*accountsv1.CreditResponse, error) {
	op, replayed, err := v.s.credit(ctx, CreditCmd{
		Key: r.GetIdempotencyKey(), Account: r.GetAccount(), Amount: r.GetAmount(),
		Currency: Currency, Note: r.GetNote(), APIVersion: "v1",
	})
	if err != nil {
		return nil, err
	}
	if op.Status == StatusFailed {
		return nil, status.Error(codes.FailedPrecondition, op.Failure)
	}
	return v1Response(op, replayed), nil
}

func v1Response(op Operation, replayed bool) *accountsv1.CreditResponse {
	return &accountsv1.CreditResponse{
		OperationId:     op.ID,
		BalanceAfter:    op.BalanceAfter,
		Replayed:        replayed,
		AppliedAtUnixMs: op.AppliedAt.UnixMilli(),
	}
}

func (v *v1Server) GetOperation(_ context.Context, r *accountsv1.GetOperationRequest) (*accountsv1.Operation, error) {
	if r.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key lipsa")
	}
	st, op := v.s.Store.Lookup(r.GetIdempotencyKey())
	out := &accountsv1.Operation{IdempotencyKey: r.GetIdempotencyKey()}
	switch st {
	case StatusNotFound:
		out.Status = accountsv1.Operation_STATUS_NOT_FOUND
	case StatusInProgress:
		out.Status = accountsv1.Operation_STATUS_IN_PROGRESS
	case StatusSucceeded:
		out.Status = accountsv1.Operation_STATUS_SUCCEEDED
		out.Result = v1Response(op, true)
	case StatusFailed:
		out.Status = accountsv1.Operation_STATUS_FAILED
		out.Error = op.Failure
	}
	return out, nil
}

func (v *v1Server) GetBalance(_ context.Context, r *accountsv1.GetBalanceRequest) (*accountsv1.Balance, error) {
	if !accountRe.MatchString(r.GetAccount()) {
		return nil, status.Error(codes.InvalidArgument, "account invalid")
	}
	amount, version := v.s.Store.Balance(r.GetAccount())
	return &accountsv1.Balance{Account: r.GetAccount(), Amount: amount, Version: version}, nil
}

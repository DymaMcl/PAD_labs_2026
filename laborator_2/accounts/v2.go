package accounts

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	accountsv2 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v2"
)

// v2Server adapteaza contractul accounts.v2 la ACELASI Store ca v1.
// Diferente fata de v1: suma este Money, iar o respingere de business NU mai
// este o eroare de transport: raspunsul e OK cu Operation.status = FAILED
// (erorile gRPC raman rezervate problemelor de cerere/transport).
type v2Server struct {
	accountsv2.UnimplementedAccountsServer
	s *Server
}

func registerV2(g *grpc.Server, s *Server) { accountsv2.RegisterAccountsServer(g, &v2Server{s: s}) }

func money(amount int64, currency string) *accountsv2.Money {
	return &accountsv2.Money{AmountMinor: amount, Currency: currency}
}

func v2Operation(op Operation, replayed bool) *accountsv2.Operation {
	out := &accountsv2.Operation{
		Id:              op.ID,
		IdempotencyKey:  op.Key,
		Account:         op.Account,
		Amount:          money(op.Amount, op.Currency),
		BalanceAfter:    money(op.BalanceAfter, op.Currency),
		AppliedAtUnixMs: op.AppliedAt.UnixMilli(),
		Replayed:        replayed,
		FailureReason:   op.Failure,
		Reference:       op.Reference,
	}
	switch op.Status {
	case StatusSucceeded:
		out.Status = accountsv2.OperationStatus_OPERATION_STATUS_SUCCEEDED
	case StatusFailed:
		out.Status = accountsv2.OperationStatus_OPERATION_STATUS_FAILED
	}
	return out
}

func (v *v2Server) Credit(ctx context.Context, r *accountsv2.CreditRequest) (*accountsv2.CreditResponse, error) {
	if r.GetAmount() == nil {
		return nil, status.Error(codes.InvalidArgument, "amount (Money) lipsa")
	}
	op, replayed, err := v.s.credit(ctx, CreditCmd{
		Key: r.GetIdempotencyKey(), Account: r.GetAccount(),
		Amount: r.GetAmount().GetAmountMinor(), Currency: r.GetAmount().GetCurrency(),
		Reference: r.GetReference(), APIVersion: "v2",
	})
	if err != nil {
		return nil, err
	}
	return &accountsv2.CreditResponse{Operation: v2Operation(op, replayed)}, nil
}

func (v *v2Server) GetOperation(_ context.Context, r *accountsv2.GetOperationRequest) (*accountsv2.Operation, error) {
	if r.GetIdempotencyKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key lipsa")
	}
	st, op := v.s.Store.Lookup(r.GetIdempotencyKey())
	switch st {
	case StatusNotFound:
		return &accountsv2.Operation{IdempotencyKey: op.Key, Status: accountsv2.OperationStatus_OPERATION_STATUS_NOT_FOUND}, nil
	case StatusInProgress:
		return &accountsv2.Operation{IdempotencyKey: op.Key, Status: accountsv2.OperationStatus_OPERATION_STATUS_IN_PROGRESS}, nil
	}
	return v2Operation(op, true), nil
}

func (v *v2Server) GetBalance(_ context.Context, r *accountsv2.GetBalanceRequest) (*accountsv2.Balance, error) {
	if !accountRe.MatchString(r.GetAccount()) {
		return nil, status.Error(codes.InvalidArgument, "account invalid")
	}
	amount, version := v.s.Store.Balance(r.GetAccount())
	return &accountsv2.Balance{Account: r.GetAccount(), Amount: money(amount, Currency), Version: version}, nil
}

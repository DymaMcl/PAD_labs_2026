package accounts

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	accountsv1 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v1"
	accountsv2 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v2"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/compat"
)

func startServer(t *testing.T, so Options, opts ServerOptions) (*Server, *grpc.ClientConn) {
	t.Helper()
	store, _ := Open("", so)
	srv := NewServer(store, opts)
	g := srv.GRPC()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go g.Serve(lis)
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return srv, conn
}

// ctxT intoarce un context cu deadline; anularea are loc la expirare.
func ctxT(d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	time.AfterFunc(d+time.Second, cancel)
	return ctx
}

func TestV1CreditAndReplay(t *testing.T) {
	_, conn := startServer(t, Options{}, ServerOptions{})
	c := accountsv1.NewAccountsClient(conn)
	req := &accountsv1.CreditRequest{IdempotencyKey: "k1", Account: "ACC-1", Amount: 100}
	var hdr metadata.MD
	r1, err := c.Credit(ctxT(time.Second), req, grpc.Header(&hdr))
	if err != nil || r1.Replayed || r1.BalanceAfter != 100 {
		t.Fatalf("%v %v", r1, err)
	}
	if hdr.Get("x-api-version")[0] != "v1" || hdr.Get("deprecation")[0] != "true" {
		t.Fatalf("antete de versiune: %v", hdr)
	}
	r2, _ := c.Credit(ctxT(time.Second), req)
	if !r2.Replayed || r2.OperationId != r1.OperationId {
		t.Fatalf("reluarea: %v", r2)
	}
}

func TestErrorCodes(t *testing.T) {
	_, conn := startServer(t, Options{}, ServerOptions{})
	c := accountsv1.NewAccountsClient(conn)
	cases := []struct {
		req  *accountsv1.CreditRequest
		want codes.Code
	}{
		{&accountsv1.CreditRequest{IdempotencyKey: "a", Account: "x", Amount: 1}, codes.InvalidArgument},
		{&accountsv1.CreditRequest{IdempotencyKey: "b", Account: "ACC-1", Amount: MaxCredit + 1}, codes.FailedPrecondition},
		{&accountsv1.CreditRequest{IdempotencyKey: "b", Account: "ACC-1", Amount: 5}, codes.AlreadyExists},
	}
	for i, tc := range cases {
		_, err := c.Credit(ctxT(time.Second), tc.req)
		if status.Code(err) != tc.want {
			t.Errorf("cazul %d: astepta %s, primit %v", i, tc.want, err)
		}
	}
}

func TestV1AndV2ShareState(t *testing.T) {
	_, conn := startServer(t, Options{}, ServerOptions{})
	c1 := accountsv1.NewAccountsClient(conn)
	c2 := accountsv2.NewAccountsClient(conn)
	c1.Credit(ctxT(time.Second), &accountsv1.CreditRequest{IdempotencyKey: "k", Account: "ACC-7", Amount: 40})
	r, err := c2.Credit(ctxT(time.Second), &accountsv2.CreditRequest{IdempotencyKey: "k2", Account: "ACC-7",
		Amount: &accountsv2.Money{AmountMinor: 60, Currency: "MDL"}, Reference: "msg-1"})
	if err != nil || r.Operation.BalanceAfter.AmountMinor != 100 {
		t.Fatalf("v2: %v %v", r, err)
	}
	// aceeasi operatie logica reluata prin v2 cu cheia data initial prin v1
	r2, err := c2.Credit(ctxT(time.Second), &accountsv2.CreditRequest{IdempotencyKey: "k", Account: "ACC-7",
		Amount: &accountsv2.Money{AmountMinor: 40, Currency: "MDL"}})
	if err != nil || !r2.Operation.Replayed {
		t.Fatalf("reluarea v1->v2: %v %v", r2, err)
	}
	b, _ := c1.GetBalance(ctxT(time.Second), &accountsv1.GetBalanceRequest{Account: "ACC-7"})
	if b.Amount != 100 || b.Version != 2 {
		t.Fatalf("sold vazut prin v1: %v", b)
	}
	// in v2 respingerea de business este un rezultat, nu o eroare
	r3, err := c2.Credit(ctxT(time.Second), &accountsv2.CreditRequest{IdempotencyKey: "big", Account: "ACC-7",
		Amount: &accountsv2.Money{AmountMinor: MaxCredit + 1, Currency: "MDL"}})
	if err != nil || r3.Operation.Status != accountsv2.OperationStatus_OPERATION_STATUS_FAILED {
		t.Fatalf("v2 respingere: %v %v", r3, err)
	}
	// moneda nesuportata -> eroare de contract
	_, err = c2.Credit(ctxT(time.Second), &accountsv2.CreditRequest{IdempotencyKey: "eur", Account: "ACC-7",
		Amount: &accountsv2.Money{AmountMinor: 1, Currency: "EUR"}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("EUR: %v", err)
	}
}

// Compatibilitate inapoi: un client compilat cu contractul v1.0 (fara `note`,
// cu campul `currency` eliminat ulterior) functioneaza cu serverul actual.
func TestOldV1_0ClientStillWorks(t *testing.T) {
	_, conn := startServer(t, Options{}, ServerOptions{})
	in := &compat.CreditRequest{IdempotencyKey: "old", Account: "ACC-3", Amount: 75, Currency: "MDL"}
	out := &compat.CreditResponse{}
	if err := conn.Invoke(ctxT(time.Second), "/accounts.v1.Accounts/Credit", in, out); err != nil {
		t.Fatal(err)
	}
	if out.BalanceAfter != 75 || out.OperationId == "" {
		t.Fatalf("raspuns pentru clientul vechi: %v", out)
	}
	// campurile noi (replayed, applied_at) sunt pastrate ca "necunoscute",
	// nu provoaca erori
	if len(out.ProtoReflect().GetUnknown()) == 0 {
		t.Fatal("astepta campuri necunoscute pastrate in raspuns")
	}
}

// Compatibilitate inainte: un mesaj produs de contractul nou (cu `note`) este
// citit fara erori de codul vechi, iar invers campul vechi este ignorat.
func TestWireCompatibility(t *testing.T) {
	newMsg := &accountsv1.CreditRequest{IdempotencyKey: "k", Account: "ACC-1", Amount: 10, Note: "nou in v1.1"}
	raw, _ := proto.Marshal(newMsg)
	old := &compat.CreditRequest{}
	if err := proto.Unmarshal(raw, old); err != nil || old.Amount != 10 || old.Account != "ACC-1" {
		t.Fatalf("vechi <- nou: %v %v", old, err)
	}
	oldRaw, _ := proto.Marshal(&compat.CreditRequest{IdempotencyKey: "k", Account: "ACC-1", Amount: 10, Currency: "MDL"})
	back := &accountsv1.CreditRequest{}
	if err := proto.Unmarshal(oldRaw, back); err != nil || back.Amount != 10 {
		t.Fatalf("nou <- vechi: %v %v", back, err)
	}
}

// Serverul respecta deadline-ul clientului: daca acesta expira inainte de
// commit, operatia NU are loc (deadline-ul se propaga automat prin gRPC).
func TestClientDeadlinePropagatesAndAbortsBeforeCommit(t *testing.T) {
	srv, conn := startServer(t, Options{ProcessingTime: 300 * time.Millisecond}, ServerOptions{})
	c := accountsv1.NewAccountsClient(conn)
	_, err := c.Credit(ctxT(100*time.Millisecond), &accountsv1.CreditRequest{IdempotencyKey: "slow", Account: "ACC-1", Amount: 5})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("astepta DeadlineExceeded, primit %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	op, _ := c.GetOperation(ctxT(time.Second), &accountsv1.GetOperationRequest{IdempotencyKey: "slow"})
	if op.Status != accountsv1.Operation_STATUS_NOT_FOUND || srv.Stats().Cancelled != 1 {
		t.Fatalf("operatia nu trebuia aplicata: %v stats=%+v", op, srv.Stats())
	}
}

func TestCancellationAbortsBeforeCommit(t *testing.T) {
	srv, conn := startServer(t, Options{ProcessingTime: 300 * time.Millisecond}, ServerOptions{})
	c := accountsv1.NewAccountsClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	_, err := c.Credit(ctx, &accountsv1.CreditRequest{IdempotencyKey: "cancel", Account: "ACC-1", Amount: 5})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("astepta Canceled, primit %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if st, _ := srv.Store.Lookup("cancel"); st != StatusNotFound {
		t.Fatalf("operatia anulata a fost aplicata: %s", st)
	}
}

func TestFailFastWhenDeadlineTooShort(t *testing.T) {
	srv, conn := startServer(t, Options{}, ServerOptions{MinBudget: 200 * time.Millisecond})
	c := accountsv1.NewAccountsClient(conn)
	_, err := c.Credit(ctxT(50*time.Millisecond), &accountsv1.CreditRequest{IdempotencyKey: "ff", Account: "ACC-1", Amount: 5})
	if status.Code(err) != codes.DeadlineExceeded || srv.Stats().FailFast != 1 {
		t.Fatalf("fail fast: %v %+v", err, srv.Stats())
	}
}

// SCENARIUL CRITIC la nivel de server: operatia se executa, raspunsul se pierde.
func TestDroppedResponseAfterCommit(t *testing.T) {
	srv, conn := startServer(t, Options{}, ServerOptions{})
	srv.SetFaults(Faults{DropResponseNext: 1})
	c := accountsv1.NewAccountsClient(conn)
	req := &accountsv1.CreditRequest{IdempotencyKey: "lost", Account: "ACC-1", Amount: 100}
	if _, err := c.Credit(ctxT(time.Second), req); status.Code(err) != codes.Unavailable {
		t.Fatalf("clientul trebuia sa vada o eroare: %v", err)
	}
	op, _ := c.GetOperation(ctxT(time.Second), &accountsv1.GetOperationRequest{IdempotencyKey: "lost"})
	if op.Status != accountsv1.Operation_STATUS_SUCCEEDED {
		t.Fatalf("operatia a avut loc, desi clientul a primit eroare: %v", op)
	}
	r, err := c.Credit(ctxT(time.Second), req)
	if err != nil || !r.Replayed || r.BalanceAfter != 100 {
		t.Fatalf("reincercarea cu aceeasi cheie: %v %v", r, err)
	}
}

func TestOldServerWithoutV2(t *testing.T) {
	_, conn := startServer(t, Options{}, ServerOptions{DisableV2: true})
	_, err := accountsv2.NewAccountsClient(conn).GetBalance(ctxT(time.Second), &accountsv2.GetBalanceRequest{Account: "ACC-1"})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("astepta Unimplemented, primit %v", err)
	}
}

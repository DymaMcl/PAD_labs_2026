package accounts

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func cmd(key string, amount int64) CreditCmd {
	return CreditCmd{Key: key, Account: "ACC-1", Amount: amount, Currency: Currency, APIVersion: "test"}
}

func TestCreditIsIdempotentByKey(t *testing.T) {
	s, _ := Open("", Options{})
	op1, replayed, err := s.Credit(context.Background(), cmd("k1", 100))
	if err != nil || replayed || op1.BalanceAfter != 100 {
		t.Fatalf("prima executie: %+v %v %v", op1, replayed, err)
	}
	op2, replayed, err := s.Credit(context.Background(), cmd("k1", 100))
	if err != nil || !replayed || op2.ID != op1.ID || op2.BalanceAfter != 100 {
		t.Fatalf("reluarea trebuie sa intoarca rezultatul original: %+v %v %v", op2, replayed, err)
	}
	if bal, ver := s.Balance("ACC-1"); bal != 100 || ver != 1 {
		t.Fatalf("efectul a fost aplicat de mai multe ori: sold=%d versiune=%d", bal, ver)
	}
}

func TestKeyReuseWithDifferentRequestRejected(t *testing.T) {
	s, _ := Open("", Options{})
	s.Credit(context.Background(), cmd("k1", 100))
	if _, _, err := s.Credit(context.Background(), cmd("k1", 999)); !errors.Is(err, ErrKeyReuse) {
		t.Fatalf("astepta ErrKeyReuse, primit %v", err)
	}
}

func TestValidation(t *testing.T) {
	s, _ := Open("", Options{})
	bad := []CreditCmd{
		{Key: "", Account: "ACC-1", Amount: 1, Currency: Currency},
		{Key: "k", Account: "contul meu", Amount: 1, Currency: Currency},
		{Key: "k", Account: "ACC-1", Amount: 0, Currency: Currency},
		{Key: "k", Account: "ACC-1", Amount: 1, Currency: "EUR"},
	}
	for i, c := range bad {
		if _, _, err := s.Credit(context.Background(), c); !errors.Is(err, ErrInvalid) {
			t.Errorf("cazul %d: astepta ErrInvalid, primit %v", i, err)
		}
	}
	if st, _ := s.Lookup("k"); st != StatusNotFound {
		t.Fatal("cererile invalide nu se memoreaza")
	}
}

func TestBusinessRejectionIsMemorized(t *testing.T) {
	s, _ := Open("", Options{})
	op, _, err := s.Credit(context.Background(), cmd("big", MaxCredit+1))
	if err != nil || op.Status != StatusFailed {
		t.Fatalf("astepta FAILED: %+v %v", op, err)
	}
	op2, replayed, _ := s.Credit(context.Background(), cmd("big", MaxCredit+1))
	if !replayed || op2.Status != StatusFailed {
		t.Fatal("respingerea trebuie sa fie determinista la reluare")
	}
	if bal, _ := s.Balance("ACC-1"); bal != 0 {
		t.Fatal("respingerea nu are efect")
	}
}

// Cancellation/deadline INAINTE de commit: nimic nu se aplica, iar cheia
// ramane libera (NOT_FOUND) -> clientul poate reincerca in siguranta.
func TestCancelBeforeCommitHasNoEffect(t *testing.T) {
	s, _ := Open("", Options{ProcessingTime: 200 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := s.Credit(ctx, cmd("k1", 100))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("astepta DeadlineExceeded, primit %v", err)
	}
	if st, _ := s.Lookup("k1"); st != StatusNotFound {
		t.Fatalf("operatia anulata nu trebuie memorata, status=%s", st)
	}
	if bal, _ := s.Balance("ACC-1"); bal != 0 {
		t.Fatal("efect aplicat dupa anulare")
	}
}

func TestConcurrentSameKeyExecutesOnce(t *testing.T) {
	s, _ := Open("", Options{ProcessingTime: 30 * time.Millisecond})
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := map[string]int{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, replayed, err := s.Credit(context.Background(), cmd("same", 50))
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, ErrInProgress):
				results["in_progress"]++
			case err != nil:
				results["error"]++
			case replayed:
				results["replayed"]++
			default:
				results["executed"]++
			}
		}()
	}
	wg.Wait()
	if results["executed"] != 1 || results["error"] != 0 {
		t.Fatalf("executii concurente: %v", results)
	}
	if bal, _ := s.Balance("ACC-1"); bal != 50 {
		t.Fatalf("sold %d", bal)
	}
}

func TestStoreRecoversFromJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.journal")
	s, _ := Open(path, Options{})
	s.Credit(context.Background(), cmd("a", 100))
	s.Credit(context.Background(), cmd("b", 250))
	s.Credit(context.Background(), cmd("c", MaxCredit+5))
	s.Close()

	s2, err := Open(path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if bal, ver := s2.Balance("ACC-1"); bal != 350 || ver != 2 {
		t.Fatalf("dupa restart: sold=%d versiune=%d", bal, ver)
	}
	if _, replayed, _ := s2.Credit(context.Background(), cmd("a", 100)); !replayed {
		t.Fatal("registrul de idempotenta trebuie sa supravietuiasca restartului")
	}
	if st, _ := s2.Lookup("c"); st != StatusFailed {
		t.Fatalf("status dupa restart: %s", st)
	}
}

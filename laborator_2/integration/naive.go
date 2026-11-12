package integration

import (
	"context"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
	accountsv1 "github.com/DymaMcl/PAD_labs_2026/laborator_2/gen/accounts/v1"
)

// NaiveCredit este ANTI-MODELUL folosit in demonstratie: trateaza orice
// eroare ca "operatia nu a avut loc" si reincearca generand o cheie NOUA la
// fiecare incercare. Dupa un raspuns pierdut, efectul este aplicat de doua ori.
func NaiveCredit(ctx context.Context, c *Client, account string, amount int64, attempts int) (int64, int, error) {
	var lastErr error
	for i := 1; i <= attempts; i++ {
		actx, cancel := context.WithTimeout(ctx, c.cfg.PerAttemptTimeout)
		r, err := c.v1.Credit(actx, &accountsv1.CreditRequest{
			IdempotencyKey: contract.NewID(), // GRESIT: cheie noua la fiecare incercare
			Account:        account, Amount: amount,
		})
		cancel()
		if err == nil {
			return r.GetBalanceAfter(), i, nil
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	return 0, attempts, lastErr
}

package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/DymaMcl/PAD_labs_2026/laborator_2/contract"
	"github.com/DymaMcl/PAD_labs_2026/laborator_2/integration"
)

// EffectStatus spune worker-ului ce sa raspunda brokerului.
type EffectStatus int

const (
	EffectDone      EffectStatus = iota // efect aplicat acum -> ACK
	EffectDuplicate                     // efect existent deja -> ACK fara efect nou
	EffectPermanent                     // respins definitiv -> NACK fara retry (DLQ)
	EffectRetry                         // neaplicat sau necunoscut -> NACK cu retry
)

// EffectResult este rezultatul aplicarii efectului unui mesaj.
type EffectResult struct {
	Status EffectStatus
	Detail string
}

// Effect este "efectul local" al unui mesaj. Laboratorul 1 il aplica intr-un
// registru local (LocalEffect); laboratorul 2 il aplica intr-un serviciu la
// distanta prin gRPC (RemoteEffect).
type Effect interface {
	Apply(ctx context.Context, d contract.Delivery, p contract.CreditPayload) EffectResult
	Name() string
}

// Gate este implementat de efectele care pot cere oprirea temporara a
// consumului (back-pressure): cat timp Ready() e false, worker-ul nu mai
// preia mesaje, iar acestea asteapta in broker fara sa-si consume tentativele.
type Gate interface {
	Ready() bool
}

// LocalEffect: comportamentul din laboratorul 1 (registru + inbox local).
type LocalEffect struct {
	Ledger     *Ledger
	Idempotent bool
}

func (e *LocalEffect) Name() string {
	if e.Idempotent {
		return "local, IDEMPOTENT (inbox)"
	}
	return "local, NAIV (fara deduplicare)"
}

func (e *LocalEffect) Apply(_ context.Context, d contract.Delivery, p contract.CreditPayload) EffectResult {
	m := d.Message
	if e.Idempotent && e.Ledger.Seen(m.ID) {
		return EffectResult{EffectDuplicate, "DUPLICAT detectat in inbox -> efect omis"}
	}
	err := e.Ledger.ApplyCredit(m, p, e.Idempotent)
	switch {
	case errors.Is(err, ErrAlreadyApplied):
		return EffectResult{EffectDuplicate, "DUPLICAT (concurent) -> efect omis"}
	case err != nil:
		return EffectResult{EffectRetry, "stocare locala: " + err.Error()}
	}
	return EffectResult{EffectDone, fmt.Sprintf("efect aplicat: %s += %d", p.Account, p.Amount)}
}

// RemoteEffect aplica efectul prin serviciul de conturi.
//
//   - cheia de idempotenta este ID-ul mesajului: idempotenta se pastreaza pe
//     tot lantul producer -> broker -> consumer -> serviciu;
//   - deadline-ul apelului este derivat din lease-ul livrarii: consumer-ul nu
//     lucreaza la un mesaj pe care brokerul l-ar putea deja relivra;
//   - un rezultat NOT_APPLIED sau UNKNOWN nu este niciodata confirmat brokerului:
//     mesajul ramane in coada (fallback durabil) si va fi reluat cu aceeasi cheie.
type RemoteEffect struct {
	Client      *integration.Client
	LeaseMargin time.Duration // rezerva pentru ACK dupa apel
	Logf        func(string, ...any)
}

func (e *RemoteEffect) Name() string {
	return "serviciu de conturi gRPC (" + e.Client.APIVersion() + ")"
}

// Ready: nu preluam mesaje cat timp circuitul catre serviciu este deschis.
func (e *RemoteEffect) Ready() bool {
	return e.Client.Breaker.State() != integration.BreakerOpen
}

func (e *RemoteEffect) Apply(ctx context.Context, d contract.Delivery, p contract.CreditPayload) EffectResult {
	margin := e.LeaseMargin
	if margin <= 0 {
		margin = 300 * time.Millisecond
	}
	deadline := d.LeaseUntil.Add(-margin)
	if time.Until(deadline) <= 0 {
		return EffectResult{EffectRetry, "lease aproape expirat: apelul nu mai este inceput"}
	}
	cctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	r := e.Client.Credit(cctx, integration.CreditInput{
		Key: d.Message.ID, Account: p.Account, Amount: p.Amount, Reference: d.Message.ID,
	})
	info := fmt.Sprintf("[%s, %d incercari, %s] %s", r.APIVersion, r.Attempts, r.Outcome, r.Resolution)
	switch r.Outcome {
	case integration.Applied:
		if r.Replayed {
			return EffectResult{EffectDuplicate, fmt.Sprintf("serviciul avea deja operatia %s, sold %d %s", r.OperationID, r.BalanceAfter, info)}
		}
		return EffectResult{EffectDone, fmt.Sprintf("serviciu: %s += %d -> sold %d (%s) %s", p.Account, p.Amount, r.BalanceAfter, r.OperationID, info)}
	case integration.Rejected:
		return EffectResult{EffectPermanent, "serviciul a respins: " + r.Reason + " " + info}
	case integration.NotApplied:
		return EffectResult{EffectRetry, "neaplicat, amanat in broker (fallback) " + info}
	default:
		return EffectResult{EffectRetry, "rezultat NECUNOSCUT: mesajul ramane in broker si va fi reluat cu aceeasi cheie " + info}
	}
}

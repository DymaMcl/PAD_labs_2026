# Contractul serviciului de conturi (`accounts`)

Sursa de adevăr: [`proto/accounts/v1/accounts.proto`](proto/accounts/v1/accounts.proto) și [`proto/accounts/v2/accounts.proto`](proto/accounts/v2/accounts.proto).
Transport: **gRPC** (HTTP/2, Protocol Buffers), port implicit `7080`. Ambele versiuni sunt servite simultan, peste aceeași stare.

## 1. Operații

| RPC | v1 | v2 | Efect | Idempotent |
|---|---|---|---|---|
| `Credit` | `amount: int64` | `amount: Money{amount_minor, currency}`, `reference`, `metadata` | alimentează un cont | **da**, după `idempotency_key` |
| `GetOperation` | `Operation{status, result, error}` | `Operation{id, status, amount, balance_after, …}` | — (citire) | da |
| `GetBalance` | `Balance{amount: int64}` | `Balance{amount: Money}` | — (citire) | da |

Moneda: doar `MDL`, sume în bani (unități minore). Conturi: `ACC-<număr>`. Limită de business: `1 000 000` per operație.

## 2. Idempotență

* `idempotency_key` este **obligatorie** la `Credit` (1..128 caractere). Clientul o generează **o singură dată per operație logică** și o refolosește la **toate** reîncercările. În lanțul din laboratorul 1, cheia este `id`-ul mesajului.
* Serviciul păstrează pentru fiecare cheie: amprenta cererii (cont + sumă + monedă), rezultatul și starea.
* **Aceeași cheie + aceeași cerere** → rezultatul original, `replayed = true`, **fără** a executa din nou.
* **Aceeași cheie + altă cerere** → `ALREADY_EXISTS` (eroare de programare la client).
* **Aceeași cheie, cerere încă în curs** → `ABORTED` (se reîncearcă după o pauză).
* Cheia este comună versiunilor: o operație creată prin v1 poate fi reluată prin v2 și invers. Amprenta nu include câmpurile informative (`note`, `reference`).
* Respingerile de business (sumă peste limită) sunt **memorate**: reluarea primește aceeași respingere.
* Cererile invalide sintactic (`INVALID_ARGUMENT`) **nu** sunt memorate.
* Retenție: registrul nu expiră în această implementare (vezi limitele din README).

## 3. Modelul de erori

| Cod gRPC | Când | Efectul a avut loc? | Ce face clientul |
|---|---|---|---|
| `OK` | succes (sau reluare, `replayed=true`) | **da** | confirmă |
| `INVALID_ARGUMENT` | cerere care încalcă contractul | **nu** | nu reîncearcă → `REJECTED` |
| `FAILED_PRECONDITION` (v1) | respingere de business | **nu** | nu reîncearcă → `REJECTED` |
| `ALREADY_EXISTS` | cheie refolosită pentru altă cerere | nu (pentru cererea curentă) | nu reîncearcă → `REJECTED` |
| `ABORTED` | aceeași cheie e în curs | **necunoscut** | reîncearcă cu aceeași cheie |
| `DEADLINE_EXCEEDED` | deadline depășit | **necunoscut** (vezi §4) | reîncearcă cu aceeași cheie / `GetOperation` |
| `CANCELED` | clientul a anulat | **necunoscut** | `GetOperation` |
| `UNAVAILABLE` | serviciu/conexiune indisponibilă, **răspuns pierdut** | **necunoscut** | reîncearcă cu aceeași cheie / `GetOperation` |
| `INTERNAL`, `UNKNOWN` | eroare neașteptată | **necunoscut** | reîncearcă cu aceeași cheie / `GetOperation` |
| `UNIMPLEMENTED` | serverul nu cunoaște versiunea | **nu** | fallback la versiunea anterioară |

În **v2**, respingerea de business nu mai e o eroare gRPC: răspunsul este `OK` cu `Operation.status = OPERATION_STATUS_FAILED` și `failure_reason`. Erorile gRPC rămân pentru problemele de cerere și transport.

Regula de bază: **orice cod care nu este `OK` sau o eroare definitivă lasă rezultatul ambiguu**. Clientul nu are voie să-l interpreteze ca „nu s-a executat”.

## 4. Deadline și cancellation

* Clientul **trebuie** să trimită un deadline. gRPC îl propagă automat la server (antetul `grpc-timeout`).
* Cererile fără deadline primesc pe server un deadline implicit (`10s`), ca nicio cerere să nu ruleze la nesfârșit.
* **Fail fast**: dacă timpul rămas e mai mic decât bugetul minim al operației, serverul răspunde imediat `DEADLINE_EXCEEDED`, **fără a începe** operația.
* **Punctul de commit**: serverul verifică deadline-ul și anularea imediat înainte de scrierea durabilă.
  * Dacă deadline-ul expiră sau clientul anulează **înainte** de commit, operația **nu are loc**: `GetOperation` → `NOT_FOUND`.
  * **După** commit, operația rămâne efectuată chiar dacă clientul a plecat. Din acest motiv `DEADLINE_EXCEEDED` și `CANCELED` sunt ambigue pentru client.
* `GetOperation` → `NOT_FOUND` după expirarea deadline-ului (plus o mică marjă de rețea) înseamnă sigur „neexecutat”: serverul nu face commit după deadline.

## 5. Versionare și compatibilitate

**Politica**

1. Versiunea majoră este în numele pachetului: `accounts.v1`, `accounts.v2`.
2. În cadrul unei versiuni majore sunt permise doar schimbări compatibile:
   * câmpuri noi, opționale, cu numere noi (ex. `note = 4` adăugat în v1.1);
   * RPC-uri noi;
   * valori noi de enum (clienții tratează valorile necunoscute ca `UNSPECIFIED`).
3. Interzise în cadrul unei versiuni: schimbarea tipului sau a numărului unui câmp, refolosirea unui număr eliminat, câmpuri noi obligatorii, schimbarea semanticii.
   * Câmpurile eliminate se trec în `reserved` (ex. `reserved 5; reserved "currency";` în v1).
4. O schimbare incompatibilă creează un pachet nou. Exemplu: în v2, `amount` devine `Money`.
5. Versiunea veche rămâne servită în paralel. Serverul anunță deprecierea în antetele răspunsului: `x-api-version: v1`, `deprecation: true`, `sunset: 2027-06-30`.
6. Clientul de integrare negociază: încearcă v2, iar la `UNIMPLEMENTED` trece la v1.

**Matricea de compatibilitate (verificată prin teste)**

| client \ server | server vechi (doar v1) | server actual (v1 + v2) |
|---|---|---|
| client v1.0 (cu `currency`, fără `note`) | ✓ | ✓ câmpul `currency` e ignorat, câmpurile noi din răspuns sunt păstrate ca „necunoscute” |
| client v1.1 | ✓ | ✓ (+ antete de depreciere) |
| client v2 / integrare | ✓ prin fallback la v1 | ✓ |

Testele relevante: `TestOldV1_0ClientStillWorks`, `TestWireCompatibility`, `TestV1AndV2ShareState`, `TestOldServerWithoutV2`, `TestContractFallbackToV1`.

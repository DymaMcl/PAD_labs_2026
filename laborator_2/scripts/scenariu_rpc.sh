#!/usr/bin/env bash
# Laboratorul 2 cu procese reale: broker + serviciu de conturi gRPC care pierde
# 30% din raspunsuri DUPA executie + producer + consumer. La mijloc serviciul
# este oprit si repornit. Soldurile finale trebuie sa fie exacte.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf data && mkdir -p bin data
go build -o bin/ ./cmd/...

./bin/broker -addr :7070 -data data/broker.journal -lease 4s > data/broker.log 2>&1 &
BROKER=$!
start_accounts() {
  ./bin/accounts -grpc :7080 -http :7081 -data data/accounts.journal -drop-rate 0.3 >> data/accounts.log 2>&1 &
  ACC=$!
}
start_accounts
trap 'kill $BROKER $ACC $CONS 2>/dev/null || true' EXIT
sleep 1

echo "== producer: 12 alimentari pe 3 conturi =="
./bin/producer -count 12 -accounts 3 -interval 0 | tail -6

echo "== consumer (efect prin serviciul gRPC) =="
./bin/consumer -name c1 > data/consumer.log 2>&1 &
CONS=$!
sleep 1.5
echo "== opresc serviciul de conturi 3 secunde =="
kill $ACC; sleep 3
./bin/producer -count 6 -accounts 3 -interval 0 -source producer-2 | tail -5
echo "== repornesc serviciul =="
start_accounts
sleep 8
kill -INT $CONS 2>/dev/null || true

echo "== jurnalul consumer-ului (extras) =="
grep -E "⏸|▶|≡|☠|ambiguu|fallback" data/consumer.log | head -15 || true
echo "== solduri in serviciu (admin) =="
curl -s localhost:7081/admin/state | python3 -c 'import json,sys; s=json.load(sys.stdin); print(s["balances"]); print(s["stats"])' 2>/dev/null || curl -s localhost:7081/admin/state

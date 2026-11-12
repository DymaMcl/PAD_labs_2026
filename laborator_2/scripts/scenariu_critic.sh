#!/usr/bin/env bash
# Scenariul critic cu procese reale: consumer-ul cade (exit 3) dupa efect,
# inainte de ACK, este repornit, iar mesajul relivrat nu dubleaza efectul.
# Utilizare: ./scripts/scenariu_critic.sh [-naive]
set -euo pipefail
cd "$(dirname "$0")/.."
MODE="${1:-}"
PORT=7070
URL="http://localhost:$PORT"
rm -rf data && mkdir -p bin data
go build -o bin/ ./cmd/...

echo "== 1. pornesc brokerul (lease 3s) =="
./bin/broker -addr ":$PORT" -data data/broker.journal -lease 3s > data/broker.log 2>&1 &
BROKER=$!
trap 'kill $BROKER 2>/dev/null || true' EXIT
sleep 1

echo "== 2. producer: 5 alimentari x 100 pe ACC-1 (sold asteptat 500) =="
./bin/producer -broker "$URL" -count 5 -accounts 1 -interval 0 | tail -4

echo "== 3. consumer: cade dupa al 3-lea efect, fara ACK =="
set +e
./bin/consumer -broker "$URL" -name c1 -effect local $MODE -crash-after-effect 3
echo "   -> consumer iesit cu codul $?"
set -e

echo "== 4. astept expirarea lease-ului si repornesc consumer-ul =="
sleep 3.5
./bin/consumer -broker "$URL" -name c1 -effect local $MODE &
CONS=$!
sleep 3
kill -INT $CONS 2>/dev/null || true
wait $CONS 2>/dev/null || true

echo "== 5. starea brokerului =="
curl -s "$URL/v1/queues"; echo

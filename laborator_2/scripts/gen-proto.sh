#!/usr/bin/env bash
# Regenereaza codul Go din contractele .proto.
# Necesita: protoc, protoc-gen-go, protoc-gen-go-grpc in PATH
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
set -euo pipefail
cd "$(dirname "$0")/.."
MOD=github.com/DymaMcl/PAD_labs_2026/laborator_2
protoc -I proto \
  --go_out=. --go_opt=module=$MOD \
  --go-grpc_out=. --go-grpc_opt=module=$MOD \
  accounts/v1/accounts.proto accounts/v2/accounts.proto
protoc -I proto --go_out=. --go_opt=module=$MOD compat/v1_0.proto
echo "cod generat in gen/"

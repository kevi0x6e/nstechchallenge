#!/usr/bin/env bash
# Shared helpers for the environment scripts.
set -euo pipefail

export NS=fleet
export CTX=kind-fleet
export CONNECTOR_PREFIX=fleet-cdc
export GROUP_ID=fleet-cache-worker
export TOPICS=(fleet.public.vehicles fleet.public.positions)

kc() { kubectl --context "$CTX" -n "$NS" "$@"; }
log() { printf '\n\033[1;34m==> %s\033[0m\n' "$*"; }
ok() { printf '\033[1;32m✔ %s\033[0m\n' "$*"; }
fail() { printf '\033[1;31m✘ %s\033[0m\n' "$*" >&2; exit 1; }

psql_exec() { kc exec -i deploy/postgres -- psql -v ON_ERROR_STOP=1 -U fleet -d fleet -qAt "$@"; }
valkey_cli() { kc exec deploy/valkey -- valkey-cli "$@"; }
kafka_bin() { local tool=$1; shift; kc exec deploy/kafka -- "/opt/kafka/bin/$tool" --bootstrap-server localhost:9092 "$@"; }
connect_api() { local method=$1 path=$2; shift 2; kc exec -i deploy/connect -- curl -sS -X "$method" -H 'Content-Type: application/json' "http://localhost:8083$path" "$@"; }

# retry <attempts> <delay-seconds> <command...>
retry() {
  local attempts=$1 delay=$2; shift 2
  for ((i = 1; i <= attempts; i++)); do
    if "$@"; then return 0; fi
    sleep "$delay"
  done
  return 1
}

scale_worker() {
  kc scale deploy/cache-worker --replicas="$1" >/dev/null
  if [[ $1 == 0 ]]; then
    kc wait --for=delete pod -l app=cache-worker --timeout=90s >/dev/null 2>&1 || true
  else
    kc rollout status deploy/cache-worker --timeout=120s >/dev/null
  fi
}

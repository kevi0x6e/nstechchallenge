#!/usr/bin/env bash
# Runs a one-shot .NET tool (mock writer / verifier) as a pod inside the cluster, streams its
# output and propagates its exit code. Usage: tool.sh <pod-name> <image> <args...>
source "$(dirname "$0")/lib.sh"

name=$1 image=$2; shift 2
kc delete pod "$name" --ignore-not-found --wait=true >/dev/null

kc run "$name" --image="$image" --image-pull-policy=IfNotPresent --restart=Never \
  --overrides='{"apiVersion":"v1","spec":{"enableServiceLinks":false}}' \
  --env=PG_CONNECTION="Host=postgres;Username=fleet;Password=fleet;Database=fleet" \
  --env=VALKEY=valkey:6379 --env=KAFKA=kafka:9092 --env=GROUP_ID="$GROUP_ID" \
  -- "$@" >/dev/null

phase() { kc get pod "$name" -o jsonpath='{.status.phase}' 2>/dev/null || true; }
for _ in $(seq 1 120); do [[ $(phase) == Pending || -z $(phase) ]] || break; sleep 1; done
kc logs -f "$name" || true
for _ in $(seq 1 60); do [[ $(phase) == Running ]] || break; sleep 1; done

result=$(phase)
kc delete pod "$name" --ignore-not-found --wait=false >/dev/null
[[ $result == Succeeded ]]

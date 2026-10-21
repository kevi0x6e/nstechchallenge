#!/usr/bin/env bash
# just e2e — end-to-end proof, re-runnable on the same cluster.
#
#  1. reset            clean source tables, CDC slot/publication, topics, consumer group and cache
#  2. snapshot seed    rows that exist *before* the connector (captured by the Debezium snapshot)
#  3. connector        register Debezium (snapshot → streaming)
#  4. chaos stream     both writers concurrently (late business times, duplicate inserts, deletes,
#                      re-inserts, positions before the vehicle exists) while we restart the
#                      connector (re-emitted events) and kill the worker (re-delivered offsets)
#  5. oracle assert    cache == view computed from the DB (only the test reads the DB)
#  6. replay (live)    rewind the worker to offset 0 over the populated cache   → identical digest
#  7. replay (rebuild) FLUSHALL + rewind to offset 0                            → identical digest
#
# `e2e.sh replay-only` runs steps 6–7 against the current state.
source "$(dirname "$0")/lib.sh"
cd "$(dirname "$0")/.."

SEED=${SEED:-42}
TOOL=scripts/tool.sh

reset_offsets_to_earliest() {
  local args=()
  for t in "${TOPICS[@]}"; do args+=(--topic "$t"); done
  kafka_bin kafka-consumer-groups.sh --group "$GROUP_ID" --reset-offsets --to-earliest --execute "${args[@]}" >/dev/null
}

replay_checks() {
  log "digest of the converged cache"
  local out digest
  out=$($TOOL e2e-digest fleet/e2e:dev digest) || { echo "$out"; fail "digest"; }
  echo "$out" | tail -1
  digest=$(echo "$out" | awk '/^DIGEST /{print $2}')

  log "replay from offset 0 over the live cache (every event is now stale → no-op)"
  scale_worker 0
  reset_offsets_to_earliest
  scale_worker 1
  $TOOL e2e-replay-live fleet/e2e:dev digest --expect "$digest" | tail -1 || fail "replay over live cache changed the view"
  ok "replay over live cache: identical"

  log "rebuild: FLUSHALL + replay from offset 0"
  scale_worker 0
  valkey_cli FLUSHALL >/dev/null
  [[ $(valkey_cli DBSIZE) == 0 ]] || fail "cache not empty"
  reset_offsets_to_earliest
  scale_worker 1
  $TOOL e2e-rebuild fleet/e2e:dev digest --expect "$digest" | tail -1 || fail "rebuilt cache differs"
  ok "replay-rebuild from offset 0: identical cache ($digest)"
}

if [[ ${1:-} == replay-only ]]; then
  replay_checks
  exit 0
fi

log "1. reset"
scale_worker 0
for c in $(connect_api GET /connectors | jq -r '.[]'); do
  connect_api DELETE "/connectors/$c" >/dev/null
done
# The slot stays active for a moment after the connector task stops.
retry 30 1 psql_exec -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name = 'fleet_cache'" >/dev/null 2>&1
retry 30 1 bash -c "[[ \$(kubectl --context $CTX -n $NS exec deploy/postgres -- psql -U fleet -d fleet -qAt -c \"SELECT count(*) FROM pg_replication_slots WHERE slot_name='fleet_cache'\") == 0 ]]"
psql_exec -c "SET client_min_messages = warning; DROP PUBLICATION IF EXISTS fleet_cache" >/dev/null
psql_exec -f /docker-entrypoint-initdb.d/01-schema.sql >/dev/null
kafka_bin kafka-topics.sh --delete --if-exists --topic 'fleet\.public\..*' >/dev/null 2>&1 || true
retry 30 1 bash -c "! kubectl --context $CTX -n $NS exec deploy/kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q '^fleet\.public\.'"
kafka_bin kafka-consumer-groups.sh --delete --group "$GROUP_ID" >/dev/null 2>&1 || true
valkey_cli FLUSHALL >/dev/null
ok "clean slate"

log "2. snapshot seed (pre-connector rows)"
$TOOL writer-snapshot fleet/mock-writers:dev snapshot --seed "$SEED"

log "3. register Debezium connector"
connector="$CONNECTOR_PREFIX-$(date +%s)" # fresh name ⇒ no stored source offsets ⇒ initial snapshot
connect_api PUT "/connectors/$connector/config" --data @- <k8s/connector/fleet-cdc.json >/dev/null
retry 60 2 bash -c "kubectl --context $CTX -n $NS exec deploy/connect -- curl -s localhost:8083/connectors/$connector/status | jq -e '.tasks[0].state == \"RUNNING\"' >/dev/null"
ok "connector $connector running"

log "4. chaos stream: writers + connector restart + worker kill"
scale_worker 1
$TOOL writer-stream fleet/mock-writers:dev stream --seed "$SEED" --positions-rate 300 --vehicles-rate 30 &
writers=$!
sleep 4
connect_api POST "/connectors/$connector/restart?includeTasks=true" >/dev/null
echo "   restarted Debezium connector (re-emits since its last flushed offset)"
sleep 3
kc delete pod -l app=cache-worker --wait=false >/dev/null
echo "   killed the cache worker mid-stream (uncommitted offsets get re-delivered)"
wait "$writers" || fail "stream writers failed"

log "5. assert cache == DB oracle"
$TOOL e2e-assert fleet/e2e:dev assert --seed "$SEED" || fail "cache does not match the database"
ok "cache matches the source of truth"
echo "   worker totals since its restart (stale = duplicates/late events rejected by the version guard):"
kc logs deploy/cache-worker | grep totals | tail -2 | sed 's/^/     /' || true

log "6-7. replay checks"
replay_checks

printf '\n\033[1;32mE2E PASSED\033[0m: snapshot→stream, duplicates, disorder, deletes, cross-stream race, restarts, replay-rebuild.\n'

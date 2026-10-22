#!/usr/bin/env bash
# just up: kind cluster + images + manifests, then wait until everything is Ready.
source "$(dirname "$0")/lib.sh"

for tool in docker kind kubectl devspace; do
  command -v "$tool" >/dev/null || fail "$tool not found: run inside 'devenv shell' (see README)"
done
docker info >/dev/null 2>&1 || fail "Docker is not running"

log "kind cluster"
if ! kind get clusters 2>/dev/null | grep -qx fleet; then
  kind create cluster --config k8s/kind-config.yaml --wait 120s
fi
kubectl --context "$CTX" create namespace "$NS" --dry-run=client -o yaml | kubectl --context "$CTX" apply -f - >/dev/null

log "build images + deploy (devspace)"
devspace deploy --kube-context "$CTX" --namespace "$NS" --skip-push-local-kube

# Belt and braces: make sure the freshly built tags are on the kind node.
for image in fleet/cache-worker:dev fleet/mock-writers:dev fleet/e2e:dev; do
  kind load docker-image "$image" --name fleet >/dev/null
done

log "waiting for rollouts"
for deploy in postgres kafka valkey connect cache-worker; do
  kc rollout status "deploy/$deploy" --timeout=300s
done
ok "environment is up (just status / just e2e)"

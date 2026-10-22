# Fleet CDC vehicle cache — every workflow is one command.
# Tools come from devenv: outside `devenv shell`, recipes re-enter it automatically when devenv is installed.

set shell := ["bash", "-euo", "pipefail", "-c"]

env := if env_var_or_default("DEVENV_ROOT", "") != "" { "" } else { `command -v devenv >/dev/null 2>&1 && echo "devenv shell -- " || true` }

# List recipes
default:
    @just --list

# Create the kind cluster (if needed) and deploy Postgres, Kafka, Debezium, Valkey and the worker
up:
    {{env}}scripts/up.sh

# Full proof: chaos run (duplicates, disorder, deletes, restarts) + DB oracle + replay-rebuild digests
e2e: up
    {{env}}scripts/e2e.sh

# Unit + Valkey integration tests (Testcontainers, needs Docker)
test:
    {{env}}dotnet test Fleet.slnx

# Read one vehicle view as a tracking UI would
read id:
    {{env}}scripts/tool.sh read-{{id}} fleet/e2e:dev read --id {{id}}

# Re-run only the replay-rebuild check against the current state
replay:
    {{env}}scripts/e2e.sh replay-only

# Follow the worker logs
logs:
    {{env}}kubectl --context kind-fleet -n fleet logs deploy/cache-worker -f

# Pods, services and connector status
status:
    {{env}}scripts/status.sh

# Delete the kind cluster
down:
    {{env}}kind delete cluster --name fleet

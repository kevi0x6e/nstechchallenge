#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
kc get pods,svc
echo
connect_api GET "/connectors?expand=status" | jq -r 'to_entries[] | "\(.key): \(.value.status.connector.state) tasks=\([.value.status.tasks[].state] | join(","))"' || true

#!/usr/bin/env bash

set -euo pipefail

for i in $(seq 101 120); do
  IP="2.2.2.$i"
  echo "=== killing on $IP ==="
  ssh navy@$IP 'pkill -f "worker.jar" || echo "no worker.jar"'
done
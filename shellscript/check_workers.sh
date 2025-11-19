#!/usr/bin/env bash

set -euo pipefail

for i in $(seq 101 120); do
  IP="2.2.2.$i"
  echo "=== $IP ==="
  ssh navy@$IP 'pgrep -af "worker.jar" || echo "no worker.jar"'
done
#!/usr/bin/env bash

set -euo pipefail

MASTER_IP="2.2.2.100"   # 마스터 고정 IP
MASTER_PORT=9000        # Master gRPC 포트 (네 Master.scala에서 쓰는 값)
WORKER_PORT=50051       # 워커 gRPC 포트 (전부 같은 포트로 띄운다고 가정)
USER="navy"             # VM에 접속하는 계정 이름

# 2.2.2.101 ~ 2.2.2.120 까지 워커라고 가정
for i in $(seq 101 120); do
  IP="2.2.2.$i"
  echo "=== Start worker on $IP ==="

  ssh -o StrictHostKeyChecking=no ${USER}@$IP \
    "LC_ALL=C LANG=C \
     MASTER_IP=$MASTER_IP MASTER_PORT=$MASTER_PORT \
     SELF_IP=$IP WORKER_PORT=$WORKER_PORT \
     nohup java -jar ~/worker.jar >> worker.log 2>&1 &"
done


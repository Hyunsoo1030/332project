#!/usr/bin/env bash

set -euo pipefail

export LC_ALL=C
export LANG=C


MASTER_IP="2.2.2.254"
MASTER_PORT=9000
WORKER_PORT=50051

# 이 스크립트가 있는 위치 기준으로 상위 폴더(332project) 구하기
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.."; pwd)"

# JAR 절대경로
JAR_PATH="$REPO_ROOT/worker/target/scala-2.13/worker_2.13-0.1.0-SNAPSHOT.jar"
REMOTE_JAR_NAME="worker.jar"

# gensort 경로
GENSORT_PATH="$REPO_ROOT/gensort"

# JAR 존재 확인
if [[ ! -f "$JAR_PATH" ]]; then
  echo "JAR not found: $JAR_PATH"
  exit 1
fi

for i in $(seq 101 120); do
  IP="2.2.2.$i"
  echo "=== Deploy to $IP ==="

  # 1) JAR 복사
  scp "$JAR_PATH" navy@$IP:~/$REMOTE_JAR_NAME

  # +) gensort 파일 생성 및 배포
      OUTPUT_FILE="$GENSORT_DIR/testinput_$i"
      echo "=== Generating data file for $IP: $OUTPUT_FILE ==="
      (cd "$GENSORT_DIR" && ./gensort -a 100000 "$OUTPUT_FILE")
      scp "$OUTPUT_FILE" navy@$IP:~/

  sbt "master/run"

  # 2) 원격에서 워커 실행
  ssh navy@$IP \
    "MASTER_IP=$MASTER_IP MASTER_PORT=$MASTER_PORT \
     WORKER_IP=$IP WORKER_PORT=$WORKER_PORT \
     nohup java -jar ~/$REMOTE_JAR_NAME > worker.log 2>&1 &"
done


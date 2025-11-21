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
# 총 데이터 파일 생성
ALL_INPUT="$GENSORT_PATH/all_input"
(cd "$GENSORT_PATH" && ./gensort -a 2000000 "$ALL_INPUT")
# 데이터 분할
(cd "$GENSORT_PATH" && split -l 100000 "$ALL_INPUT" chunk_)
echo "=== Complete splitting big input file into 100000-line chunks ==="

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

  # +) 데이터 chunk 각 워커에 보내기
  idx=$((i - 101))
  # 알파벳 두 글자(aa, ab, ...)로 split chunk명 구함
  first=$((idx / 26))
  second=$((idx % 26))
  letters=abcdefghijklmnopqrstuvwxyz
  CHUNK_SUFFIX="${letters:$first:1}${letters:$second:1}"
  CHUNK_FILE="chunk_$CHUNK_SUFFIX"
  FULL_CHUNK_PATH="$GENSORT_PATH/$CHUNK_FILE"

  # chunk 존재 체크
  if [[ ! -f "$FULL_CHUNK_PATH" ]]; then
    echo "Error: chunk file not found: $FULL_CHUNK_PATH"
    exit 1
  fi

  scp "$FULL_CHUNK_PATH" navy@$IP:~/$CHUNK_FILE

  #sbt "master/run"

  # 2) 원격에서 워커 실행
  ssh navy@$IP \
    "MASTER_IP=$MASTER_IP MASTER_PORT=$MASTER_PORT \
     WORKER_IP=$IP WORKER_PORT=$WORKER_PORT \
     nohup java -jar ~/$REMOTE_JAR_NAME > worker.log 2>&1 &"
done


#!/usr/bin/env bash
set -e

HOST=$1
PORT=$2
NAME=$3
TIMEOUT=60

echo "Waiting for $NAME at $HOST:$PORT..."

for i in $(seq 1 $TIMEOUT); do
  if nc -z "$HOST" "$PORT"; then
    echo "$NAME is ready"
    exit 0
  fi
  sleep 1
done

echo "Timeout waiting for $NAME"
exit 1

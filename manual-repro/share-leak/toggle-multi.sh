#!/usr/bin/env bash
# Rapidly flips leadership for ALL 24 partitions of leak-test-multi between broker 2 and 3
# simultaneously, via alternating reassignment + a single bulk preferred election. Broker 1
# (the controller) is never touched.
set -u
KAFKA_DIR=/Users/apoorvmittal/workspace/prod/kafka
SCRATCH=/Users/apoorvmittal/workspace/prod/kafka/manual-repro/share-leak
CYCLES=${1:-300}
SLEEP=${2:-0.3}

for i in $(seq 1 "$CYCLES"); do
  if [ $((i % 2)) -eq 0 ]; then
    f="$SCRATCH/state-a.json"
  else
    f="$SCRATCH/state-b.json"
  fi
  "$KAFKA_DIR/bin/kafka-reassign-partitions.sh" --bootstrap-server localhost:9092 \
    --reassignment-json-file "$f" --execute >/dev/null 2>&1
  "$KAFKA_DIR/bin/kafka-leader-election.sh" --bootstrap-server localhost:9092 \
    --all-topic-partitions --election-type preferred >/dev/null 2>&1
  if [ $((i % 20)) -eq 0 ]; then
    echo "[$i] $(date +%T) cycle done"
  fi
  sleep "$SLEEP"
done
echo "MULTI TOGGLE LOOP DONE"

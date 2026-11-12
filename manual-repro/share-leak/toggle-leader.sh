#!/usr/bin/env bash
# Rapidly toggles leak-test-0's leader between broker 2 and broker 3 via replica reassignment +
# preferred leader election, WITHOUT ever killing/disconnecting a broker. The goal is to catch the
# client with a fetch request in flight to the old leader right as leadership flips.
set -u
KAFKA_DIR=/Users/apoorvmittal/workspace/prod/kafka
SCRATCH=/Users/apoorvmittal/workspace/prod/kafka/manual-repro/share-leak
CYCLES=${1:-100}
SLEEP=${2:-0.5}

for i in $(seq 1 "$CYCLES"); do
  if [ $((i % 2)) -eq 0 ]; then
    order="[3,2,1]"
  else
    order="[2,3,1]"
  fi
  echo "{\"version\":1,\"partitions\":[{\"topic\":\"leak-test\",\"partition\":0,\"replicas\":$order}]}" \
    > "$SCRATCH/reassign-toggle.json"
  "$KAFKA_DIR/bin/kafka-reassign-partitions.sh" --bootstrap-server localhost:9092 \
    --reassignment-json-file "$SCRATCH/reassign-toggle.json" --execute >/dev/null 2>&1
  "$KAFKA_DIR/bin/kafka-leader-election.sh" --bootstrap-server localhost:9092 \
    --topic leak-test --partition 0 --election-type preferred >/dev/null 2>&1
  if [ $((i % 10)) -eq 0 ]; then
    leader=$("$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic leak-test \
      | grep -o 'Leader: [0-9]*' | awk '{print $2}')
    echo "[$i] $(date +%T) leader=$leader"
  fi
  sleep "$SLEEP"
done
echo "TOGGLE LOOP DONE"

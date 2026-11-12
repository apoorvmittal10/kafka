#!/usr/bin/env bash
# Repeatedly kills whichever broker (2 or 3) currently holds leadership for leak-test-0,
# waits for failover + restart, and repeats. Never touches broker 1 (the sole controller).
set -u
KAFKA_DIR=/Users/apoorvmittal/workspace/prod/kafka
SCRATCH=/Users/apoorvmittal/workspace/prod/kafka/manual-repro/share-leak
CYCLES=${1:-20}

pid_for_broker() {
  ps aux | grep "server-$1.properties" | grep -v grep | awk '{print $2}'
}

current_leader() {
  "$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --describe --topic leak-test \
    | grep -o 'Leader: [0-9]*' | awk '{print $2}'
}

for i in $(seq 1 "$CYCLES"); do
  leader=$(current_leader)
  if [ "$leader" != "2" ] && [ "$leader" != "3" ]; then
    echo "[$i] leader is $leader (not 2/3, skipping kill this cycle)"
    sleep 2
    continue
  fi
  pid=$(pid_for_broker "$leader")
  echo "[$i] $(date +%T) killing broker $leader (pid $pid), current leader"
  kill -9 "$pid" 2>/dev/null
  sleep 1
  echo "[$i] $(date +%T) restarting broker $leader"
  nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$SCRATCH/config/server-$leader.properties" \
    >> "$SCRATCH/logs/server-$leader.out" 2>&1 &
  disown
  sleep 4
  newleader=$(current_leader)
  echo "[$i] $(date +%T) leader is now $newleader"
done

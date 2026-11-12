# Share-consumer merge-leak: manual repro setup

This documents a manual (non-JUnit) attempt to reproduce the `ShareInFlightBatch` merge-leak bug live
against a real cluster, by rapidly changing partition leadership under a running share consumer. It
**never actually triggered the leak** — see "Outcome" at the bottom. The reliable proof of the bug and
its fix is the unit-level regression tests in this repo (`ShareInFlightBatchTest`,
`ShareFetchCollectorTest`), not this setup. It's preserved here in case someone wants to try further,
or try a different reproduction angle.

**Update:** an in-process `@ClusterTest` (`ShareConsumerTest.
testRapidLeaderChangesDoNotLeakShareCompletedFetch`) attempting the same idea via rapid in-process
`Admin.electLeaders` calls was added, investigated, and then **removed** — it passed whether the fix
was present or not (confirmed by disabling the fix and rerunning it 3+ times), so it provided no real
regression coverage. In its place, a **deterministic, mock-network-level** reproduction was added at
`clients/src/test/java/org/apache/kafka/clients/consumer/internals/ShareConsumeRequestManagerTest.
testSecondFetchForSamePartitionBeforeCollectDoesNotLeak`. It doesn't need a real cluster or an actual
leader change: it drives the real `ShareConsumeRequestManager` against a `MockClient` and exploits the
fact that `ShareConsumeRequestManager.fetch()` unconditionally re-arms `fetchMoreRecords` on every call
(exactly what happens on every `KafkaShareConsumer.poll()`) regardless of whether the previous
response for that partition has been collected yet — so calling it twice before collecting is enough

**Update 2 — live reproduction succeeded, no leader change needed at all.** Tracing
`ShareConsumerImpl.collect()` showed that `fetchCollector.collect(fetchBuffer)` (the call that drains
the buffer) is gated on `currentFetch.isEmpty() && !currentFetch.hasRenewals()`. While any record is in
the "renewing" state (`AcknowledgeType.RENEW`, the acquisition-lock-renewal acknowledgement), *every*
subsequent `poll()` call instead takes the renewals branch, which sends a brand new `ShareFetchEvent`
(triggering a real background fetch) **without ever draining the buffer**. So an application that keeps
renewing the same record instead of accepting it right away can, on its own, force multiple real
completed fetches for the same partition to stack up uncollected — no leader change, no multi-broker
cluster, no timing race to win. `ShareLeakReproConsumer.java` was updated accordingly: it renews the
first record of every batch `RENEW_CYCLES = 6` times before finally accepting it, and accepts everything
else immediately. Run against a **single broker** with the fix's `other.inFlightRecords.clear();` line
commented out, `pendingAcknowledgementFetches.size` (reflection diagnostic printed every 3s) grew
monotonically and without bound — `0 → 11 → 29 → 41 → 59 → 71 → 89 → 100 → 112 → 130 → 142 → 160 → 172 → ...`
— exactly the merge-leak signature (`isConsumed=true`, `deliveredBatch.inFlightRecords.size` 1-2,
never clearing). Restoring the fix line and restarting the consumer (fresh group) stopped the growth.
This is now the confirmed, reliable, minimal live reproduction of the bug — see "Live reproduction via
acquisition-lock renewal" below for the exact steps.

The original leader-toggling strategy documented below is kept for historical reference; it never
worked and is superseded by the renewal-based approach.
to get two completed fetches for the same partition queued at once, which is exactly the precondition
`ShareFetch.add()`'s merge branch needs. Verified: fails 100% reproducibly with the fix disabled, passes
with it enabled. This is now the real regression guard for this bug, alongside `ShareInFlightBatchTest`
and `ShareFetchCollectorTest`.

All manual test processes from this setup have been **torn down** (brokers, producers, and the custom
consumer killed). This directory holds everything needed to stand it back up.

## The bug being chased

`ShareFetch.add()` merges a second `ShareInFlightBatch` for the same partition into the first one when
two share-fetch responses for that partition complete within the same `poll()` cycle (this can happen
around a partition leader change). Before the fix, `ShareInFlightBatch.merge()` copied the second
batch's records into the first but never cleared them from the second batch's own map. The
`ShareCompletedFetch` that owned the second batch still thought it had pending records forever,
because `hasPendingAcknowledgements()` checks `deliveredBatch.numRecords() > 0` — so that
`ShareCompletedFetch` object sat permanently in `ShareFetchBuffer.pendingAcknowledgementFetches`, one
leaked object per occurrence. That's the client-side memory leak seen in production heap dumps.

The fix (already applied, in `clients/src/main/java/org/apache/kafka/clients/consumer/internals/ShareInFlightBatch.java`):
```java
public void merge(ShareInFlightBatch<K, V> other) {
    inFlightRecords.putAll(other.inFlightRecords);
    other.inFlightRecords.clear();   // <-- the fix
    if (other.checkForRenewAcknowledgements) {
        checkForRenewAcknowledgements = true;
    }
}
```
This setup tries to force the "two fetches for the same partition complete in the same poll cycle"
condition live, by changing partition leadership rapidly while a consumer polls continuously. (This
approach never worked — see Update 2 above and "Live reproduction via acquisition-lock renewal" below
for the method that does.)

## Live reproduction via acquisition-lock renewal (the method that actually works)

No leader change, no multi-broker cluster, no timing race — just one broker and an app that renews
records instead of accepting them right away.

```bash
cd /Users/apoorvmittal/workspace/prod/kafka
REPRO=manual-repro/share-leak

# 1. Single-broker config with internal-topic replication factors turned down to 1
#    (config/server-single.properties — a copy of server-1.properties with offsets.topic.replication.factor,
#    share.coordinator.state.topic.replication.factor/min.isr and transaction.state.log.replication.factor/min.isr
#    all set to 1, since only one broker is running).
CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format --feature share.version=2 -t "$CLUSTER_ID" -c "$REPRO/config/server-single.properties"
mkdir -p "$REPRO/logs"
nohup bin/kafka-server-start.sh "$REPRO/config/server-single.properties" >> "$REPRO/logs/server-single.out" 2>&1 &
disown

# 2. Topic + a slow continuous producer (throughput doesn't matter much; low keeps logs readable)
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic leak-test --partitions 1 --replication-factor 1
nohup bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance \
    --topic leak-test --num-records 10000000 --throughput 5 --record-size 50 \
    --producer-props bootstrap.servers=localhost:9092 acks=1 >> "$REPRO/logs/producer.out" 2>&1 &
disown

# 3. Build the consumer against this repo's own trunk client
./gradlew clients:jar
# Must be a real slf4j-api jar under modules-2/files-2.1, NOT a -sources.jar (no compiled classes in
# those) and NOT one under gradle's jars-9 cache (those are Gradle-instrumented and reference
# org.gradle.internal.classpath.Instrumented, which won't resolve outside Gradle's own classloader).
SLF4J_JAR=$(find ~/.gradle/caches/modules-2 -iname "slf4j-api-*.jar" ! -iname "*sources*" | head -1)
CP="$REPRO/build:clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar:$SLF4J_JAR"
mkdir -p "$REPRO/build"
javac -cp "$CP" -d "$REPRO/build" "$REPRO/ShareLeakReproConsumer.java"

# 4. Run it. It renews the first record of every batch it sees RENEW_CYCLES (6) times before
#    accepting it, accepts everything else immediately, and every 3s prints a heartbeat plus a
#    reflection-based dump of ShareFetchBuffer.pendingAcknowledgementFetches.size (and each entry's
#    partition/isConsumed/deliveredBatch.inFlightRecords.size).
nohup java -cp "$CP" ShareLeakReproConsumer localhost:9092 leak-test leak-test-group >> "$REPRO/logs/consumer.out" 2>&1 &
disown
```

To see the leak, temporarily comment out the fix line in `ShareInFlightBatch.merge()`
(`other.inFlightRecords.clear();`), rebuild (`./gradlew clients:jar`), and restart just the consumer
(new group id, since the old one's offsets/state don't matter). Watch the diagnostic output:
```bash
tail -f manual-repro/share-leak/logs/consumer.out | grep pendingAcknowledgementFetches.size
```
With the fix disabled this was observed to grow monotonically and without bound (`0 → 11 → 29 → 41 →
59 → 71 → 89 → 100 → 112 → 130 → 142 → 160 → 172 → ...`), every entry showing `isConsumed=true` and a
non-zero, non-clearing `deliveredBatch.inFlightRecords.size` (1-2) — the exact merge-leak signature.
Restoring the fix line, rebuilding, and restarting the consumer stopped the growth immediately.

Why this works: `ShareConsumerImpl.collect()` only calls `fetchCollector.collect(fetchBuffer)` (the
call that actually drains the buffer) once `currentFetch.isEmpty() && !currentFetch.hasRenewals()`.
While a record is being renewed, every `poll()` call instead takes the "renewals" branch, which sends a
brand new `ShareFetchEvent` (triggering a real fetch to the broker) *without draining the buffer at
all*. Renew the same record enough times in a row and multiple real, distinct completed fetches for
that partition pile up uncollected — then accepting it lets `collect()` run and merge them all at once.

## Directory layout

```
manual-repro/share-leak/
├── README.md                     (this file)
├── config/
│   ├── server-1.properties       broker 1 (controller + broker)
│   ├── server-2.properties       broker 2
│   └── server-3.properties       broker 3
├── ShareLeakReproConsumer.java    custom minimal share-consumer app
├── log4j2-debug.properties       log4j2 config for running the app (root=INFO, org.apache.kafka=DEBUG)
├── toggle-leader.sh              leader-toggle strategy #1 (single partition)
├── toggle-multi.sh               leader-toggle strategy #2 (24 partitions at once)
├── force-failovers.sh            leader-toggle strategy #3 (hard-kills the current leader)
├── state-a.json / state-b.json   reassignment files used by toggle-multi.sh
└── data/, logs/, cekafka-build/  created at runtime (gitignored-style scratch, not checked in)
```

## 1. Start the 3-broker KRaft cluster

```bash
cd /Users/apoorvmittal/workspace/prod/kafka
KAFKA_DIR=$(pwd)
REPRO=$KAFKA_DIR/manual-repro/share-leak

# One-time: build a cluster ID and format all 3 broker data dirs with share.version=2
CLUSTER_ID=$("$KAFKA_DIR/bin/kafka-storage.sh" random-uuid)
for n in 1 2 3; do
  "$KAFKA_DIR/bin/kafka-storage.sh" format --feature share.version=2 \
    -t "$CLUSTER_ID" -c "$REPRO/config/server-$n.properties"
done

mkdir -p "$REPRO/logs"
for n in 1 2 3; do
  nohup "$KAFKA_DIR/bin/kafka-server-start.sh" "$REPRO/config/server-$n.properties" \
    >> "$REPRO/logs/server-$n.out" 2>&1 &
  disown
done

# Confirm all 3 are up and the share.version feature is finalized
"$KAFKA_DIR/bin/kafka-features.sh" --bootstrap-server localhost:9092 describe | grep share
```

### Topology

- Broker 1: `node.id=1`, `process.roles=broker,controller`, `PLAINTEXT://:9092`, `CONTROLLER://:9093`,
  sole controller (`controller.quorum.voters=1@localhost:9093`)
- Broker 2: `node.id=2`, `process.roles=broker`, `PLAINTEXT://:9094`
- Broker 3: `node.id=3`, `process.roles=broker`, `PLAINTEXT://:9096`

Share-group settings present on all three configs:
```
share.coordinator.state.topic.replication.factor=2
share.coordinator.state.topic.min.isr=2
group.share.min.record.lock.duration.ms=1000
unstable.api.versions.enable=true
```

`server-2.properties`/`server-3.properties` are derived from a Confluent Platform template and carry a
lot of irrelevant commented-out CP-only settings (data balancer, RBAC, telemetry, etc.) — only
`node.id`, `listeners`/`advertised.listeners`, `log.dirs`, `controller.quorum.voters`, and the
share-group settings above actually matter.

### Optional: re-add the artificial fetch delay

An earlier attempt widened the race window by holding every `ShareFetch` request outstanding on the
broker for a configurable delay before processing it, via a temporary hook in
`KafkaApis.handleShareFetchRequest`:
```scala
val reproDelayMs = System.getProperty("share.fetch.repro.delay.ms", "0").toLong
if (reproDelayMs > 0) Thread.sleep(reproDelayMs)
```
**This hook has been reverted from `core/src/main/scala/kafka/server/KafkaApis.scala`** — it was
temporary instrumentation, not part of the real fix. If you want to use it again, re-add those 4 lines
at the top of `handleShareFetchRequest`, rebuild (`./gradlew core:jar` or just restart via
`kafka-server-start.sh` which recompiles via Gradle if needed), and launch broker 2 and/or 3 with
`KAFKA_OPTS="-Dshare.fetch.repro.delay.ms=4000"` before the `kafka-server-start.sh` call (that's how it
was run originally — brokers 2 and 3 delayed, broker 1 the controller left fast).

## 2. Create topics and load traffic

```bash
"$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create \
  --topic leak-test --partitions 1 --replication-factor 3

"$KAFKA_DIR/bin/kafka-topics.sh" --bootstrap-server localhost:9092 --create \
  --topic leak-test-multi --partitions 24 --replication-factor 3

# Continuous low-rate producers (run in background, one per topic)
"$KAFKA_DIR/bin/kafka-run-class.sh" org.apache.kafka.tools.ProducerPerformance \
    --topic leak-test --num-records 10000000 --throughput 20 --record-size 100 \
    --producer-props bootstrap.servers=localhost:9092,localhost:9094,localhost:9096 acks=1 &

"$KAFKA_DIR/bin/kafka-run-class.sh" org.apache.kafka.tools.ProducerPerformance \
    --topic leak-test-multi --num-records 100000000 --throughput 200 --record-size 100 \
    --producer-props bootstrap.servers=localhost:9092,localhost:9094,localhost:9096 acks=1 &
```

Share group config used: `share.auto.offset.reset=latest` (the default — not overridden), so a
consumer only sees records produced *after* it first subscribes. If you want it to read from the
beginning instead, set it before subscribing:
```bash
"$KAFKA_DIR/bin/kafka-configs.sh" --bootstrap-server localhost:9092 --entity-type groups \
  --entity-name <group-id> --alter --add-config share.auto.offset.reset=earliest
```

## 3. Build and run the custom consumer

`ShareLeakReproConsumer.java` is a minimal share consumer: polls continuously, ACKs every record
immediately (`AcknowledgeType.ACCEPT`), logs each record plus a 5s heartbeat, and refreshes metadata
aggressively (`metadata.max.age.ms=500`) so a leader change is noticed quickly:
```java
props.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");
props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "500");
ShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props);
consumer.subscribe(List.of(topic));
while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<String, String> record : records) {
        // log RECV partition=... offset=... deliveryCount=... total=...
        consumer.acknowledge(record, AcknowledgeType.ACCEPT);
    }
    // periodic HEARTBEAT log every 5s
}
```
Args: `bootstrap.servers` (default `localhost:9092,localhost:9094,localhost:9096`), `topic` (default
`leak-test`), `group.id` (default `leak-test-group`).

### Option A — build/run against Apache Kafka trunk's own client (this repo)

```bash
cd /Users/apoorvmittal/workspace/prod/kafka
./gradlew clients:jar   # produces clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar
# Must be a real jar (not -sources.jar, not one from Gradle's jars-9 cache) -- see the note in the
# "Live reproduction via acquisition-lock renewal" section above for why.
CP="clients/build/libs/kafka-clients-4.5.0-SNAPSHOT.jar:$(find ~/.gradle/caches/modules-2 -iname 'slf4j-api-*.jar' ! -iname '*sources*' | head -1)"
mkdir -p manual-repro/share-leak/build
javac -cp "$CP" -d manual-repro/share-leak/build manual-repro/share-leak/ShareLeakReproConsumer.java
java -cp "manual-repro/share-leak/build:$CP" ShareLeakReproConsumer \
    localhost:9092,localhost:9094,localhost:9096 leak-test leak-test-group
```

### Option B — build/run against the exact production client (ce-kafka tag `v8.4.0-950-0-ce`)

This is the client build that was actually running in production when the leak was observed
(commit `c8c653b91e3`). Building it requires the `ce-kafka` repo and Bazel:
```bash
cd /Users/apoorvmittal/workspace/prod/ce-kafka
git status   # make sure master is clean / nothing to lose before checking out a tag
git checkout v8.4.0-950-0-ce   # detached HEAD
bazel build //clients:clients

# Resolve the FULL transitive runtime classpath (this client pulls in ~490 jars, including
# io.confluent.telemetry / io.opentelemetry deps that aren't obvious from clients/BUILD.bazel alone —
# don't try to hand-pick jars, use bazel cquery):
REPRO=/Users/apoorvmittal/workspace/prod/kafka/manual-repro/share-leak
bazel cquery 'deps(//clients:clients)' --output=files > "$REPRO/classpath-cekafka-full.txt"

CP=$(tr '\n' ':' < "$REPRO/classpath-cekafka-full.txt")
mkdir -p "$REPRO/cekafka-build"
javac -cp "$CP" -d "$REPRO/cekafka-build" "$REPRO/ShareLeakReproConsumer.java"
java -Dlog4j2.configurationFile="file://$REPRO/log4j2-debug.properties" \
    -cp "$REPRO/cekafka-build:$CP" ShareLeakReproConsumer \
    localhost:9092,localhost:9094,localhost:9096 leak-test-multi leak-test-multi-cekafka-group

# When done, return ce-kafka to master:
cd /Users/apoorvmittal/workspace/prod/ce-kafka && git checkout master
```
`log4j2-debug.properties` sets root logging to `INFO` and `org.apache.kafka` to `DEBUG`, console only.

## 4. Trigger leader changes

Three strategies were tried, in order of increasing aggressiveness. None kill a broker except
strategy 3; the first two only reassign replicas and force a preferred-leader election, trying to
catch an in-flight fetch to the old leader right as leadership flips.

**Strategy 1 — `toggle-leader.sh <cycles> <sleepSeconds>`**: single partition (`leak-test`-0),
alternates its replica order between `[2,3,1]` and `[3,2,1]` and runs preferred-leader election each
cycle:
```bash
cd manual-repro/share-leak && ./toggle-leader.sh 100 0.5
```

**Strategy 2 — `toggle-multi.sh <cycles> <sleepSeconds>`**: all 24 partitions of `leak-test-multi` at
once, alternating between prebuilt reassignment files `state-a.json`/`state-b.json` (each flips every
partition's leader between broker 2 and 3) plus one bulk `--all-topic-partitions` preferred election
per cycle:
```bash
./toggle-multi.sh 300 0.3
```

**Strategy 3 — `force-failovers.sh <cycles>`**: the most aggressive. Finds whichever of broker 2/3
currently leads `leak-test`-0, hard-kills it (`kill -9`), waits, restarts it from the same config,
waits for the new leader, repeats. Never touches broker 1 (the controller):
```bash
./force-failovers.sh 20
```

## 5. Tearing down

```bash
pkill -f "ShareLeakReproConsumer"
pkill -f "org.apache.kafka.tools.ProducerPerformance"
pkill -f "kafka\.Kafka .*manual-repro/share-leak/config/server-"

# Wipe cluster state/logs for a clean slate next time
rm -rf /Users/apoorvmittal/workspace/prod/kafka/manual-repro/share-leak/data
rm -rf /Users/apoorvmittal/workspace/prod/kafka/manual-repro/share-leak/logs
```
(This exact teardown has already been run once — as of this writing, no repro processes are running.)

## Outcome

**Leader-toggling (original approach, kept for history): never worked.** None of the three toggling
strategies, across either client build (Apache Kafka trunk or the exact ce-kafka production tag), ever
reproduced the leak live. The structural reason: `ShareConsumeRequestManager.shareSessionLeaderMap`
(the client's per-partition leader cache) is only invalidated by an explicit
`NOT_LEADER_OR_FOLLOWER`/`FENCED_LEADER_EPOCH` error response from a fetch — not by a routine metadata
refresh — so admin-driven leader elections here didn't reliably create the exact "two fetches for the
same partition in flight at once" race the `merge()` bug needs.

An in-process JUnit integration test attempting the same idea (`ShareConsumerTest.
testRapidLeaderChangesDoNotLeakShareCompletedFetch`, using in-process `Admin.electLeaders` instead of
CLI tools for speed) also did not reliably reproduce it, and was ultimately deleted — see that test's
history for two false-positive detours (a test-harness ack-mode bug, and a case where the fix itself
had been temporarily disabled by earlier repro instrumentation and not restored).

**Acquisition-lock renewal (see the section above): works, reliably, on a single broker.** Repeatedly
renewing the same record instead of accepting it keeps `ShareConsumerImpl.collect()` on the
"renewals" branch, which sends new `ShareFetchEvent`s without ever draining the buffer — no leader
change needed. With the fix disabled, `pendingAcknowledgementFetches.size` grew monotonically and
without bound; with it restored, growth stopped immediately. This is now the confirmed live
reproduction of the bug.

The reliable proof of the bug and its fix remains the deterministic unit/mock-level tests
(`ShareInFlightBatchTest`, `ShareFetchCollectorTest`,
`ShareConsumeRequestManagerTest#testSecondFetchForSamePartitionBeforeCollectDoesNotLeak`) in
`clients/src/test/java/org/apache/kafka/clients/consumer/internals/` — the live renewal-based
reproduction above additionally corroborates them end-to-end against a real cluster and the real
public `KafkaShareConsumer` API.

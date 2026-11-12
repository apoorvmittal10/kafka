# Share Session Retention Defect — Reproduction Guide

Working notes for reproducing the Share Groups (KIP-932) session-accounting race
described in `kafka-share-partition-retention-report.txt`. Everything here has
been run against current `trunk` (commit `9e88924a177`) and verified to still
reproduce, unmodified.

**Status: live, unpatched, unmodified production code path. Do not commit
these files to any branch that syncs with a public or shared remote, and do
not run the broker-level variant against a shared/production cluster.**

## Files in this directory

| File | Purpose |
|---|---|
| `kafka-share-partition-retention-report.txt` | Root-cause write-up: the accounting/lifecycle defect. |
| `SharePartitionCacheRetentionPoC.java` | Deterministic JUnit PoC. Uses a barrier so the race fires on **every** cycle. Good for proving the mechanism and for a future regression test. In-process only — calls `newContext` directly, does not go through the network. |
| `ManualRaceDriver.java` | Probabilistic driver. Fires the same two calls with real thread-timing jitter instead of a forced barrier — closer to what happens on a live broker with two racing connections. In-process only. Prints a hit-rate instead of asserting pass/fail. |
| `RealClientRaceReproTest.scala` | Real broker, real sockets, but launched **through the JUnit `ClusterTestExtensions` test harness** — not run by hand. See Path C below. |
| `ManualRaceClient.java` | **Fully manual.** No JUnit, no test harness. A standalone class with a `main` method that talks the wire protocol directly against a broker you start yourself with the normal `bin/kafka-server-start.sh`. Run it twice from two terminals (or two backgrounded processes) with the same `--member-id` to race it by hand. See Path D below. |
| `README.md` | This file. |

## The bug, in one paragraph

`SharePartitionManager.newContext` (INITIAL_EPOCH branch,
`core/src/main/java/kafka/server/share/SharePartitionManager.java:538-561`)
calls `cache.remove(key)` and then `cache.maybeCreateSession(...)` as two
separate, independently-synchronized calls instead of one atomic unit — unlike
the subsequent-epoch branch a few lines down (`:562-589`), which wraps the
equivalent work in a single `synchronized (cache)` block. When two INITIAL
ShareFetch requests for the same `(groupId, memberId)` land close enough
together, the second `maybeCreateSession` silently orphans the first
session's bookkeeping: `numMembersPerGroup` gets incremented twice but only
decremented once on disconnect, so the group's member count never returns to
zero, `onGroupEmpty` never fires for that group, and the associated
`SharePartitionCache` entries are retained forever (no TTL exists — see
`ShareSessionCache.java`).

## Prerequisites

- This repo, checked out at or after commit `9e88924a177` (current trunk).
- JDK 23 (whatever `./gradlew` already resolves to in this checkout).
- No running broker needed for Paths A and B — both talk to the real
  production classes in-process via JUnit. Path C starts a real single-node
  KRaft broker itself via the repo's own test harness — no external setup
  required, but it's slower (~1 minute) and does real socket I/O.

## Path A — Deterministic reproduction (fastest, 100% hit rate)

This forces the race with a barrier, so every cycle leaks. Use this to prove
the mechanism quickly or to seed a regression test once the fix lands.

```bash
mkdir -p core/src/test/java/kafka/server/share
cp session-leak/SharePartitionCacheRetentionPoC.java core/src/test/java/kafka/server/share/

# sanity check it still compiles against current production code
./gradlew :core:compileTestJava -q

# run it
./gradlew :core:test --tests "kafka.server.share.SharePartitionCacheRetentionPoC"

# IMPORTANT: remove the copy afterwards, do not leave it in the tracked tree
rm core/src/test/java/kafka/server/share/SharePartitionCacheRetentionPoC.java
rmdir core/src/test/java/kafka/server/share 2>/dev/null
```

The file's constants control scale:

```java
private static final int DEFAULT_CYCLES = 10000;       // number of races to run
private static final int DEFAULT_PARTITIONS_PER_GROUP = 1;
private static final int DEFAULT_BALLAST_BYTES = 131072; // 0 = fast correctness-only run
```

**Verified results:**

| Run | Cycles | Ballast | Result |
|---|---|---|---|
| Correctness-only | 200 | 0 | 200/200 cycles leaked a phantom member; `groupEmptyCount() == 0` across all cycles; all 200 `SharePartitionCache` entries retained after both connections disconnected. Test time: 0.59s. |
| Memory quantification | 2000 | 131072 (128 KiB) | `RETAINED_ENTRIES=2000`, `USED_HEAP_AFTER_GC_BYTES=286747304` (~273 MB, after 3× forced `System.gc()`). Matches `2000 × 131072 ≈ 262 MB` plus map/mock overhead. |

To reproduce the memory run yourself, add this immediately before
`pauseForHeapInspection()` at the end of the test method:

```java
System.out.println("RETAINED_ENTRIES=" + partitionCache.size());
for (int i = 0; i < 3; i++) System.gc();
Runtime runtime = Runtime.getRuntime();
System.out.println("USED_HEAP_AFTER_GC_BYTES=" + (runtime.totalMemory() - runtime.freeMemory()));
```

and set `DEFAULT_CYCLES = 2000`, `DEFAULT_BALLAST_BYTES = 131072`, then run
with `--info` so Gradle surfaces the `System.out` lines:

```bash
./gradlew :core:test --tests "kafka.server.share.SharePartitionCacheRetentionPoC" --info \
  | grep -E "RETAINED_ENTRIES|USED_HEAP_AFTER_GC_BYTES"
```

## Path B — Manual / realistic-timing reproduction

`ManualRaceDriver.java` does **not** use a barrier. Two threads each sleep a
random 0–200µs jitter and then call `newContext` with the same
`(groupId, memberId)` on different connections — closer to how two
independent client connections would actually interleave. It prints results
instead of asserting, since some iterations will legitimately miss the race.

```bash
mkdir -p core/src/test/java/kafka/server/share
cp session-leak/ManualRaceDriver.java core/src/test/java/kafka/server/share/
./gradlew :core:compileTestJava -q
./gradlew :core:test --tests "kafka.server.share.ManualRaceDriver" --info \
  | grep -E "ITERATIONS=|LEAKED_CYCLES=|HIT_RATE_PERCENT=|GROUP_EMPTY|GROUPS_WITH|RETAINED_PARTITION_CACHE_ENTRIES="

# cleanup
rm core/src/test/java/kafka/server/share/ManualRaceDriver.java
rmdir core/src/test/java/kafka/server/share 2>/dev/null
```

**Verified output (5000 iterations, jitter window 0–200µs):**

```
ITERATIONS=5000
LEAKED_CYCLES=47
HIT_RATE_PERCENT=0.94
GROUP_EMPTY_CALLBACKS_TOTAL=7339
GROUPS_WITH_ZERO_CALLBACKS=47 (phantom member leak)
GROUPS_WITH_ONE_CALLBACK=2567 (correct behavior)
GROUPS_WITH_MULTIPLE_CALLBACKS=2386 (duplicate/premature notification)
RETAINED_PARTITION_CACHE_ENTRIES=47
```

Two distinct, confirmed behaviors here:

1. **The leak (primary defect, matches the report):** 47/5000 cycles (0.94%)
   end with zero `onGroupEmpty` calls for that group — the phantom-member
   case. `GROUPS_WITH_ZERO_CALLBACKS` always equals `LEAKED_CYCLES` exactly,
   which is a useful cross-check if you modify the driver.
2. **A second anomaly worth flagging separately (not in the original
   report):** 2386/5000 cycles (47.7%) fire `onGroupEmpty` **more than once**
   for a single session lifecycle. This wasn't asserted or investigated by
   the original analysis — it surfaced only from this realistic-timing
   driver, and the root cause hasn't been traced through a debugger, only
   inferred from reading the disconnect/removal code path. Worth a follow-up
   before it goes into any fix write-up as a confirmed second bug, since a
   duplicate/premature `onGroupEmpty` could tear down live `SharePartition`
   state for a group that isn't actually empty yet — a correctness issue,
   not just a resource leak.

Increase `ITERATIONS` or widen the jitter window in `jitter()` if you want a
higher absolute leak count; hit rate will vary run to run since it depends on
real OS thread scheduling, not a forced barrier.

## Path C — Real client, real broker, real sockets (strongest evidence)

Paths A and B both call `SharePartitionManager.newContext` directly from two
threads on one in-process manager instance. That proves the mechanism, but it
sidesteps two real questions: do two genuinely separate network connections
actually reach `newContext` concurrently, and does an attacker even need to
be a real group member to control the `memberId` involved? `RealClientRaceReproTest.scala`
answers both by starting one real single-node KRaft broker (via this repo's
own `ClusterTestExtensions` test harness) and racing two spec-compliant
`ShareFetchRequest`s over two independent TCP sockets — no in-process
shortcut, no mocked `SharePartitionManager`. It deliberately sends no
`ShareGroupHeartbeat` first, to demonstrate that no prior group membership is
required to reach the vulnerable code path.

```bash
cp session-leak/RealClientRaceReproTest.scala core/src/test/scala/unit/kafka/server/
./gradlew :core:compileTestScala -q
./gradlew :core:test --tests "kafka.server.RealClientRaceReproTest" --info \
  | grep -E "cycle=|ITERATIONS=|LEAKED_CYCLES=|HIT_RATE_PERCENT="

# cleanup
rm core/src/test/scala/unit/kafka/server/RealClientRaceReproTest.scala
```

**Verified result (80 cycles, one real broker, loopback sockets):**

```
ITERATIONS=80
LEAKED_CYCLES=68
HIT_RATE_PERCENT=85.0
```

85% — this lines up almost exactly with the original artifact's reported
84–88% hit rate against an embedded broker, and this run used real
`SocketServer`/`KafkaRequestHandlerPool`/`KafkaApis` request handling end to
end, not a simulation of it.

This run also surfaced a precise, real broker-side log line worth using as a
detection signal (see below): on every leaked cycle, the broker logs

```
ERROR Share session error for ShareSessionKey[groupId=..., memberId=...]: no such share session found (kafka.server.share.SharePartitionManager:426)
```

This comes from `SharePartitionManager.releaseSession` (`SharePartitionManager.java:416-429`),
which is invoked via `ShareGroupListenerImpl.onMemberLeave` → `cache.remove(key)`
finding nothing, because the race already replaced/removed the entry under
that key through the other connection. It fires reliably, one line per leaked
cycle, which makes it a good, cheap signal to grep for on a real cluster.

**Note on the per-test timeout:** the `ClusterTestExtensions` harness enforces
a per-test timeout (observed ~60s) that did not appear to respect this test's
class-level `@Timeout(600)` annotation — a 150-cycle run was cut off by a
`TimeoutException` partway through (it had still reached 66/75 leaked cycles,
88%, before being killed). `ITERATIONS` is set to 80 as a safe default that
finishes inside that window (~0.6s/cycle observed on loopback); raise it if
you want a larger sample, or split the loop across multiple `@ClusterTest`
methods.

## Path D — Fully manual: real broker CLI, real standalone client (no JUnit at all)

Path C still runs *through* the JUnit `ClusterTestExtensions` harness — the
harness starts the broker and drives the sockets, you never type a command
against a broker yourself. This path is different: you start the broker with
the exact same script Kafka ships (`bin/kafka-server-start.sh`), and race it
with a standalone Java class you run by hand, twice, from two terminals (or
two backgrounded shell processes). No test framework is involved anywhere.

The stock `kafka-console-share-consumer.sh` / `KafkaShareConsumer` **cannot**
be used for this: the client generates its own random `memberId` internally
(`AbstractMembershipManager.java:81`, `Uuid.randomUuid().toString()`) with no
public config to override it, so there is no supported way to make two stock
consumer instances share one `memberId`. `ManualRaceClient.java` talks the
wire protocol directly instead, which requires nothing exotic: `MemberId` is
just a plain client-supplied string on the wire (`ShareFetchRequest.json`),
never assigned or validated by the broker — so controlling it is simply a
matter of setting that one field ourselves when building the request.

### 1. Start a real broker the normal way

```bash
# format storage (once)
UUID=$(bin/kafka-storage.sh random-uuid)
bin/kafka-storage.sh format -t "$UUID" -c config/server.properties --standalone

# start it backgrounded with output going to a file -- lets you grep the log
# from the same shell instead of needing a separate terminal to watch it in.
# Add JMX_PORT=9999 in front if you also want the JMX verification further down.
bin/kafka-server-start.sh config/server.properties > /tmp/broker.log 2>&1 &

# (or just run it in the foreground in its own terminal if you'd rather watch
# it live: `bin/kafka-server-start.sh config/server.properties`)
```

Share groups are on by default — no extra config needed (confirmed via the
bootstrap metadata: `share.version` feature level 2 is formatted in
automatically).

### 2. Create a topic and get its real topic ID

```bash
bin/kafka-topics.sh --create --topic race-topic --partitions 1 --replication-factor 1 \
  --bootstrap-server localhost:9092

bin/kafka-topics.sh --describe --topic race-topic --bootstrap-server localhost:9092
# Topic: race-topic  TopicId: GWeqVbARRT6q2SuMg_TjQQ  PartitionCount: 1 ...
```

`ShareFetchRequest` addresses partitions by `(topicId, partition)`, not by
name, so you need that `TopicId` value for the next step.

### 3. Compile the standalone client

It only needs classes already in `clients`' production jar — no test
dependencies. Reuse the same classpath `bin/kafka-run-class.sh` already
knows how to build:

```bash
CP=$(bash -x bin/kafka-run-class.sh org.apache.kafka.tools.consumer.ConsoleShareConsumer --version 2>&1 \
  | grep -m1 "exec java" | sed -E "s/^\+ exec java.*-cp '([^']*)'.*/\1/")

mkdir -p /tmp/manual-race-classes
javac -cp "$CP" -d /tmp/manual-race-classes session-leak/ManualRaceClient.java
```

### 4. Race it — two real, independent processes, one command

You don't need two terminals — launch both as backgrounded processes from the
same shell with `&`, then `wait` on both. They're still two genuinely
separate OS processes and two separate TCP connections; only the shell
invocation is single. Generate a shared member id first, in Kafka's own
`Uuid` format (not a standard Java UUID — `Uuid.fromString` uses a base64
encoding, so use `Uuid.randomUuid()`):

```bash
MEMBER_ID=$(echo 'System.out.println(org.apache.kafka.common.Uuid.randomUuid().toString());' \
  | jshell --class-path "$CP" -q -)
GROUP_ID="race-group-1"
TOPIC_ID="GWeqVbARRT6q2SuMg_TjQQ"   # from step 2's --describe output

java -cp "/tmp/manual-race-classes:$CP" ManualRaceClient localhost 9092 "$GROUP_ID" "$MEMBER_ID" "$TOPIC_ID" race-topic 0 &
java -cp "/tmp/manual-race-classes:$CP" ManualRaceClient localhost 9092 "$GROUP_ID" "$MEMBER_ID" "$TOPIC_ID" race-topic 0 &
wait
```

Each prints its own request/response and exits (closing its connection).
Then check the broker's own terminal/log — a leaked race prints one of:

```
ERROR Share session error for ShareSessionKey[groupId=race-group-1, memberId=<the shared member id>]: no such share session found (kafka.server.share.SharePartitionManager)
ERROR No share partition found for groupId race-group-1 topicPartition <topicId>:race-topic-0 while releasing acquired topic partitions (kafka.server.share.SharePartitionManager)
```

If you want to keep retrying automatically instead of running the block
above by hand each time (see the hit-rate caveat further down — a single
attempt has roughly a 1-2% chance of landing in this environment), loop it
with a fresh group/member id per attempt and grep the broker log after each.
This requires the broker's own output going to a file so you can grep it —
start it with `bin/kafka-server-start.sh config/server.properties > /tmp/broker.log 2>&1 &`
in step 1 instead of running it in the foreground, then:

```bash
for i in $(seq 1 100); do
  MEMBER_ID=$(echo 'System.out.println(org.apache.kafka.common.Uuid.randomUuid().toString());' \
    | jshell --class-path "$CP" -q -)
  GROUP_ID="race-loop-$i"

  java -cp "/tmp/manual-race-classes:$CP" ManualRaceClient localhost 9092 "$GROUP_ID" "$MEMBER_ID" "$TOPIC_ID" race-topic 0 > /tmp/race-a.log 2>&1 &
  java -cp "/tmp/manual-race-classes:$CP" ManualRaceClient localhost 9092 "$GROUP_ID" "$MEMBER_ID" "$TOPIC_ID" race-topic 0 > /tmp/race-b.log 2>&1 &
  wait

  if grep -q "groupId=$GROUP_ID," /tmp/broker.log 2>/dev/null; then
    echo ">>> LEAKED at attempt $i (group=$GROUP_ID, member=$MEMBER_ID)"
    break
  fi
done
```

This is exactly the pattern used to produce the verified results below: the
very first manual attempt (two `java` processes launched via `&`/`wait` from
one script, functionally identical to two terminals) produced exactly one of
those log lines on the first try, against a broker started with the plain
`bin/kafka-server-start.sh` command from step 1 — proof this is not an
artifact of any test harness, and proof you don't need two terminals to get
two genuinely independent, racing OS processes.

### Honest hit-rate caveat — read this before assuming it'll fire instantly

Getting a *repeat* hit this way turned out to be much harder than Paths A–C.
After the first success, a further **65 racing attempts in this session
(including 12-way and 40-way concurrent swarms against the same `groupId`/
`memberId`, and a variant that pre-connects both processes and synchronizes
their send instant via a shared timestamp) produced zero additional hits** —
roughly a 1.5% overall hit rate for genuinely separate OS processes, in this
environment. That's lower than even the original artifact's own reported 25%
for real production traffic over the public internet, and far below the 85%
this repo's own Path C achieves running client and broker in one JVM.

The likely reason: Paths A–C all keep the racing client-side calls within one
JVM (either calling `newContext` directly, or two executor threads sending
sockets), so the actual send instants are only microseconds apart. Two
genuinely independent OS processes (plus, here, a third independent broker
process) add scheduling jitter — JVM startup, OS thread scheduling, GC
pauses — that is very likely orders of magnitude larger than the actual
danger window inside `newContext` (the gap between one `synchronized`
`cache.remove()` call returning and the next `synchronized`
`cache.maybeCreateSession()` call starting, which is only a few lines of
code). This is consistent with, not contradictory to, the original report:
production/cross-network hit rates are meaningfully lower than same-JVM test
hit rates, and this data point is just further along that same curve.

**Practical takeaway:** if you want to *watch it happen live in your own
terminal* at least once, Path D does that, and it worked on the first try
here. If you want a *reliable, repeatable* demonstration (for a demo, or to
verify a fix), use Path C — it's still a real broker and real protocol
requests, just orchestrated by JUnit instead of by hand. Loop Path D's two
commands in a shell `for` loop with a fresh `groupId`/`memberId` each time
and grep the broker log if you want to keep trying by hand; expect to need
many more than two attempts.

### Confirming the leak directly in `ShareSessionCache` via JMX (not just the log line)

The broker log line is suggestive but indirect. `ShareSessionCache` itself
exposes two real JMX gauges (`server/.../ShareSessionCache.java:84-86`):
`ShareSessionsCount` (= `cache.size()`, live sessions) and
`SharePartitionsCount` (= `cache.totalPartitions()`, the counter that a leak
corrupts). Reading these from *outside* the broker process — no reflection,
no in-JVM access, genuinely external — is the strongest evidence available
against a real, separate broker process, and it directly answers "is
`ShareSessionCache` itself actually leaked" rather than inferring it from a
log message.

**Enable JMX when starting the broker** (add `JMX_PORT=9999` before the
command from step 1):

```bash
JMX_PORT=9999 bin/kafka-server-start.sh config/server.properties
```

**Poll the gauge** (one-shot; `kafka.tools.JmxTool` moved package —
it's `org.apache.kafka.tools.JmxTool` on current trunk):

```bash
bin/kafka-run-class.sh org.apache.kafka.tools.JmxTool \
  --object-name "kafka.server:type=ShareSessionCache,name=SharePartitionsCount" \
  --jmx-url "service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi" \
  --one-time true
```

**Verified result from this session:** baseline was `0`. Racing
`ManualRaceClient` in a loop (fresh `groupId`/`memberId` each cycle, exactly
as in step 4) hit a leak on the **3rd cycle** — `SharePartitionsCount` jumped
to `1` and **stayed at `1`** on every subsequent poll (confirmed non-transient
by re-polling, and unaffected by two further clean cycles run afterward: it
never returned to `0` and never incremented further on the clean cycles
either — exactly the expected signature). At the same moment,
`ShareSessionsCount` read `0` — i.e., **zero live sessions, but a permanently
retained partition count** — precisely the "sessions.size() returns to zero,
`numPartitions`/`numMembersPerGroup` do not" signature described in
`kafka-share-partition-retention-report.txt`, now confirmed via a real,
external, black-box metric rather than an inference from a log line or
in-process reflection.

That same leaked cycle also logged a second, previously-unseen-in-this-repo
variant of the detection signal — worth noting since it's a different code
path than the one documented earlier:

```
ERROR No share partition found for groupId manual-race-jmxloop-3 topicPartition D_sjOpYxRYunvrzDXNTbPg:race-topic-0 while releasing acquired topic partitions (kafka.server.share.SharePartitionManager)
```

This is the exact phrasing the original triage artifact used for its
detection signal — both this and the `Share session error for
ShareSessionKey[...]: no such share session found` line from earlier are real
and both fire depending on exactly how the race interleaves; treat either as
a hit when grepping broker logs.

### Cleanup

```bash
# stop the broker (Ctrl-C if foreground, or `kill <pid>` if backgrounded)
rm -rf /tmp/kraft-combined-logs   # or wherever log.dirs pointed
rm -rf /tmp/manual-race-classes
```

## Interpreting a "leak" in either driver

A cycle is leaked when, after both connections have disconnected:

- `ShareSessionCache.numMembers(groupId) > 0` (accessed via reflection in
  both drivers — it's a package-private method), and/or
- `sessionCache.size() > 0` for sessions that should have been fully removed, and/or
- `SharePartitionCache.containsKey(partitionKey)` still returns `true`.

All three should be false/zero/empty after both connections belonging to a
group have disconnected. If any holds, the group's cache entries are
permanently unreachable via any listener-driven cleanup path — only a broker
restart clears them, since there is no TTL or eviction (see
`ShareSessionCache.java`; time-based eviction was deliberately removed
upstream in `KAFKA-19159`).

## Signals to look for on a real broker (no PoC involved)

If you want to correlate this with a live cluster instead of the in-process
drivers:

- Broker log lines — two confirmed variants, both real, both fire depending on
  exactly how the race interleaves:
  - `ERROR Share session error for ShareSessionKey[groupId=..., memberId=...]:
    no such share session found` (confirmed via Path C, `SharePartitionManager.java:426`)
  - `ERROR No share partition found for groupId ... topicPartition ...
    while releasing acquired topic partitions` (confirmed via Path D)
  Either at an elevated, sustained rate is a signal; each fires once per
  leaked cycle.
- Bursts of distinct/short-lived group IDs with empty final assignments.
- **Best external signal, no broker-internal access needed:** poll the JMX
  gauge `kafka.server:type=ShareSessionCache,name=SharePartitionsCount`
  (`org.apache.kafka.tools.JmxTool`, see Path D) after connections churn —
  it should return to whatever it was before the churn; if it doesn't, and
  `ShareSessionsCount` reads back to `0` in the same poll, that's the leak
  confirmed directly, live, with no PoC code involved at all.
- `group.share.max.share.sessions` (default `2000`) being hit and new share
  sessions failing with `SHARE_SESSION_LIMIT_REACHED` under otherwise normal
  load — this is the practical ceiling: growth is capped at this value, not
  literally unbounded, but once the cache fills with orphaned entries every
  legitimate share-fetch client is locked out until the broker restarts.

## Handling notes

- Do not commit `session-leak/` to a shared or public remote branch. This is
  an active, unpatched defect in Apache Kafka trunk, not a
  Confluent-fork-only issue (`core/src/main/java/kafka/server/share/` is
  upstream code).
- Disclosure should go through Apache Kafka's private security process
  (security@apache.org / Kafka PMC), coordinated with Confluent's internal
  security tracking for the CVSS scores already assigned to CP/CCloud.
- Neither PoC file belongs in the tracked test tree permanently — copy them
  in only for a run, then remove them, per the steps above. A cleaned-up,
  inverted version (asserting the fix instead of the bug) should be what
  actually lands in `core/src/test/java` once `newContext`'s INITIAL_EPOCH
  branch is fixed.

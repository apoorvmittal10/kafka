# Key-based ordering for share groups: spike benchmark results

Branch: `spike/share-key-ordering` (worktree `../kafka-spike-key-ordering`, based on trunk c8374d3966b). Nothing committed.
Benchmark: `jmh-benchmarks/.../kafka/server/share/SharePartitionKeyOrderingBenchmark.java`
Semantics test: `core/.../kafka/server/share/SharePartitionKeyOrderingSpikeTest.java` (passes)
Raw data: `run4-key-ordering-jmh.json` / `.log`. Final numbers are run 4 only.

## What is measured
Broker-side `SharePartition` only: `nextFetchOffset` -> read a window of 5 batches x 100 records from an in-memory log ->
`acquire` -> ack ACCEPT, in rounds, until 5000 records are acknowledged. No network, no persister latency (NoOp persister),
no consumer processing time (acks happen at the end of each round). Single partition, max in-flight 2000, uniform random keys.
Parameters: ordering OFF/ON, distinct keys (0 = null keys), compression NONE/LZ4, consumers 1/8.
"rounds" = fetch rounds needed to drain, a proxy for parallelism lost to key blocking. "idle%" = ON fetches that acquired nothing.

## Results (run 4)
```
Run 4 (final). 5000 records drained per op; rounds per op; 1 fork, 2 warmup + 4 measured iterations.
comp mem   keys |  OFF ms    ON ms      x | OFF rnd  ON rnd | ON ms/rnd | ON idle%
NONE   1      0 |    0.01     0.94   66.3 |      10      10 |     0.094 |      0.0
NONE   1      1 |    0.01   403.45 27770.6 |      10    5000 |     0.081 |      0.0
NONE   1     16 |    0.01    49.86 3546.4 |      10     330 |     0.151 |      0.0
NONE   1   1000 |    0.01     3.95  279.1 |      10      29 |     0.136 |      0.0
NONE   1 100000 |    0.01     2.14  159.3 |      10      12 |     0.178 |      0.0
NONE   8      0 |    0.02     0.95   43.7 |       3       3 |     0.317 |     44.4
NONE   8      1 |    0.02  1197.86 60787.6 |       3    5000 |     0.240 |     77.8
NONE   8     16 |    0.02   140.16 7608.8 |       3     330 |     0.425 |     69.7
NONE   8   1000 |    0.02    11.57  652.5 |       3      15 |     0.771 |     25.8
NONE   8 100000 |    0.02     2.89  158.8 |       3       4 |     0.724 |     31.0
LZ4    1      0 |    0.01     1.20   89.7 |      10      10 |     0.120 |      0.0
LZ4    1      1 |    0.01   500.80 36515.3 |      10    5000 |     0.100 |      0.0
LZ4    1     16 |    0.01    59.36 4141.0 |      10     330 |     0.180 |      0.0
LZ4    1   1000 |    0.01     4.97  348.5 |      10      29 |     0.171 |      0.0
LZ4    1 100000 |    0.01     2.72  195.5 |      10      12 |     0.226 |      0.0
LZ4    8      0 |    0.02     1.21   64.2 |       3       3 |     0.404 |     44.4
LZ4    8      1 |    0.02  1279.56 70767.4 |       3    5000 |     0.256 |     77.8
LZ4    8     16 |    0.02   153.75 8359.0 |       3     330 |     0.466 |     69.7
LZ4    8   1000 |    0.02    13.44  781.8 |       3      15 |     0.896 |     25.8
LZ4    8 100000 |    0.02     4.48  255.0 |       3       4 |     1.121 |     31.0
```

## Findings
1. **Per-record CPU cost of ordering is small when keys are plentiful.** ON with 100000 keys: 2.1 ms per 5000 records
   (~0.43 us/record, ~0.54 with LZ4). Null keys: ~0.19 us/record, which is the per-offset in-flight state overhead alone.
   LZ4 adds roughly 0.1 us/record for decompressing batches to read keys.
2. **Ratios against OFF are not meaningful on their own.** OFF drains 5000 records in 10 whole-batch acquires (~0.01 ms),
   so any per-record work looks like a 100x+ slowdown. Use the absolute us/record and the rounds columns.
3. **The real cost of low key cardinality is serialization, not CPU.** Rounds to drain: 5000 for 1 key, 330 for 16, 29 for 1000,
   12 for 100000 (OFF: 10). In a real cluster each round is at least a fetch round trip plus consumer processing, so wall-clock
   throughput is bounded by distinct keys in the in-flight window. This simulation has zero processing time, so it understates it.
4. **More consumers do not help once keys are the limit.** With 8 consumers and 16 keys the round count is unchanged (330) and 70%
   of fetches acquire nothing. Each idle fetch is a wasted log read in a real broker. Needs a mitigation in the KIP
   (for example parking fetches until a key unblocks).
5. **Per-round CPU is wasteful in the prototype.** At 16 keys (JFR, 896 samples, 0% Mockito frames): `acquire` 90%,
   of which `registerKeys` 40% (re-iterating and re-indexing the same fetched window every round) and
   `isKeyEligible` 39% (TreeMap lookups for blocked offsets, repeated each round). Ack is 8.5%, `nextFetchOffset` 0.6%.
   Both hot spots look fixable (skip already-registered batches, cache eligibility per key head), but that is a hypothesis.

## Caveats
- Single fork, 2 warmup + 4 measured iterations, on a laptop; error bars on low-round configs are large. Treat as order of magnitude.
- Prototype is naive and incomplete: always recomputes next fetch offset, TreeSet per key, no handling of DLQ, start-offset moves,
  aborted transactions, or persister-gap windows. ON numbers are an upper bound for the algorithm, not for an optimized design.
- Uniform keys only. Skewed (hot key) distributions are not measured and would behave like the low-cardinality rows.
- Memory cost (per-offset state, key index) not measured. Network and persister latency not measured.

## Benchmark validity history (kept for transparency)
- run1 (`run1-...-INVALID`): ON ~200 ms for null keys. Cause: per-offset calls into a Mockito-mocked GroupConfigManager.
- run2 (`run2-...-INVALID`): stub-only mocks did not help; JFR showed 95% of time in the mocked `GroupConfigManager.groupConfig`.
- run3 (`run3-...-superseded`): real GroupConfigManager, but the mocked delayed-fetch notifier still cost ~0.27 ms per run
  on the ack path (inflated OFF 0.28 ms and ON by up to ~15%).
- run4: no mocks on the hot path; verification JFR shows 0% Mockito frames.

## Suggested next steps
1. Optimize the two hotspots and re-measure (registerKeys skip, eligibility caching).
2. Add a skewed-key (Zipf) scenario and a processing-time model so throughput, not only rounds, can be compared.
3. Measure memory (heap per in-flight record, ON vs OFF) and an end-to-end run with a real broker and consumers.
4. Decide on the idle-fetch mitigation.

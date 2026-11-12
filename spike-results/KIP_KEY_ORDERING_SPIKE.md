# Spike: Key-based ordering for share groups (read-only code analysis)

Scope: validate the design in the approach proposal against the current code. No prototype was written.
Files read: `core/.../share/SharePartition.java`, `ShareFetchUtils.java`, `DelayedShareFetch.java`,
`SharePartitionManager.java`, `KafkaApis.scala` (handleShareFetchRequest), `ShareCompletedFetch.java`, `GroupConfig.java`.

## Summary

The design is feasible with no wire-protocol and no share-coordinator schema change. Two claims in the
original proposal need correcting (items 1 and 7), and one constraint (item 3) drives most of the
implementation cost.

## Findings

### 1. Ack-before-acquire is NOT sequenced (correction)
`KafkaApis.handleShareFetchRequest` starts `handleAcknowledgements` (async, persister write) and
`handleFetchFromShareFetchRequest` independently. The fetch does not wait on the ack future.
- Correctness holds: a key only unblocks once the ack's state transition has committed (acquire skips
  records with `hasOngoingStateTransition`).
- Latency: a consumer's piggybacked ack and its fetch race, so the next record of the same key usually
  arrives on the following round trip.
- Wake-up path exists: `SharePartitionManager.acknowledge` calls `delayedRequestNotifier` with a
  `DelayedShareFetchGroupKey` on ack completion, so parked fetches retry when a key unblocks.
- Option for the KIP: sequence ack then fetch when ordering mode is on.

### 2. Per-offset state exists, but acquire is batch-oriented
`InFlightBatch.maybeInitializeOffsetStateUpdate()` already supports per-offset state (partial acks,
record_limit mode), so non-contiguous acquisition is representable in the cache. But `acquire()` never
iterates records today; it works from batch base offsets. Reading keys means iterating (and
decompressing) every fetched batch. This is a new CPU cost and needs a JMH measurement.
Existing precedent for post-filtering acquired ranges: `filterAbortedTransactionalAcquiredRecords` (batch level only).

### 3. Blocked records must be excluded BEFORE the state transition
Acquiring increments the delivery count (`DeliveryCountOps.INCREASE`) and archives at the delivery limit.
So "acquire then release the blocked ones" would burn delivery attempts and could archive healthy records.
The key filter has to sit inside all three acquire paths: new batch, gap, and subset/existing in-flight batch.
This is the main cost: `acquireNewBatchRecords` and `createBatches` assume a contiguous
`[first,last]` range, and counts are computed as `last - first + 1`, which breaks once there are holes.
Blocked offsets in a newly fetched range must be inserted into the cache as AVAILABLE (offset state) so
SPSO tracking and later re-fetch still work.

### 4. Response slicing is wire-compatible but wasteful
`ShareFetchUtils.maybeSliceFetchRecords` slices from the first to the last acquired offset. The client
(`ShareCompletedFetch.buildAcquiredRecordList`) builds its list per acquired offset, so records in the
middle that were not acquired are skipped. No protocol change, but blocked records inside the slice are
shipped and discarded, which is wasted bandwidth on hot-key partitions.

### 5. `nextFetchOffset()` would pin to blocked records
It returns the lowest AVAILABLE offset. Blocked records are AVAILABLE, so every fetch would restart at
the same blocked offset and re-read the same bytes. Needed changes:
- skip AVAILABLE records that are blocked by key;
- set `findNextFetchOffset = true` whenever a terminal transition unblocks a successor.
Terminal-transition sites to hook (SharePartition.java): ~1178, ~1222, ~1491, ~1514, ~2405.

### 6. Window and hot-key stall confirmed
`numInFlightRecords()` is `endOffset - startOffset + 1`, a span, not a count. Blocked records count
against `share.partition.max.record.locks`, and SPSO only advances over a contiguous terminal prefix.
Maximum parallelism is therefore the number of distinct keys inside the span. A hot key (or one stuck
head record) can stall the partition once the span fills.

### 7. No log scan needed on load (correction)
The original proposal said keys would be rebuilt by reading the log on load. Not needed:
- `initialize` loads state batches from the persister only. Persisted states are AVAILABLE/ACKNOWLEDGED/ARCHIVED(/ARCHIVING), with no key info.
- A record can only be acquired when its batch has been fetched, so its key is known at that moment.
- Fetch always starts at the lowest AVAILABLE offset and ascends, so any earlier same-key record is scanned
  (key hash recorded) before a later one can be acquired.
- Invariant to test: every non-terminal record with an unresolved key is never acquirable, and scans are offset-ordered.

### 8. Fetch lock serializes acquisition
`maybeAcquireFetchLock` / `releaseFetchLock` give one concurrent fetch per share partition. Ack, release
and lock-timeout paths take the partition write lock. The key index can live under the existing
`SharePartition.lock` with no new locking.

### 9. Config fits the existing pattern, but mode switching is unsafe on a live group
Group configs are `share.*` keys in `GroupConfig` with defaults in `ShareGroupConfig`, read dynamically
through `configProvider`. A `share.ordering.mode` key fits.
Switching `none -> key` on a live group is unsafe: records currently ACQUIRED were fetched under `none`
mode and have no recorded key, so a later record of the same key could be acquired concurrently with an
earlier one. Recommendation for v1: reject the change unless the group has no non-terminal records
(empty group), or document it as requiring an empty group.

## Implementation surface (estimate)
- `SharePartition`: three acquire paths, `createBatches`/`addBatches` (non-contiguous), `nextFetchOffset`, terminal-transition hooks, load path.
- `InFlightBatch`/`InFlightState`: per-offset key hash (8 bytes per non-terminal record) plus per-key ordered offset index.
- Config plumbing (`GroupConfig`, validation, feature/metadata-version gate) and metrics (blocked records, tracked keys).
- Optional: sequence ack before fetch in `KafkaApis`.
- No change to: ShareFetch/ShareAcknowledge protocol, share-coordinator state schema.

## Still unmeasured / open
- CPU and latency cost of decompressing batches to read keys (JMH).
- Memory cost of forcing per-offset state in ordering mode.
- Interaction with the DLQ ARCHIVING flow and with RENEW acks (a renewed record keeps its key locked; should be fine, not verified).
- Behavior with compacted topics and `read_committed` aborted-transaction archiving (the archive path must also unblock keys).

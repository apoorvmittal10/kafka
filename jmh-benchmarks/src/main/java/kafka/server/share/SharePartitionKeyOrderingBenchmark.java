/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server.share;

import kafka.server.share.SharePartitionManager.SharePartitionListener;

import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.internals.ShareAcquireMode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ShareFetchResponseData.AcquiredRecords;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.record.internal.SimpleRecord;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigProvider;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.dlq.NoOpShareGroupDLQManager;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.MockTimer;

import org.mockito.Mockito;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * SPIKE benchmark for key-based ordering in share groups.
 *
 * Drives a single {@link SharePartition} the way DelayedShareFetch would: for each consumer in a round it
 * reads the next fetch offset, reads a window of batches from an in-memory log and acquires from it; at the
 * end of the round every consumer acknowledges (ACCEPT) what it acquired. The partition is drained until all
 * records are acknowledged.
 *
 * Reported per invocation (one full drain of {@code totalRecords}):
 * <ul>
 *   <li>JMH time: CPU cost of the broker-side fetch/acquire/ack work.</li>
 *   <li>rounds: number of fetch rounds needed to drain. It is a proxy for achievable parallelism, because
 *       with key ordering ON consumers may find no eligible record and idle.</li>
 * </ul>
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-Xms2g", "-Xmx2g"})
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SharePartitionKeyOrderingBenchmark {

    private static final int RECORDS_PER_BATCH = 100;
    private static final int BATCHES_PER_FETCH = 5;
    private static final int MAX_FETCH_RECORDS = 500;
    private static final int BATCH_SIZE = 500;
    private static final int VALUE_SIZE = 100;
    private static final String MEMBER_PREFIX = "member-";
    // stubOnly: do not record invocations, otherwise Mockito stack-walking dominates the per-offset path.
    private static final org.mockito.MockSettings STUB_ONLY = Mockito.withSettings().stubOnly();

    @Param({"OFF", "ON"})
    public String ordering;

    /** 0 means null keys; otherwise the number of distinct keys drawn uniformly at random. */
    @Param({"0", "1", "16", "1000", "100000"})
    public int distinctKeys;

    @Param({"NONE", "LZ4"})
    public String compression;

    /** Number of concurrent consumers fetching each round. */
    @Param({"1", "8"})
    public int members;

    @Param({"5000"})
    public int totalRecords;

    @Param({"2000"})
    public int maxInFlight;

    private ByteBuffer log;
    private int[] batchPositions;
    private int[] batchSizes;
    private int numBatches;

    private SharePartition sharePartition;
    private MockTimer timer;
    private String[] memberIds;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Counters {
        public long rounds;
        public long fetches;
        public long emptyFetches;
        public long records;
        public long maxTrackedKeys;

        @Setup(Level.Iteration)
        public void reset() {
            rounds = 0;
            fetches = 0;
            emptyFetches = 0;
            records = 0;
            maxTrackedKeys = 0;
        }
    }

    @Setup(Level.Trial)
    public void setupLog() {
        memberIds = new String[members];
        for (int i = 0; i < members; i++) {
            memberIds[i] = MEMBER_PREFIX + i;
        }
        Compression comp = compression.equals("LZ4") ? Compression.lz4().build() : Compression.NONE;
        Random random = new Random(42);
        numBatches = totalRecords / RECORDS_PER_BATCH;
        List<MemoryRecords> batches = new ArrayList<>(numBatches);
        int total = 0;
        for (int b = 0; b < numBatches; b++) {
            SimpleRecord[] records = new SimpleRecord[RECORDS_PER_BATCH];
            for (int r = 0; r < RECORDS_PER_BATCH; r++) {
                byte[] key = null;
                if (distinctKeys > 0) {
                    key = ("key-" + random.nextInt(distinctKeys)).getBytes(StandardCharsets.UTF_8);
                }
                byte[] value = new byte[VALUE_SIZE];
                random.nextBytes(value);
                records[r] = new SimpleRecord(0L, key, value);
            }
            MemoryRecords mr = MemoryRecords.withRecords((long) b * RECORDS_PER_BATCH, comp, records);
            batches.add(mr);
            total += mr.sizeInBytes();
        }
        log = ByteBuffer.allocate(total);
        batchPositions = new int[numBatches];
        batchSizes = new int[numBatches];
        for (int b = 0; b < numBatches; b++) {
            MemoryRecords mr = batches.get(b);
            batchPositions[b] = log.position();
            batchSizes[b] = mr.sizeInBytes();
            log.put(mr.buffer().duplicate());
        }
        log.flip();
    }

    @Setup(Level.Invocation)
    public void setupPartition() {
        timer = new MockTimer();
        TopicIdPartition tip = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("bench", 0));
        Consumer<org.apache.kafka.server.share.fetch.DelayedShareFetchKey> notifier = key -> { };
        sharePartition = new SharePartition("bench-group", tip, 0, maxInFlight, 5, 30000, timer, Time.SYSTEM,
            new NoOpStatePersister(), Mockito.mock(org.apache.kafka.server.share.PartitionMetadataProvider.class, STUB_ONLY),
            notifier, new ShareGroupConfigProvider(new GroupConfigManager(Mockito.mock(GroupCoordinatorConfig.class), Mockito.mock(ShareGroupConfig.class))),
            Mockito.mock(SharePartitionListener.class, STUB_ONLY), () -> false, new NoOpShareGroupDLQManager());
        sharePartition.setKeyOrderingEnabled(ordering.equals("ON"));
        sharePartition.maybeInitialize().join();
    }

    @TearDown(Level.Invocation)
    public void tearDownPartition() throws Exception {
        sharePartition.close();
        timer.close();
    }

    private FetchPartitionData fetchFrom(long fetchOffset) {
        int first = (int) (fetchOffset / RECORDS_PER_BATCH);
        if (first >= numBatches) {
            return null;
        }
        int last = Math.min(numBatches, first + BATCHES_PER_FETCH) - 1;
        int start = batchPositions[first];
        int end = batchPositions[last] + batchSizes[last];
        ByteBuffer slice = log.duplicate();
        slice.position(start).limit(end);
        MemoryRecords records = MemoryRecords.readableRecords(slice.slice());
        return new FetchPartitionData(Errors.NONE, totalRecords, 0, records,
            Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false);
    }

    @Benchmark
    public void drain(Counters counters) {
        long acked = 0;
        int idleRounds = 0;
        while (acked < totalRecords) {
            counters.rounds++;
            List<List<AcquiredRecords>> acquiredByMember = new ArrayList<>(members);
            int acquiredThisRound = 0;
            for (int m = 0; m < members; m++) {
                long fetchOffset = sharePartition.nextFetchOffset();
                FetchPartitionData data = fetchOffset >= totalRecords ? null : fetchFrom(fetchOffset);
                if (data == null) {
                    acquiredByMember.add(List.of());
                    continue;
                }
                counters.fetches++;
                ShareAcquiredRecords acquired = sharePartition.acquire(memberIds[m], ShareAcquireMode.BATCH_OPTIMIZED,
                    BATCH_SIZE, MAX_FETCH_RECORDS, fetchOffset, data, FetchIsolation.HIGH_WATERMARK);
                if (acquired.count() == 0) {
                    counters.emptyFetches++;
                }
                acquiredByMember.add(acquired.acquiredRecords());
                acquiredThisRound += acquired.count();
            }
            counters.maxTrackedKeys = Math.max(counters.maxTrackedKeys, sharePartition.trackedKeyCount());
            if (acquiredThisRound == 0) {
                // Nothing acquirable anywhere; guard against an infinite loop in a broken configuration.
                if (++idleRounds > 3) {
                    throw new IllegalStateException("Stalled after acking " + acked + " of " + totalRecords);
                }
                continue;
            }
            idleRounds = 0;
            for (int m = 0; m < members; m++) {
                List<AcquiredRecords> acquired = acquiredByMember.get(m);
                if (acquired.isEmpty()) {
                    continue;
                }
                List<ShareAcknowledgementBatch> acks = new ArrayList<>(acquired.size());
                for (AcquiredRecords ar : acquired) {
                    acks.add(new ShareAcknowledgementBatch(ar.firstOffset(), ar.lastOffset(),
                        List.of(AcknowledgeType.ACCEPT.id)));
                    acked += ar.lastOffset() - ar.firstOffset() + 1;
                }
                sharePartition.acknowledge(memberIds[m], acks).join();
            }
        }
        counters.records += acked;
    }
}

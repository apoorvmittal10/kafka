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
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigProvider;
import org.apache.kafka.server.share.PartitionMetadataProvider;
import org.apache.kafka.server.share.acknowledge.ShareAcknowledgementBatch;
import org.apache.kafka.server.share.dlq.NoOpShareGroupDLQManager;
import org.apache.kafka.server.share.fetch.DelayedShareFetchKey;
import org.apache.kafka.server.share.fetch.ShareAcquiredRecords;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.storage.log.FetchIsolation;
import org.apache.kafka.server.storage.log.FetchPartitionData;
import org.apache.kafka.server.util.timer.MockTimer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * SPIKE: validates the key-ordering prototype semantics that the benchmark relies on.
 */
public class SharePartitionKeyOrderingSpikeTest {

    private MockTimer timer;
    private SharePartition sharePartition;

    @BeforeEach
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        timer = new MockTimer();
        sharePartition = new SharePartition("g", new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("t", 0)),
            0, 2000, 5, 30000, timer, Time.SYSTEM, new NoOpStatePersister(), Mockito.mock(PartitionMetadataProvider.class),
            Mockito.mock(Consumer.class), new ShareGroupConfigProvider(Mockito.mock(GroupConfigManager.class)),
            Mockito.mock(SharePartitionListener.class), () -> false, new NoOpShareGroupDLQManager());
        sharePartition.setKeyOrderingEnabled(true);
        sharePartition.maybeInitialize().join();
    }

    @AfterEach
    public void tearDown() throws Exception {
        sharePartition.close();
        timer.close();
    }

    // Keys by offset: 0=A 1=B 2=A 3=C 4=B 5=A
    private MemoryRecords records() {
        String[] keys = {"A", "B", "A", "C", "B", "A"};
        SimpleRecord[] rs = new SimpleRecord[keys.length];
        for (int i = 0; i < keys.length; i++) {
            rs[i] = new SimpleRecord(0L, keys[i].getBytes(StandardCharsets.UTF_8), new byte[8]);
        }
        return MemoryRecords.withRecords(0L, Compression.NONE, rs);
    }

    private ShareAcquiredRecords acquire(String member, long fetchOffset) {
        FetchPartitionData data = new FetchPartitionData(Errors.NONE, 10, 0, records(),
            Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false);
        return sharePartition.acquire(member, ShareAcquireMode.BATCH_OPTIMIZED, 500, 500, fetchOffset, data, FetchIsolation.HIGH_WATERMARK);
    }

    private void ack(String member, long first, long last, AcknowledgeType type) {
        sharePartition.acknowledge(member, List.of(new ShareAcknowledgementBatch(first, last, List.of(type.id)))).join();
    }

    private static void assertRanges(List<AcquiredRecords> actual, long[][] expected, int deliveryCount) {
        assertEquals(expected.length, actual.size(), actual.toString());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i][0], actual.get(i).firstOffset(), actual.toString());
            assertEquals(expected[i][1], actual.get(i).lastOffset(), actual.toString());
            assertEquals(deliveryCount, actual.get(i).deliveryCount(), actual.toString());
        }
    }

    @Test
    public void testOnlyHeadOfEachKeyIsAcquiredAndSuccessorsUnblockAfterTerminalState() {
        // First fetch: heads of A (0), B (1), C (3) are acquired; 2, 4, 5 are blocked.
        ShareAcquiredRecords first = acquire("m1", 0);
        assertEquals(3, first.count());
        assertRanges(first.acquiredRecords(), new long[][]{{0, 1}, {3, 3}}, 1);

        // Everything eligible is acquired, so the next fetch offset is past the end.
        assertEquals(6, sharePartition.nextFetchOffset());

        // Ack A(0): A(2) becomes the head and is eligible, so the fetch offset moves back to 2.
        ack("m1", 0, 0, AcknowledgeType.ACCEPT);
        assertEquals(2, sharePartition.nextFetchOffset());

        // Another member acquires only A(2); B(4) is blocked by B(1) still acquired; A(5) is blocked by A(2).
        ShareAcquiredRecords second = acquire("m2", 2);
        assertRanges(second.acquiredRecords(), new long[][]{{2, 2}}, 1);
        assertEquals(6, sharePartition.nextFetchOffset());

        // Release B(1): it is redelivered (delivery count 2) before B(4).
        ack("m1", 1, 1, AcknowledgeType.RELEASE);
        assertEquals(1, sharePartition.nextFetchOffset());
        ShareAcquiredRecords redelivery = acquire("m3", 1);
        assertRanges(redelivery.acquiredRecords(), new long[][]{{1, 1}}, 2);

        // Accept B(1): B(4) is now eligible and has not had its delivery count burned while blocked.
        ack("m3", 1, 1, AcknowledgeType.ACCEPT);
        ShareAcquiredRecords last = acquire("m3", sharePartition.nextFetchOffset());
        assertRanges(last.acquiredRecords(), new long[][]{{4, 4}}, 1);
    }

    @Test
    public void testNullKeysAreUnordered() {
        SimpleRecord[] rs = new SimpleRecord[4];
        for (int i = 0; i < rs.length; i++) {
            rs[i] = new SimpleRecord(0L, null, new byte[8]);
        }
        FetchPartitionData data = new FetchPartitionData(Errors.NONE, 10, 0, MemoryRecords.withRecords(0L, Compression.NONE, rs),
            Optional.empty(), OptionalLong.empty(), Optional.empty(), OptionalInt.empty(), false);
        ShareAcquiredRecords acquired = sharePartition.acquire("m1", ShareAcquireMode.BATCH_OPTIMIZED, 500, 500, 0, data, FetchIsolation.HIGH_WATERMARK);
        assertEquals(4, acquired.count());
    }
}

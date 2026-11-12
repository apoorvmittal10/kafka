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

import kafka.server.ReplicaManager;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.internals.ImplicitLinkedHashCollection;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigProvider;
import org.apache.kafka.server.share.CachedSharePartition;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.PartitionMetadataProvider;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.dlq.NoOpShareGroupDLQManager;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.share.session.ShareSession;
import org.apache.kafka.server.share.session.ShareSessionCache;
import org.apache.kafka.server.share.session.ShareSessionKey;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * <p>Each iteration creates a distinct group and partition-cache key through
 * the actual concurrent {@link SharePartitionManager#newContext} path. The
 * entry models a SharePartition that a subsequent fetch would have created.
 * This test proves that the entry remains reachable from SharePartitionCache
 * after both connections have disconnected; it does not measure heap bytes or
 * claim that a broker has exhausted memory.</p>
 */
@Timeout(600)
public class SharePartitionCacheRetentionPoC {

    private static final int DEFAULT_CYCLES = 10000;
    private static final int DEFAULT_PARTITIONS_PER_GROUP = 1;
    private static final int DEFAULT_BALLAST_BYTES = 131072;
    private static final int DEFAULT_ROUND_TIMEOUT_SECONDS = 10;
    private static final boolean PAUSE_FOR_HEAP_INSPECTION = false;
    private static final String MEMBER_ID = "retention-member";

    private SharePartitionManager manager;
    private BrokerTopicStats brokerTopicStats;

    @AfterEach
    public void tearDown() throws Exception {
        if (manager != null) {
            manager.close();
        }
        if (brokerTopicStats != null) {
            brokerTopicStats.close();
        }
    }

    @Test
    public void repeatedConcurrentInitialContextsRetainOnePartitionPerGroup() throws Exception {
        int cycles = DEFAULT_CYCLES;
        int partitionsPerGroup = DEFAULT_PARTITIONS_PER_GROUP;
        int ballastBytes = DEFAULT_BALLAST_BYTES;
        if (cycles < 0 || partitionsPerGroup <= 0 || ballastBytes < 0) {
            throw new IllegalArgumentException(
                "test constants must be non-negative, with partitionsPerGroup positive");
        }

        RoundBarrierShareSessionCache sessionCache = new RoundBarrierShareSessionCache(cycles * 2);
        SharePartitionCache partitionCache = new SharePartitionCache();
        manager = newManager(sessionCache, partitionCache);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            for (int cycle = 0; cycle < cycles; cycle++) {
                String groupId = "retention-group-" + cycle;
                String connectionA = "retention-a-" + cycle;
                String connectionB = "retention-b-" + cycle;
                Uuid topicId = Uuid.randomUuid();
                List<TopicIdPartition> requestedPartitions = new ArrayList<>(partitionsPerGroup);
                for (int partition = 0; partition < partitionsPerGroup; partition++) {
                    TopicIdPartition topicPartition = new TopicIdPartition(
                        topicId, new TopicPartition("retention-topic-" + cycle, partition));
                    requestedPartitions.add(topicPartition);
                    SharePartitionKey partitionKey = new SharePartitionKey(groupId, topicPartition);
                    SharePartition sharePartition = mock(SharePartition.class);
                    if (ballastBytes == 0) {
                        when(sharePartition.releaseAcquiredRecords(MEMBER_ID))
                            .thenReturn(CompletableFuture.completedFuture(null));
                    } else {
                        // Diagnostic-only ballast. Mockito retains this captured array through
                        // the mock's answer, so it is reachable only while the cache retains the mock.
                        final byte[] ballast = new byte[ballastBytes];
                        for (int offset = 0; offset < ballast.length; offset += 4096) {
                            ballast[offset] = (byte) (cycle + partition);
                        }
                        ballast[ballast.length - 1] = (byte) cycle;
                        when(sharePartition.releaseAcquiredRecords(MEMBER_ID)).thenAnswer(invocation -> {
                            ballast[0] ^= 1;
                            return CompletableFuture.completedFuture(null);
                        });
                    }
                    partitionCache.computeIfAbsent(partitionKey, ignored -> sharePartition);
                }
                SharePartitionKey partitionKey = new SharePartitionKey(groupId, requestedPartitions.get(0));

                sessionCache.arm(connectionA, connectionB);
                Future<?> first = executor.submit(() -> manager.newContext(
                    groupId,
                    List.of(requestedPartitions.get(0)),
                    List.of(),
                    MEMBER_ID,
                    ShareRequestMetadata.INITIAL_EPOCH,
                    false,
                    connectionA));
                Future<?> second = executor.submit(() -> manager.newContext(
                    groupId,
                    requestedPartitions,
                    List.of(),
                    MEMBER_ID,
                    ShareRequestMetadata.INITIAL_EPOCH,
                    false,
                    connectionB));

                sessionCache.awaitBothRemoves();
                sessionCache.allowFirstCreate();
                sessionCache.awaitFirstCreate();
                sessionCache.allowSecondCreate();
                await(first);
                await(second);
                sessionCache.disarm();

                sessionCache.connectionDisconnectListener().onDisconnect(connectionA);
                sessionCache.connectionDisconnectListener().onDisconnect(connectionB);

                assertEquals(0, sessionCache.size(), "all sessions must be gone after cycle " + cycle);
                assertEquals(1, memberCount(sessionCache, groupId),
                    "cycle " + cycle + " leaves a phantom group member");
                assertTrue(sessionCache.totalPartitions() >= cycle + 1L,
                    "phantom session partition count must not return to zero");
                assertTrue(partitionCache.containsKey(partitionKey),
                    "the current group's partition remains retained after disconnect");
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertEquals((long) cycles * partitionsPerGroup, partitionCache.size(),
            "each unique group retains all SharePartitionCache entries");
        assertEquals((long) cycles * partitionsPerGroup, partitionCache.cachedSharePartitionKeys().size());
        assertEquals(0, sessionCache.groupEmptyCount(),
            "phantom member counts prevent all group-empty callbacks");
        pauseForHeapInspection();
    }

    private SharePartitionManager newManager(ShareSessionCache sessionCache,
                                             SharePartitionCache partitionCache) {
        brokerTopicStats = new BrokerTopicStats();
        MockTime time = new MockTime();
        return new SharePartitionManager(
            mock(ReplicaManager.class),
            mock(LogReader.class),
            mock(PartitionMetadataProvider.class),
            ignored -> { },
            time,
            sessionCache,
            partitionCache,
            30000,
            new MockTimer(),
            5,
            200,
            6000L,
            new NoOpStatePersister(),
            new ShareGroupConfigProvider(mock(GroupConfigManager.class)),
            new ShareGroupMetrics(time),
            brokerTopicStats,
            () -> false,
            new NoOpShareGroupDLQManager()
        );
    }

    private static void await(Future<?> future) throws Exception {
        try {
            future.get(10, TimeUnit.SECONDS);
        } catch (ExecutionException exception) {
            throw new AssertionError("newContext failed", exception.getCause());
        }
    }

    private static int memberCount(ShareSessionCache cache, String groupId) throws Exception {
        Method method = ShareSessionCache.class.getDeclaredMethod("numMembers", String.class);
        method.setAccessible(true);
        Integer value = (Integer) method.invoke(cache, groupId);
        return value == null ? 0 : value;
    }

    private static void pauseForHeapInspection() throws InterruptedException {
        if (!PAUSE_FOR_HEAP_INSPECTION) return;
        long pid = ProcessHandle.current().pid();
        System.out.println("SHARE_PARTITION_RETENTION_TEST_PID=" + pid);
        System.out.println("SHARE_PARTITION_RETENTION_TEST_PAUSED=true");
        System.out.flush();
        new CountDownLatch(1).await();
    }

    private static final class RoundBarrierShareSessionCache extends ShareSessionCache {
        private final AtomicReference<Round> activeRound = new AtomicReference<>();
        private int groupEmptyCount;

        private RoundBarrierShareSessionCache(int maxEntries) {
            super(maxEntries);
        }

        private void arm(String firstConnection, String secondConnection) {
            Round round = new Round(firstConnection, secondConnection);
            if (!activeRound.compareAndSet(null, round)) {
                throw new AssertionError("previous round is still armed");
            }
        }

        private void awaitBothRemoves() throws InterruptedException {
            round().await(round().bothRemoves, "both newContext remove calls");
        }

        private void allowFirstCreate() {
            round().allowFirstCreate.countDown();
        }

        private void awaitFirstCreate() throws InterruptedException {
            round().await(round().firstCreated, "first maybeCreateSession call");
        }

        private void allowSecondCreate() {
            round().allowSecondCreate.countDown();
        }

        private void disarm() {
            if (!activeRound.compareAndSet(round(), null)) {
                throw new AssertionError("unexpected active round");
            }
        }

        @Override
        public ShareSession remove(ShareSessionKey key) {
            ShareSession session = super.remove(key);
            Round round = activeRound.get();
            if (round != null) {
                round.bothRemoves.countDown();
                round.await(round.bothRemoves, "both newContext remove calls");
            }
            return session;
        }

        @Override
        public ShareSessionKey maybeCreateSession(
            String groupId,
            String memberId,
            ImplicitLinkedHashCollection<CachedSharePartition> partitionMap,
            String connectionId
        ) {
            Round round = activeRound.get();
            if (round != null && connectionId.equals(round.firstConnection)) {
                round.await(round.allowFirstCreate, "first create permission");
                ShareSessionKey key = super.maybeCreateSession(groupId, memberId, partitionMap, connectionId);
                round.firstCreated.countDown();
                return key;
            }
            if (round != null && connectionId.equals(round.secondConnection)) {
                round.await(round.allowSecondCreate, "second create permission");
            }
            return super.maybeCreateSession(groupId, memberId, partitionMap, connectionId);
        }

        @Override
        public synchronized void registerShareGroupListener(org.apache.kafka.server.share.ShareGroupListener listener) {
            super.registerShareGroupListener(new org.apache.kafka.server.share.ShareGroupListener() {
                @Override
                public void onMemberLeave(String groupId, String memberId) {
                    listener.onMemberLeave(groupId, memberId);
                }

                @Override
                public void onGroupEmpty(String groupId) {
                    groupEmptyCount++;
                    listener.onGroupEmpty(groupId);
                }
            });
        }

        private synchronized int groupEmptyCount() {
            return groupEmptyCount;
        }

        private Round round() {
            Round round = activeRound.get();
            if (round == null) {
                throw new AssertionError("no active round");
            }
            return round;
        }
    }

    private static final class Round {
        private final String firstConnection;
        private final String secondConnection;
        private final CountDownLatch bothRemoves = new CountDownLatch(2);
        private final CountDownLatch allowFirstCreate = new CountDownLatch(1);
        private final CountDownLatch firstCreated = new CountDownLatch(1);
        private final CountDownLatch allowSecondCreate = new CountDownLatch(1);

        private Round(String firstConnection, String secondConnection) {
            this.firstConnection = firstConnection;
            this.secondConnection = secondConnection;
        }

        private void await(CountDownLatch latch, String event) {
            try {
                if (!latch.await(DEFAULT_ROUND_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                    throw new AssertionError("timed out waiting for " + event);
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while waiting for " + event, exception);
            }
        }
    }
}

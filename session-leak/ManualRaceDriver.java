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
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfigProvider;
import org.apache.kafka.server.share.LogReader;
import org.apache.kafka.server.share.PartitionMetadataProvider;
import org.apache.kafka.server.share.SharePartitionKey;
import org.apache.kafka.server.share.dlq.NoOpShareGroupDLQManager;
import org.apache.kafka.server.share.metrics.ShareGroupMetrics;
import org.apache.kafka.server.share.persister.NoOpStatePersister;
import org.apache.kafka.server.share.session.ShareSessionCache;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.server.util.timer.MockTimer;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;

/**
 * <p>Unlike {@link SharePartitionCacheRetentionPoC}, this driver does not force the race with a
 * barrier. It fires two real INITIAL_EPOCH {@code newContext} calls back-to-back with a small
 * random jitter, closer to how two independent client connections would actually interleave on a
 * live broker. It is a manual, print-driven observation tool, not a pass/fail test: some iterations
 * will legitimately miss the race window. Run it directly and read the printed hit rate.</p>
 */
@Timeout(300)
public class ManualRaceDriver {

    private static final int ITERATIONS = 5000;
    private static final String MEMBER_ID = "manual-race-member";

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
    public void raceRealTimingAndReportHitRate() throws Exception {
        AtomicInteger groupEmptyCount = new AtomicInteger();
        Map<String, AtomicInteger> groupEmptyCallsByGroup = new ConcurrentHashMap<>();
        ShareSessionCache sessionCache = new ShareSessionCache(ITERATIONS * 2) {
            @Override
            public synchronized void registerShareGroupListener(org.apache.kafka.server.share.ShareGroupListener listener) {
                super.registerShareGroupListener(new org.apache.kafka.server.share.ShareGroupListener() {
                    @Override
                    public void onMemberLeave(String groupId, String memberId) {
                        listener.onMemberLeave(groupId, memberId);
                    }

                    @Override
                    public void onGroupEmpty(String groupId) {
                        groupEmptyCount.incrementAndGet();
                        groupEmptyCallsByGroup.computeIfAbsent(groupId, g -> new AtomicInteger()).incrementAndGet();
                        listener.onGroupEmpty(groupId);
                    }
                });
            }
        };
        SharePartitionCache partitionCache = new SharePartitionCache();
        manager = newManager(sessionCache, partitionCache);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        int leaks = 0;
        try {
            for (int cycle = 0; cycle < ITERATIONS; cycle++) {
                String groupId = "manual-race-group-" + cycle;
                String connectionA = "manual-race-a-" + cycle;
                String connectionB = "manual-race-b-" + cycle;
                Uuid topicId = Uuid.randomUuid();
                TopicIdPartition partition = new TopicIdPartition(
                    topicId, new TopicPartition("manual-race-topic-" + cycle, 0));
                SharePartitionKey partitionKey = new SharePartitionKey(groupId, partition);
                SharePartition sharePartition = mock(SharePartition.class);
                partitionCache.computeIfAbsent(partitionKey, ignored -> sharePartition);

                Future<?> first = executor.submit(() -> {
                    jitter();
                    manager.newContext(groupId, List.of(partition), List.of(), MEMBER_ID,
                        ShareRequestMetadata.INITIAL_EPOCH, false, connectionA);
                });
                Future<?> second = executor.submit(() -> {
                    jitter();
                    manager.newContext(groupId, List.of(partition), List.of(), MEMBER_ID,
                        ShareRequestMetadata.INITIAL_EPOCH, false, connectionB);
                });
                first.get(10, TimeUnit.SECONDS);
                second.get(10, TimeUnit.SECONDS);

                sessionCache.connectionDisconnectListener().onDisconnect(connectionA);
                sessionCache.connectionDisconnectListener().onDisconnect(connectionB);

                boolean leaked = memberCount(sessionCache, groupId) > 0 || sessionCache.size() > 0;
                if (leaked) {
                    leaks++;
                }
                if (cycle % 500 == 0) {
                    System.out.println("cycle=" + cycle + " leaksSoFar=" + leaks);
                }
            }
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }

        long zeroCallbackGroups = ITERATIONS - groupEmptyCallsByGroup.size();
        long oneCallbackGroups = groupEmptyCallsByGroup.values().stream().filter(c -> c.get() == 1).count();
        long multiCallbackGroups = groupEmptyCallsByGroup.values().stream().filter(c -> c.get() > 1).count();

        System.out.println("ITERATIONS=" + ITERATIONS);
        System.out.println("LEAKED_CYCLES=" + leaks);
        System.out.println("HIT_RATE_PERCENT=" + (100.0 * leaks / ITERATIONS));
        System.out.println("GROUP_EMPTY_CALLBACKS_TOTAL=" + groupEmptyCount.get());
        System.out.println("GROUPS_WITH_ZERO_CALLBACKS=" + zeroCallbackGroups + " (phantom member leak)");
        System.out.println("GROUPS_WITH_ONE_CALLBACK=" + oneCallbackGroups + " (correct behavior)");
        System.out.println("GROUPS_WITH_MULTIPLE_CALLBACKS=" + multiCallbackGroups + " (duplicate/premature notification)");
        System.out.println("RETAINED_PARTITION_CACHE_ENTRIES=" + partitionCache.size());
        System.out.flush();
    }

    private static void jitter() {
        int nanos = ThreadLocalRandom.current().nextInt(0, 200_000);
        long start = System.nanoTime();
        while (System.nanoTime() - start < nanos) {
            Thread.onSpinWait();
        }
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

    private static int memberCount(ShareSessionCache cache, String groupId) throws Exception {
        Method method = ShareSessionCache.class.getDeclaredMethod("numMembers", String.class);
        method.setAccessible(true);
        Integer value = (Integer) method.invoke(cache, groupId);
        return value == null ? 0 : value;
    }
}

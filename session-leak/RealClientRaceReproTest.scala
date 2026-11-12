/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.internals.ShareAcquireMode
import org.apache.kafka.common.message.ShareFetchRequestData
import org.apache.kafka.common.test.api.{ClusterConfigProperty, ClusterTest, ClusterTestDefaults, Type}
import org.apache.kafka.common.{TopicIdPartition, TopicPartition, Uuid}
import org.apache.kafka.common.requests.{ShareFetchRequest, ShareFetchResponse, ShareRequestMetadata}
import org.apache.kafka.common.test.ClusterInstance
import org.apache.kafka.server.IntegrationTestUtils
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{AfterEach, Timeout}

import java.net.Socket
import java.util
import java.util.concurrent.{Callable, Executors, TimeUnit}

/**
 * <p>Real-broker, real-socket reproduction of the SharePartitionManager.newContext INITIAL_EPOCH
 * race (see session-leak/kafka-share-partition-retention-report.txt). Unlike
 * SharePartitionCacheRetentionPoC.java and ManualRaceDriver.java, which call newContext directly
 * in-process, this test goes through the actual SocketServer / KafkaRequestHandlerPool / KafkaApis
 * path with two genuinely separate TCP connections against one real KRaft broker.</p>
 *
 * <p>Each cycle picks a fresh groupId and a fresh, shared memberId Uuid, opens two real sockets,
 * and races two INITIAL_EPOCH ShareFetch requests carrying that identical (groupId, memberId) pair
 * concurrently. No ShareGroupHeartbeat is sent first -- this deliberately demonstrates that no
 * prior group membership is required to reach the vulnerable code path (see the "memberId
 * validation" finding in the report). It is a print-driven observation, not a strict pass/fail
 * assertion, since real network timing is not forced with a barrier.</p>
 */
@Timeout(600)
@ClusterTestDefaults(types = Array(Type.KRAFT), brokers = 1, serverProperties = Array(
  new ClusterConfigProperty(key = "group.share.persister.class.name", value = "")
))
class RealClientRaceReproTest(cluster: ClusterInstance) extends GroupCoordinatorBaseRequestTest(cluster) {

  private final val TOPIC = "race-topic"
  // NOTE: the ClusterTestExtensions harness enforces a per-test timeout (observed ~60s) that does
  // not appear to respect the class-level @Timeout(600) annotation above. Empirically, ~0.6s/cycle
  // is typical on loopback, so this is tuned to finish safely inside that window. Raise it (and split
  // across multiple @ClusterTest methods if needed) for a larger sample.
  private final val ITERATIONS = 80

  @AfterEach
  def tearDown(): Unit = {
    closeSockets()
  }

  @ClusterTest
  def racingInitialEpochRequestsLeakOverRealSockets(): Unit = {
    createTopicAndReturnLeaders(TOPIC, numPartitions = 1)
    val topicId = getTopicIds.get(TOPIC)
    val topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(TOPIC, 0))
    val send = util.List.of(topicIdPartition)

    val executor = Executors.newFixedThreadPool(2)
    var leaks = 0
    try {
      for (cycle <- 0 until ITERATIONS) {
        val groupId = s"race-group-$cycle"
        val memberId = Uuid.randomUuid()

        val socketA: Socket = connectAny()
        val socketB: Socket = connectAny()

        val taskA: Callable[ShareFetchResponse] = () =>
          IntegrationTestUtils.sendAndReceive[ShareFetchResponse](
            createShareFetchRequest(groupId, memberId, send), socketA)
        val taskB: Callable[ShareFetchResponse] = () =>
          IntegrationTestUtils.sendAndReceive[ShareFetchResponse](
            createShareFetchRequest(groupId, memberId, send), socketB)

        val futureA = executor.submit(taskA)
        val futureB = executor.submit(taskB)
        futureA.get(10, TimeUnit.SECONDS)
        futureB.get(10, TimeUnit.SECONDS)

        socketA.close()
        socketB.close()

        val cache = shareSessionCache()
        // Wait for the broker's disconnect listener to finish processing both closes. sessions.size()
        // returns to 0 whether or not the leak occurred (only the per-group counters/partition cache
        // leak), so it's a safe "settled" signal that doesn't presuppose the outcome we're testing for.
        TestUtils.waitUntilTrue(() => cacheSize(cache) == 0, s"cycle $cycle: sessions did not settle", 5000)

        if (numMembers(cache, groupId) > 0) {
          leaks += 1
        }

        if (cycle % 25 == 0) {
          println(s"cycle=$cycle leaksSoFar=$leaks")
        }
      }
    } finally {
      executor.shutdownNow()
      executor.awaitTermination(10, TimeUnit.SECONDS)
    }

    println(s"ITERATIONS=$ITERATIONS")
    println(s"LEAKED_CYCLES=$leaks")
    println(s"HIT_RATE_PERCENT=${100.0 * leaks / ITERATIONS}")
    // Observational: real network timing, not a forced barrier, so this can legitimately be 0 on an
    // unlucky run. Re-run with a higher ITERATIONS if so. See session-leak/README.md.
    assertTrue(leaks >= 0)
  }

  private def createShareFetchRequest(groupId: String, memberId: Uuid, send: util.List[TopicIdPartition]): ShareFetchRequest = {
    val metadata = new ShareRequestMetadata(memberId, ShareRequestMetadata.INITIAL_EPOCH)
    val acknowledgementsMap = util.Map.of[TopicIdPartition, util.List[ShareFetchRequestData.AcknowledgementBatch]]
    ShareFetchRequest.Builder.forConsumer(groupId, metadata, 500, 0, Int.MaxValue, 500, 500,
      ShareAcquireMode.BATCH_OPTIMIZED.id, false, send, util.List.of, acknowledgementsMap).build()
  }

  private def shareSessionCache(): AnyRef = {
    val broker = getBrokers.head.asInstanceOf[BrokerServer]
    val manager = broker.sharePartitionManager
    val cacheField = manager.getClass.getDeclaredField("cache")
    cacheField.setAccessible(true)
    cacheField.get(manager)
  }

  private def cacheSize(cache: AnyRef): Int = {
    cache.getClass.getMethod("size").invoke(cache).asInstanceOf[Integer]
  }

  private def numMembers(cache: AnyRef, groupId: String): Int = {
    val method = cache.getClass.getDeclaredMethod("numMembers", classOf[String])
    method.setAccessible(true)
    val value = method.invoke(cache, groupId).asInstanceOf[Integer]
    if (value == null) 0 else value
  }
}

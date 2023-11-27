/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.metrics

import org.junit.jupiter.api.{AfterEach, Test}

class ClientMetricsCacheTest {

//  private var clientMetricsCache: ClientMetricsCache = _

  @AfterEach
  def cleanup(): Unit = {
//    ClientMetricsManager.instance().clearSubscriptionInfo()
//    clientMetricsCache = new ClientMetricsCache()
  }

  @Test
  def testClientMetricsSubscription(): Unit = {
    // create a client metric subscription.
//    val subscription1 = ClientMetricsTestUtils.createClientMetricsSubscription("cm_1")
//    assertNotNull(subscription1)

    // create a client instance state object and make sure it picks up the metrics from the previously created
    // metrics subscription.
//    val client = new ClientMetricsInstanceMetadata("testClient1", "clientId", "Java", "11.1.0.1", "", "")
//    val clientState = ClientMetricsTestUtils.createClientMetricsInstance(client)
//    val clientStateFromCache = ClientMetricsTestUtils.getClientMetricsInstance(clientState.clientInstanceId)
//    assertEquals(clientState, clientStateFromCache)
//    assertEquals(clientStateFromCache.subscriptions.size(), 1)
//    assertEquals(clientStateFromCache.pushIntervalMs, ClientMetricsTestUtils.DEFAULT_PUSH_INTERVAL_MS)
//    assertEquals(clientStateFromCache.metrics.size, 2)
//    assertTrue(clientStateFromCache.metrics.asScala.mkString(",").equals(ClientMetricsTestUtils.DEFAULT_METRICS))
  }

  @Test
  def testAddingSubscriptionsAfterClients(): Unit = {
    // create a client instance state object  when there are no client metrics subscriptions exists
//    val client = new ClientMetricsInstanceMetadata("testClient1", "clientId", "Java", "11.1.0.1", "", "")
//    val clientState = ClientMetricsTestUtils.createClientMetricsInstance(client)
//    val clientStateFromCache = ClientMetricsTestUtils.getClientMetricsInstance(clientState.clientInstanceId)
//    assertEquals(clientState, clientStateFromCache)
//    assertTrue(clientStateFromCache.subscriptions.isEmpty)
//    assertTrue(clientStateFromCache.metrics.isEmpty)
//    val oldSubscriptionId = clientStateFromCache.subscriptionId
//
//    // Now create a new client subscription and make sure the client instance is updated with the metrics.
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_1")
//    clientStateFromCache = ClientMetricsTestUtils.getClientMetricsInstance(clientState.clientInstanceId)
//    assertEquals(clientStateFromCache.subscriptions.size(), 1)
//    assertEquals(clientStateFromCache.pushIntervalMs, ClientMetricsTestUtils.DEFAULT_PUSH_INTERVAL_MS)
//    assertNotEquals(clientStateFromCache.subscriptionId, oldSubscriptionId)
//    assertEquals(clientStateFromCache.metrics.size, 2)
//    assertTrue(clientStateFromCache.metrics.asScala.mkString(",").equals(ClientMetricsTestUtils.DEFAULT_METRICS))
  }

  @Test
  def testAddingMultipleSubscriptions(): Unit = {
//    val props = new Properties()
//    val clientMatchPatterns = List(s"${ClientMetricsConfigs.CLIENT_SOFTWARE_NAME}=Java", s"${ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION}=8.1.*")
//    props.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, clientMatchPatterns.mkString(","))

    // TEST-1: CREATE new metric subscriptions and make sure client instance picks up those metrics.
//    val subscription1 = ClientMetricsTestUtils.createClientMetricsSubscription("cm_1")
//    val subscription2 = ClientMetricsTestUtils.createClientMetricsSubscription("cm_2", props)
//    assertNotNull(subscription1)
//    assertNotNull(subscription2)

    // create a client instance state object and make sure every thing is in correct order.
//    val client = new ClientMetricsInstanceMetadata("testClient1", "clientId", "Java", "11.1.0.1", "", "")
//    val clientState = ClientMetricsTestUtils.createClientMetricsInstance(client)
//    val clientStateFromCache = ClientMetricsTestUtils.getClientMetricsInstance(clientState.clientInstanceId)
//    assertEquals(clientState, clientStateFromCache)

//    val res = clientState.subscriptions
//    assertEquals(res.size(), 1)
//    assertTrue(res.contains(subscription1))
//    assertEquals(clientState.pushIntervalMs, ClientMetricsTestUtils.DEFAULT_PUSH_INTERVAL_MS)
//    assertEquals(clientState.metrics.size, 2)
//    assertTrue(clientState.metrics.asScala.mkString(",").equals(ClientMetricsTestUtils.DEFAULT_METRICS))

    // TEST-2: UPDATE the metrics subscription: Create update the metrics subscriptions by adding new
    // subscription with different metrics and make sure that client instance object is updated
    // with the new metric and new subscription id.
//    val metrics3 = "org.apache.kafka/client.producer.write.latency"
//    val props3 = new Properties()
//    props3.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, metrics3)
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_3", props3)
//    val afterAddingNewSubscription = ClientMetricsTestUtils.getClientMetricsInstance(clientState.clientInstanceId)
//    assertEquals(clientStateFromCache.clientInstanceId, afterAddingNewSubscription.clientInstanceId)
//    assertNotEquals(clientState.subscriptionId, afterAddingNewSubscription.subscriptionId)
//    assertEquals(afterAddingNewSubscription.metrics.size, 3)
//    assertTrue(afterAddingNewSubscription.metrics.asScala.mkString(",").equals(ClientMetricsTestUtils.DEFAULT_METRICS + "," + metrics3))
//
//    // TEST-3: UPDATE the first subscription's metrics and make sure
//    // client instance picked up the change.
//    val updated_metrics = "updated_metrics_for_clients"
//    val updatedProps = new Properties()
//    updatedProps.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, updated_metrics)
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_1", updatedProps)
//    val afterSecondUpdate = ClientMetricsTestUtils.getClientMetricsInstance(clientState.clientInstanceId)
//    assertEquals(afterSecondUpdate.clientInstanceId, afterAddingNewSubscription.clientInstanceId)
//    assertNotEquals(afterSecondUpdate.subscriptionId, afterAddingNewSubscription.subscriptionId)
//    assertEquals(afterSecondUpdate.metrics.size, 2)
//    assertTrue(afterSecondUpdate.metrics.asScala.mkString(",").equals(metrics3 + "," + updated_metrics))

    // TEST3: DELETE the metrics subscription: Delete the first subscription and make sure
    // client instance is updated
//    val props4 = new Properties()
//    props4.put(ClientMetricsConfig.ClientMetrics.DeleteSubscription, "true")
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_1", props4)
//
//    // subscription should have been deleted.
//    assertNull(ClientMetricsManager.instance().subscriptionInfo("cm_1"))

//    val afterDeleting = ClientMetricsTestUtils.getClientMetricsInstance(clientState.clientInstanceId)
//    assertEquals(afterAddingNewSubscription.clientInstanceId, afterDeleting.clientInstanceId)
//    assertNotEquals(afterAddingNewSubscription.subscriptionId, afterDeleting.subscriptionId)
//    assertEquals(afterAddingNewSubscription.subscriptions.size() - afterDeleting.subscriptions.size(), 1)
//    assertEquals(afterDeleting.metrics.size, 1)
//    assertTrue(afterDeleting.metrics.asScala.mkString(",").equals(metrics3))
  }

  @Test
  def testMultipleSubscriptionsAndClients(): Unit = {
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_1")

//    val metrics2 = "org.apache.kafka/client.producer.write.latency"
//    val props2 = new Properties()
//    props2.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, metrics2)
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_2", props2)
//
//    val props3 = new Properties()
//    val clientPatterns3 = List(s"${ClientMetricsConfigs.CLIENT_SOFTWARE_NAME}=Python", s"${ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION}=8.*")
//    val metrics3 = "org.apache.kafka/client.consumer.read.latency"
//    props3.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, metrics3)
//    props3.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, clientPatterns3.mkString(","))
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_3", props3)
//
//    val props4 = new Properties()
//    val clientPatterns4 = List(s"${ClientMetricsConfigs.CLIENT_SOFTWARE_NAME}=Python",
//                               s"${ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION}=8.*",
//                               s"${ClientMetricsConfigs.CLIENT_SOURCE_ADDRESS} = 1.2.3.4")
//    val metrics4 = "org.apache.kafka/client.consumer.*.latency"
//    props4.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, clientPatterns4.mkString(","))
//    props4.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, metrics4)
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_4", props4)
//    assertEquals(ClientMetricsManager.instance().getSubscriptionCount, 4)

//    val client1 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient1", "Id1", "Java", "11.1.0.1", "", ""))
//    val client2 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient2", "Id2", "Python", "8.2.1", "abcd", "0"))
//    val client3 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient3", "Id3", "C++", "12.1", "192.168.1.7", "9093"))
//    val client4 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient4", "Id4", "Java", "11.1", "1.2.3.4", "8080"))
//    val client5 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient2", "Id5", "Python", "8.2.1", "1.2.3.4", "0"))
//    assertEquals(ClientMetricsCache.getInstance.getSize, 5)
//
//    // Verifications:
//    // Client 1 should have the metrics from the subscription1 and subscription2
//    assertTrue(client1.metrics.mkString(",").equals(DefaultMetrics + "," + metrics2))
//
//    // Client 2 should have the subscription3 which is just default metrics
//    assertTrue(client2.metrics.mkString(",").equals(metrics3))
//
//    // client 3 should end up with nothing.
//    assertTrue(client3.metrics.isEmpty)
//
//    // Client 4 should have the metrics from subscription1 and subscription2
//    assertTrue(client4.metrics.mkString(",").equals(DefaultMetrics + "," + metrics2))
//
//    // Client 5 should have the metrics from subscription-3 and subscription-4
//    assertTrue(client5.metrics.mkString(",").equals(metrics3 + "," + metrics4))
  }

  @Test
  def testMultipleClientsAndSubscriptions(): Unit = {
    // Create the Client instances first
//    val client1 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("t1", "c1", "Java", "11.1.0.1", "", "")).getId
//    val client2 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("t2", "c2", "Python", "8.2.1", "abcd", "0")).getId
//    val client3 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("t3", "c3", "C++", "12.1", "192.168.1.7", "9093")).getId
//    val client4 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("t4", "c4", "Java", "11.1", "1.2.3.4", "8080")).getId
//    val client5 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("t5", "c5", "Python", "8.2.1", "1.2.3.4", "0")).getId
//    assertEquals(ClientMetricsCache.getInstance.getSize, 5)

    // Now create the subscriptions.
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_1")

//    val metrics2 = "org.apache.kafka/client.producer.write.latency"
//    val props2 = new Properties()
//    props2.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics2)
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_2", props2)
//
//    val props3 = new Properties()
//    val clientPatterns3 = List(s"${CLIENT_SOFTWARE_NAME}=Python", s"${CLIENT_SOFTWARE_VERSION}=8.*")
//    val metrics3 = "org.apache.kafka/client.consumer.read.latency"
//    props3.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics3)
//    props3.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns3.mkString(","))
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_3", props3)
//
//    val props4 = new Properties()
//    val clientPatterns4 = List(s"${CLIENT_SOFTWARE_NAME}=Python",
//                               s"${CLIENT_SOFTWARE_VERSION}=8.*",
//                               s"${CLIENT_SOURCE_ADDRESS} = 1.2.3.4")
//    val metrics4 = "org.apache.kafka/client.consumer.*.latency"
//    props4.put(ClientMetricsConfig.ClientMetrics.ClientMatchPattern, clientPatterns4.mkString(","))
//    props4.put(ClientMetricsConfig.ClientMetrics.SubscriptionMetrics, metrics4)
//    ClientMetricsTestUtils.createClientMetricsSubscription("cm_4", props4)
//    assertEquals(ClientMetricsConfig.getSubscriptionsCount, 4)

    // Verifications:
    // Client 1 should have the metrics from subscription1 and subscription2
//    assertTrue(ClientMetricsTestUtils.getClientMetricsInstance(client1).metrics.mkString(",").equals(DefaultMetrics + "," + metrics2))
//
//    // Client 2 should have the subscription3 which is just default metrics
//    assertTrue(ClientMetricsTestUtils.getClientMetricsInstance(client2).metrics.mkString(",").equals(metrics3))
//
//    // client 3 should end up with nothing.
//    assertTrue(ClientMetricsTestUtils.getClientMetricsInstance(client3).metrics.isEmpty)
//
//    // Client 4 should have the metrics from subscription1 and subscription2
//    assertTrue(ClientMetricsTestUtils.getClientMetricsInstance(client4).metrics.mkString(",").equals(DefaultMetrics + "," + metrics2))
//
//    // Client 5 should have the metrics from subscription-3 and subscription-4
//    assertTrue(ClientMetricsTestUtils.getClientMetricsInstance(client5).metrics.mkString(",").equals(metrics3 + "," + metrics4))
  }


//  @Test
//  def testCleanupTtlEntries(): Unit = {
//    val cache = ClientMetricsCache.getInstance
//    val client1 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient1", "clientId1", "Java", "11.1.0.1", "", ""))
//    val client2 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient2", "clientId2", "Python", "8.2.1", "", ""))
//    val client3 = ClientMetricsTestUtils.createClientMetricsInstance(ClientMetricsInstanceMetadata("testClient3", "clientId3", "C++", "12.1", "", ""))
//    assertEquals(cache.getSize, 3)
//
//    // Modify client3's timestamp to meet the TTL expiry limit.
//    val ts = client3.getLastAccessTs - (Math.max(3 * client3.getPushIntervalMs, ClientMetricsCache.getTtlTs) + 10)
//    client3.updateLastAccessTs(ts)
//    ClientMetricsCache.setLastCleanupTs(ClientMetricsCache.getLastCleanupTs - (ClientMetricsCache.getCleanupInterval + 10))
//
//    // Run the GC and wait until client3 entry is removed from the cache
////    ClientMetricsCache.deleteExpiredEntries(true)
//    TestUtils.waitUntilTrue(() => ClientMetricsCache.getInstance.getSize == 2, "Failed to run GC on Client Metrics Cache", 6000)
//
//    // Make sure that client3 is removed from the cache.
//    assertTrue(cache.get(client3.getId).isEmpty)
//
//    // client1 and client2 should remain in the cache.
//    assertTrue(!cache.get(client1.getId).isEmpty)
//    assertTrue(!cache.get(client2.getId).isEmpty)
//  }
}

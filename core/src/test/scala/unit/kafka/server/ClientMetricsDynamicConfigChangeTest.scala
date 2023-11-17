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

package unit.kafka.server

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class ClientMetricsDynamicConfigChangeTest extends KafkaServerTestHarness {
  def generateConfigs = List(KafkaConfig.fromProps(TestUtils.createBrokerConfig(0, zkConnectOrNull)))

//  private def createAdminClient(): Admin = {
//    val props = new Properties()
//    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers())
//    Admin.create(props)
//  }

//  private def updateClientSubscription(subscriptionId :String,
//                                       properties: Properties,
//                                       waitingCondition: () => Boolean): Unit = {
//    val admin = createAdminClient()
//    try {
//      val resource = new ConfigResource(ConfigResource.Type.CLIENT_METRICS, subscriptionId)
//      val entries = new ListBuffer[AlterConfigOp]
//      properties.forEach((k, v) => {
//        entries.append(new AlterConfigOp(new ConfigEntry(k.toString, v.toString), AlterConfigOp.OpType.SET))
//      })
//      admin.incrementalAlterConfigs(Map(resource -> entries.asJavaCollection).asJava).all.get
//    } finally {
//      admin.close()
//    }
//
//    // Wait until notification is delivered and processed
//    val maxWaitTime = 60 * 1000 // 1 minute wait time
//    TestUtils.waitUntilTrue(waitingCondition, "Failed to update the client metrics subscription", maxWaitTime)
//  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testClientMetricsConfigChange(quorum: String): Unit = {
//    val configEntityName: String = "subscription-2"
//    val metrics =
//      "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
//    val pushInterval = 30 * 1000 // 30 seconds
//    val pattern1 = ClientMetricsConfigs.CLIENT_SOFTWARE_NAME + " = Java       "
//    val pattern2 = ClientMetricsConfigs.CLIENT_SOFTWARE_VERSION + " =     11.*   "
//    val patternsList = List(pattern1, pattern2)
//
//    val props = ClientMetricsTestUtils.defaultProperties
//
//    // ********  Test Create the new client subscription with multiple client matching patterns *********
//    updateClientSubscription(configEntityName, props,
//      () =>  ClientMetricsManager.instance().subscriptionInfo(configEntityName) != null)
//    val cmSubscription = ClientMetricsManager.instance().subscriptionInfo(configEntityName)
//    assertTrue(cmSubscription.intervalMs() == pushInterval)
//    assertTrue(cmSubscription.metrics().size == 2 &&
//      cmSubscription.metrics().asScala.mkString(",").equals(metrics))
//    assertTrue(cmSubscription.matchPattern().size == patternsList.size)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  def testClientMetricsConfigMultipleUpdates(quorum: String): Unit = {
//    val configEntityName: String = "subscription-1"
//    val metrics =
//      "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
//    val clientMatchingPattern = "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538"
//    val pushInterval = 30 * 1000 // 30 seconds
//
//    val props = ClientMetricsTestUtils.defaultProperties
//    props.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, clientMatchingPattern)
//
//    // ********  Test-1 Create the new client subscription *********
//    updateClientSubscription(configEntityName, props,
//      () => ClientMetricsManager.instance().subscriptionInfo(configEntityName) != null)
//    val cmSubscription = ClientMetricsManager.instance().subscriptionInfo(configEntityName)
//    assertTrue(cmSubscription.intervalMs() == pushInterval)
//    assertTrue(cmSubscription.metrics().size == 2 &&
//      cmSubscription.metrics().asScala.mkString(",").equals(metrics))
//    val res = cmSubscription.matchPattern.asScala.mkString(",").replace(" -> ", "=")
//    assertTrue(cmSubscription.matchPattern.size == 1 && res.equals(clientMatchingPattern))
//
//    // *******  Test-2 Update the existing metric subscriptions  *********
//    val updatedMetrics = "org.apache.kafka/client.producer.partition.latency"
//    props.put(ClientMetricsConfigs.SUBSCRIPTION_METRICS, updatedMetrics)
//    updateClientSubscription(configEntityName, props,
//      () => ClientMetricsManager.instance().subscriptionInfo(configEntityName).metrics().size == 1)
//    assertTrue(ClientMetricsManager.instance().subscriptionInfo(configEntityName)
//      .metrics().asScala.mkString(",").equals(updatedMetrics))

    // *******  Test-3 Delete the metric subscriptions  *********
//    props.put(ClientMetricsConfig.ClientMetrics.DeleteSubscription, "true")
//    updateClientSubscription(configEntityName, props,
//      () => ClientMetricsManager.instance().subscriptionInfo(configEntityName) == null)
//    assertTrue(ClientMetricsManager.instance().subscriptionInfo(configEntityName) == null)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk"))
  // TODO: needs to figure out how to run the same test in kraft mode.
  def testClientMetricsRestartServer(quorum: String): Unit = {
//    val configEntityName: String = "subscription-1"
//    val metrics =
//      "org.apache.kafka/client.producer.partition.queue.,org.apache.kafka/client.producer.partition.latency"
//    val clientMatchingPattern = "client_instance_id=b69cc35a-7a54-4790-aa69-cc2bd4ee4538"
//    val pushInterval = 30 * 1000 // 30 seconds
//
//    val props = ClientMetricsTestUtils.defaultProperties
//    props.put(ClientMetricsConfigs.CLIENT_MATCH_PATTERN, clientMatchingPattern)
//
//    // Create the new client subscription
//    updateClientSubscription(configEntityName, props,
//      () => ClientMetricsManager.instance().subscriptionInfo(configEntityName) != null)
//    val cmSubscription = ClientMetricsManager.instance().subscriptionInfo(configEntityName)
//    assertTrue(cmSubscription.intervalMs() == pushInterval)
//    assertTrue(cmSubscription.metrics().size == 2 &&
//      cmSubscription.metrics().asScala.mkString(",").equals(metrics))
//    val res = cmSubscription.matchPattern.asScala.mkString(",").replace(" -> ", "=")
//    assertTrue(cmSubscription.matchPattern.size == 1 && res.equals(clientMatchingPattern))
//
//    // Restart the server and make sure that client metric subscription data is intact
//    ClientMetricsManager.instance().clearSubscriptionInfo()
////    assertTrue(ClientMetricsManager.instance().getSubscriptionCount == 0)
//    val server = servers.head
//    server.shutdown()
//    server.startup()
////    assertTrue(ClientMetricsManager.instance().getSubscriptionCount != 0)
//    assertTrue(ClientMetricsManager.instance().subscriptionInfo(configEntityName) != null)
//    assertTrue(ClientMetricsManager.instance().subscriptionInfo(configEntityName)
//      .metrics().asScala.mkString(",").equals(metrics))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  // TODO: needs to figure out how to run the same test in kraft mode.
  def testAllMetrics(quorum: String): Unit = {
//    val configEntityName: String = "subscription-4"
//    val pushInterval = 5 * 1000 // 5 seconds
//
//    val props = new Properties()
////    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")
//    props.put(ClientMetricsConfigs.PUSH_INTERVAL_MS, pushInterval.toString)
//
//    // ********  Test Create the new client subscription with all metrics *********
//    updateClientSubscription(configEntityName, props,
//      () =>  ClientMetricsManager.instance().subscriptionInfo(configEntityName) != null)
//    val cmSubscription = ClientMetricsManager.instance().subscriptionInfo(configEntityName)
//    assertTrue(cmSubscription.intervalMs() == pushInterval)
////    assertTrue(cmSubscription.getAllMetricsSubscribed)
//    assertTrue(cmSubscription.metrics().size == 1)
//    assertTrue(cmSubscription.metrics().asScala.mkString(",").equals(""))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("zk", "kraft"))
  // TODO: needs to figure out how to run the same test in kraft mode.
  def testClientMetricsDefaults(quorum: String): Unit = {
//    val configEntityName: String = "subscription-5"
//
//    // We create a mostly blank properties object. This should work and will assume all metrics,
//    // all clients (no filtering), and a default push interval.
//    val props = new Properties()
////    props.put(ClientMetricsConfig.ClientMetrics.AllMetricsFlag, "true")
//
//    // ********  Test Create the new client subscription with all metrics *********
//    updateClientSubscription(configEntityName, props,
//      () =>  ClientMetricsManager.instance().subscriptionInfo(configEntityName) != null)
//    val cmSubscription = ClientMetricsManager.instance().subscriptionInfo(configEntityName)
//    assertTrue(cmSubscription.matchPattern.isEmpty)
//    assertTrue(cmSubscription.intervalMs() == ClientMetricsConfigs.DEFAULT_INTERVAL_MS)
////    assertTrue(cmSubscription.getAllMetricsSubscribed)
//    assertTrue(cmSubscription.metrics().size == 1)
//    assertTrue(cmSubscription.metrics().asScala.mkString(",").equals(""))
  }

}

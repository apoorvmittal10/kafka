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

import ClientMetricsConfig1.ClientMatchingParams.{CLIENT_SOFTWARE_NAME, CLIENT_SOFTWARE_VERSION}
import ClientMetricsConfig1.SubscriptionInfo
import kafka.server.ClientMetricsManager
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.server.authorizer.AuthorizableRequestContext
import org.apache.kafka.server.telemetry.{ClientTelemetryPayload, ClientTelemetryReceiver}

import java.nio.ByteBuffer
import java.util.Properties

object ClientMetricsTestUtils {
  val DefaultPushIntervalMs: Int = 30 * 1000 // 30 seconds

  val DefaultMetrics =
    "org.apache.kafka.client.producer.partition.queue.,org.apache.kafka.client.producer.partition.latency"

  val DefaultClientMatchPatters =
    List(s"${CLIENT_SOFTWARE_NAME}=Java", s"${CLIENT_SOFTWARE_VERSION}=11.1.*")

  val cmInstance :ClientMetricsManager = ClientMetricsManager.getInstance
  def getCM = cmInstance

  def updateClientSubscription(subscriptionId :String, properties: Properties): Unit = {
    getCM.updateSubscription(subscriptionId, properties)
  }

  def getClientInstance(id: Uuid): ClientMetricsInstanceState = {
    //    getCM.getClientInstance(id).get
    null
  }

  def createClientInstance(selector: ClientMetricsMetadata): ClientMetricsInstanceState = {
    //    getCM.createClientInstance(Uuid.randomUuid(), selector)
    null
  }

  def getDefaultProperties: Properties = {
    val props = new Properties()
    props.put(ClientMetricsConfig1.ClientMetrics.SubscriptionMetrics, DefaultMetrics)
    props.put(ClientMetricsConfig1.ClientMetrics.ClientMatchPattern, DefaultClientMatchPatters.mkString(","))
    props.put(ClientMetricsConfig1.ClientMetrics.PushIntervalMs, DefaultPushIntervalMs.toString)
    props
  }

  def createCMSubscription(subscriptionId: String, overrideProps: Properties = null): SubscriptionInfo = {
    val props = getDefaultProperties
    if (overrideProps != null) {
      overrideProps.entrySet().forEach(x => props.put(x.getKey, x.getValue))
    }
    updateClientSubscription(subscriptionId, props)
    ClientMetricsConfig1.getClientSubscriptionInfo(subscriptionId)
  }

  class TestClientMetricsReceiver extends ClientTelemetryReceiver {
    var exportMetricsInvoked = 0
    var metricsData: ByteBuffer = null

    def getMetricsDataAsCopy() : ByteBuffer = {
      import java.nio.ByteBuffer
      val data = ByteBuffer.allocate(metricsData.capacity)
      metricsData.flip()
      data.put(metricsData)
      data
    }

    def exportMetrics(context: AuthorizableRequestContext, payload: ClientTelemetryPayload) = {
      exportMetricsInvoked += 1
//      metricsData = payload.data()
    }
  }

  def setupClientMetricsPlugin(): TestClientMetricsReceiver = {
    val plugin = new TestClientMetricsReceiver
    ClientMetricsReceiverPlugin.add(plugin)
    plugin
  }

  def getSerializedMetricsData(compressionType: CompressionType, metrics: Map[String, Int]): (ByteBuffer, String) = {
//    def newMetricName(name: String) = new MetricName(name, "g_" + name, "desc_" + name, Collections.emptyMap())
//    def newTelemetryMetric(metricName: MetricName,  value: Long) = new TelemetryMetric(metricName, MetricType.sum, value)
//    val telemetryMetrics = new util.ArrayList[TelemetryMetric]
//    val metricsStr = new StringBuilder()
//    for ((k, v) <- metrics) {
//     val name = newMetricName(k)
//      val metric = newTelemetryMetric(name, v)
//      val tmpStr = name.toString + ": " + v + "\n"
//      metricsStr.append(tmpStr)
//      telemetryMetrics.add(metric)
//    }
//    val telemetrySerializer = new StringTelemetryMetricConverter
//    val buffer = ClientTelemetryUtils.serialize(telemetryMetrics, compressionType, telemetrySerializer)
    (ByteBuffer.allocate(10), "change me")
  }
}

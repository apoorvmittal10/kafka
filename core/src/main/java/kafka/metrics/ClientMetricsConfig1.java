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
package kafka.metrics;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.InvalidRequestException;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client metric configuration related parameters and the supporting methods like validation and update methods
 * are defined in this class.
 * <p>
 * SubscriptionInfo: Contains the client metric subscription information. Supported operations from the CLI are
 * add/delete/update operations. Every subscription object contains the following parameters that are populated
 * during the creation of the subscription.
 * <p>
 * {
 * <ul>
 *   <li> subscriptionId: Name/ID supplied by CLI during the creation of the client metric subscription.
 *   <li> subscribedMetrics: List of metric prefixes
 *   <li> pushIntervalMs: A positive integer value >=0  tells the client that how often a client can push the metrics
 *   <li> matchingPatternsList: List of client matching patterns, that are used by broker to match the client instance
 *   with the subscription.
 * </ul>
 * }
 * <p>
 * At present, CLI can pass the following parameters in request to add/delete/update the client metrics
 * subscription:
 * <ul>
 * <li> "metrics" value should be comma separated metrics list. A prefix match on the requested metrics
 *      is performed in clients to determine subscribed metrics. An empty list means no metrics subscribed.
 *      A list containing just an empty string means all metrics subscribed.
 *      Ex: "org.apache.kafka.producer.partition.queue.,org.apache.kafka.producer.partition.latency"
 *
 * <li> "interval.ms" should be between 100 and 3600000 (1 hour). This is the interval at which the client
 *      should push the metrics to the broker.
 *
 * <li> "match" is a comma separated list of client match patterns, in case if there is no matching
 *      pattern specified then broker considers that as all match which means the associated metrics
 *      applies to all the clients. Ex: "client_software_name = Java, client_software_version = 11.1.*"
 *      which means all Java clients with any sub versions of 11.1 will be matched i.e. 11.1.1, 11.1.2 etc.
 *
 * </ul>
 * For more information please look at kip-714:
 * https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability#KIP714:Clientmetricsandobservability-Clientmetricsconfiguration
 */
public class ClientMetricsConfig1 {

  private final Properties props;
  private final ConcurrentHashMap<String, SubscriptionInfo> subscriptionMap;

  public ClientMetricsConfig1(Properties props) {
    this.props = props;
    this.subscriptionMap = new ConcurrentHashMap<>();
  }

  public SubscriptionInfo getSubscriptionInfo(String subscriptionName) {
    return subscriptionMap.get(subscriptionName);
  }

  public void clearSubscriptionInfo() {
    subscriptionMap.clear();
  }

  public int getSubscriptionCount() {
    return subscriptionMap.size();
  }

  public Collection<SubscriptionInfo> getSubscriptions() {
    return subscriptionMap.values();
  }

  public void updateSubscriptionInfo(String subscriptionName, Properties properties) {
    Map<String, Object> parsed = ClientMetrics.configDef.parse(properties);


    boolean subscriptionDeleted = false;
    if (subscriptionDeleted) {
      SubscriptionInfo deletedSubscription = subscriptionMap.remove(subscriptionName);
      ClientMetricsCache.getInstance().invalidate(deletedSubscription, null);
    } else {
      List<String> clientMatchPattern = toList(parsed.get(MATCH));
      int pushInterval = parsed.get(INTERVAL_MS);
//      val allMetricsSubscribed = parsed.getOrDefault(AllMetricsFlag, javaFalse).asInstanceOf[Boolean]
      boolean allMetricsSubscribed = false;
      val metrics = if (allMetricsSubscribed) List("") else toList(parsed.get(SubscriptionMetrics));
      SubscriptionInfo newSubscription =
          new SubscriptionInfo(subscriptionId, metrics, pushInterval, clientMatchPattern);
      SubscriptionInfo oldSubscription = subscriptionMap.put(subscriptionId, newSubscription)
      ClientMetricsCache.getInstance().invalidate(oldSubscription, newSubscription);
    }
  }

  private List<String> toList(Properties properties) {
    return Arrays.asList(properties.getProperty(ClientMetrics.METRICS).split(","));
  }

  public void validateConfig(String subscriptionName, Properties properties) {
    ClientMetrics.validate(subscriptionName, properties);
  }

  public static class ClientMetrics {

    private static final String METRICS = "metrics";
    private static final String MATCH = "match";
    private static final String INTERVAL_MS = "interval.ms";

    public static final ConfigDef configDef = new ConfigDef()
        .define(METRICS, Type.LIST, Importance.MEDIUM, "Subscription metrics list")
        .define(MATCH, Type.LIST, Importance.MEDIUM, "Client match pattern list")
        .define(INTERVAL_MS, Type.INT, Importance.MEDIUM, "Push interval in milliseconds");

    public static final Set<String> names() {
      return configDef.names();
    }

    public static void validate(String subscriptionName, Properties properties) {
      if (subscriptionName == null || subscriptionName.isEmpty()) {
        throw new InvalidRequestException("Subscription name is required");
      }
      configDef.validate(properties);
      validateProperties(properties);
    }

    private static void validateProperties(Properties properties) {
      // Make sure that all the properties are valid
      properties.forEach((key, value) -> {
        if (!names().contains(key)) {
          throw new InvalidRequestException("Unknown client metrics configuration: " + key);
        }
      });

      // Make sure that push interval is between 100ms and 1 hour.
      if (properties.containsKey(INTERVAL_MS)) {
        int pushIntervalMs = Integer.parseInt(properties.getProperty(INTERVAL_MS));
        if (pushIntervalMs < 100 || pushIntervalMs > 3600000) {
          throw new InvalidRequestException("Interval must be between 100ms and 3600000 (1 hour)");
        }
      }

      // Make sure that client match patterns are valid by parsing them.
      if (properties.containsKey(MATCH)) {
        List<String> clientMatchPattern = properties.get(MATCH);
        for (String pattern : clientMatchPattern) {
          ClientMatchPattern.parse(pattern);
        }
      }
    }
  }

  public static class ClientMatchingParams {

      private final String clientId = "client_id";
      private final String clientInstanceId = "client_instance_id";
      private final String clientSoftwareName = "client_software_name";
      private final String clientSoftwareVersion = "client_software_version";
      private final String clientSourceAddress = "client_source_address";
      private final String clientSourcePort = "client_source_port";

      private final Set<String> MATCHERS_SET = new HashSet<>(Arrays.asList(clientId, clientInstanceId,
          clientSoftwareName, clientSoftwareVersion, clientSourceAddress, clientSourcePort));

      public boolean isValidParams(String param) {
        return MATCHERS_SET.contains(param);
      }
  }

  public static class SubscriptionInfo {

    private final String id;
    private final List<String> metrics;
    private final int intervalMs;
    private final List<String> match;

    public SubscriptionInfo(String id, List<String> metrics, int intervalMs,
        List<String> match) {
      this.id = id;
      this.metrics = metrics;
      this.intervalMs = intervalMs;
      this.match = match;
    }

    public String id() {
      return id;
    }

    public List<String> metrics() {
      return metrics;
    }

    public int intervalMs() {
      return intervalMs;
    }

    public List<String> match() {
      return match;
    }
  }
}

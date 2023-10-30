package kafka.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import kafka.metrics.ClientMetricsConfig1.SubscriptionInfo;
import org.apache.kafka.common.Uuid;

public class ClientMetricsInstanceState1 {

  private Uuid clientInstanceId;
  private ClientMetricsMetadata clientInfo;
  private Collection<SubscriptionInfo> subscriptions;
  private List<String> metrics;
  private int pushIntervalMs;
  private boolean allMetricsSubscribed;
  private long lastAccessTs;
  private int subscriptionId;
  private boolean terminating;

  public ClientMetricsInstanceState(Uuid clientInstanceId, ClientMetricsMetadata clientInfo,
      Collection<SubscriptionInfo> subscriptions, List<String> metrics,
      int pushIntervalMs, boolean allMetricsSubscribed) {
    this.clientInstanceId = clientInstanceId;
    this.clientInfo = clientInfo;
    this.subscriptions = subscriptions;
    this.metrics = metrics;
    this.pushIntervalMs = pushIntervalMs;
    this.allMetricsSubscribed = allMetricsSubscribed;
    this.lastAccessTs = getCurrentTime();
    this.subscriptionId = computeSubscriptionId();
    this.terminating = false;
  }

  public int getPushIntervalMs() {
    return pushIntervalMs;
  }

  public long getLastAccessTs() {
    return lastAccessTs;
  }

  public int getSubscriptionId() {
    return subscriptionId;
  }

  public Uuid getId() {
    return clientInstanceId;
  }

  public ClientMetricsMetadata getClientInfo() {
    return clientInfo;
  }

  public Collection<SubscriptionInfo> getSubscriptions() {
    return subscriptions;
  }

  public List<String> getMetrics() {
    return metrics;
  }

  public boolean getAllMetricsSubscribed() {
    return allMetricsSubscribed;
  }

  public boolean isClientTerminating() {
    return terminating;
  }

  public void updateLastAccessTs(long tsInMs) {
    this.lastAccessTs = tsInMs;
  }

  public void setTerminatingFlag(boolean f) {
    this.terminating = f;
  }

  // Whenever push-interval for a client is set to 0 means metric collection for this specific client is disabled.
  public boolean isDisabledForMetricsCollection() {
    return getPushIntervalMs() == 0;
  }

  // Computes the SubscriptionId as a unique identifier for a client instance's subscription set, the id is generated
// by calculating a CRC32 of the configured metrics subscriptions including the PushIntervalMs,
// XORed with the ClientInstanceId.
  private int computeSubscriptionId() {
    CRC32 crc = new CRC32();
    String metricsStr = metrics.toString() + pushIntervalMs.toString();
    crc.update(metricsStr.getBytes(StandardCharsets.UTF_8));
    return (int) crc.getValue() ^ clientInstanceId.hashCode();
  }

  public boolean canAcceptPushRequest() {
    // A fix is required to support the initial request jitter which may up to half the
    // configured push interval.
    //
    // See https://confluentinc.atlassian.net/browse/CLIENTS-2418
    // int timeElapsedSinceLastMsg = getCurrentTime() - getLastAccessTs();
    // return timeElapsedSinceLastMsg >= getPushIntervalMs();
    return true;
    //(timeElapsedSinceLastMsg >= getPushIntervalMs()) || (clientTerminatingFlagSet && !this.isClientTerminating());
  }

  // Returns the current push interval if timeElapsed since last message > configured pushInterval
// otherwise, returns the delta (pushInterval - timeElapsedSinceLastMsg)
  public int getAdjustedPushInterval() {
    int timeElapsedSinceLastMsg = (int) (getCurrentTime() - getLastAccessTs());
    if (timeElapsedSinceLastMsg < getPushIntervalMs())
      return getPushIntervalMs() - timeElapsedSinceLastMsg;
    else
      return getPushIntervalMs();
  }

  public ClientMetricsInstanceState1 apply(ClientMetricsInstanceState instance, java.util.Collection<SubscriptionInfo> subscriptions) {
    ClientMetricsInstanceState newInstance = create(instance.getId(), instance.getClientInfo(), subscriptions);
    newInstance.updateLastAccessTs(instance.getLastAccessTs());
    return newInstance;
  }

  public ClientMetricsInstanceState1 apply(Uuid id, ClientMetricsMetadata clientInfo, java.util.Collection<SubscriptionInfo> subscriptions) {
    return create(id, clientInfo, subscriptions);
  }

  private ClientMetricsInstanceState1 create(Uuid id, ClientMetricsMetadata clientInfo, java.util.Collection<SubscriptionInfo> subscriptions) {
    List<String> targetMetrics = new ArrayList<>();
    int pushInterval = DEFAULT_INTERVAL_MS;
    java.util.ArrayList<SubscriptionInfo> targetSubscriptions = new java.util.ArrayList<>();
    boolean allMetricsSubscribed = false;

    subscriptions.forEach(v -> {
      if (clientInfo.isMatched(v.getClientMatchingPatterns())) {
        allMetricsSubscribed = allMetricsSubscribed || v.isAllMetricsSubscribed();
        targetMetrics.addAll(v.getSubscribedMetrics());
        targetSubscriptions.add(v);
        pushInterval = Math.min(pushInterval, v.getPushIntervalMs());
      }
    });

    if (pushInterval == 0) {
      info(s"Metrics collection is disabled for the client: ${id.toString()}");
      targetMetrics.clear();
    } else if (allMetricsSubscribed) {
      targetMetrics.clear();
      targetMetrics.add("");
    }

    return new ClientMetricsInstanceState(id, clientInfo, targetSubscriptions, targetMetrics, pushInterval, allMetricsSubscribed);
  }

}

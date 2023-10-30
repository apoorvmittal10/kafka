package kafka.metrics;

import java.util.UUID;
import kafka.metrics.ClientMetricsCache.ClientMetricsCacheValue;
import org.apache.kafka.common.cache.LRUCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client Metrics Cache:
 *   Standard LRU Cache of the ClientInstanceState objects that are created during to the client's
 *   GetTelemetrySubscriptionsRequest message.
 *
 *   Eviction Policy:
 *      1. Standard LRU eviction policy applies once cache size reaches its max size.
 *      2. In addition to the LRU eviction there is a GC for the elements that have stayed too long
 *      in the cache. There is a last accessed timestamp is set for every cached object which gets
 *      updated every time a cache object is accessed by GetTelemetrySubscriptionsRequest or
 *      PushTelemetrySubscriptionRequest. During the GC, all the elements that are inactive beyond
 *      TTL time period would be cleaned up from the cache. GC operation is an asynchronous task
 *      triggered by ClientMetricManager in specific intervals governed by CM_CACHE_GC_INTERVAL.
 *
 *   Invalidation of the Cached objects:
 *      Since ClientInstanceState objects are created by compiling qualified client metric subscriptions
 *      they can go out of sync whenever client metric subscriptions changed like adding a new subscriptions or
 *      updating an existing subscription. So there is a need to invalidate the cached objects whenever
 *      client metric subscription is updated. Invalidation method iterates through all the matched client
 *      instances and applies the subscription changes by replacing it with a new ClientInstanceState object.
 *
 *   Locking:
 *      All the cache modifiers (add/delete/replace) are synchronized through standard scala object
 *      level synchronized method. For better concurrency there is no explicit locking applied on
 *      read/get operations.
 */
public class ClientMetricsCache1 {

  private static final Logger log = LoggerFactory.getLogger(ClientMetricsCache1.class);
  private static final long DEFAULT_TTL_MS = 60 * 1000;  // One minute
  private static final long CM_CACHE_CLEANUP_INTERVAL_MS = 5 * 60 * 1000; // 5 minutes
  private static final int CM_CACHE_MAX_SIZE = 16384; // Max cache size (16k active client connections per broker)
  private static long lastCleanupTs = getCurrentTime();
  private static final ClientMetricsCache1 cmCache = new ClientMetricsCache1(CM_CACHE_MAX_SIZE);
  private LRUCache<UUID, ClientMetricsCacheValue> _cache = new LRUCache<>(maxSize);

  public static ClientMetricsCache getInstance() {
    return cmCache;
  }

  public static long getTtlTs() {
    return DEFAULT_TTL_MS;
  }

  public static long getCleanupInterval() {
    return CM_CACHE_CLEANUP_INTERVAL_MS;
  }

  public static long getLastCleanupTs() {
    return lastCleanupTs;
  }

  public static void setLastCleanupTs(long ts) {
    lastCleanupTs = ts;
  }

  /**
   * Launches the asynchronous task to clean the client metric subscriptions that are expired in the cache.
   */
  public static void deleteExpiredEntries(boolean force) {
    synchronized (ClientMetricsCache.class) {
      long timeElapsed = getCurrentTime() - lastCleanupTs;
      if (force || cmCache.getSize() > CM_CACHE_MAX_SIZE && timeElapsed > CM_CACHE_CLEANUP_INTERVAL_MS) {
        setLastCleanupTs(getCurrentTime());
        cmCache.cleanupExpiredEntries("GC").thenAccept(value -> info("Client Metrics subscriptions cache cleaned up " + value + " entries"))
            .exceptionally(e -> {
              error("Client Metrics subscription cache cleanup failed: " + e.getMessage());
              return null;
            });
      }
    }
  }

  public int getSize() {
    return _cache.size();
  }

  public void clear() {
    _cache.clear();
  }

  public Optional<ClientMetricsInstanceState> get(UUID id) {
    Optional<ClientMetricsCacheValue> value = Optional.ofNullable(_cache.get(id));
    if (value.isPresent()) {
      return Optional.ofNullable(value.get().getClientInstance());
    } else {
      return Optional.empty();
    }
  }

  /**
   * Iterates through all the elements of the cache and updates the client instance state objects that
   * matches the client subscription that is being updated.
   * @param oldSubscriptionInfo -- Subscription that has been deleted from the client metrics subscription
   * @param newSubscriptionInfo -- subscription that has been added to the client metrics subscription
   */
  public void invalidate(SubscriptionInfo oldSubscriptionInfo, SubscriptionInfo newSubscriptionInfo) {
    synchronized (_cache) {
      update(oldSubscriptionInfo, newSubscriptionInfo);
    }
  }

  public void add(ClientMetricsInstanceState instance) {
    synchronized (_cache) {
      _cache.put(instance.getId(), new ClientMetricsCacheValue(instance));
    }
  }

  public void remove(UUID id) {
    synchronized (_cache) {
      _cache.remove(id);
    }
  }

  public Future<Long> cleanupExpiredEntries(String reason) {
    return Future.supplyAsync(() -> {
      synchronized (_cache) {
        int preCleanupSize = _cache.size();
        cmCache.cleanupTtlEntries();
        return (long) (preCleanupSize - _cache.size());
      }
    });
  }

  ///////// **** PRIVATE - METHODS **** /////////////
  private void update(SubscriptionInfo oldSubscription, SubscriptionInfo newSubscription) {
    _cache.entrySet().forEach(element -> {
      ClientInstance clientInstance = element.getValue().getClientInstance();
      Set<SubscriptionInfo> updatedMetricSubscriptions = clientInstance.getSubscriptions();
      if (oldSubscription != null && clientInstance.getClientInfo().isMatched(Optional.ofNullable(oldSubscription.getClientMatchingPatterns()))) {
        updatedMetricSubscriptions.remove(oldSubscription);
      }
      if (newSubscription != null && clientInstance.getClientInfo().isMatched(Optional.ofNullable(newSubscription.getClientMatchingPatterns()))) {
        updatedMetricSubscriptions.add(newSubscription);
      }
      element.getValue().replace(new ClientMetricsInstanceState(clientInstance, updatedMetricSubscriptions));
    });
  }

  private boolean isExpired(ClientMetricsInstanceState element) {
    long delta = getCurrentTime() - element.getLastAccessTs();
    return delta > Math.max(3 * element.getPushIntervalMs(), DEFAULT_TTL_MS);
  }

  private void cleanupTtlEntries() {
    Iterator<Map.Entry<String, ClientMetricsInstanceState>> iter = _cache.entrySet().iterator();
    while (iter.hasNext()) {
      ClientMetricsInstanceState element = iter.next().getValue().getClientInstance();
      if (isExpired(element)) {
        debug("Client subscription entry " + element + " is expired removing it from the cache");
        iter.remove();
      }
    }
  }

  /**
   * Wrapper class to hold the CmClientInstance object and helps in preserving the LRU order.
   *
   * Background:
   * Whenever client metrics subscription is added/updated that results in updating the
   * affected client instance state objects to absorb the new changes, that's done by replacing
   * the old client instance with the new instance object as explained in invalidate method.
   * If we directly replace the cached object that would alter the LRU order, so having the wrapper
   * will allow us to replace the client instance object in the wrapper itself without changing
   * the LRU order.
   * @param
   */
  public class ClientMetricsCacheValue {
    private ClientMetricsInstanceState clientInstance;

    public ClientMetricsCacheValue(ClientMetricsInstanceState instance) {
      clientInstance = instance;
    }

    public ClientMetricsInstanceState getClientInstance() {
      return clientInstance;
    }

    public void replace(ClientMetricsInstanceState instance) {
      clientInstance = instance;
    }
  }
}

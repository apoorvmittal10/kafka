package kafka.server;

import java.util.Optional;
import kafka.metrics.ClientMetricsInstanceState;
import kafka.metrics.ClientMetricsMetadata;
import org.apache.kafka.common.message.PushTelemetryResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.requests.PushTelemetryResponse;
import org.apache.kafka.common.requests.RequestContext;

public class ClientMetricsManager1 {

  public List<Byte> getSupportedCompressionTypes() {
    List<Byte> compressionTypes = new ArrayList<>();
    for (CompressionType compressionType : CompressionType.values()) {
      if (compressionType != CompressionType.NONE) {
        compressionTypes.add((byte) compressionType.id());
      }
    }
    return compressionTypes;
  }

  public GetTelemetrySubscriptionsResponse processGetTelemetrySubscriptionRequest(RequestChannel.Request request, int throttleMs) {
    GetTelemetrySubscriptionsRequest subscriptionRequest = request.body(GetTelemetrySubscriptionsRequest.class);
    if (ClientMetricsReceiverPlugin.isEmpty()) {
      return subscriptionRequest.getErrorResponse(throttleMs, new ClientMetricsReceiverPluginNotFoundException("Broker does not have any configured client metrics receiver plugin"));
    } else {
      ClientInfo clientInfo = new ClientMetricsMetadata(request, subscriptionRequest.data().clientInstanceId().toString());
      return _instance.processGetSubscriptionRequest(subscriptionRequest, clientInfo, throttleMs);
    }
  }

  public PushTelemetryResponse processPushTelemetryRequest(RequestChannel.Request request, int throttleMs) {
    PushTelemetryRequest pushTelemetryRequest = request.body(PushTelemetryRequest.class);
    if (ClientMetricsReceiverPlugin.isEmpty()) {
      System.out.println("[APM] - ClientMetricsReceiverPlugin is empty");
      return pushTelemetryRequest.getErrorResponse(throttleMs, new ClientMetricsReceiverPluginNotFoundException("Broker does not have any configured client metrics receiver plugin"));
    } else {
      ClientInfo clientInfo = new ClientInfo(request, pushTelemetryRequest.data.clientInstanceId.toString());
      System.out.println("[APM] - client info: " + clientInfo);
      return _instance.processPushTelemetryRequest(pushTelemetryRequest, request.context(), clientInfo, throttleMs);
    }
  }

  class ClientMetricsException extends KafkaException {
    private String s;
    private Errors errorCode;

    public ClientMetricsException(String s, Errors errorCode) {
      super(s);
      this.s = s;
      this.errorCode = errorCode;
    }

    public Errors getErrorCode() {
      return errorCode;
    }
  }

  public Optional<ClientMetricsInstanceState> getClientInstance(UUID id) {
    if (id == null || id.equals(UUID.ZERO)) {
      return Optional.empty();
    } else {
      return ClientMetricsCache.getInstance().get(id);
    }
  }

  public UUID generateNewClientId() {
    UUID id = UUID.randomUUID();
    while (ClientMetricsCache.getInstance().get(id).isPresent()) {
      id = UUID.randomUUID();
    }
    return id;
  }

  public GetTelemetrySubscriptionsResponse processGetSubscriptionRequest(GetTelemetrySubscriptionsRequest subscriptionRequest,
      ClientMetricsMetadata clientInfo,
      int throttleMs) {
    String clientInstanceId = Optional.ofNullable(subscriptionRequest.data().clientInstanceId())
        .filter(id -> !id.equals(Uuid.ZERO_UUID))
        .orElse(generateNewClientId());
    ClientInstance clientInstance = getClientInstance(clientInstanceId)
        .orElseGet(() -> createClientInstance(clientInstanceId, clientInfo));
    GetTelemetrySubscriptionsResponseData data = new GetTelemetrySubscriptionsResponseData();
    data.setThrottleTimeMs(throttleMs);
    data.setClientInstanceId(clientInstanceId);
    data.setSubscriptionId(clientInstance.getSubscriptionId());
    data.setAcceptedCompressionTypes(getSupportedCompressionTypes());
    data.setPushIntervalMs(clientInstance.getPushIntervalMs());
    data.setDeltaTemporality(true);
    data.setErrorCode(Errors.NONE.code());
    data.setRequestedMetrics(clientInstance.getMetrics());
    if (clientInstance.isDisabledForMetricsCollection()) {
      info("Metrics collection is disabled for the client: " + clientInstance.getId().toString());
    }
    clientInstance.updateLastAccessTs(getCurrentTime());
    return new GetTelemetrySubscriptionsResponse(data);
  }

  public static boolean isSupportedCompressionType(int id) {
    try {
      CompressionType.forId(id);
      return true;
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  private static void forId(int id) throws IllegalArgumentException {
    // implementation of CompressionType.forId() method
  }

  public void validatePushRequest(PushTelemetryRequest pushTelemetryRequest, ClientMetricsMetadata clientInfo) {
    Optional<String> clientInstanceId = Optional.ofNullable(
        pushTelemetryRequest.getData().getClientInstanceId());
    if (!clientInstanceId.isPresent() || clientInstanceId.get().equals(Uuid.ZERO_UUID)) {
      String msg = String.format("Invalid request from the client [%s], missing client instance id",
          clientInfo.getClientId());
      throw new ClientMetricsException(msg, Errors.INVALID_REQUEST);
    } else {
      clientInstanceId = Optional.of(clientInstanceId.get());
    }
    System.out.println("[APM] - client instance id: " + clientInstanceId.get());
    Optional<ClientInstance> clientInstance = getClientInstance(clientInstanceId.get());
    if (!clientInstance.isPresent()) {
      clientInstance = Optional.of(createClientInstance(clientInstanceId.get(), clientInfo));
    }
    if (clientInstance.get().isClientTerminating()) {
      String msg = String.format(
          "Client [%s] sent the previous request with state terminating to TRUE, can not accept any requests after that",
          pushTelemetryRequest.getData().getClientInstanceId().toString());
      throw new ClientMetricsException(msg, Errors.INVALID_REQUEST);
    }

    // Make sure that this request is arrived after the next push interval, but this check would be bypassed if the
    // client has set the Terminating flag.
    if (!clientInstance.canAcceptPushRequest() && !pushTelemetryRequest.getData().isTerminating()) {
      String msg = String.format("Request from the client [%s] arrived before the next push interval time",
          pushTelemetryRequest.getData().getClientInstanceId().toString());
      throw new ClientMetricsException(msg, Errors.CLIENT_METRICS_RATE_LIMITED);
    }
    if (!pushTelemetryRequest.getData().getSubscriptionId().equals(clientInstance.getSubscriptionId())) {
      String msg = String.format("Client's subscription id [%s] != Broker's cached client's subscription id [%s]",
          pushTelemetryRequest.getData().getSubscriptionId().toString(), clientInstance.getSubscriptionId().toString());
      throw new ClientMetricsException(msg, Errors.UNKNOWN_CLIENT_METRICS_SUBSCRIPTION_ID);
    }
    if (!isSupportedCompressionType(pushTelemetryRequest.getData().getCompressionType())) {
      String msg = String.format("Unknown compression type [%s] is received in PushTelemetryRequest from %s",
          pushTelemetryRequest.getData().getCompressionType().toString(), pushTelemetryRequest.getData().getClientInstanceId().toString());
      throw new ClientMetricsException(msg, Errors.UNSUPPORTED_COMPRESSION_TYPE);
    }
  }

  public PushTelemetryResponse processPushTelemetryRequest(
      PushTelemetryRequest pushTelemetryRequest,
      RequestContext context,
      ClientMetricsMetadata clientInfo,
      int throttleMs) {
    int errorCode = Errors.NONE;
    try {
      System.out.println("[APM] - validate");
      // Validate the push request parameters
      validatePushRequest(pushTelemetryRequest, clientInfo);
      System.out.println("[APM] - metrics");
      // Push the metrics to the external client receiver plugin.
      Optional<Metrics> metrics = Optional.ofNullable(pushTelemetryRequest.data().metrics());
      if (metrics.isPresent() && !metrics.get().isEmpty()) {
        System.out.println("[APM] - export");
        ClientMetricsReceiverPlugin.exportMetrics(requestContext, pushTelemetryRequest);
      }
    } catch (ClientMetricsException e) {
      warn("PushTelemetry request raised an exception: " + e.getMessage());
      System.out.println("[APM] Error: " + e.getMessage());
      errorCode = e.getErrorCode();
    }
    // Finally, send the response back to the client
    createResponse(Optional.ofNullable(errorCode));
  }

  public PushTelemetryResponse createResponse(Optional<Errors> errors) {
    System.out.println("[APM] - create response");
    int adjustedThrottleMs = throttleMs;
    Optional<ClientInstance> clientInstance = getClientInstance(pushTelemetryRequest.getData().getClientInstanceId());
    // Before sending the response make sure to update the book keeping markers like
    // lastAccessTime, isTerminating flag etc..
    if (clientInstance.isPresent()) {
      System.out.println("[APM] - clientInstance is not empty");
      adjustedThrottleMs = Math.max(clientInstance.get().getAdjustedPushInterval(), throttleMs);
      clientInstance.get().updateLastAccessTs(getCurrentTime());
      // update the client terminating flag only once
      if (!pushTelemetryRequest.getData().isTerminating()) {
        clientInstance.get().setTerminatingFlag(pushTelemetryRequest.getData().isTerminating());
      }
    }
    return pushTelemetryRequest.createResponse(adjustedThrottleMs, errors.orElse(Errors.NONE));
  }

  public void updateSubscription(String groupId, Properties properties) {
    ClientMetricsConfig1.updateClientSubscription(groupId, properties);
  }

  public ClientMetricsInstanceState createClientInstance(Uuid clientInstanceId, ClientMetricsMetadata clientInfo) {
    ClientMetricsInstanceState clientInstance = new ClientMetricsInstanceState(clientInstanceId, clientInfo, ClientMetricsConfig1.getClientSubscriptions());
    ClientMetricsCache.getInstance().add(clientInstance);
    ClientMetricsCache.deleteExpiredEntries();
    return clientInstance;
  }

}

package kafka.metrics;

public class ClientMetricsReceiverPlugin {

  private List<ClientTelemetryReceiver> cmReceivers = new ArrayList<>();

  public boolean isEmpty() {
    return cmReceivers.isEmpty();
  }

  public void add(ClientTelemetryReceiver receiver) {
    cmReceivers.add(receiver);
  }

  public ClientMetricsPayload getPayLoad(PushTelemetryRequest request) {
    return new ClientMetricsPayload(request.data().clientInstanceId(), request.data().terminating(),
        request.getMetricsContentType(), request.getMetricsData());
  }

  public void exportMetrics(RequestContext context, PushTelemetryRequest request) {
    System.out.println("[APM] - export metrics");
    ClientMetricsPayload payload = getPayLoad(request);
    System.out.println("[APM] - payload: " + payload);
    for (ClientTelemetryReceiver receiver : cmReceivers) {
      receiver.exportMetrics(context, payload);
    }
  }
}

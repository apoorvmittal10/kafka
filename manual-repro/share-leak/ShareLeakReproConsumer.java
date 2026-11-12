import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Reproduces the ShareInFlightBatch merge leak WITHOUT any leader change, via the acquisition-lock
 * renewal path instead. {@code ShareConsumerImpl.collect()} only calls {@code ShareFetchCollector.collect()}
 * (which drains the buffer) once {@code currentFetch.isEmpty() && !currentFetch.hasRenewals()}. While a
 * record is being RENEWed, every poll() call instead takes the "renewals" branch, which sends a brand new
 * {@code ShareFetchEvent} (triggering a real background fetch) WITHOUT ever draining the buffer. So if we
 * keep renewing the same record for several cycles in a row -- instead of accepting it immediately -- each
 * cycle can queue another real, uncollected ShareCompletedFetch for that partition. Once we finally accept
 * it, the buffer may contain several stacked completions for the same partition, and collect() merges them
 * all in one shot, exercising the exact ShareFetch.add() merge path -- deterministically, no leader change,
 * single broker.
 */
public class ShareLeakReproConsumer {
    private static final int RENEW_CYCLES = 6;

    public static void main(String[] args) throws Exception {
        String bootstrap = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "leak-test";
        String groupId = args.length > 2 ? args[2] : "leak-test-group";

        System.out.println("PID=" + ProcessHandle.current().pid());
        System.out.println("bootstrap=" + bootstrap + " topic=" + topic + " group=" + groupId
                + " renewCycles=" + RENEW_CYCLES);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.SHARE_ACKNOWLEDGEMENT_MODE_CONFIG, "explicit");

        ShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props);
        consumer.subscribe(List.of(topic));

        long lastHeartbeat = System.currentTimeMillis();
        long totalRecords = 0;
        long totalRenewals = 0;
        // offset -> renewals remaining for that offset before we finally accept it.
        Map<Long, Integer> renewalsRemaining = new HashMap<>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
            for (ConsumerRecord<String, String> record : records) {
                totalRecords++;
                int remaining = renewalsRemaining.getOrDefault(record.offset(), RENEW_CYCLES);
                if (remaining > 0) {
                    renewalsRemaining.put(record.offset(), remaining - 1);
                    totalRenewals++;
                    consumer.acknowledge(record, AcknowledgeType.RENEW);
                    System.out.println("RENEW partition=" + record.partition() + " offset=" + record.offset()
                            + " deliveryCount=" + record.deliveryCount() + " remaining=" + (remaining - 1));
                } else {
                    renewalsRemaining.remove(record.offset());
                    consumer.acknowledge(record, AcknowledgeType.ACCEPT);
                    System.out.println("ACCEPT partition=" + record.partition() + " offset=" + record.offset()
                            + " deliveryCount=" + record.deliveryCount() + " total=" + totalRecords);
                }
            }

            long now = System.currentTimeMillis();
            if (now - lastHeartbeat > 3000) {
                System.out.println("HEARTBEAT total=" + totalRecords + " renewals=" + totalRenewals + " at " + now);
                lastHeartbeat = now;
                printBufferDiagnostics(consumer);
            }
        }
    }

    /**
     * Reflects into KafkaShareConsumer -> ShareConsumerImpl -> ShareFetchBuffer to report how many
     * ShareCompletedFetch objects are stuck in pendingAcknowledgementFetches, and their state. A
     * non-empty/growing count here, for records we have already fully accepted, is the leak.
     */
    private static void printBufferDiagnostics(ShareConsumer<?, ?> consumer) throws Exception {
        Field delegateField = consumer.getClass().getDeclaredField("delegate");
        delegateField.setAccessible(true);
        Object delegate = delegateField.get(consumer);

        Field fetchBufferField = delegate.getClass().getDeclaredField("fetchBuffer");
        fetchBufferField.setAccessible(true);
        Object fetchBuffer = fetchBufferField.get(delegate);

        Field pendingField = fetchBuffer.getClass().getDeclaredField("pendingAcknowledgementFetches");
        pendingField.setAccessible(true);
        Set<?> pending = (Set<?>) pendingField.get(fetchBuffer);

        System.out.println("### DIAGNOSTIC pendingAcknowledgementFetches.size=" + pending.size());
        for (Object completedFetch : pending) {
            System.out.println("### DIAGNOSTIC entry: " + describeShareCompletedFetch(completedFetch));
        }
    }

    private static String describeShareCompletedFetch(Object completedFetch) throws Exception {
        Class<?> clazz = completedFetch.getClass();
        Field partitionField = clazz.getDeclaredField("partition");
        partitionField.setAccessible(true);
        Field nodeIdField = clazz.getDeclaredField("nodeId");
        nodeIdField.setAccessible(true);
        Field isConsumedField = clazz.getDeclaredField("isConsumed");
        isConsumedField.setAccessible(true);
        Field deliveredBatchField = clazz.getDeclaredField("deliveredBatch");
        deliveredBatchField.setAccessible(true);

        Object partition = partitionField.get(completedFetch);
        Object nodeId = nodeIdField.get(completedFetch);
        Object isConsumed = isConsumedField.get(completedFetch);
        Object deliveredBatch = deliveredBatchField.get(completedFetch);

        String batchDescription = "null";
        if (deliveredBatch != null) {
            Field inFlightRecordsField = deliveredBatch.getClass().getDeclaredField("inFlightRecords");
            inFlightRecordsField.setAccessible(true);
            Object inFlightRecords = inFlightRecordsField.get(deliveredBatch);
            int numRecords = inFlightRecords == null ? -1 : ((Map<?, ?>) inFlightRecords).size();
            batchDescription = "inFlightRecords.size=" + numRecords;
        }

        return "partition=" + partition + " nodeId=" + nodeId + " isConsumed=" + isConsumed
            + " deliveredBatch=[" + batchDescription + "]";
    }
}

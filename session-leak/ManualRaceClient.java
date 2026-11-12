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

import org.apache.kafka.clients.consumer.internals.ShareAcquireMode;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ShareFetchRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.requests.ResponseHeader;
import org.apache.kafka.common.requests.ShareFetchRequest;
import org.apache.kafka.common.requests.ShareFetchResponse;
import org.apache.kafka.common.requests.ShareRequestMetadata;
import org.apache.kafka.common.utils.Utils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * <p>Standalone (no JUnit, no test harness) real-client reproduction of the
 * SharePartitionManager INITIAL_EPOCH race. Run this exact class twice -- from two separate
 * terminals or as two backgrounded processes in a script -- against the SAME groupId and
 * memberId, pointed at a real, already-running broker started the normal way
 * (bin/kafka-server-start.sh). Each invocation opens its own real TCP connection, sends one
 * INITIAL_EPOCH ShareFetchRequest, prints the response, and exits (closing the connection).
 *
 * <p>The stock KafkaShareConsumer cannot be coerced into using a caller-chosen memberId --
 * ShareConsumerConfig explicitly rejects group.instance.id (static membership) for share
 * consumers (see clients/src/main/java/org/apache/kafka/clients/consumer/ShareConsumerConfig.java).
 * This talks the wire protocol directly instead, which is unremarkable: MemberId is a plain
 * client-supplied string on the wire (ShareFetchRequest.json), not something the broker assigns.
 *
 * <p>Usage:
 *   java ManualRaceClient &lt;host&gt; &lt;port&gt; &lt;groupId&gt; &lt;memberIdUuid&gt; &lt;topicIdUuid&gt; &lt;topicName&gt; &lt;partition&gt;
 *
 * See session-leak/README.md ("Path D") for exact end-to-end steps, including how to get the
 * topicIdUuid from `kafka-topics.sh --describe`.
 */
public class ManualRaceClient {

    public static void main(String[] args) throws Exception {
        if (args.length != 7 && args.length != 8) {
            System.err.println("Usage: java ManualRaceClient <host> <port> <groupId> <memberIdUuid> <topicIdUuid> <topicName> <partition> [startAtEpochMillis]");
            System.exit(1);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String groupId = args[2];
        Uuid memberId = Uuid.fromString(args[3]);
        Uuid topicId = Uuid.fromString(args[4]);
        String topicName = args[5];
        int partition = Integer.parseInt(args[6]);
        Long startAtEpochMillis = args.length == 8 ? Long.parseLong(args[7]) : null;

        TopicIdPartition topicIdPartition = new TopicIdPartition(topicId, new TopicPartition(topicName, partition));
        ShareRequestMetadata metadata = new ShareRequestMetadata(memberId, ShareRequestMetadata.INITIAL_EPOCH);
        ShareFetchRequest request = ShareFetchRequest.Builder.forConsumer(
            groupId,
            metadata,
            500,
            0,
            Integer.MAX_VALUE,
            500,
            500,
            ShareAcquireMode.BATCH_OPTIMIZED.id(),
            false,
            List.of(topicIdPartition),
            List.of(),
            Map.<TopicIdPartition, List<ShareFetchRequestData.AcknowledgementBatch>>of()
        ).build();

        System.out.println("[" + memberId + "] connecting to " + host + ":" + port);
        try (Socket socket = new Socket(host, port)) {
            // Connect well ahead of time so TCP/JVM warm-up is not part of the race window; only the
            // actual request send is synchronized against the sibling process via startAtEpochMillis.
            // Without this, two independently-launched JVMs racing "naturally" almost never overlap --
            // JVM startup jitter (classloading a large classpath, JIT warm-up) is orders of magnitude
            // bigger than the actual remove()/maybeCreateSession() window on the broker.
            if (startAtEpochMillis != null) {
                long waitMs = startAtEpochMillis - System.currentTimeMillis();
                if (waitMs > 0) {
                    Thread.sleep(waitMs);
                }
            }
            RequestHeader header = new RequestHeader(request.apiKey(), request.version(), "manual-race-client", 0);
            byte[] payload = Utils.toArray(request.serializeWithHeader(header));

            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            out.writeInt(payload.length);
            out.write(payload);
            out.flush();
            System.out.println("[" + memberId + "] sent INITIAL_EPOCH ShareFetch for group=" + groupId
                + " topicIdPartition=" + topicIdPartition);

            DataInputStream in = new DataInputStream(socket.getInputStream());
            int len = in.readInt();
            byte[] responseBytes = new byte[len];
            in.readFully(responseBytes);

            ByteBuffer responseBuffer = ByteBuffer.wrap(responseBytes);
            ResponseHeader.parse(responseBuffer, ApiKeys.SHARE_FETCH.responseHeaderVersion(request.version()));
            ShareFetchResponse response = (ShareFetchResponse) AbstractResponse.parseResponse(
                ApiKeys.SHARE_FETCH, new ByteBufferAccessor(responseBuffer), request.version());

            System.out.println("[" + memberId + "] response errorCode=" + response.data().errorCode()
                + " errorMessage=" + response.data().errorMessage());
            response.data().responses().forEach(topicResponse ->
                topicResponse.partitions().forEach(p ->
                    System.out.println("[" + memberId + "]   partition=" + p.partitionIndex()
                        + " errorCode=" + p.errorCode() + " acquiredRecords=" + p.acquiredRecords().size())));
        }
        System.out.println("[" + memberId + "] connection closed, process exiting");
    }
}

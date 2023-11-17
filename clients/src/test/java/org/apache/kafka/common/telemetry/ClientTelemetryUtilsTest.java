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
package org.apache.kafka.common.telemetry;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.opentelemetry.proto.metrics.v1.Metric;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.OptionalLong;
import org.apache.kafka.clients.ClientTelemetryUtils;
import org.apache.kafka.common.message.PushTelemetryRequestData;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.PushTelemetryRequest;
import org.apache.kafka.common.telemetry.internals.MetricKey;
import org.apache.kafka.common.telemetry.internals.SinglePointMetric;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.CloseableIterator;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Test;

public class ClientTelemetryUtilsTest {

    @Test
    public void testSerializeDeserializeNone() throws Exception {
        SinglePointMetric sum = SinglePointMetric.sum(
            new MetricKey("metricName"), 1.0, true, Instant.now());
        MetricsData.Builder builder = MetricsData.newBuilder();
        try {
                Metric m = sum.builder().build();
                ResourceMetrics rm = ResourceMetrics.newBuilder()
                    .setResource(Resource.newBuilder().build())
                    .addScopeMetrics(ScopeMetrics.newBuilder()
                        .addMetrics(m)
                        .build()
                    ).build();
                builder.addResourceMetrics(rm);
        } catch (Exception e) {
            System.err.println("Error constructing payload: " + e);
        }

        byte[] data = builder.build().toByteArray();

        // None
        ByteBuffer buf = ClientTelemetryUtils.compress(data, CompressionType.NONE);
        // Send
        PushTelemetryRequest pushTelemetryRequest = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setMetrics(Utils.readBytes(buf))
                .setCompressionType(CompressionType.NONE.id)
        ).build();

        ByteBuffer serialized = pushTelemetryRequest.metricsData();
        MetricsData receivedData = ClientTelemetryUtils.deserializeMetricsData(serialized);

        assertEquals(1, receivedData.getResourceMetricsCount());

        ResourceMetrics receivedResourceMetrics = receivedData.getResourceMetrics(0);
        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());
    }

    @Test
    public void testSerializeDeserializeGzip() throws Exception {
        SinglePointMetric sum = SinglePointMetric.sum(
            new MetricKey("metricName"), 1.0, true, Instant.now());
        MetricsData.Builder builder = MetricsData.newBuilder();
        try {
            Metric m = sum.builder().build();
            ResourceMetrics rm = ResourceMetrics.newBuilder()
                .setResource(Resource.newBuilder().build())
                .addScopeMetrics(ScopeMetrics.newBuilder()
                    .addMetrics(m)
                    .build()
                ).build();
            builder.addResourceMetrics(rm);
        } catch (Exception e) {
            System.err.println("Error constructing payload: " + e);
        }

        byte[] data = builder.build().toByteArray();
        ByteBuffer buf = ClientTelemetryUtils.compress(data, CompressionType.GZIP);
        // Send
        PushTelemetryRequest pushTelemetryRequest = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setMetrics(Utils.readBytes(buf))
                .setCompressionType(CompressionType.GZIP.id)
        ).build();

        ByteBuffer serialized = pushTelemetryRequest.metricsData();
        MetricsData receivedData = ClientTelemetryUtils.deserializeMetricsData(serialized);

        assertEquals(1, receivedData.getResourceMetricsCount());
        ResourceMetrics receivedResourceMetrics = receivedData.getResourceMetrics(0);
        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());

//        ByteBuffer decompressedData = PushTelemetryRequest.decompressMetricsData(CompressionType.GZIP, buf.array());
//        receivedData = ClientTelemetryUtils.deserializeMetricsData(decompressedData);
//        assertEquals(1, receivedData.getResourceMetricsCount());
//        receivedResourceMetrics = receivedData.getResourceMetrics(0);
//        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());
    }

    @Test
    public void testSerializeDeserializeLZ4() throws Exception {
        SinglePointMetric sum = SinglePointMetric.sum(
            new MetricKey("metricName"), 1.0, true, Instant.now());
        MetricsData.Builder builder = MetricsData.newBuilder();
        try {
            Metric m = sum.builder().build();
            ResourceMetrics rm = ResourceMetrics.newBuilder()
                .setResource(Resource.newBuilder().build())
                .addScopeMetrics(ScopeMetrics.newBuilder()
                    .addMetrics(m)
                    .build()
                ).build();
            builder.addResourceMetrics(rm);
        } catch (Exception e) {
            System.err.println("Error constructing payload: " + e);
        }

        byte[] data = builder.build().toByteArray();

        // None
        ByteBuffer buf = ClientTelemetryUtils.compress(data, CompressionType.LZ4);
        // Send
        PushTelemetryRequest pushTelemetryRequest = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setMetrics(Utils.readBytes(buf))
                .setCompressionType(CompressionType.LZ4.id)
        ).build();

        ByteBuffer serialized = pushTelemetryRequest.metricsData();
        MetricsData receivedData = ClientTelemetryUtils.deserializeMetricsData(serialized);

        assertEquals(1, receivedData.getResourceMetricsCount());
        ResourceMetrics receivedResourceMetrics = receivedData.getResourceMetrics(0);
        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());

//        ByteBuffer decompressedData = PushTelemetryRequest.decompressMetricsData(CompressionType.LZ4, buf.array());
//        receivedData = ClientTelemetryUtils.deserializeMetricsData(decompressedData);
//        assertEquals(1, receivedData.getResourceMetricsCount());
//        receivedResourceMetrics = receivedData.getResourceMetrics(0);
//        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());
    }

    @Test
    public void testSerializeDeserializeSnappy() throws Exception {
        SinglePointMetric sum = SinglePointMetric.sum(
            new MetricKey("metricName"), 1.0, true, Instant.now());
        MetricsData.Builder builder = MetricsData.newBuilder();
        try {
            Metric m = sum.builder().build();
            ResourceMetrics rm = ResourceMetrics.newBuilder()
                .setResource(Resource.newBuilder().build())
                .addScopeMetrics(ScopeMetrics.newBuilder()
                    .addMetrics(m)
                    .build()
                ).build();
            builder.addResourceMetrics(rm);
        } catch (Exception e) {
            System.err.println("Error constructing payload: " + e);
        }

        byte[] data = builder.build().toByteArray();

        ByteBuffer buf = ClientTelemetryUtils.compress(data, CompressionType.SNAPPY);
        // Send
        PushTelemetryRequest pushTelemetryRequest = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setMetrics(Utils.readBytes(buf))
                .setCompressionType(CompressionType.SNAPPY.id)
        ).build();

        ByteBuffer serialized = pushTelemetryRequest.metricsData();
        MetricsData receivedData = ClientTelemetryUtils.deserializeMetricsData(serialized);

        assertEquals(1, receivedData.getResourceMetricsCount());
        ResourceMetrics receivedResourceMetrics = receivedData.getResourceMetrics(0);
        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());

//        ByteBuffer decompressedData = PushTelemetryRequest.decompressMetricsData(CompressionType.SNAPPY, Utils.readBytes(buf));
//        receivedData = ClientTelemetryUtils.deserializeMetricsData(decompressedData);
//        assertEquals(1, receivedData.getResourceMetricsCount());
//        receivedResourceMetrics = receivedData.getResourceMetrics(0);
//        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());
    }

    @Test
    public void testSerializeDeserializeZstd() throws Exception {
        SinglePointMetric sum = SinglePointMetric.sum(
            new MetricKey("metricName"), 1.0, true, Instant.now());
        MetricsData.Builder builder = MetricsData.newBuilder();
        try {
            Metric m = sum.builder().build();
            ResourceMetrics rm = ResourceMetrics.newBuilder()
                .setResource(Resource.newBuilder().build())
                .addScopeMetrics(ScopeMetrics.newBuilder()
                    .addMetrics(m)
                    .build()
                ).build();
            builder.addResourceMetrics(rm);
        } catch (Exception e) {
            System.err.println("Error constructing payload: " + e);
        }

        byte[] data = builder.build().toByteArray();


        long deleteHorizon = 100;
        int payloadLen = 1024 * 1024;
        ByteBuffer buffer = ByteBuffer.allocate(payloadLen * 2);
        ByteBufferOutputStream byteBufferOutputStream = new ByteBufferOutputStream(buffer);
        MemoryRecordsBuilder mBuilder = new MemoryRecordsBuilder(byteBufferOutputStream, (byte) 2, CompressionType.ZSTD,
            TimestampType.CREATE_TIME, 0L, 0L, RecordBatch.NO_PRODUCER_ID,
            RecordBatch.NO_PRODUCER_EPOCH, RecordBatch.NO_SEQUENCE, false, false,
            RecordBatch.NO_PARTITION_LEADER_EPOCH, 0, deleteHorizon);

        mBuilder.append(50L, null, data);

        MemoryRecords records = mBuilder.build();
        List<MutableRecordBatch> batches = TestUtils.toList(records.batches());
        assertEquals(OptionalLong.of(deleteHorizon), batches.get(0).deleteHorizonMs());

        CloseableIterator<Record> recordIterator = batches.get(0).streamingIterator(BufferSupplier.create());
        Record record = recordIterator.next();
        assertEquals(50L, record.timestamp());

        ByteBuffer buffer2 = record.value();
        MetricsData receivedData1 = ClientTelemetryUtils.deserializeMetricsData(buffer2);
        assertEquals(1, receivedData1.getResourceMetricsCount());


        recordIterator.close();

        // None
        ByteBuffer buf = ClientTelemetryUtils.compress(data, CompressionType.ZSTD);
        // Send
        PushTelemetryRequest pushTelemetryRequest = new PushTelemetryRequest.Builder(
            new PushTelemetryRequestData()
                .setMetrics(Utils.readBytes(buf))
                .setCompressionType(CompressionType.ZSTD.id)
        ).build();

//        ByteBuffer decompressedData = PushTelemetryRequest.decompressMetricsData(CompressionType.ZSTD, Utils.readBytes(buf));
//        MetricsData receivedData = ClientTelemetryUtils.deserializeMetricsData(decompressedData);
//        assertEquals(1, receivedData.getResourceMetricsCount());
//        ResourceMetrics receivedResourceMetrics = receivedData.getResourceMetrics(0);
//        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());
//
//
//        ByteBuffer serialized = pushTelemetryRequest.metricsData();
//        receivedData = ClientTelemetryUtils.deserializeMetricsData(serialized);
//
//        assertEquals(1, receivedData.getResourceMetricsCount());
//        receivedResourceMetrics = receivedData.getResourceMetrics(0);
//        assertEquals(1, receivedResourceMetrics.getScopeMetricsCount());
    }

}

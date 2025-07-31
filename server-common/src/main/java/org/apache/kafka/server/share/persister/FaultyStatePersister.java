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

package org.apache.kafka.server.share.persister;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An implementation of {@link Persister} interface which is faulty i.e. errors out some of
 * the requests.
 */
public class FaultyStatePersister implements Persister {

    public static final FaultyStatePersister INSTANCE = new FaultyStatePersister(25); // 25% of the requests will fail

    private final int faultPercentage;

    private long successfulRequests = 0;
    private long failedRequests = 0;

    public FaultyStatePersister(int faultPercentage) {
        this.faultPercentage = faultPercentage;
    }

    @Override
    public CompletableFuture<InitializeShareGroupStateResult> initializeState(InitializeShareGroupStateParameters request) {
        if (checkIfFailRequests()) {
            // Simulate a failure
            return CompletableFuture.failedFuture(new RuntimeException("Simulated failure in initializeState"));
        }

        GroupTopicPartitionData<PartitionStateData> reqData = request.groupTopicPartitionData();
        List<TopicData<PartitionErrorData>> resultArgs = new ArrayList<>();
        for (TopicData<PartitionStateData> topicData : reqData.topicsData()) {
            resultArgs.add(new TopicData<>(topicData.topicId(), topicData.partitions().stream()
                .map(partStateData -> PartitionFactory.newPartitionErrorData(partStateData.partition(), PartitionFactory.DEFAULT_ERROR_CODE, PartitionFactory.DEFAULT_ERR_MESSAGE))
                .collect(Collectors.toList())));
        }
        return CompletableFuture.completedFuture(new InitializeShareGroupStateResult.Builder().setTopicsData(resultArgs).build());
    }

    @Override
    public CompletableFuture<ReadShareGroupStateResult> readState(ReadShareGroupStateParameters request) {
        if (checkIfFailRequests()) {
            // Simulate a failure
            return CompletableFuture.failedFuture(new RuntimeException("Simulated failure in readState"));
        }

        GroupTopicPartitionData<PartitionIdLeaderEpochData> reqData = request.groupTopicPartitionData();
        List<TopicData<PartitionAllData>> resultArgs = new ArrayList<>();
        // we will fetch topic and partition info from the request and
        // return valid but default response (keep partition id and topic from request but initialize other
        // values as default).
        for (TopicData<PartitionIdLeaderEpochData> topicData : reqData.topicsData()) {
            resultArgs.add(new TopicData<>(topicData.topicId(), topicData.partitions().stream().
                map(partitionIdData -> PartitionFactory.newPartitionAllData(
                    partitionIdData.partition(), PartitionFactory.DEFAULT_STATE_EPOCH, PartitionFactory.UNINITIALIZED_START_OFFSET, PartitionFactory.DEFAULT_ERROR_CODE, PartitionFactory.DEFAULT_ERR_MESSAGE, List.of()))
                .collect(Collectors.toList())));
        }
        return CompletableFuture.completedFuture(new ReadShareGroupStateResult.Builder().setTopicsData(resultArgs).build());
    }

    @Override
    public CompletableFuture<WriteShareGroupStateResult> writeState(WriteShareGroupStateParameters request) {
        if (checkIfFailRequests()) {
            // Simulate a failure
            return CompletableFuture.failedFuture(new RuntimeException("Simulated failure in writeState"));
        }

        GroupTopicPartitionData<PartitionStateBatchData> reqData = request.groupTopicPartitionData();
        List<TopicData<PartitionErrorData>> resultArgs = new ArrayList<>();
        for (TopicData<PartitionStateBatchData> topicData : reqData.topicsData()) {
            resultArgs.add(new TopicData<>(topicData.topicId(), topicData.partitions().stream()
                .map(batch -> PartitionFactory.newPartitionErrorData(batch.partition(), PartitionFactory.DEFAULT_ERROR_CODE, PartitionFactory.DEFAULT_ERR_MESSAGE))
                .collect(Collectors.toList())));
        }
        return CompletableFuture.completedFuture(new WriteShareGroupStateResult.Builder().setTopicsData(resultArgs).build());
    }

    @Override
    public CompletableFuture<DeleteShareGroupStateResult> deleteState(DeleteShareGroupStateParameters request) {
        if (checkIfFailRequests()) {
            // Simulate a failure
            return CompletableFuture.failedFuture(new RuntimeException("Simulated failure in deleteState"));
        }

        GroupTopicPartitionData<PartitionIdData> reqData = request.groupTopicPartitionData();
        List<TopicData<PartitionErrorData>> resultArgs = new ArrayList<>();
        for (TopicData<PartitionIdData> topicData : reqData.topicsData()) {
            resultArgs.add(new TopicData<>(topicData.topicId(), topicData.partitions().stream()
                .map(batch -> PartitionFactory.newPartitionErrorData(batch.partition(), PartitionFactory.DEFAULT_ERROR_CODE, PartitionFactory.DEFAULT_ERR_MESSAGE))
                .collect(Collectors.toList())));
        }
        return CompletableFuture.completedFuture(new DeleteShareGroupStateResult.Builder().setTopicsData(resultArgs).build());
    }

    @Override
    public CompletableFuture<ReadShareGroupStateSummaryResult> readSummary(ReadShareGroupStateSummaryParameters request) {
        if (checkIfFailRequests()) {
            // Simulate a failure
            return CompletableFuture.failedFuture(new RuntimeException("Simulated failure in readSummary"));
        }

        GroupTopicPartitionData<PartitionIdLeaderEpochData> reqData = request.groupTopicPartitionData();
        List<TopicData<PartitionStateSummaryData>> resultArgs = new ArrayList<>();
        // we will fetch topic and partition info from the request and
        // return valid but default response (keep partition id and topic from request but initialize other
        // values as default).
        for (TopicData<PartitionIdLeaderEpochData> topicData : reqData.topicsData()) {
            resultArgs.add(new TopicData<>(topicData.topicId(), topicData.partitions().stream().
                map(partitionIdData -> PartitionFactory.newPartitionStateSummaryData(
                    partitionIdData.partition(), PartitionFactory.DEFAULT_STATE_EPOCH, PartitionFactory.UNINITIALIZED_START_OFFSET,
                    PartitionFactory.DEFAULT_LEADER_EPOCH, PartitionFactory.DEFAULT_ERROR_CODE, PartitionFactory.DEFAULT_ERR_MESSAGE))
                .collect(Collectors.toList())));
        }
        return CompletableFuture.completedFuture(new ReadShareGroupStateSummaryResult.Builder().setTopicsData(resultArgs).build());
    }

    @Override
    public void stop() {
        //noop
    }

    private synchronized boolean checkIfFailRequests() {
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
        // After 5000 requests, reset the counters
          if (failedRequests + successfulRequests >= 5000) {
  //            System.out.println("Resetting counters after 5000 requests");
              successfulRequests = 0;
              failedRequests = 0;
          }
          // Simulate a fault based on the fault percentage
          int currentFailedRequestsPercentage = (int) ((successfulRequests + failedRequests) == 0 ? 0 : (failedRequests * 100) / (successfulRequests + failedRequests));
  //        System.out.println("Current failed requests percentage: " + currentFailedRequestsPercentage);
          if (currentFailedRequestsPercentage <= faultPercentage) {
              failedRequests++;
              return true;
          } else {
              successfulRequests++;
          }
          return false;
    }
}

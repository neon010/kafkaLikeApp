package org.example;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

class TopicManager {
    private final Map<String, List<Partition>> topicPartitions;
    private final Map<String, Map<String, List<Integer>>> consumerGroupAssignments;
    private final Map<String, Map<String, Map<Integer, ReentrantLock>>> consumerPartitionLocks;
    private final int defaultPartitionCount;

    public TopicManager(int partitionCount) {
        this.topicPartitions = new ConcurrentHashMap<>();
        this.consumerGroupAssignments = new ConcurrentHashMap<>();
        this.consumerPartitionLocks = new ConcurrentHashMap<>();
        this.defaultPartitionCount = partitionCount;
    }

    public void createTopic(String topic) {
        topicPartitions.putIfAbsent(topic, new ArrayList<>());
        if (topicPartitions.get(topic).isEmpty()) {
            List<Partition> partitions = new ArrayList<>();
            for (int i = 0; i < defaultPartitionCount; i++) {
                partitions.add(new Partition(topic, i));
            }
            topicPartitions.put(topic, partitions);
        }
    }
    public String listTopics() {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, List<Partition>> entry : topicPartitions.entrySet()) {
            String topic = entry.getKey();
            List<Partition> partitions = entry.getValue();
            result.append(topic).append(" (Partitions: ").append(partitions.size()).append(") [");
            for (Partition partition : partitions) {
                result.append("P").append(partition.getPartitionId()).append(", ");
            }
            if (!partitions.isEmpty()) {
                result.setLength(result.length() - 2); // Remove the last comma and space
            }
            result.append("]\n");
        }
        return result.toString().trim();
//        Map<String, Integer> topicDetails = new HashMap<>();
//        for (Map.Entry<String, List<Partition>> entry : topicPartitions.entrySet()) {
//            topicDetails.put(entry.getKey(), entry.getValue());
//        }
//        return topicDetails;
    }
    public void addMessage(String topic, String key, String message) {
        createTopic(topic);
        List<Partition> partitions = topicPartitions.get(topic);

        // Use a consistent hashing mechanism to ensure same keys go to same partitions
        int partition = Math.abs(key.hashCode() % defaultPartitionCount);
        partitions.get(partition).addMessage(message);
    }

    public String consumeMessage(String topic, String groupId, String consumerId) {
        if (!topicPartitions.containsKey(topic)) {
            return null;
        }

        List<Integer> assignedPartitions = getAssignedPartitions(topic, groupId, consumerId);
        Map<Integer, ReentrantLock> partitionLocks = getConsumerPartitionLocks(topic, groupId, consumerId);

        // Iterate over assigned partitions in order and block until message is read
        for (Integer partitionId : assignedPartitions) {
            ReentrantLock partitionLock = partitionLocks.get(partitionId);
            partitionLock.lock();  // Ensure message order by blocking
            try {
                Partition partition = topicPartitions.get(topic).get(partitionId);
                if (partition.hasMessages(groupId)) {
                    return partition.consumeMessage(groupId);
                }
            } finally {
                partitionLock.unlock();
            }
        }
        return null;
    }

    private Map<Integer, ReentrantLock> getConsumerPartitionLocks(String topic, String groupId, String consumerId) {
        consumerPartitionLocks.putIfAbsent(topic, new ConcurrentHashMap<>());
        Map<String, Map<Integer, ReentrantLock>> topicLocks = consumerPartitionLocks.get(topic);

        String consumerKey = groupId + "-" + consumerId;
        topicLocks.putIfAbsent(consumerKey, new ConcurrentHashMap<>());

        Map<Integer, ReentrantLock> partitionLocks = topicLocks.get(consumerKey);
        List<Integer> assignedPartitions = getAssignedPartitions(topic, groupId, consumerId);

        // Ensure we have locks for all assigned partitions
        for (Integer partitionId : assignedPartitions) {
            partitionLocks.putIfAbsent(partitionId, new ReentrantLock());
        }

        return partitionLocks;
    }

    public List<Integer> getAssignedPartitions(String topic, String groupId, String consumerId) {
        consumerGroupAssignments.putIfAbsent(groupId, new ConcurrentHashMap<>());
        Map<String, List<Integer>> groupAssignments = consumerGroupAssignments.get(groupId);
        groupAssignments.putIfAbsent(consumerId, new ArrayList<>());

        if (groupAssignments.get(consumerId).isEmpty()) {
            rebalanceGroup(topic, groupId);
        }

        return groupAssignments.get(consumerId);
    }

    private void rebalanceGroup(String topic, String groupId) {
        Map<String, List<Integer>> groupAssignments = consumerGroupAssignments.get(groupId);
        List<String> consumers = new ArrayList<>(groupAssignments.keySet());

        if (consumers.isEmpty()) {
            return;
        }

        // Sort consumers to ensure consistent assignment
        Collections.sort(consumers);

        // Clear existing assignments
        for (String consumer : consumers) {
            groupAssignments.put(consumer, new ArrayList<>());
        }

        // Assign partitions in a balanced way
        int consumerCount = consumers.size();
        int partitionsPerConsumer = defaultPartitionCount / consumerCount;
        int remainingPartitions = defaultPartitionCount % consumerCount;

        int currentPartition = 0;
        for (int i = 0; i < consumerCount; i++) {
            String consumer = consumers.get(i);
            int partitionsToAssign = partitionsPerConsumer + (i < remainingPartitions ? 1 : 0);

            for (int j = 0; j < partitionsToAssign; j++) {
                groupAssignments.get(consumer).add(currentPartition++);
            }
        }
    }
}

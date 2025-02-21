package org.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class TopicManager {
    private final Map<String, List<Partition>> topicPartitions;
    private final Map<String, Map<String, List<Integer>>> consumerGroupAssignments; // group -> (consumer -> partitions)
    private final int defaultPartitionCount = 3;

    public TopicManager() {
        this.topicPartitions = new ConcurrentHashMap<>();
        this.consumerGroupAssignments = new ConcurrentHashMap<>();
    }

    public void createTopic(String topic) {
        if (!topicPartitions.containsKey(topic)) {
            List<Partition> partitions = new ArrayList<>();
            for (int i = 0; i < defaultPartitionCount; i++) {
                partitions.add(new Partition(topic, i));
            }
            topicPartitions.put(topic, partitions);
        }
    }

    public void addMessage(String topic, String message) {
        createTopic(topic); // Ensure topic exists
        List<Partition> partitions = topicPartitions.get(topic);
        // Simple hash-based partitioning
        int partition = Math.abs(message.hashCode() % defaultPartitionCount);
        partitions.get(partition).addMessage(message);
    }

    public String consumeMessage(String topic, String groupId, String consumerId) {
        if (!topicPartitions.containsKey(topic)) {
            return null;
        }

        // Get assigned partitions for this consumer
        List<Integer> assignedPartitions = getAssignedPartitions(topic, groupId, consumerId);

        // Try to consume from assigned partitions
        for (Integer partitionId : assignedPartitions) {
            Partition partition = topicPartitions.get(topic).get(partitionId);
            if (partition.hasMessages()) {
                return partition.consumeMessage();
            }
        }

        return null;
    }

    public List<Integer> getAssignedPartitions(String topic, String groupId, String consumerId) {
        // Initialize group if it doesn't exist
        consumerGroupAssignments.putIfAbsent(groupId, new ConcurrentHashMap<>());
        Map<String, List<Integer>> groupAssignments = consumerGroupAssignments.get(groupId);

        // Add the consumer to the group if it's new
        groupAssignments.putIfAbsent(consumerId, new ArrayList<>());

        // If no partitions are assigned, trigger rebalancing
        if (groupAssignments.get(consumerId).isEmpty()) {
            rebalanceGroup(topic, groupId);
        }

        return groupAssignments.get(consumerId);
    }

    private void rebalanceGroup(String topic, String groupId) {
        Map<String, List<Integer>> groupAssignments = consumerGroupAssignments.get(groupId);
        List<String> consumers = new ArrayList<>(groupAssignments.keySet());

        if (consumers.isEmpty()) {
            return; // No consumers to assign partitions to
        }

        // Clear existing assignments but maintain consumer entries
        for (String consumer : consumers) {
            groupAssignments.put(consumer, new ArrayList<>());
        }

        // Simple round-robin partition assignment
        for (int i = 0; i < defaultPartitionCount; i++) {
            String consumer = consumers.get(i % consumers.size());
            groupAssignments.get(consumer).add(i);
        }
    }
}

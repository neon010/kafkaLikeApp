package org.example;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

class Partition {
    private final String topic;
    private final int partitionId;
    private final List<String> messages;
    private final Map<String, Integer> groupOffsets;
    private final ReentrantLock lock;

    public Partition(String topic, int partitionId) {
        this.topic = topic;
        this.partitionId = partitionId;
        this.messages = new ArrayList<>();
        this.groupOffsets = new HashMap<>();
        this.lock = new ReentrantLock();
    }

    public void addMessage(String message) {
        lock.lock();
        try {
            messages.add(message);
        } finally {
            lock.unlock();
        }
    }

    public String consumeMessage(String groupId) {
        lock.lock();
        try {
            int offset = groupOffsets.getOrDefault(groupId, 0);
            if (offset >= messages.size()) {
                return null;
            }
            String message = messages.get(offset);
            groupOffsets.put(groupId, offset + 1);
            return message;
        } finally {
            lock.unlock();
        }
    }

    public boolean hasMessages(String groupId) {
        lock.lock();
        try {
            int offset = groupOffsets.getOrDefault(groupId, 0);
            return offset < messages.size();
        } finally {
            lock.unlock();
        }
    }

    public int getPartitionId() {
        return partitionId;
    }
}
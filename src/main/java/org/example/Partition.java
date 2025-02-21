package org.example;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

class Partition {
    private final Queue<String> messages;
    private final int partitionId;
    private final String topic;

    public Partition(String topic, int partitionId) {
        this.messages = new ConcurrentLinkedQueue<>();
        this.partitionId = partitionId;
        this.topic = topic;
    }

    public void addMessage(String message) {
        messages.offer(message);
    }

    public String consumeMessage() {
        return messages.poll();
    }

    public boolean hasMessages() {
        return !messages.isEmpty();
    }
}

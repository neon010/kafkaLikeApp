package org.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class KafkaTest {
    // ANSI color codes for clear logging
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_YELLOW = "\u001B[33m";
    private static final String ANSI_RED = "\u001B[31m";
    private static final String ANSI_RESET = "\u001B[0m";

    public static void main(String[] args) throws InterruptedException, IOException {
        // Start broker in a separate thread
        Thread brokerThread = new Thread(() -> {
            try {
                KafkaBroker.main(new String[]{});
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        brokerThread.start();

        // Wait for broker to initialize
        Thread.sleep(2000);

        // Create topic first
        KafkaProducer setupProducer = new KafkaProducer();
        try {
            setupProducer.connect();
            setupProducer.createTopic("test-topic");
            setupProducer.listTopics();
        } catch (IOException e) {
            System.err.println("Setup error: " + e.getMessage());
        } finally {
            setupProducer.disconnect();
        }

        System.out.println("\n=== Test Case 1: Same Consumer Group (Load Balancing) ===");
        testSameConsumerGroup();

//        Thread.sleep(2000); // Pause between tests
//
//        System.out.println("\n=== Test Case 2: Different Consumer Groups (Broadcasting) ===");
//        testDifferentConsumerGroups();
    }

    /**
     * Test where two consumers with the SAME group ID should split messages
     */
    private static void testSameConsumerGroup() throws InterruptedException {
        int numConsumers = 2; // Consumers in the same group
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(numConsumers);
        CountDownLatch latch = new CountDownLatch(numConsumers);

        // Create consumers first so they're ready for assignment
        List<KafkaConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            KafkaConsumer consumer = new KafkaConsumer("same-group");
            consumers.add(consumer);
            consumer.connect("localhost", 9092);
            consumer.subscribe("test-topic");
            System.out.println(ANSI_YELLOW + "Created and subscribed consumer " + consumer.getConsumerId() + ANSI_RESET);
        }

        // Allow time for subscription and rebalancing
        Thread.sleep(1000);

        // Start consumer threads
        for (KafkaConsumer consumer : consumers) {
            consumerExecutor.submit(new ConsumerTask(consumer, ANSI_YELLOW, latch));
        }

        Thread.sleep(1000); // Ensure consumers connect before producing messages

        // Produce messages
        produceMessages(10);

        // Wait for consumers to finish
        latch.await(10, TimeUnit.SECONDS);
        consumerExecutor.shutdown();

        // Disconnect all consumers
        for (KafkaConsumer consumer : consumers) {
            consumer.disconnect();
        }
    }

    /**
     * Test where consumers in different groups should each receive all messages
     */
    private static void testDifferentConsumerGroups() throws InterruptedException {
        // Create and start consumers from group1
        List<KafkaConsumer> group1Consumers = new ArrayList<>();
        String group1Id = "group1";
        for (int i = 0; i < 2; i++) {
            KafkaConsumer consumer = new KafkaConsumer(group1Id);
            group1Consumers.add(consumer);
            consumer.connect("localhost", 9092);
            consumer.subscribe("test-topic");
            System.out.println(ANSI_YELLOW + "Created and subscribed consumer " + consumer.getConsumerId() +
                    " in " + group1Id + ANSI_RESET);
        }

        // Create and start consumers from group2
        List<KafkaConsumer> group2Consumers = new ArrayList<>();
        String group2Id = "group2";
        for (int i = 0; i < 2; i++) {
            KafkaConsumer consumer = new KafkaConsumer(group2Id);
            group2Consumers.add(consumer);
            consumer.connect("localhost", 9092);
            consumer.subscribe("test-topic");
            System.out.println(ANSI_RED + "Created and subscribed consumer " + consumer.getConsumerId() +
                    " in " + group2Id + ANSI_RESET);
        }

        // Allow time for subscription and rebalancing
        Thread.sleep(1000);

        // Prepare executors and latches for both groups
        ExecutorService executorService = Executors.newFixedThreadPool(group1Consumers.size() + group2Consumers.size());
        CountDownLatch latch = new CountDownLatch(group1Consumers.size() + group2Consumers.size());

        // Start consumer threads for group1
        for (KafkaConsumer consumer : group1Consumers) {
            executorService.submit(new ConsumerTask(consumer, ANSI_YELLOW, "group1", latch));
        }

        // Start consumer threads for group2
        for (KafkaConsumer consumer : group2Consumers) {
            executorService.submit(new ConsumerTask(consumer, ANSI_RED, "group2", latch));
        }

        Thread.sleep(1000); // Ensure consumers connect before producing messages

        // Produce messages
        produceMessages(10);

        // Wait for consumers to finish
        latch.await(15, TimeUnit.SECONDS);
        executorService.shutdown();

        // Print summary of results
        System.out.println("\n=== Test Summary ===");
        System.out.println("Each consumer in group1 should receive approximately half of the messages");
        System.out.println("Each consumer in group2 should receive approximately half of the messages");
        System.out.println("Together, group1 and group2 should each receive ALL messages");
        System.out.println("This demonstrates that Kafka delivers all messages to each consumer group");

        // Disconnect all consumers
        for (KafkaConsumer consumer : group1Consumers) {
            consumer.disconnect();
        }
        for (KafkaConsumer consumer : group2Consumers) {
            consumer.disconnect();
        }
    }

    /**
     * Produces a set number of messages to the Kafka topic.
     */
    private static void produceMessages(int count) {
        KafkaProducer producer = new KafkaProducer();
        try {
            producer.connect();
            String messageKey = UUID.randomUUID().toString();
            for (int i = 0; i < count; i++) {
                // Use different keys to distribute messages across partitions
                String message = "Message " + i;
                producer.send("test-topic", messageKey, message);
                System.out.println(ANSI_BLUE + "[Producer] Sent: " + message + " with key: " + messageKey + ANSI_RESET);
                Thread.sleep(200);
            }
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
        } finally {
            producer.disconnect();
        }
    }

    /**
     * ConsumerTask represents a Kafka consumer
     */
    static class ConsumerTask implements Runnable {
        private final KafkaConsumer consumer;
        private final String color;
        private final CountDownLatch latch;
        private final String groupId; // Store group ID for logging

        ConsumerTask(KafkaConsumer consumer, String color, CountDownLatch latch) {
            this(consumer, color, "same-group", latch);
        }

        ConsumerTask(KafkaConsumer consumer, String color, String groupId, CountDownLatch latch) {
            this.consumer = consumer;
            this.color = color;
            this.groupId = groupId;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                long endTime = System.currentTimeMillis() + 10000; // Run for 10 seconds
                int messagesConsumed = 0;

                while (System.currentTimeMillis() < endTime) {
                    List<String> records = consumer.poll(1000);
                    for (String record : records) {
                        messagesConsumed++;
                        System.out.println(color + "[" + consumer.getConsumerId() +
                                " in " + groupId + "] received: " + record + ANSI_RESET);
                    }
                    Thread.sleep(500); // Avoid spamming poll requests
                }

                System.out.println(color + "Consumer " + consumer.getConsumerId() +
                        " in group " + groupId +
                        " received a total of " + messagesConsumed + " messages" + ANSI_RESET);
            } catch (Exception e) {
                System.err.println(color + "Consumer error in " + consumer.getConsumerId() +
                        ": " + e.getMessage() + ANSI_RESET);
            } finally {
                latch.countDown();
            }
        }
    }
}
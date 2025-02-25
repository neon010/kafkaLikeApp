package org.example;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

public class KafkaTest {
    // ANSI color codes for clear logging
    private static final String ANSI_GREEN = "\u001B[32m";
    private static final String ANSI_BLUE = "\u001B[34m";
    private static final String ANSI_YELLOW = "\u001B[33m";
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

//        System.out.println("\n=== Test Case 1: Different Consumer Groups ===");
//        testMultipleConsumerGroups();

        System.out.println("\n=== Test Case 2: Same Consumer Group (Load Balancing) ===");
        testSameConsumerGroup();
    }

    /**
     * Test where two different consumer groups consume independently
     */
    private static void testMultipleConsumerGroups() throws InterruptedException {
        int numConsumersPerGroup = 2;
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(numConsumersPerGroup * 2);
        CountDownLatch latch = new CountDownLatch(numConsumersPerGroup * 2);

        // Create consumers from different groups
        for (int i = 0; i < numConsumersPerGroup; i++) {
            consumerExecutor.submit(new ConsumerTask("group1", ANSI_GREEN, latch));
            consumerExecutor.submit(new ConsumerTask("group2", ANSI_BLUE, latch));
        }

        Thread.sleep(1000); // Ensure consumers connect before producing messages

        // Produce messages
        produceMessages(6);

        // Wait for consumers to finish
        latch.await();
        consumerExecutor.shutdown();
    }

    /**
     * Test where two consumers with the SAME group ID should split messages
     */
    private static void testSameConsumerGroup() throws InterruptedException {
        int numConsumers = 2; // Consumers in the same group
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(numConsumers);
        CountDownLatch latch = new CountDownLatch(numConsumers);

        // Start consumers in the same group
        for (int i = 0; i < numConsumers; i++) {
            consumerExecutor.submit(new ConsumerTask("same-group", ANSI_YELLOW, latch));
        }

        Thread.sleep(1000); // Ensure consumers connect before producing messages

        // Produce messages
        produceMessages(6);

        // Wait for consumers to finish
        latch.await();
        consumerExecutor.shutdown();
    }

    /**
     * Produces a set number of messages to the Kafka topic.
     */
    private static void produceMessages(int count) {
        KafkaProducer producer = new KafkaProducer();
        try {
            producer.connect();
            UUID uuid = UUID.randomUUID();
            for (int i = 0; i < count; i++) {
                String message = "Message " + i;
                producer.send("test-topic", String.valueOf(uuid), message);
                System.out.println(ANSI_BLUE + "[Producer] Sent: " + message + ANSI_RESET);
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
        private final String groupId;
        private final String color;
        private final CountDownLatch latch;

        ConsumerTask(String groupId, String color, CountDownLatch latch) {
            this.groupId = groupId;
            this.color = color;
            this.latch = latch;
        }

        @Override
        public void run() {
            KafkaConsumer consumer = new KafkaConsumer(groupId);
            try {
                consumer.connect("localhost", 9092);
                consumer.subscribe("test-topic");

                long endTime = System.currentTimeMillis() + 6000; // Run for 6 seconds
                while (System.currentTimeMillis() < endTime) {
                    List<String> records = consumer.poll(1500);
                    for (String record : records) {
                        System.out.println(color + "[" + groupId + "] Consumer-" + Thread.currentThread().getId() +
                                " received: " + record + ANSI_RESET);
                    }
                    Thread.sleep(500); // Avoid spamming poll requests
                }
            } catch (Exception e) {
                System.err.println(color + "Consumer error in " + groupId + ": " + e.getMessage() + ANSI_RESET);
            } finally {
                consumer.disconnect();
                latch.countDown();
            }
        }
    }
}

package org.example;

import java.io.IOException;

public class KafkaTest {
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

        // Wait for broker to start
        Thread.sleep(1000);

        // Test Case 1: Single Producer, Single Consumer
        System.out.println("\n=== Test Case 1: Single Producer, Single Consumer ===");
        testSingleProducerConsumer();

        // Test Case 2: Single Producer, Multiple Consumers in Same Group
        System.out.println("\n=== Test Case 2: Consumer Group Test ===");
        testConsumerGroup();

        // Test Case 3: Multiple Producers, Multiple Consumer Groups
        System.out.println("\n=== Test Case 3: Multiple Producers and Consumer Groups ===");
        testMultipleProducersAndGroups();
    }

    private static void testSingleProducerConsumer() throws InterruptedException, IOException {
        // Start a consumer
        Thread consumerThread = new Thread(() -> {
            KafkaConsumer consumer = new KafkaConsumer("group1");
            consumer.connect("localhost", 9092);
            consumer.subscribe("test-topic");
        });
        consumerThread.start();

        // Wait for consumer to connect
        Thread.sleep(1000);

        // Produce messages
        KafkaProducer producer = new KafkaProducer();
        producer.connect();

        // Send multiple messages
        String[] messages = {"Message 1", "Message 2", "Message 3"};
        for (String message : messages) {
            producer.send("test-topic", message);
            Thread.sleep(500);
        }

        producer.disconnect();
        Thread.sleep(2000); // Wait for messages to be consumed
    }

    private static void testConsumerGroup() throws InterruptedException, IOException {
        // Start multiple consumers in the same group
        for (int i = 0; i < 3; i++) {
            final int consumerId = i;
            new Thread(() -> {
                KafkaConsumer consumer = new KafkaConsumer("group2");
                consumer.connect("localhost", 9092);
                consumer.subscribe("group-test-topic");
            }).start();
        }

        // Wait for consumers to connect
        Thread.sleep(1000);

        // Produce messages
        KafkaProducer producer = new KafkaProducer();
        producer.connect();

        // Send multiple messages
        for (int i = 0; i < 10; i++) {
            producer.send("group-test-topic", "Group Message " + i);
            Thread.sleep(200);
        }

        producer.disconnect();
        Thread.sleep(3000); // Wait for messages to be consumed
    }

    private static void testMultipleProducersAndGroups() throws InterruptedException {
        // Start consumers in different groups
        String[] groups = {"groupA", "groupB"};
        for (String group : groups) {
            for (int i = 0; i < 2; i++) {
                final String currentGroup = group;
                new Thread(() -> {
                    KafkaConsumer consumer = new KafkaConsumer(currentGroup);
                    consumer.connect("localhost", 9092);
                    consumer.subscribe("multi-test-topic");
                }).start();
            }
        }

        // Wait for consumers to connect
        Thread.sleep(1000);

        // Start multiple producers
        Thread[] producers = new Thread[3];
        for (int i = 0; i < producers.length; i++) {
            final int producerId = i;
            producers[i] = new Thread(() -> {
                KafkaProducer producer = new KafkaProducer();
                try {
                    producer.connect();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < 5; j++) {
                    try {
                        producer.send("multi-test-topic",
                                "Producer " + producerId + " Message " + j);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                producer.disconnect();
            });
            producers[i].start();
        }

        // Wait for all producers to finish
        for (Thread producer : producers) {
            producer.join();
        }

        Thread.sleep(3000); // Wait for messages to be consumed
    }
}
package org.example;

import java.io.IOException;
import java.util.List;

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
//        System.out.println("\n=== Test Case 1: Single Producer, Single Consumer ===");
//        testSingleProducerConsumer();

        // Test Case 2: Single Producer, Multiple Consumers in Same Group
        System.out.println("\n=== Test Case 2: Consumer Group Test ===");
        testMultipleConsumerGroups();

        // Test Case 3: Multiple Producers, Multiple Consumer Groups
//        System.out.println("\n=== Test Case 3: Multiple Producers and Consumer Groups ===");
//        testMultipleProducersAndGroups();
    }

    private static void testSingleProducerConsumer() throws InterruptedException, IOException {
        // Start a consumer

        Thread consumerThread = new Thread(() -> {
            KafkaConsumer consumer = new KafkaConsumer("group1");
            try {
                consumer.connect("localhost", 9092);
                consumer.subscribe("test-topic");
                while (true) {
                    List<String> records = consumer.poll(1000);
                    for (String record : records) {
                        System.out.println("Received: " + record);
                    }
                }
            } finally {
                consumer.disconnect();
            }

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

    private static void testMultipleConsumerGroups() throws InterruptedException {
        // Create a consumer runnable for group1
        Runnable consumerTaskGroup1 = () -> {
            KafkaConsumer consumer = new KafkaConsumer("group1");
            try {
                consumer.connect("localhost", 9092);
                consumer.subscribe("test-topic");

                long endTime = System.currentTimeMillis() + 5000;
                while (System.currentTimeMillis() < endTime) {
                    try {
                        List<String> records = consumer.poll(1000);
                        for (String record : records) {
                            System.out.println("[Group1] " + Thread.currentThread().getName() +
                                    " received: " + record);
                        }
                    } catch (Exception e) {
                        System.err.println("Error polling messages in Group1: " + e.getMessage());
                        break;
                    }
                }
            } catch (Exception e) {
                System.err.println("Consumer error in Group1: " + e.getMessage());
            } finally {
                consumer.disconnect();
            }
        };

        // Create a consumer runnable for group2
        Runnable consumerTaskGroup2 = () -> {
            KafkaConsumer consumer = new KafkaConsumer("group2");
            try {
                consumer.connect("localhost", 9092);
                consumer.subscribe("test-topic");

                long endTime = System.currentTimeMillis() + 5000;
                while (System.currentTimeMillis() < endTime) {
                    try {
                        List<String> records = consumer.poll(1000);
                        for (String record : records) {
                            System.out.println("[Group2] " + Thread.currentThread().getName() +
                                    " received: " + record);
                        }
                    } catch (Exception e) {
                        System.err.println("Error polling messages in Group2: " + e.getMessage());
                        break;
                    }
                }
            } catch (Exception e) {
                System.err.println("Consumer error in Group2: " + e.getMessage());
            } finally {
                consumer.disconnect();
            }
        };

        // Start multiple consumers in each group
        Thread consumerThread1 = new Thread(consumerTaskGroup1, "Consumer-1-Group1");
//        Thread consumerThread2 = new Thread(consumerTaskGroup1, "Consumer-2-Group1");
        Thread consumerThread3 = new Thread(consumerTaskGroup2, "Consumer-1-Group2");
//        Thread consumerThread4 = new Thread(consumerTaskGroup2, "Consumer-2-Group2");

        consumerThread1.start();
//        consumerThread2.start();
        consumerThread3.start();
//        consumerThread4.start();

        Thread.sleep(1000);

        // Produce messages
        KafkaProducer producer = new KafkaProducer();
        try {
            producer.connect();

            for (int i = 0; i < 10; i++) {
                producer.send("test-topic", "Group Message " + i);
                Thread.sleep(200);
            }
        } catch (Exception e) {
            System.err.println("Producer error: " + e.getMessage());
        } finally {
            producer.disconnect();
        }

        // Wait for consumers to process messages
        consumerThread1.join(5000);
//        consumerThread2.join(5000);
        consumerThread3.join(5000);
//        consumerThread4.join(5000);
    }




}
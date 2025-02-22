package org.example;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class KafkaConsumer {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final String consumerId;
    private final String groupId;
    private String subscribedTopic;
    private static final int DEFAULT_POLL_TIMEOUT = 1000; // milliseconds

    public KafkaConsumer(String groupId) {
        this.consumerId = UUID.randomUUID().toString();
        this.groupId = groupId;
    }

    public void connect(String host, int port) {
        try {
            socket = new Socket(host, port);
            out = new PrintWriter(socket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            // Send consumer registration
            out.println("REGISTER:" + groupId + ":" + consumerId);
            String response = in.readLine();
            System.out.println("Broker Response: " + response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void subscribe(String topic) {
        if (socket == null || out == null) {
            System.out.println("ERROR: Consumer is not connected to broker!");
            return;
        }
        this.subscribedTopic = topic;
        System.out.println("Subscribed to topic: " + topic);
    }

    public List<String> poll() {
        return poll(DEFAULT_POLL_TIMEOUT);
    }

    public List<String> poll(long timeoutMs) {
        if (subscribedTopic == null) {
            throw new IllegalStateException("No topic subscribed. Please subscribe to a topic first.");
        }

        if (socket == null || out == null) {
            throw new IllegalStateException("Consumer is not connected to broker!");
        }

        List<String> records = new ArrayList<>();
        long startTime = System.currentTimeMillis();

        try {
            while (System.currentTimeMillis() - startTime < timeoutMs) {
                out.println("CONSUME:" + subscribedTopic + ":" + groupId + ":" + consumerId);
                String response = in.readLine();

                if (response != null && !response.contains("NO_MESSAGES")) {
                    String message = response.replace("MESSAGE: ", "");
                    records.add(message);
                } else {
                    // If no messages, wait a bit before trying again
                    Thread.sleep(100);
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return records;
    }

    public void disconnect() {
        try {
            if (out != null) {
                out.println("DISCONNECT:" + groupId + ":" + consumerId);
            }
            if (socket != null) socket.close();
            System.out.println("Consumer disconnected from broker.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        if (args.length < 2) {
//            System.out.println("Usage: java KafkaConsumer <group-id> <topic>");
//            return;
//        }

        KafkaConsumer consumer1 = new KafkaConsumer("group1");
        KafkaConsumer consumer2 = new KafkaConsumer("group2");
        consumer1.connect("localhost", 9092);
        consumer2.connect("localhost", 9092);
        consumer1.subscribe("test-topic");
        consumer2.subscribe("test-topic");

        // Example usage with polling
        try {
            while (true) {
                List<String> records1 = consumer1.poll(1000);
                List<String> records2 = consumer1.poll(1000);
                for (String record : records1) {
                    System.out.println("Received on group1: " + record);
                }
                for (String record : records2) {
                    System.out.println("Received on group2: " + record);
                }
            }
        } finally {
            consumer1.disconnect();
            consumer2.disconnect();
        }
    }
}
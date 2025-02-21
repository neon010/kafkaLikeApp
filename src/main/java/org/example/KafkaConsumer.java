package org.example;

import java.io.*;
import java.net.Socket;
import java.util.UUID;

public class KafkaConsumer {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private final String consumerId;
    private final String groupId;

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

        new Thread(() -> {
            try {
                while (true) {
                    out.println("CONSUME:" + topic + ":" + groupId + ":" + consumerId);
                    String response = in.readLine();
                    if (!response.contains("NO_MESSAGES")) {
                        System.out.println("Received: " + response);
                    }
                    Thread.sleep(1000);  // Poll interval
                }
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }).start();
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

        KafkaConsumer consumer = new KafkaConsumer("group1");
        consumer.connect("localhost", 9092);
        consumer.subscribe("test-topic");
    }
}
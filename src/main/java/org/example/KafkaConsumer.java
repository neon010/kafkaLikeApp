package org.example;

import java.io.*;
import java.net.Socket;

public class KafkaConsumer {
    private final String brokerAddress;
    private final int brokerPort;
    private BufferedReader in;

    public KafkaConsumer(String brokerAddress, int brokerPort) {
        this.brokerAddress = brokerAddress;
        this.brokerPort = brokerPort;
    }

    public void subscribe(String topic) {
        try (Socket socket = new Socket(brokerAddress, brokerPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.println("SUBSCRIBE " + topic);
            System.out.println("Subscribed to topic '" + topic + "'. Listening for messages...");

            while (true) {
                String message = readMessages();
                if (message != null) {
                    System.out.println("Received: " + message);
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to connect to broker: " + e.getMessage());
        }
    }

    String readMessages() {
        try {
            return in.readLine();  // Read a single message from the broker
        } catch (IOException e) {
            System.err.println("Failed to read message: " + e.getMessage());
            return null;
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java KafkaConsumer <brokerAddress> <port> <topic>");
            return;
        }

        String brokerAddress = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        String topic = args[2];

        KafkaConsumer consumer = new KafkaConsumer(brokerAddress, brokerPort);
        consumer.subscribe(topic);
    }
}

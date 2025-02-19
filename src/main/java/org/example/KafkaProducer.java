package org.example;

import java.io.*;
import java.net.Socket;

public class KafkaProducer {
    private final String brokerAddress;
    private final int brokerPort;

    public KafkaProducer(String brokerAddress, int brokerPort) {
        this.brokerAddress = brokerAddress;
        this.brokerPort = brokerPort;
    }

    public void send(String topic, String message) {
        try (Socket socket = new Socket(brokerAddress, brokerPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            String request = "PUBLISH " + topic + " " + message;
            out.println(request);

            String response = in.readLine();

            System.out.println("Response from broker: " + response);
        } catch (IOException e) {
            System.err.println("Failed to connect to broker: " + e.getMessage());
        }
    }

    public void createTopic(String topic) {
        System.out.println("Creating topic: " + topic);
        // Logic to create a topic (e.g., sending a "CREATE TOPIC" request to the broker)
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java org.example.KafkaProducer <brokerAddress> <brokerPort> <command> [<topic>] [<message>]");
            System.out.println("Commands:");
            System.out.println("  createTopic <topic>         Create a new topic");
            System.out.println("  send <topic> <message>      Send a message to a topic");
            return;
        }

        String brokerAddress = args[0];
        int brokerPort = Integer.parseInt(args[1]);
        String command = args[2];

        KafkaProducer producer = new KafkaProducer(brokerAddress, brokerPort);

        switch (command) {
            case "createTopic":
                if (args.length < 4) {
                    System.out.println("Usage: createTopic <topic>");
                } else {
                    String topic = args[3];
                    producer.createTopic(topic);
                }
                break;
            case "send":
                if (args.length < 5) {
                    System.out.println("Usage: send <topic> <message>");
                } else {
                    String topic = args[3];
                    String message = args[4];
                    producer.send(topic, message);
                }
                break;
            default:
                System.out.println("Unknown command: " + command);
                break;
        }
    }
}


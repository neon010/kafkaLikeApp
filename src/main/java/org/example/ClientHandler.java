package org.example;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
             PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

            String request;
            while ((request = in.readLine()) != null) {
                if (request.startsWith("CREATE")) {
                    createTopic(request, out);
                } else if (request.startsWith("LIST")) {
                    listTopics(out);
                } else if (request.startsWith("PUBLISH")) {
                    handlePublish(request, out);
                } else if (request.startsWith("SUBSCRIBE")) {
                    handleSubscribe(request, out);
                } else {
                    out.println("Invalid command.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void createTopic(String request, PrintWriter out) {
        String[] parts = request.split(" ");
        if (parts.length < 2) {
            out.println("Usage: CREATE <topic>");
            return;
        }
        String topic = parts[1];
        Map<String, List<String>> topics = KafkaBroker.getTopics();
        Map<String, List<PrintWriter>> subscribers = KafkaBroker.getSubscribers();

        if (topics.containsKey(topic)) {
            out.println("Topic already exists.");
        } else {
            topics.put(topic, new ArrayList<>());
            subscribers.put(topic, new ArrayList<>());
            out.println("Topic '" + topic + "' created successfully.");
        }
    }

    private void listTopics(PrintWriter out) {
        Map<String, List<String>> topics = KafkaBroker.getTopics();
        if (topics.isEmpty()) {
            out.println("No topics available.");
        } else {
            out.println("Available topics: " + String.join(", ", topics.keySet()));
        }
    }

    private void handlePublish(String request, PrintWriter out) {
        String[] parts = request.split(" ", 3);
        if (parts.length < 3) {
            out.println("Usage: PUBLISH <topic> <message>");
            out.flush();
            return;
        }
        String topic = parts[1];
        String message = parts[2];

        Map<String, List<String>> topics = KafkaBroker.getTopics();
        Map<String, List<PrintWriter>> subscribers = KafkaBroker.getSubscribers();

        // If the topic doesn't exist, create it
        topics.putIfAbsent(topic, new ArrayList<>());
        subscribers.putIfAbsent(topic, new ArrayList<>());

        // Add the message to the topic's message list
        topics.get(topic).add(message);

        // Notify all active subscribers of this topic
        for (PrintWriter subscriber : subscribers.get(topic)) {
            subscriber.println("New message on topic '" + topic + "': " + message);
            subscriber.flush();
        }

        // Send a response back to the original publisher
        out.println("Message published successfully.");
        out.flush();
    }

    private void handleSubscribe(String request, PrintWriter out) {
        String[] parts = request.split(" ");
        if (parts.length < 2) {
            out.println("Usage: SUBSCRIBE <topic>");
            return;
        }
        String topic = parts[1];

        Map<String, List<String>> topics = KafkaBroker.getTopics();
        Map<String, List<PrintWriter>> subscribers = KafkaBroker.getSubscribers();

        if (topics.containsKey(topic)) {
            out.println("Subscribed to topic '" + topic + "'.");
            subscribers.get(topic).add(out);

            // Send all existing messages in the topic
            for (String message : topics.get(topic)) {
                out.println("Message: " + message);
            }
        } else {
            out.println("Topic '" + topic + "' does not exist.");
        }
    }
}
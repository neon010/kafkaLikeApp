package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KafkaBroker {
    private static final int PORT = 9092;
    private static final Map<String, List<String>> topicMessages = new HashMap<>();
    private static final Map<String, List<PrintWriter>> subscribers = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("KafkaBroker is running...");
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)) {

                String request;
                while ((request = in.readLine()) != null) {
                    System.out.println("Received request: " + request);

                    if (request.startsWith("PUBLISH")) {
                        handlePublish(request);
                        out.println("Message published.");
                    } else if (request.startsWith("SUBSCRIBE")) {
                        handleSubscribe(request, out);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void handlePublish(String request) {
            String[] parts = request.split(" ", 3);
            if (parts.length < 3) return;
            String topic = parts[1];
            String message = parts[2];

            topicMessages.computeIfAbsent(topic, k -> new ArrayList<>()).add(message);
            List<PrintWriter> topicSubscribers = subscribers.get(topic);
            if (topicSubscribers != null) {
                for (PrintWriter subscriber : topicSubscribers) {
                    subscriber.println(message);  // Send message to all subscribers
                }
            }
        }

        private void handleSubscribe(String request, PrintWriter out) {
            String[] parts = request.split(" ");
            if (parts.length < 2) return;
            String topic = parts[1];

            subscribers.computeIfAbsent(topic, k -> new ArrayList<>()).add(out);
            out.println("Subscribed to topic: " + topic);
        }
    }
}


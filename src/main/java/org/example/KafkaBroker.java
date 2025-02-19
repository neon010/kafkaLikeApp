package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class KafkaBroker {
    private static final int PORT = 9092;
    private static final Map<String, List<String>> topics = new HashMap<>();
    private static final Map<String, List<PrintWriter>> subscribers = new HashMap<>();

    public static void main(String[] args) {
        System.out.println("Kafka Broker started on port " + PORT);
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // Getter methods for ClientHandler to access the static collections
    public static Map<String, List<String>> getTopics() {
        return topics;
    }

    public static Map<String, List<PrintWriter>> getSubscribers() {
        return subscribers;
    }
}
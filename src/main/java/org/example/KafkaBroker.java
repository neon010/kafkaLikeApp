package org.example;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

public class KafkaBroker {
    private static final int PORT = 9092;
    private static final TopicManager topicManager = new TopicManager(3);

    public static void main(String[] args) throws IOException {
        ServerSocket serverSocket = new ServerSocket(PORT);
        System.out.println("Broker is running on port " + PORT);

        while (true) {
            try {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket);
                new ClientHandler(clientSocket, topicManager).start();
            } catch (IOException e) {
                System.err.println("Error accepting client connection: " + e.getMessage());
            }
        }
    }
}
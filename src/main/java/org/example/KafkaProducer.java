package org.example;

import java.io.*;
import java.net.Socket;
import java.util.Properties;

public class KafkaProducer {
    private static final String HOST = "localhost";
    private static final int PORT = 9092;
    private static final int DEFAULT_RETRIES = 3;
    private static final long RETRY_BACKOFF_MS = 1000;

    private final Properties properties;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;
    private boolean connected = false;

    public KafkaProducer() {
        this(new Properties());
    }

    public KafkaProducer(Properties properties) {
        this.properties = properties;
    }

    public void connect() throws IOException {
        if (connected) {
            return;
        }

        int retries = 0;
        while (retries < DEFAULT_RETRIES) {
            try {
                socket = new Socket(HOST, PORT);
                out = new PrintWriter(socket.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

                String welcome = in.readLine();
                if (welcome != null && welcome.startsWith("CONNECTED")) {
                    connected = true;
                    System.out.println("Connected to KafkaLikeBroker");
                    System.out.println(welcome);
                    return;
                }
            } catch (IOException e) {
                retries++;
                if (retries >= DEFAULT_RETRIES) {
                    throw new IOException("Failed to connect after " + DEFAULT_RETRIES + " attempts", e);
                }
                try {
                    Thread.sleep(RETRY_BACKOFF_MS);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Connection interrupted", ie);
                }
            }
        }
    }

    public boolean send(String topic, String message) throws IOException {
        if (!connected) {
            throw new IllegalStateException("Producer is not connected to broker");
        }

        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }

        if (message == null) {
            throw new IllegalArgumentException("Message cannot be null");
        }

        try {
            // Send the message
            out.println("PRODUCE:" + topic + ":" + message);

            // Wait for acknowledgment
            String response = in.readLine();
            if (response != null && response.startsWith("ACK")) {
                System.out.println("Message sent successfully: " + response);
                return true;
            } else {
                System.err.println("Failed to send message: " + response);
                return false;
            }
        } catch (IOException e) {
            System.err.println("Error sending message: " + e.getMessage());
            // Try to reconnect and retry once
            reconnect();
            out.println("PRODUCE:" + topic + ":" + message);
            String response = in.readLine();
            return response != null && response.startsWith("ACK");
        }
    }

    private void reconnect() throws IOException {
        disconnect();
        connect();
    }

    public void disconnect() {
        if (!connected) {
            return;
        }

        try {
            if (out != null) {
                out.println("DISCONNECT");
                String response = in.readLine();
                System.out.println(response);
            }
        } catch (IOException e) {
            System.err.println("Error during disconnect: " + e.getMessage());
        } finally {
            connected = false;
            try {
                if (socket != null) socket.close();
                if (out != null) out.close();
                if (in != null) in.close();
            } catch (IOException e) {
                System.err.println("Error closing resources: " + e.getMessage());
            }
            System.out.println("Disconnected from broker.");
        }
    }

    public static void main(String[] args) {
//        if (args.length < 2) {
//            System.out.println("Usage: java KafkaProducer <topic> <message>");
//            return;
//        }

//        String topic = args[0];
//        StringBuilder messageBuilder = new StringBuilder();
//        for (int i = 1; i < args.length; i++) {
//            messageBuilder.append(args[i]).append(" ");
//        }
//        String message = messageBuilder.toString().trim();

        KafkaProducer producer = new KafkaProducer();
        try {
            producer.connect();
            producer.send("test-topic", "hello kafka test");
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            producer.disconnect();
        }
    }
}
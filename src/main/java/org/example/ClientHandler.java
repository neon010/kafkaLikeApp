package org.example;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;

public class ClientHandler extends Thread {
    private final Socket socket;
    private final TopicManager topicManager;

    public ClientHandler(Socket socket, TopicManager topicManager) {
        this.socket = socket;
        this.topicManager = topicManager;
    }

    public void run() {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("CONNECTED: Welcome to KafkaLikeBroker");

            String input;
            String currentGroupId = null;
            String currentConsumerId = null;

            while ((input = in.readLine()) != null) {
                if (input.startsWith("REGISTER:")) {
                    String[] parts = input.split(":", 3);
                    currentGroupId = parts[1];
                    currentConsumerId = parts[2];
                    out.println("REGISTERED: Consumer " + currentConsumerId + " in group " + currentGroupId);
                }else if (input.startsWith("PRODUCE:")) {
                    String[] parts = input.split(":", 4);
                    if (parts.length < 4) {
                        out.println("ERROR: Invalid produce format. Use PRODUCE:topic:key:message");
                        continue;
                    }
                    String topic = parts[1];
                    String key = parts[2];
                    String message = parts[3];

                    topicManager.addMessage(topic, key, message);
                    out.println("ACK: Message stored in topic " + topic);
                } else if (input.startsWith("CONSUME:")) {
                    String[] parts = input.split(":", 4);
                    if (parts.length < 4) {
                        out.println("ERROR: Invalid consume format. Use CONSUME:topic:groupId:consumerId");
                        continue;
                    }
                    String topic = parts[1];
                    String groupId = parts[2];
                    String consumerId = parts[3];

                    String message = topicManager.consumeMessage(topic, groupId, consumerId);
                    out.println("MESSAGE: " + (message != null ? message : "NO_MESSAGES"));
                } else if (input.startsWith("DISCONNECT:")) {
                    String[] parts = input.split(":", 3);
                    if (parts.length >= 3) {
                        currentGroupId = parts[1];
                        currentConsumerId = parts[2];
                    }
                    out.println("DISCONNECTED: Goodbye!");
                    socket.close();
                    break;
                } else if(input.startsWith("CREATE-TOPIC:")){
                    String[] parts = input.split(":", 2);

                    String topicName = parts[1];
                    System.out.println("TOPIC NAME from createTopic: "+topicName);
                    topicManager.createTopic(topicName);
                    out.println("ACK: TOPIC CREATED");
                }else if(input.startsWith("LIST-TOPIC:")){
                    String topicMetaData = topicManager.listTopics();
                    if (topicMetaData.isEmpty()) {
                        out.println("TOPIC-METADATA: No topics available");
                    } else {
                        out.println("TOPIC-METADATA: " + topicMetaData);
                    }
                } else {
                    out.println("ERROR: Unknown command");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
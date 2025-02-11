package org.example;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            KafkaConsumer consumer = new KafkaConsumer("localhost");
            consumer.subscribe("topic1");

            // Read messages in a separate thread
            new Thread(consumer::readMessages).start();

            // Simulate running forever (or until interrupted)
            Thread.sleep(Long.MAX_VALUE);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
package org.example;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            KafkaConsumer consumer = new KafkaConsumer("localhost", 9092);
            consumer.subscribe("testTopic2");

            // Read messages in a separate thread
            new Thread(consumer::readMessages).start();

            // Simulate running forever (or until interrupted)
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
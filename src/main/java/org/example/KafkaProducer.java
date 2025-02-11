package org.example;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;

public class KafkaProducer {
    private final String brokerAddress;

    public KafkaProducer(String brokerAddress) {
        this.brokerAddress = brokerAddress;
    }

    public void send(String topic, String message) {
        try (Socket socket = new Socket(brokerAddress, 9092);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            out.println("PUBLISH " + topic + " " + message);
            Thread.sleep(100);  // Allow time for the broker to read the request
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        KafkaProducer producer = new KafkaProducer("localhost");
        producer.send("topic1", "test 123");
    }
}

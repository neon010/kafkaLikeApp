package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class KafkaConsumer {
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public KafkaConsumer(String brokerAddress) throws IOException {
        socket = new Socket(brokerAddress, 9092);
        out = new PrintWriter(socket.getOutputStream(), true);
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
    }


    public void subscribe(String topic) {
        out.println("SUBSCRIBE " + topic);
        System.out.println("Subscribed to topic: " + topic);
    }

    public void readMessages() {
        try {
            String response;
            while ((response = in.readLine()) != null) {
                System.out.println("Received message: " + response);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        socket.close();
    }

}

package org.example;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class KafkaProducerCLI {
    public static void main(String[] args) {
        try (Socket socket = new Socket("localhost", 9092);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            Scanner scanner = new Scanner(System.in);

            System.out.println("Kafka Producer CLI");
            System.out.println("====================");

            while (true) {
                System.out.println("\nOptions:");
                System.out.println("1. CREATE topic");
                System.out.println("2. LIST topics");
                System.out.println("3. PUBLISH message");
                System.out.print("Choose an option: ");
                int choice = Integer.parseInt(scanner.nextLine());

                switch (choice) {
                    case 1 -> {
                        System.out.print("Enter topic name: ");
                        String topic = scanner.nextLine();
                        out.println("CREATE " + topic);
                    }
                    case 2 -> {
                        out.println("LIST");
                    }
                    case 3 -> {
                        System.out.print("Enter topic: ");
                        String topic = scanner.nextLine();
                        System.out.print("Enter message: ");
                        String message = scanner.nextLine();
                        out.println("PUBLISH " + topic + " " + message);
                    }
                    default -> System.out.println("Invalid option.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


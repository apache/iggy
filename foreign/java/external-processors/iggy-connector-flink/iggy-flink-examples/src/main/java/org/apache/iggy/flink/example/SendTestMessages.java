package org.apache.iggy.flink.example;

import org.apache.iggy.client.blocking.http.IggyHttpClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;
import org.apache.iggy.message.Message;
import org.apache.iggy.message.Partitioning;

import java.util.List;

/**
 * Simple utility to send test messages to Iggy for WordCountJob testing.
 */
public class SendTestMessages {
    public static void main(String[] args) throws Exception {
        String serverAddress = System.getenv().getOrDefault("IGGY_SERVER", "localhost:3000");
        String username = System.getenv().getOrDefault("IGGY_USERNAME", "iggy");
        String password = System.getenv().getOrDefault("IGGY_PASSWORD", "iggy");

        // Create HTTP client
        IggyHttpClient client = new IggyHttpClient("http://" + serverAddress);

        System.out.println("Connecting to Iggy at " + serverAddress);

        // Login
        client.users().login(username, password);
        System.out.println("Logged in successfully!");

        // Test messages
        String[] messages = {
            "hello world hello flink",
            "apache flink connector for iggy",
            "streaming data processing with flink",
            "hello iggy hello streaming",
            "real time analytics with apache flink",
            "message broker and stream processing"
        };

        // Send messages using stream/topic names
        System.out.println("\nSending messages to text-input/lines...");
        for (String text : messages) {
            client.messages().sendMessages(
                StreamId.of("text-input"),
                TopicId.of("lines"),
                Partitioning.balanced(),
                List.of(Message.of(text))
            );
            System.out.println("✓ Sent: " + text);
        }

        System.out.println("\nDone! Sent " + messages.length + " messages to text-input/lines.");
        System.out.println("Check Flink job logs to see word counts being processed.");
    }
}

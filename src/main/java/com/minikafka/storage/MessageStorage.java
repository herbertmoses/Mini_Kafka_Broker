package com.minikafka.storage;

import com.minikafka.model.Message;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class MessageStorage {

    private static final String FILE_PATH =
            "data/messages.log";

    public synchronized void saveMessage(Message message) {

        try (
                BufferedWriter writer =
                        new BufferedWriter(
                                new FileWriter(
                                        FILE_PATH,
                                        true
                                )
                        )
        ) {

            writer.write(
                    message.getOffset() +
                            "," +
                            message.getId() +
                            "," +
                            message.getContent() +
                            "," +
                            message.getTimestamp()
            );

            writer.newLine();

        } catch (IOException e) {

            System.out.println(
                    "Error saving message: " +
                            e.getMessage()
            );
        }
    }

    public List<Message> loadMessages() {

        List<Message> recoveredMessages =
                new ArrayList<>();

        try (
                BufferedReader reader =
                        new BufferedReader(
                                new FileReader(FILE_PATH)
                        )
                ) {

            String line;

            while ((line = reader.readLine()) != null) {

                String[] parts = line.split(",");

                long offset =
                        Long.parseLong(parts[0]);

                String id = parts[1];

                String content = parts[2];

                Message message = new Message(
                        id,
                        content,
                        offset,
                        false
                );

                recoveredMessages.add(message);
            }
        } catch (IOException e) {

            System.out.println(
                    "No previous messages found."
            );
        }

        return recoveredMessages;
    }
}
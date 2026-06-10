package com.minikafka.storage;

import java.io.*;
import java.util.Map;
import java.util.HashMap;

public class OffsetStorage {

    private static final String FILE_PATH =
            "data/offsets.log";

    public synchronized void saveOffset(
            String groupId,
            int partitionId,
            long offset
    ) {

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
                    groupId +
                            "," +
                            partitionId +
                            "," +
                            offset
            );

            writer.newLine();

        } catch (IOException e) {

            System.out.println(
                    "Error saving offset: " +
                            e.getMessage()
            );
        }
    }

    public Map<String, Map<Integer, Long>>
    loadOffsets() {

        Map<String, Map<Integer, Long>>
                recoveredOffsets =
                new HashMap<>();
        try (
                BufferedReader reader =
                        new BufferedReader(
                                new FileReader(FILE_PATH)
                        )
                ) {

            String line;

            while ((line = reader.readLine()) !=  null) {

                String[] parts =
                        line.split(",");

                String groupId =
                        parts[0];

                int partitionId =
                        Integer.parseInt(parts[1]);

                long offset =
                        Long.parseLong(parts[2]);

                recoveredOffsets
                        .computeIfAbsent(
                                groupId,
                                k -> new HashMap<>()
                        )
                        .put(
                                partitionId,
                                offset
                        );
            }
        } catch (IOException e) {

            System.out.println(
                    "No previous offsets found."
            );
        }

        return recoveredOffsets;
    }
}

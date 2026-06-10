package com.minikafka.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {

    private final Map<String, Long> committedOffsets =
            new ConcurrentHashMap<>();

    public void commitOffset(
            String groupId,
            int partitionId,
            long offset
    ) {

        String key =
                groupId + "-" + partitionId;

        committedOffsets.put(
                key,
                offset
        );
    }

    public long getCommittedOffset (
            String groupId,
            int partitionId
    ) {
        String key =
                groupId + "-" + partitionId;

        return committedOffsets.getOrDefault(
                key,
                -1L
        );
    }
}

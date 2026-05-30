package com.minikafka.broker;

import com.minikafka.model.Message;

import java.util.ArrayList;
import java.util.List;
import java.awt.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;


public class Topic {
    private final String name;
//    private final List<Message> messages;
//    private final BlockingQueue<Message> messages;
    private final List<Partition> partitions;

    private final AtomicLong offsetCounter;

    private static final long MAX_OFFSET = 20;

    public Topic(String name) {
        this.name = name;
//        this.messages = new ArrayList<>();
//        this.messages = new LinkedBlockingQueue<>();

        this.offsetCounter = new AtomicLong(0);

        this.partitions = new ArrayList<>();

        for (int i = 0; i < 3; i++) {

            partitions.add(
                    new Partition(i)
            );
        }
    }

    public Partition getPartition (
            String key
    ) {
        int partitionIndex =
                Math.abs(
                        key.hashCode()
                ) % partitions.size();

        return partitions.get(
                partitionIndex
        );
    }

//    public void addMessage(Message message) {
//        messages.add(message);
//    }

//    public List<Message> getMessages() {
//        return messages;
//    }

//    public BlockingQueue<Message> getMessages() {
//        return messages;
//    }

    public String getName() {
        return name;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public long getNextOffset() {
        return offsetCounter.getAndIncrement();
    }

//    public boolean isLimitReached() {
//        return offsetCounter.get() >= MAX_OFFSET;
//    }
    public boolean isLimitReached() {

        long totalOffsets = 0;

        for (Partition partition : partitions) {

            totalOffsets +=
                    partition.getCurrentOffset();
        }

        return totalOffsets >= MAX_OFFSET;
    }

    public void updateOffset(long offset) {
        offsetCounter.set(offset + 1);
    }
}
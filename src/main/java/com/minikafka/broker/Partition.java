package com.minikafka.broker;

import com.minikafka.model.Message;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {

    private final int partitionId;

    private final BlockingQueue<Message> messages;

    private final AtomicLong offsetCounter;

    public Partition(int partitionId) {

        this.partitionId = partitionId;

        this.messages = new LinkedBlockingQueue<>();

        this.offsetCounter = new AtomicLong(0);
    }

    public void addMessage(Message message) {

        messages.add(message);
    }

    public BlockingQueue<Message> getMessages() {

        return messages;
    }

    public int getPartitionId() {

        return partitionId;
    }

    public long getNextOffset() {

        return offsetCounter.getAndIncrement();
    }

    public long getCurrentOffset() {

        return offsetCounter.get();
    }

    public void updateOffset(
            long offset
    ) {

        offsetCounter.set(
                Math.max(
                        offsetCounter.get(),
                        offset += 1
                )
        );
    }
}
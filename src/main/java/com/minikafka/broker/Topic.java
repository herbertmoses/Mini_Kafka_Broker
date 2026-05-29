package com.minikafka.broker;

import com.minikafka.model.Message;

//import java.util.ArrayList;
//import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;


public class Topic {
    private final String name;
//    private final List<Message> messages;
    private final BlockingQueue<Message> messages;

    private final AtomicLong offsetCounter;

    private static final long MAX_OFFSET = 20;

    public Topic(String name) {
        this.name = name;
//        this.messages = new ArrayList<>();
        this.messages = new LinkedBlockingQueue<>();

        this.offsetCounter = new AtomicLong(0);
    }

    public void addMessage(Message message) {
        messages.add(message);
    }

//    public List<Message> getMessages() {
//        return messages;
//    }

    public BlockingQueue<Message> getMessages() {
        return messages;
    }

    public String getName() {
        return name;
    }

    public long getNextOffset() {
        return offsetCounter.getAndIncrement();
    }

    public boolean isLimitReached() {
        return offsetCounter.get() >= MAX_OFFSET;
    }

    public void updateOffset(long offset) {
        offsetCounter.set(offset + 1);
    }
}
package com.minikafka.model;

public class Message {

    private final String id;
    private final String content;
    private final long timestamp;
    private final long offset;
    private final boolean shutdownMessage;

    public Message(String id, String content, long offset, boolean shutdownMessage) {
        this.id = id;
        this.content = content;
        this.offset = offset;
        this.shutdownMessage = shutdownMessage;
        this.timestamp = System.currentTimeMillis();
    }



    public String getId() {
        return id;
    }

    public String getContent() {
        return content;
    }

    public long getOffset() {
        return offset;
    }

    public boolean isShutdownMessage() {
        return shutdownMessage;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                " offset=" + offset +
                ", id='" + id + '\'' +
                ", content='" + content + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
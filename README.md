# Mini Kafka Broker

A simplified distributed event-streaming broker built using Core Java concurrency APIs.

Inspired by:
- Apache Kafka
- RabbitMQ
- Event-driven backend architectures

---

# Features

## Core Features

- Concurrent Producers
- Concurrent Consumers
- Thread-safe Topic Queues
- Atomic Offset Management
- Persistent Message Storage
- Log Recovery on Restart
- Graceful Shutdown
- Poison Pill Pattern
- ExecutorService-based Thread Management

---

# Architecture

```text
Producer Threads
        |
        v
      Broker
        |
        v
   Topic Queue
        |
        v
 Persistent Logs
        |
        v
 Consumer Threads
```

---

#Technologies Used

- Java 21
- Maven
- ExecutorService
- BlockingQueue
- ConcurrentHashMap
- AtomicLong
- BufferedReader / BufferedWriter
- Github Actions CI

---

# Project Structure

```text
mini-kafka/
|
|--.github/workflows/
|--data/
|--src/main/java/com/minikafka
|   |--app
|   |--broker
|   |--consumer
|   |--producer
|   |--model
|   |--storage
|
|--README.md
|--pom.xml
|--.gitignore
```

---

# How to Run

## Clone Repository

```bash
git clone <repo-url>
cd mini-kafka
```

---

## Build Project

```bash
mvn clean install
```

---

## Run Application

```bash
mvn exec:java - Dexec.mainClass="com.minikafka.app.Main"
```

---

# Example Output

```text
Producer-1 produced -> producer-1 Message-1
Consumer-1 consumed -> [Offset 0] Producer-1 Message-1
```

---

# Current Features Implemented

- [x] Producer-Consumer Architecture
- [x] Concurrent Processing
- [x] Persistent Logging
- [x] Offset Tracking
- [x] Recovery System
- [x] Graceful Shutdown

---

# Planned Features

- [ ] Topic Partitions
- [ ] Consumer Groups
- [ ] Retry Queue
- [ ] Dead Letter Queue
- [ ] Metrics Dashboard
- [ ] REST API Integration
- [ ] Docker Support

---

# Learning Outcomes

This project demonstrates:
- Core Java concurrency
- Distributed systems concepts
- Event-driven architecture
- Thread-safe programming
- Asynchronous processing
- Persistent Event Logs

---


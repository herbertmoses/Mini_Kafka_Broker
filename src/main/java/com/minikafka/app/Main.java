package com.minikafka.app;

import com.minikafka.broker.Broker;
import com.minikafka.consumer.Consumer;
import com.minikafka.model.Message;
import com.minikafka.producer.Producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {

    private static Message createShutdownMessage() {

        return new Message(
                "shutdown",
                "SHUTDOWN",
                -1,
                true
        );
    }

    public static void main(String[] args) {

        Broker broker = new Broker();

        broker.createTopic("orders");

        Producer producer1 =
                new Producer(
                        broker,
                        "orders",
                        "Producer-1"
                );

        Producer producer2 =
                new Producer(
                        broker,
                        "orders",
                        "Producer-2"
                );

        Consumer consumer1 =
                new Consumer(
                        broker.getTopic("orders"),
                        "Consumer-1"
                );

        Consumer consumer2 =
                new Consumer(
                        broker.getTopic("orders"),
                        "Consumer-2"
                );

        broker.recoverTopic("orders");

        ExecutorService executorService =
                Executors.newFixedThreadPool(4);

        executorService.submit(producer1);
        executorService.submit(producer2);

        executorService.submit(consumer1);
        executorService.submit(consumer2);

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            System.out.println("Main Thread Interrupted");
        }

        broker.publish(
                "orders",
                createShutdownMessage()
        );

        broker.publish(
                "orders",
                createShutdownMessage()
        );

        executorService.shutdown();

//        Thread producerThread1 =
//                new Thread(producer1);
//
//        Thread producerThread2 =
//                new Thread(producer2);
//
//        Thread consumerThread1 =
//                new Thread(consumer1);
//
//        Thread consumerThread2 =
//                new Thread(consumer2);
//
//        producerThread1.start();
//        producerThread2.start();
//
//        consumerThread1.start();
//        consumerThread2.start();

//        Producer producer = new Producer(broker);
//
//        producer.send("orders", "Order Created.");
//        producer.send("orders", "Payment Completed");
//        producer.send("orders", "Orders Shipped.");
//
//        Consumer consumer = new Consumer();
//
//        consumer.consume(
//                broker.getTopic("orders"
    }
}
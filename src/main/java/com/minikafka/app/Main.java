package com.minikafka.app;

import com.minikafka.broker.Broker;
import com.minikafka.consumer.Consumer;
import com.minikafka.group.ConsumerGroup;
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
                true,
                -1
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
//                        broker.getTopic("orders")
//                                .getPartitions()
//                                .get(0),
                        "Consumer-1"
                );

        Consumer consumer2 =
                new Consumer(
//                        broker.getTopic("orders")
//                                .getPartitions()
//                                .get(2),
                        "Consumer-2"
                );

        ConsumerGroup group =
                new ConsumerGroup(
                        "order-processors"
                );

        group.registerConsumer(
                consumer1
        );

        group.registerConsumer(
                consumer2
        );

        group.assignPartitions(
                broker.getTopic("orders")
        );

        broker.recoverTopic("orders");

        ExecutorService executorService =
                Executors.newFixedThreadPool(4);

        executorService.submit(producer1);
        executorService.submit(producer2);

        executorService.submit(consumer1);
        executorService.submit(consumer2);

        try {
            Thread.sleep(5000);

            Consumer consumer3 =
                    new Consumer(
                            "Consumer-3"
                    );

            group.registerConsumer(
                    consumer3
            );

            executorService.submit(
                    consumer3
            );

            Thread.sleep(10000);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            System.out.println("Main Thread Interrupted");
        }

        broker.broadcastShutdown(
                "orders"
        );

        executorService.shutdown();

    }
}

//        broker.publish(
//                "orders",
//                "shutdown",
//                createShutdownMessage()
//        );
//
//        broker.publish(
//                "orders",
//                "shutdown",
//                createShutdownMessage()
//        );
//        broker.broadcastShutdown(
//                "orders"
//        );
//
//        executorService.shutdown();

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
//    }
//}
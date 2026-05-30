package com.minikafka.producer;

import com.minikafka.broker.Broker;
import com.minikafka.broker.Partition;
import com.minikafka.model.Message;
import com.minikafka.broker.Partition;

import java.util.UUID;

//public class Producer {
public class Producer implements Runnable {

    private final Broker broker;
    private final String topicName;
    private final String producerName;

    public Producer(
            Broker broker,
            String topicName,
            String producerName
    ) {
        this.broker = broker;
        this.topicName = topicName;
        this.producerName = producerName;
    }

    @Override
    public void run() {

        int counter = 1;

//        while (true) {
        while (!broker.getTopic(topicName)
                .isLimitReached())
        {

            try {
                String content =
                        producerName +
                                " Message-" +
                                counter++;

//                long offset =
//                        broker.getTopic(topicName).getNextOffset();

                Partition partition =
                        broker.getTopic(topicName)
                                .getPartition(producerName);

                long offset =
                        partition.getNextOffset();


                Message message = new Message(
                        UUID.randomUUID().toString(),
                        content,
                        offset,
                        false,
                        partition.getPartitionId()
                );

                broker.publish(topicName, producerName, message);

                System.out.println(
                        producerName +
                                " produced -> " +
                                content
                );

//                System.out.println(
//                        producerName +
//                                " --> partition " +
//                                partition.getPartitionId()
//                );

//                Thread.sleep(2000);
                Thread.sleep((long) (Math.random() * 3000));

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                System.out.println(
                        producerName + " interrupted"
                );
            }
        }

        System.out.println(
                producerName +
                        " stopped. Max offset reached."
        );
    }
}

//    public Producer(Broker broker) {
//        this.broker = broker;
//    }

//    public void send(String topicName, String content) {
//
//        Message message = new Message(
//                UUID.randomUUID().toString(),
//                content
//        );
//        broker.publish(topicName, message);
//
//        System.out.println("Producer sent:" + content);
//    }
//}
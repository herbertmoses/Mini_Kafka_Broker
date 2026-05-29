package com.minikafka.consumer;

import com.minikafka.model.Message;
import com.minikafka.broker.Topic;

//public class Consumer {
public class Consumer implements Runnable {

    private final Topic topic;
    private final String consumerName;

    public Consumer(Topic topic, String consumerName) {
        this.topic = topic;
        this.consumerName = consumerName;
    }

    @Override
    public void run()

    {
        while (true) {
            try {

                Message message = topic.getMessages().take();

                if(message.isShutdownMessage()) {

                    System.out.println(
                            consumerName +
                                    " Shutting down gracefully. "
                    );

                    break;
                }

                System.out.println(
                        consumerName +
                                " consumed -> " +
                                "[Offset " +
                                message.getOffset() +
                                "] " +
                                message.getContent()
                );

                Thread.sleep(1000);
            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                System.out.println(
                        consumerName + " interrupted"
                );
            }
        }
    }
}

//    public void consume (Topic topic) {
//
//        System.out.println("\nConsuming message from topic: " + topic.getName());
//
//        for (Message message: topic.getMessages()) {
//            System.out.println("Consumed ->" + message.getContent());
//        }
//    }

package com.minikafka.broker;

import com.minikafka.model.Message;
import com.minikafka.storage.MessageStorage;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Broker {

//    private final Map<String, Topic> topics = new HashMap<>();

    private final Map<String, Topic> topics = new ConcurrentHashMap<>();

    private final MessageStorage storage =
            new MessageStorage();

    public void createTopic (String topicName) {

        if(!topics.containsKey(topicName)) {
            topics.put(topicName, new Topic(topicName));
            System.out.println("Topic created:" + topicName);
        }
    }

//    public void publish (String topicName, Message message) {
//
//        Topic topic = topics.get(topicName);
//
//        if(topic == null) {
//            throw new RuntimeException("Topic does not exist.");
//        }
//
//        topic.addMessage(message);
//
//        storage.saveMessage(message);
//
//        System.out.println("Message published to topic: " + topicName);
//    }

    public void publish (String topicName, String key, Message message) {

        Topic topic = topics.get(topicName);

        if(topic != null) {

            Partition partition =
                    topic.getPartition(key);

            partition.addMessage(message);

            storage.saveMessage(message);

            System.out.println(
                    "Message published to topic: "
                    + topicName
                    + " partition: "
                    + partition.getPartitionId()
            );
        }
    }

    public void recoverTopic(String topicName) {

//        createTopic(topicName);

        Topic topic = topics.get(topicName);

        List<Message> recovered =
                storage.loadMessages();

        for (Message message :
                recovered) {

            int partitionId =
                    message.getPartitionId();

            if (partitionId < 0 ||
            partitionId >= topic.getPartitions().size()) {

                System.out.println(
                        "Skipping invalid partition: "
                        + partitionId
                );

                continue;
            }

            Partition partition =
                    topic.getPartitions()
                            .get(partitionId);

            partition.addMessage(message);

            partition.updateOffset(message.getOffset());
        }

        System.out.println(
                "Recovered "
                        + recovered.size()
                        + " messages for topic: "
                        + topicName
        );
    }

//        int recoveredCount = 0;
//
//        for (Partition partition :
//        topic.getPartitions()) {
//
//            recoveredCount +=
//                    partition.getMessages().size();
//        }
//
//
////        storage.loadMessages()
////                .forEach(topic::addMessage);
//        storage.loadMessages()
//                        .forEach(message -> {
////                            topic.addMessage(message);
//
//                            topic.updateOffset(
//                                    message.getOffset()
//                            );
//                        });
//
////        System.out.println(
////                "Recovered " +
//////                        topic.getMessages().size() +
////                        " messages for topic: " +
////                        topicName
////        );
//
//        System.out.println(
//                "Recovered" +
//                        recoveredCount +
//                        " messages for topic: " +
//                        topicName
//        );

//    }

    public Topic getTopic(String topicName) {
        return topics.get(topicName);
    }

    public void broadcastShutdown(
            String topicName
    ) {

        Topic topic = topics.get(topicName);

        if ( topic != null) {

           for (Partition partition :
           topic.getPartitions()) {

               Message shutdownMessage =
                        new Message(
                                "shutdown",
                                "SHUTDOWN",
                                -1,
                                true,
                                partition.getPartitionId()
                        );

               partition.addMessage(shutdownMessage);

//               partition.updateOffset(shutdownMessage.getOffset());

//               partition.addMessage(
//                       shutdownMessage
//               );
           }
        }
    }
}

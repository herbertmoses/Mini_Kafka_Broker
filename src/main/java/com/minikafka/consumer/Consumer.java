package com.minikafka.consumer;

import com.minikafka.broker.Partition;
import com.minikafka.model.Message;
import com.minikafka.broker.Topic;
import java.util.ArrayList;
import java.util.List;

//public class Consumer {
public class Consumer implements Runnable {

    //    private final Topic topic;
//    private final Partition partition;
    private final String consumerName;
    //    private Partition partition;
    private final List<Partition> assignedPartitions =
            new ArrayList<>();

//    public Consumer(Topic topic, String consumerName) {
//        this.topic = topic;
//        this.consumerName = consumerName;
//    }

    public Consumer(
//            Partition partition,
            String consumerName
    ) {
        this.consumerName = consumerName;
//        this.partition = partition;
    }

//    public String getConsumerName() {
//        return consumerName;
//    }

//    public Partition getPartition() {
//        return partition;
//    }

//    public void setPartition(Partition partition) {
//        this.partition = partition;
//    }

    public void assignPartition(
            Partition partition
    ) {
        assignedPartitions.add(partition);
    }

    public List<Partition> getAssignedPartitions() {
        return assignedPartitions;
    }

    public String getConsumerName() {
        return consumerName;
    }

    public Partition getPrimaryPartition() {

        if (assignedPartitions.isEmpty()) {
            return null;
        }

        return assignedPartitions.get(0);
    }

    @Override
    public void run() {
        while (true) {
            try {

                boolean messageProcessed = false;

                for (Partition partition :
                        assignedPartitions) {

                    Message message =
                            partition.getMessages()
                                    .poll();

                    if (message == null) {

                        continue;
                    }
                    messageProcessed = true;

                    if (message.isShutdownMessage()) {

                        System.out.println(
                                consumerName +
                                        " Shutting down gracefully. "
                        );

                        return;
                    }

                    System.out.println(
                            consumerName +
                                    " consumed-> " +
                                    " [Partition " +
                                    partition.getPartitionId() +
                                    "] " +
                                    "[Offset " +
                                    message.getOffset() +
                                    "]" +
                                    message.getContent()
                    );
                }

                if (!messageProcessed) {

                    Thread.sleep(100);
                }
            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();

                System.out.println(
                        consumerName +
                                " Interrupted"
                );
                return;
            }
        }
    }
}


//                Message message = topic.getMessages().take();
//                Message message = partition.getMessages().take();
//                Message message = getPrimaryPartition().getMessages().take();
//                Partition partition = getPrimaryPartition();
//
//                if (partition == null) {
//
//                    Thread.sleep(1000);
//
//                    continue;
//                }
//
//                Message message = partition.getMessages().take();
//
//
//                if(message.isShutdownMessage()) {
//
//                    System.out.println(
//                            consumerName +
//                                    " Shutting down gracefully. "
//                    );
//
//                    break;
//                }
//
//                System.out.println(
//                        consumerName +
//                                " consumed -> " +
//                                "[Offset " +
//                                message.getOffset() +
//                                "] " +
//                                message.getContent()
//                );
//
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//
//                Thread.currentThread().interrupt();
//
//                System.out.println(
//                        consumerName + " interrupted"
//                );
//            }
//        }
//    }
//}

//    public void consume (Topic topic) {
//
//        System.out.println("\nConsuming message from topic: " + topic.getName());
//
//        for (Message message: topic.getMessages()) {
//            System.out.println("Consumed ->" + message.getContent());
//        }
//    }

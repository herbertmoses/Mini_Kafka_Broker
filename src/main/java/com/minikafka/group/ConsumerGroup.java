package com.minikafka.group;

import com.minikafka.broker.Partition;
import com.minikafka.broker.Topic;
import com.minikafka.consumer.Consumer;

import java.util.List;
import java.util.ArrayList;

public class ConsumerGroup {

    private final String groupId;

    private Topic topic;

    private final List<Consumer> consumers;

    public ConsumerGroup(String groupId) {

        this.groupId = groupId;

        this.consumers = new ArrayList<>();
    }

    public void registerConsumer (
        Consumer consumer
    ) {
        consumers.add(consumer);

        System.out.println(
                consumer.getConsumerName()
                + " joined group "
                + groupId
        );

        rebalance();
    }

    public String getGroupId() {
        return groupId;
    }

    public List<Consumer> getConsumers() {
        return consumers;
    }

    public void assignPartitions (
            Topic topic
    ) {

        this.topic = topic;

        consumers.forEach(
                consumer ->
                        consumer.getAssignedPartitions()
                                .clear()
        );

        List<Partition> partitions =
                topic.getPartitions();

        for (int i = 0;
        i<partitions.size();
        i++) {

            Consumer consumer =
                    consumers.get(
                            i % consumers.size()
                    );
            consumer.assignPartition(
                    partitions.get(i)
            );
//            consumer.setPartition(
//                    partitions.get(i)
//            );
            System.out.println(
                    "Assigned Partition-"
                    + partitions.get(i)
                            .getPartitionId()
                    + " -> "
                    + consumer.getConsumerName()
            );
        }
    }

    public void rebalance() {

        if (topic == null) {
            return;
        }

        consumers.forEach(
                consumer ->
                        consumer
                                .getAssignedPartitions()
                                .clear()
        );

        List<Partition>partitions =
                topic.getPartitions();

        for(int i=0;
        i<partitions.size();
        i++) {

            Consumer consumer =
                    consumers.get(
                            i % consumers.size()
                    );
            consumer.assignPartition(
                    partitions.get(i)
            );
            System.out.println(
                    "Reassign Partition-"
                    + partitions.get(i)
                            .getPartitionId()
                    + " -> "
                    + consumer.getConsumerName()
            );
        }
    }

    public void removeConsumer(
            Consumer consumer
    ) {

        consumers.remove(consumer);

        System.out.println(
                consumer.getConsumerName()
                + " left group "
                + groupId
        );

        rebalance();
    }
}

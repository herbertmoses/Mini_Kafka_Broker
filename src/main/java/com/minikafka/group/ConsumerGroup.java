package com.minikafka.group;

import com.minikafka.broker.Partition;
import com.minikafka.broker.Topic;
import com.minikafka.consumer.Consumer;

import java.util.List;
import java.util.ArrayList;

public class ConsumerGroup {

    private final String groupId;

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
        List<Partition> partitions =
                topic.getPartitions();

        for (int i = 0;
        i<partitions.size();
        i++) {

            Consumer consumer =
                    consumers.get(
                            i % consumers.size()
                    );
            consumer.setPartition(
                    partitions.get(i)
            );
            System.out.println(
                    "Assigned Partition-"
                    + partitions.get(i)
                            .getPartitionId()
                    + " -> "
                    + consumer.getConsumerName()
            );
        }
    }
}

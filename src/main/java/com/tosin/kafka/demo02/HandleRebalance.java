package com.tosin.kafka.demo02;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

//重平衡监听器（Rebalance Listener）
public class HandleRebalance implements ConsumerRebalanceListener{
    //此方法会在消费者停止消费消费后，在重平衡开始前调用。
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

    }

    //此方法在分区分配给消费者后，在消费者开始读取消息前调用。
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Lost partitions in rebalance. Committing current offsets:");
    }
}

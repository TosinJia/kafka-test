package com.tosin.kafka.itstar.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class Partitioner1 implements Partitioner {
    //设置
    public void configure(Map<String, ?> configs) {
        System.out.println("Partition_configure:"+configs.toString());
    }

    //分区逻辑
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 2;
    }

    //释放资源
    public void close() {

    }

}

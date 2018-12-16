package com.tosin.kafka.demo01;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * https://blog.csdn.net/qq_20641565/article/details/64440425
 * */
public class MyConsumer {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.1.150:9092,192.168.1.150:9093,192.168.1.150:9094");
        //设置不自动提交，自己手动更新offset
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "latest");
        properties.put("zookeeper.connect", "192.168.1.150:2181,192.168.1.151:2181,192.168.1.152:2181");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "tosinGroup");
//        properties.put("zookeeper.connect", "192.168.80.123:2181");
        properties.put("auto.commit.interval.ms", "1000");

        //创建主题时使用比较多的分区数，这样可以在消费负载高的情况下增加消费者来提升性能.消费者的数量不应该比分区数多，因为多出来的消费者是空闲的，没有任何帮助。
        //[root@bd kafka_2.12-2.0.0]# ./bin/kafka-topics.sh --describe --zookeeper 192.168.1.150:2181,192.168.1.151:2181,192.168.1.152:2181 --topic my-cluster-topic
        //[root@bd kafka_2.12-2.0.0]# ./bin/kafka-topics.sh --create --zookeeper 192.168.1.150:2181,192.168.1.151:2181,192.168.1.152:2181 --replication-factor 3 --partitions 2 --topic my-cluster-topic
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        for (int i=0; i< 3; i++){
            executorService.execute(new MyConsumerThread(new KafkaConsumer<String, String>(properties), "消费者"+i, "my-cluster-topic"));
        }
    }
}

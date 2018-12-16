package com.tosin.kafka.itstar.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * [root@bd-01 kafka]# kafka-console-consumer.sh --bootstrap-server bd-01:9092 --topic demo1216 --from-beginning
 * */
public class Consumer1 {
    public static void main(String[] args){
        //1. 配置属性
        Properties properties = new Properties();

        //指定服务器地址
        properties.put("bootstrap.servers", "192.168.1.150:9092");
        //配置消费者组
        properties.put("group.id", "tosin1");
        //配置是否自动确认offset
        properties.put("enable.auto.commit", "true");
        //反序列化
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //2. 实例化消费者
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //4. 释放资源 线程安全
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                if(consumer != null){
                    consumer.close();
                }
            }
        }));

        consumer.subscribe(Arrays.asList("demo121602"));
        //3. 拉消息
        while(true){
            //consumer.poll(Duration.ofMillis(100));
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record: records){
                System.out.println("\ttopic:"+record.topic()+"\tpartition:"+record.partition()+"\toffset:"+record.offset()+"\tvalue:"+record.value()+"\trecord:"+record.toString());
            }
        }
    }
}

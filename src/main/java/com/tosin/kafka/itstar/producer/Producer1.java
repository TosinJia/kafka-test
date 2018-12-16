package com.tosin.kafka.itstar.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka生产者API
 * [root@bd kafka]# kafka-console-producer.sh --broker-list bd:9092 --topic demo1216
 *
 * 测试准备
 * [root@bd kafka]# kafka-topics.sh --zookeeper bd:2181 --create --replication-factor 2 --partitions 3 --topic demo121602
 * */
public class Producer1 {

    public static void main(String[] args) throws InterruptedException {
        //1. 配置属性 指定多个参数
        Properties properties = new Properties();
        //参数配置
        //kafka节点地址
        properties.put("bootstrap.servers", "192.168.1.150:9092");
        //发送消息是否等待应答
        properties.put("acks", "all");
        //发送消息失败重试
        properties.put("retries", "0");
        //批处理消息大小
        properties.put("batch.size", "1024");
        //批处理数据延迟
        properties.put("linger.ms", "5");
        //内存缓冲大小
        properties.put("buffer.memory", "10240");
        //消息在发送前必须序列化
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //指定分区配置
        properties.put("partitioner.class", "com.tosin.kafka.itstar.producer.Partitioner1");

        //2. 实例化Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //3. 发送消息
        for (int i= 0; i< 10; i++){

//            producer.send(new ProducerRecord<String, String>("demo121602", "test"+i));
            //回调
            producer.send(new ProducerRecord<String, String>("demo121602", "testCallBackPartitioner:" + i), new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //如果metadata不为空，拿出当前数据偏移量与分区
                    if(metadata != null){
                        System.out.println("topic:"+metadata.topic()+"\toffset:"+metadata.offset()+"\tpartition:"+metadata.partition()+ "\tmetadata:"+metadata.toString());
                    }
                }
            });
            Thread.sleep(1000);
        }

        //4. 释放资源
        producer.close();
    }
}

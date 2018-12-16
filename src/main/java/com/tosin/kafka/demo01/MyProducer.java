package com.tosin.kafka.demo01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MyProducer {
    public static KafkaProducer<String, String> kafkaProducer;
    static{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.150:9092,192.168.1.150:9093,192.168.1.150:9094");
//        properties.setProperty("broker-list", "192.168.1.150:9092,192.168.1.150:9093,192.168.1.150:9094");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws InterruptedException {
        produce("my-cluster-topic");
    }

    public static void produce(String topic) throws InterruptedException {
        for(int i=0; i<10000; i++){
            String msg = "API测试："+i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, msg);
            kafkaProducer.send(producerRecord);
            System.out.println(msg+"已发布！");
            Thread.sleep(1000);
        }
    }
}

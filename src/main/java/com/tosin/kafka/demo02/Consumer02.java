package com.tosin.kafka.demo02;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * http://www.dengshenyu.com/%E5%88%86%E5%B8%83%E5%BC%8F%E7%B3%BB%E7%BB%9F/2017/11/14/kafka-consumer.html
 * 《Kafka权威指南》 第4章 Kafka 消费者一一从Kafka 读取数据
 * */
public class Consumer02 {
    public static void main(String[] args){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.150:9092,192.168.1.150:9093,192.168.1.150:9094");
        //key.deserializer和value.deserializer是用来做反序列化的，也就是将字节数组转换成对象
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //group.id不是严格必须的，但通常都会指定，这个参数是消费者的消费组
        properties.setProperty("group.id", "consumer02");
        //这个参数指定了当消费者第一次读取分区或者上一次的位置太老（比如消费者下线时间太久）时的行为，可以取值为latest（从最新的消息开始消费）或者earliest（从最老的消息开始消费）。
        properties.setProperty("auto.offset.reset", "latest");
        //这个参数指定了消费者是否自动提交消费位移，默认为true。如果需要减少重复消费或者数据丢失，你可以设置为false。如果为true，你可能需要关注自动提交的时间间隔，该间隔由auto.commit.interval.ms设置。
        //自动提交
        //  enable.auto.commit设置为true，那么消费者会在poll方法调用后每隔5秒（由auto.commit.interval.ms指定）提交一次位移。和很多其他操作一样，自动提交也是由poll()方法来驱动的；在调用poll()时，消费者判断是否到达提交时间，如果是则提交上一次poll返回的最大位移。
        //  这种方式可能会导致消息重复消费。假如，某个消费者poll消息后，应用正在处理消息，在3秒后Kafka进行了重平衡，那么由于没有更新位移导致重平衡后这部分消息重复消费。
        properties.setProperty("enable.auto.commit", "false");

        //创建Kafka消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //订阅主题
        //1. 只订阅了一个customerCountries主题
        consumer.subscribe(Collections.singletonList("customerCountries"));

        //2. 也可以使用正则表达式来匹配多个主题，而且订阅之后如果又有匹配的新主题，那么这个消费组会立即对其进行消费
        consumer.subscribe(Pattern.compile("test.*"));
    }

    //拉取循环
    //一个线程一个消费者，如果需要多个消费者那么请使用多线程来进行一一对应
    //自动提交
    // enable.auto.commit设置为true，那么消费者会在poll方法调用后每隔5秒（由auto.commit.interval.ms指定）提交一次位移
    // 注意 这种方式可能会导致消息重复消费。假如，某个消费者poll消息后，应用正在处理消息，在3秒后Kafka进行了重平衡，那么由于没有更新位移导致重平衡后这部分消息重复消费
    public static void testAutocommit(KafkaConsumer<String, String> consumer){
        try{
            //使用无限循环消费并处理数据
            while(true){
                //我们不断调用poll拉取数据，如果停止拉取，那么Kafka会认为此消费者已经死亡并进行重平衡。参数值是一个超时时间，指明线程如果没有数据时等待多长时间，0表示不等待立即返回。
                //poll()方法返回记录的列表，每条记录包含key/value以及主题、分区、位移信息
                ConsumerRecords<String, String> records = consumer.poll(100);
                for(ConsumerRecord<String, String> record: records){
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
                            record.topic(), record.partition(), record.offset(),
                            record.key(), record.value());
                }
            }
        }finally{
            //主动关闭可以使得Kafka立即进行重平衡而不需要等待会话过期。
            consumer.close();
        }
    }

    /**
     * 提交当前位移
     * 为了减少消息重复消费或者避免消息丢失，很多应用选择自己主动提交位移
     * 设置auto.commit.offset为false，那么应用需要自己通过调用commitSync()来主动提交位移，该方法会提交poll返回的最后位移。
     * 手动提交有一个缺点，那就是当发起提交调用时应用会阻塞 可以以减少手动提交的频率，但这个会增加消息重复的概率（和自动提交一样）
     * */
    public static void testAutocommit1(KafkaConsumer<String, String> consumer){
        try{
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                try {
                    consumer.commitSync();
                } catch (CommitFailedException e) {
                    System.out.println("commit failed:"+e);
                }
            }
        }finally{
            consumer.close();
        }
    }
    /**
     * 异步提交
     * 缺点，如果服务器返回提交失败，异步提交不会进行重试
     * 异步提交没有实现重试是因为，如果同时存在多个异步提交，进行重试可能会导致位移覆盖
     * **/
    public static void testAsynccommit1(KafkaConsumer<String, String> consumer){
        try{
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
//                consumer.commitAsync();

                //般情况下对于异步提交，我们可能会通过回调的方式记录提交结果
                consumer.commitAsync(new OffsetCommitCallback() {
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if(exception != null){
                            System.out.println("commit failed:"+offsets+"\t"+exception);
                        }
                    }
                });
            }
        }finally{
            consumer.close();
        }
    }
    /**
     *
     * 正常处理流程中，我们使用异步提交来提高性能，但最后使用同步提交来保证位移提交成功
     * */
    public static void testMixingCommit(KafkaConsumer<String, String> consumer){
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                consumer.commitAsync();
            }
        }catch (Exception e){
            System.out.println("Unexpected error:"+e);
        }finally{
            try{
                consumer.commitAsync();
            }finally{
                consumer.close();
            }
        }
    }

    /**
     * 提交特定位移
     * commitSync()和commitAsync()允许我们指定特定的位移参数，参数为一个分区与位移的map
     * */
    public static void testSpecificOffsetCommit(KafkaConsumer<String, String> consumer){
        int count = 0;
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1, "no metadata"));
                    if(count%100==0){
                        consumer.commitAsync(currentOffsets, null);
                    }
                    count++;
                }
            }
        }catch (Exception e){
            System.out.println("Unexpected error:"+e);
        }finally{
            consumer.close();
        }
    }

    /**
     * 重平衡监听器（Rebalance Listener）
     * */
}

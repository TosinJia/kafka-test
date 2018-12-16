package com.tosin.kafka.demo01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class MyConsumerThread implements Runnable {

    private KafkaConsumer<String, String> consumer;
    private String name;
    private List<String> topicList;

    public MyConsumerThread(KafkaConsumer<String, String> consumer, String name, String topic) {
        this.consumer = consumer;
        this.name = name;
        this.topicList = Arrays.asList(topic);
    }

    public void run() {
        consumer.subscribe(topicList);
//        test01();
        test0102();
//        test02();
    }


    public void test02(){
        try{
            ConsumerRecords<String, String> records = consumer.poll(100);
            //records.partitions() 为0
            for(TopicPartition partition: records.partitions()){
                List<ConsumerRecord<String, String>> partionRecords = records.records(partition);
                for(ConsumerRecord<String, String> record: partionRecords){
                    System.out.println("消费："+record.toString());
                }
                long lastOffset = partionRecords.get(partionRecords.size()-1).offset();
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1)));
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    public void test0102(){
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        final int minBatchSize = 1;
        boolean flag = true;

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord record: records){
                System.out.printf("topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n", record.topic(), record.partition(), record.offset(), record.key(), record.value());
                System.out.println("消费者名字："+name+",消费的消息"+record.offset()+"\t"+record.value()+"\t"+record.toString());

                Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<TopicPartition, OffsetAndMetadata>();
                long offset = record.offset()+1;
                offset=10;
                currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(offset, "no metadata"));
                consumer.commitAsync(currentOffsets, null);
//                consumer.commitAsync();
//                consumer.commitSync();

                buffer.add(record);
            }

        }
    }

    public void test01(){
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        final int minBatchSize = 1;
        boolean flag = true;

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord record: records){
                System.out.println("消费者名字："+name+",消费的消息"+record.offset()+"\t"+record.value()+"\t"+record.toString());

//                consumer.commitAsync();
                consumer.commitSync();
                consumer.commitSync();

                buffer.add(record);
            }
            if(buffer.size() >= minBatchSize){
//               System.out.println("业务逻辑处理...");
//               if(flag){
//                   System.out.println("业务逻辑正常执行");
//                   consumer.commitAsync();
//                   System.out.println("提交完毕！    ");
//                   buffer.clear();
//               }else{
//                   System.out.println("业务逻辑执行过程中出现异常");
//               }
//               flag = !flag;
            }
        }
    }
}

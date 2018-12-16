package com.tosin.kafka.itstar.producer;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class TimeInterceptor implements ProducerInterceptor<String, String> {
    //配置信息
    public void configure(Map<String, ?> configs) {
        System.out.println("TimeInterceptor_configure"+configs.toString());
    }

    //业务逻辑
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return new ProducerRecord<String, String>(record.topic(),
                record.partition(),
                record.key(),
                System.currentTimeMillis()+"-"+record.value());
    }

    //发送失败调用
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    //关闭资源
    public void close() {

    }
}

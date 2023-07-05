package com.hw.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerSendSync {
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>("first", "getvalue" + i)).get();
        }
        /**
         * TODO 同步和ack是什么样子的关系？？？？，同步是客户端，ack是服务端？二者有没有什么关系？？？
         */
        Thread.currentThread().join();
        producer.close();
    }
}

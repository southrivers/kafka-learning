package com.hw.consumer1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

public class ConsumerAutoCommit {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 重启消费任务可以快速生效，让之前的consumer快速死亡
//        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);
//        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
        // 不自动提交如果下次消费还是从同一个地方消费，说明需要手动提交；再看一下手动提交的结果
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        HashSet<String> topics = new HashSet<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            // 这里不手动commit再次重启会从上次消费的数据offset重复消费，不过重启需要大于45s，这个时间是session的过期时间
//            kafkaConsumer.commitSync();
        }
    }
}

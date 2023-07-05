package com.hw.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Hello world!
 *
 */
public class CustomConsumerCommitSync {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "wes-test11");
        // 如果指定是none的話則需要原來就存在offset，否則會拋出異常
        // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 這裡配置為latest，因此即便是group之前並沒有消費過數據也沒有問題
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
        // properties.put(ConsumerConfig, properties)

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");
        kafkaConsumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println(consumerRecord);
            }
            kafkaConsumer.commitAsync();
        }
    }
}

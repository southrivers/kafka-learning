package com.hw.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Hello world!
 *
 */
public class CustomOffsetSeek {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 指定offset来消费的方式需要重新指定一个消费者分组(或者等待consumer超时)，否则数据还是使用上次提交的offset
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "wes");
        // properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        ArrayList<String> topics = new ArrayList<>();
        // 需要指定每一个分区的offset来进行消费
        topics.add("first");
        kafkaConsumer.subscribe(topics);
        // 首先获取分区信息
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        while(assignment.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
        }
        for (TopicPartition topicPartition : assignment) {
            // 上面可以订阅多个topic，因此在设置offset偏移量的时候，可以针对topic来进行设置
            System.out.println(topicPartition);
        }
        System.out.println("=============================================");
        // 遍历分区信息并seek的方式来指定每一个分区的offset信息
        for (TopicPartition topicPartition : assignment) {
            kafkaConsumer.seek(topicPartition, 2100);
        }
        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println(consumerRecord);
            }
        }
    }
}

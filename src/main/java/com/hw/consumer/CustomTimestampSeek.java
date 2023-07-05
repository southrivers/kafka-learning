package com.hw.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Hello world!
 *
 */
public class CustomTimestampSeek {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 指定offset来消费的方式需要重新指定一个消费者分组(或者等待consumer超时)，否则数据还是使用上次提交的offset
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "wes1dsdfsdrrr");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000");
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
        HashMap<TopicPartition,Long> topicHashMap = new HashMap<TopicPartition, Long>();
        for (TopicPartition topicPartition : assignment) {
            // 这里在配置时间的时候，如果本地代码和服务器时间不同步也可能会出现消费不了数据的情况
            topicHashMap.put(topicPartition, System.currentTimeMillis() - 2*24 *60 * 60 *1000);
        }

        Map<TopicPartition,OffsetAndTimestamp> offsetsForTimes = kafkaConsumer.offsetsForTimes(topicHashMap);

        // 遍历分区信息并seek的方式来指定每一个分区的offset信息
        for (TopicPartition topicPartition : assignment) {
            OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(topicPartition);
            System.out.println("------------------------------");
            System.out.println(offsetAndTimestamp);
            System.out.println("------------------------------");
            if (null != offsetAndTimestamp) {
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }
        
        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                System.out.println(consumerRecord);
            }
        }
    }
}

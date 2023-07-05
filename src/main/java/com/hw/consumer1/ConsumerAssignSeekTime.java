package com.hw.consumer1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 给消费者组指定消费分区
 */
public class ConsumerAssignSeekTime {
    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 这里可以实现自定义的分区策略并设置
//        properties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, )
        // 这两个属性用于配置consumer的超时属性，分别是心跳时间和session的超时时间
//        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 1000);
//        properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 100);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        HashSet<String> strings = new HashSet<>();
        strings.add("first");
        kafkaConsumer.subscribe(strings);
        // 在订阅的前提下获取对应的topicpartition，并在后续的seek方法设定消费的机制
        Set<TopicPartition> assignment = kafkaConsumer.assignment();
        while(assignment.size() == 0) {
            kafkaConsumer.poll(Duration.ofSeconds(1));
            assignment = kafkaConsumer.assignment();
        }

        HashMap<TopicPartition, Long> partitionLongHashMap = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            partitionLongHashMap.put(topicPartition, System.currentTimeMillis() - 5*3600*1000);
        }
        // 获取时间相关的信息
        Map<TopicPartition, OffsetAndTimestamp> offsetAndTimestampMap = kafkaConsumer.offsetsForTimes(partitionLongHashMap);

        for (TopicPartition topicPartition : assignment) {
            System.out.println(topicPartition);
            OffsetAndTimestamp offsetAndTimestamp = offsetAndTimestampMap.get(topicPartition);
            // 可能存在一种情况就是对应的partition并没有数据，因此获取到的上述变量可能为空，所以下面要对空进行判断
            if (null != offsetAndTimestamp) {
                kafkaConsumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }

        System.out.println("======================");
        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    System.out.println(offsets);
                }
            });
        }
    }
}

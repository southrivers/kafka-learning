package com.hw.consumer1;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashSet;
import java.util.Properties;

public class CustomConsumer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.28.3:9092,192.168.28.5:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        HashSet<String> strings = new HashSet<>();
        // 指定了group之后数据的断点消费就是从上一次消费的offset继续消费，不过这里有一点需要记住，就是session.timeout.ms默认为45s
        // 也就是说如果不更换消费者组的话，重启需要等到45s之后才会再次消费到数据
        strings.add("first");
        kafkaConsumer.subscribe(strings);

        while(true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }
        }
    }
}
